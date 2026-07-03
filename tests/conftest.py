"""Pytest configuration and shared fixtures for the test suite."""

import os

# Allow the server to boot with no auth in the test harness.
# create_asgi_app() refuses to start when no credentials are configured UNLESS
# allow_unauthenticated=True — this env var is the test-suite's explicit opt-out.
# Never set this in production.
os.environ.setdefault(
    "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_ALLOW_UNAUTHENTICATED", "true"
)

from collections.abc import AsyncGenerator, Generator  # noqa: E402
from typing import Any  # noqa: E402

import httpx  # noqa: E402
import pytest  # noqa: E402


from context_intelligence_server.main import app, asgi_app, registry  # noqa: E402
from context_intelligence_server.services import HookStateService  # noqa: E402


# ---------------------------------------------------------------------------
# Shared Neo4j mock helpers (used by POST /cypher tests)
# ---------------------------------------------------------------------------


class MockNeo4jResult:
    """Async-iterable result mock that yields a fixed list of rows."""

    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self._rows = list(rows or [])
        self._index = 0

    def __aiter__(self) -> "MockNeo4jResult":
        return self

    async def __anext__(self) -> dict[str, Any]:
        if self._index >= len(self._rows):
            raise StopAsyncIteration
        row = self._rows[self._index]
        self._index += 1
        return row


class MockNeo4jSession:
    """Async context-manager session mock; captures params and/or raises exceptions."""

    def __init__(
        self,
        rows: list[dict[str, Any]] | None = None,
        exc: Exception | None = None,
        captured: dict[str, Any] | None = None,
    ) -> None:
        self._rows = rows
        self._exc = exc
        self._captured = captured

    async def run(self, query: str, params: dict[str, Any]) -> MockNeo4jResult:
        if self._captured is not None:
            self._captured.update(params)
        if self._exc is not None:
            raise self._exc
        return MockNeo4jResult(self._rows)

    async def __aenter__(self) -> "MockNeo4jSession":
        return self

    async def __aexit__(self, *args: object) -> None:
        pass


class MockNeo4jDriver:
    """Driver mock; delegates to a single MockNeo4jSession with the given config.

    Accepts (and ignores) ``default_access_mode`` so it stays compatible with
    the two-client split's ``driver.session(default_access_mode=...)`` call in
    ``post_cypher`` (main.py).
    """

    def __init__(
        self,
        rows: list[dict[str, Any]] | None = None,
        exc: Exception | None = None,
        captured: dict[str, Any] | None = None,
    ) -> None:
        self._rows = rows
        self._exc = exc
        self._captured = captured

    def session(self, default_access_mode: str | None = None) -> MockNeo4jSession:
        return MockNeo4jSession(self._rows, self._exc, self._captured)


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture(autouse=True)
def safe_settings(tmp_path: Any) -> Generator[None, None, None]:
    from unittest.mock import patch
    from context_intelligence_server.config import Neo4jClientConfig
    from context_intelligence_server.config import Settings as _Settings

    _real = _Settings()

    class _SettingsProxy:
        blob_path: str = _real.blob_path
        queues_path: str = str(tmp_path / "queues")
        # Redirect identity-store paths so the registry proxy never touches the
        # real /data/identity/ defaults on machines where those files exist.
        api_keys_store_path: str = str(tmp_path / "api-keys.json")
        entra_identities_store_path: str = str(tmp_path / "entra-identities.json")
        neo4j_url: str = _real.neo4j_url
        neo4j_user: str = _real.neo4j_user
        neo4j_password: str = _real.neo4j_password
        stale_session_timeout: float = _real.stale_session_timeout
        write_concurrency: int = _real.write_concurrency
        max_delivery_attempts: int = _real.max_delivery_attempts
        neo4j_flush_chunk_rows: int = _real.neo4j_flush_chunk_rows
        neo4j_flush_chunk_bytes: int = _real.neo4j_flush_chunk_bytes
        neo4j_lock_timeout: float = _real.neo4j_lock_timeout

        # Neo4j two-client split (doc 12): SessionRegistry.get_or_create() calls
        # settings.resolve_neo4j_admin() directly, so this proxy (which stands
        # in for get_settings() inside registry.py) must implement it too --
        # mirrors Settings' legacy-fallback resolver behavior exactly.
        def resolve_neo4j_admin(self) -> Neo4jClientConfig:
            return Neo4jClientConfig(
                url=self.neo4j_url,
                username=self.neo4j_user,
                password=self.neo4j_password,
                access_mode="WRITE",
            )

        def resolve_neo4j_query(self) -> Neo4jClientConfig:
            return Neo4jClientConfig(
                url=self.neo4j_url,
                username=self.neo4j_user,
                password=self.neo4j_password,
                access_mode="READ",
            )

    with patch(
        "context_intelligence_server.registry.get_settings",
        return_value=_SettingsProxy(),
    ):
        yield


@pytest.fixture(autouse=True)
def reset_registry() -> Generator[None, None, None]:
    """Ensure each test starts with a clean session registry."""
    registry._workers.clear()
    if hasattr(registry, "_completed"):
        registry._completed.clear()
    # Reset durable infra so each test rebuilds it against its own tmp_path
    # queues dir (the module-level registry is constructed once at import).
    registry._queue_manager = None
    registry._write_semaphore = None
    # Zero the live pipeline-conservation counters on the shared singleton so
    # each test starts from a clean conservation baseline (D2).
    registry._accepted_total = 0
    registry._written_total = 0
    registry._replayed_total = 0
    registry._write_retries_total = 0
    yield
    # Explicitly cancel running drain tasks before clearing so teardown intent is clear
    for w in list(registry._workers.values()):
        if w.task and not w.task.done():
            w.task.cancel()
    registry._workers.clear()
    if hasattr(registry, "_completed"):
        registry._completed.clear()
    registry._queue_manager = None
    registry._write_semaphore = None


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    # Commit 3 (doc 16 §9): route the default fixture through `asgi_app` — the
    # auth-wrapped ASGI app — NOT the bare `app`. Under the suite's
    # ALLOW_UNAUTHENTICATED=true opt-out the middleware short-circuits (no scope
    # state populated) and W1's _is_write_capable honours the flag, so these tests
    # stay green — but they now traverse the REAL middleware stack instead of
    # passing gated routes via "no middleware ran".
    #
    # TB-N1 guard: create_asgi_app(settings=...) MUTATES the shared module-level
    # app.state (there is a single FastAPI `app`; every call reconfigures it). A
    # sibling test building an auth-enabled app (allow_unauthenticated=False) leaves
    # app.state.allow_unauthenticated=False behind; W1's _is_write_capable reads that
    # flag LIVE, so it would fail-close and 403 this fixture's gated-route requests.
    # Reset it to the suite's dev opt-out (conftest sets ALLOW_UNAUTHENTICATED=true at
    # import) so gated-route `client` tests are order-independent w.r.t. THIS flag.
    #
    # SCOPE (do not over-read): this resets ONLY allow_unauthenticated — the one field
    # proven to poison client tests (adversarial order: 28 failures without this line).
    # create_asgi_app also leaks auth_mode / reader_role / service_data_role / the
    # stores onto the same singleton; those are NOT reset here because no current
    # `client` test observes them (the client /status callers assert status_code +
    # neo4j fields only, never response["auth"]). The real fix — create_asgi_app
    # mutates a shared singleton with no teardown — is tracked (SCRATCH: Commit-3
    # follow-ups), not patched field-by-field here.
    app.state.allow_unauthenticated = True
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=asgi_app),
        base_url="http://test",
    ) as c:
        yield c


@pytest.fixture
async def auth_client(
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Client routed through asgi_app (auth middleware applied) with a test API key set."""
    import hashlib  # noqa: PLC0415

    from context_intelligence_server.auth import StaticKeyResolver  # noqa: PLC0415

    # Build a StaticKeyResolver that maps sha256("test-secret") → "owner" so existing
    # integration tests that send `Authorization: Bearer test-secret` continue to work.
    # We patch asgi_app.resolver (the PrincipalResolver seam introduced by T2) rather
    # than the old asgi_app.keystore attribute which no longer exists.
    test_keystore = {hashlib.sha256(b"test-secret").hexdigest(): "owner"}
    monkeypatch.setattr(asgi_app, "resolver", StaticKeyResolver(test_keystore))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=asgi_app),
        base_url="http://test",
    ) as c:
        yield c


@pytest.fixture
def services() -> HookStateService:
    """Return a fresh HookStateService bound to the test workspace."""
    return HookStateService(workspace="test-workspace")
