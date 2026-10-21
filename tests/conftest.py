"""Pytest configuration and shared fixtures for the test suite."""

import os
import tempfile
from pathlib import Path

# Hermetic HOME, set BEFORE any server import.  Default identity-store paths
# derive from Path.home() when the config module is imported, and an EXISTING
# store file selects (and exclusively locks) that store.  Without this a
# developer's real ~/.local/share/ci-server/identity files would be read and
# locked by the test run.
os.environ["HOME"] = tempfile.mkdtemp(prefix="ci-server-test-home-")

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


from context_intelligence_server.main import app, registry  # noqa: E402
from context_intelligence_server.services import HookStateService  # noqa: E402

# Guarded import so the suite still COLLECTS against unfixed source (where the
# process-wide schema latch does not exist yet). Without this, reverting the
# fix would break collection for every test in the repo, and the new guards
# below would "fail" at import time rather than for the reason they exist.
try:  # pragma: no cover - import resolution differs pre/post fix
    from context_intelligence_server.neo4j_store import (  # noqa: E402
        reset_schema_state,
    )
except ImportError:  # pragma: no cover - exercised only against unfixed code

    def reset_schema_state() -> None:  # type: ignore[misc]
        """No-op stand-in: unfixed code has no process-wide latch to reset."""


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "raw_identity_factory: do not auto-start identity ownership after "
        "create_asgi_app() (the test drives the real lifespan itself)",
    )


@pytest.fixture(autouse=True)
def _own_identity_after_factory(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> Generator[None, None, None]:
    """Give legacy wire tests the identity state the lifespan used to leave.

    ``create_asgi_app()`` now only CONSTRUCTS identity objects; the store is
    loaded, seeded and exclusively locked by the ASGI lifespan, and the auth
    middleware answers 503 until then.  httpx's ``ASGITransport`` never runs
    the lifespan, so after each factory call this harness drives the REAL
    ownership startup (``_start_identity_ownership``: sidecar lock, load, seed,
    readiness) and the matching shutdown at teardown.  Tests that exercise the
    lifespan or ownership themselves opt out with
    ``@pytest.mark.raw_identity_factory``.
    """
    import shutil

    import context_intelligence_server.main as main_module

    # Default identity paths live under the (hermetic) HOME and an existing
    # store file now SELECTS that store, so a store written by one test must
    # not leak into the next test's default configuration.  The lazily built
    # module-level ``asgi_app`` is also per-test: its identity gate is tied to
    # the factory call that created it.
    default_state = Path.home() / ".local" / "share" / "ci-server"
    shutil.rmtree(default_state, ignore_errors=True)
    main_module._asgi_app = None
    # The module-level FastAPI app is a process singleton: drop identity wiring
    # a previous test's factory call left on it, so lifespan tests that use a
    # settings stand-in do not inherit someone else's identity store.
    for _attr in ("api_key_store", "entra_identity_store", "identity_config_token"):
        setattr(main_module.app.state, _attr, None)
    main_module.app.state.identity_ready_token = None
    main_module.app.state.identity_lifespan_active = False
    if request.node.get_closest_marker("raw_identity_factory"):
        yield
        shutil.rmtree(default_state, ignore_errors=True)
        main_module._asgi_app = None
        return

    real_factory = main_module.create_asgi_app
    owned: list[Any] = []

    def _stop_all() -> None:
        while owned:
            main_module._stop_identity_ownership(main_module.app, owned.pop())

    def factory_with_ownership(*args: Any, **kwargs: Any) -> Any:
        _stop_all()
        wrapper = real_factory(*args, **kwargs)
        owned.append(
            main_module._start_identity_ownership(
                main_module.app, main_module.app.state.settings
            )
        )
        return wrapper

    monkeypatch.setattr(main_module, "create_asgi_app", factory_with_ownership)
    yield
    _stop_all()
    shutil.rmtree(default_state, ignore_errors=True)
    main_module._asgi_app = None


@pytest.fixture(autouse=True)
def _reset_neo4j_schema_state() -> Generator[None, None, None]:
    """Clear the process-wide Neo4j schema latch around EVERY test.

    ``neo4j_store._SCHEMA_READY`` is deliberately process-global: it is what
    stops every per-session store from re-running the same ~11-statement
    catalog pass on its first flush. Process-global state is also test-order
    poison -- without this reset, the first test that fully establishes the
    schema would silently short-circuit the schema path in every test that ran
    after it, and those tests would pass or fail depending on collection order.
    Resetting on both sides of the yield keeps each test's view independent.
    """
    reset_schema_state()
    yield
    reset_schema_state()


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
        blob_path: str = str(tmp_path / "blobs")
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
        neo4j_max_connection_pool_size: int = _real.neo4j_max_connection_pool_size

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

        def stateful_root(self) -> Path:
            """Match Settings' lock-root derivation without touching /data in tests."""
            return Path(self.queues_path).parent

    with patch(
        "context_intelligence_server.registry.get_settings",
        return_value=_SettingsProxy(),
    ):
        # Lifespan acquires its process lock before it initializes drivers or
        # workers. Point the module-level ASGI app at this test's isolated root
        # rather than the production default (/data), which is intentionally
        # not writable from the test runner.
        with patch.object(app.state, "settings", _SettingsProxy(), create=True):
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
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
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
    from context_intelligence_server.main import asgi_app  # noqa: PLC0415

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
