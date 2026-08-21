"""Pytest configuration and shared fixtures for the test suite."""

import os

# Allow the server to boot with no auth in the test harness.
# create_asgi_app() refuses to start when no credentials are configured UNLESS
# allow_unauthenticated=True — this env var is the test-suite's explicit opt-out.
# Never set this in production.
os.environ.setdefault(
    "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_ALLOW_UNAUTHENTICATED", "true"
)

from collections.abc import AsyncGenerator, Generator
from typing import Any, Self

import httpx
import pytest

from context_intelligence_server.main import app, registry
from context_intelligence_server.services import HookStateService

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

    async def __aenter__(self) -> Self:
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
        # drain_worker's Trigger H/I read these directly off get_settings()
        # (config.py's lru_cache'd accessor) -- this proxy stands in for it
        # inside registry.py, so it must carry the same fields real Settings
        # does, at the same shipped defaults.
        queue_compact_enabled: bool = _real.queue_compact_enabled
        queue_compact_min_prefix_bytes: int = _real.queue_compact_min_prefix_bytes
        queue_compact_max_tail_bytes: int = _real.queue_compact_max_tail_bytes

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
def reset_boot_state() -> Generator[None, None, None]:
    """Reset the module-level ``BootState`` singleton around each test.

    ``/status`` is phase-gated: ``metrics``/``spool`` are populated only
    once ``boot_state.phase`` reaches ``"ready"`` (or ``"failed"``) --
    otherwise it serves the lean, zero-disk boot response. The overwhelming
    majority of existing tests exercise ordinary ``/status`` behaviour, NOT
    boot itself, and never invoke the real ``lifespan()`` (the plain
    ``client``/``auth_client`` fixtures wrap the ASGI app directly, without
    running its lifespan protocol) -- so without a reset, ``boot_state``
    would default to its module-import value (``"recovering"``) for the
    entire test session, permanently nulling ``metrics``/``spool`` for every
    test that doesn't explicitly drive a real boot.

    Default here to ``"ready"`` -- exactly what a real, running server is
    for the overwhelming majority of its lifetime -- so pre-existing tests
    keep seeing the populated shape they always have. Tests that need to
    exercise a SPECIFIC boot phase (the boot-safety suite) set
    ``boot_state.phase`` (and any other field) explicitly; mutated IN PLACE
    on the same singleton object ``main.py`` imported, so both modules'
    references see the change.
    """
    from context_intelligence_server.status import boot_state as _boot_state

    def _reset() -> None:
        _boot_state.phase = "ready"
        _boot_state.started_at = 0.0
        _boot_state.completed_at = None
        _boot_state.reclaimed = 0
        _boot_state.reclaimed_bytes = 0
        _boot_state.kept = 0
        _boot_state.failed = 0
        _boot_state.resumed = 0
        _boot_state.deferred = 0
        _boot_state.error = None
        _boot_state.failed_step = None
        _boot_state.fallback_workspace_byte0 = 0
        _boot_state.fallback_workspace_sentinel = 0
        _boot_state.reclaim_enabled = False

    _reset()
    yield
    _reset()


@pytest.fixture(autouse=True)
def _restore_lease_io() -> Generator[None, None, None]:
    """The private `_LEASE_IO` executor in ``writer_lease`` is process-wide
    (module-level, by design). A test that drives the real `lifespan()` to a
    normal, non-raising completion calls `shutdown_lease_io()` in its
    `finally`, which permanently shuts that shared executor down for every
    later test in the same pytest process. Detect that (submitting after
    shutdown raises `RuntimeError`, a documented `ThreadPoolExecutor`
    contract) and transparently recreate it -- this is test-isolation
    plumbing only; production shuts the executor down exactly once, at real
    process exit.

    Defined here (rather than only in the module that first needed it) so it
    guards every test module regardless of collection/run order.
    """
    yield
    import concurrent.futures

    from context_intelligence_server import writer_lease as wl_module

    try:
        wl_module._LEASE_IO.submit(lambda: None).result(timeout=1.0)
    except RuntimeError:
        wl_module._LEASE_IO = concurrent.futures.ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="writer-lease-io"
        )


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
    # each test starts from a clean conservation baseline.
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
    import hashlib

    from context_intelligence_server.auth import StaticKeyResolver
    from context_intelligence_server.main import asgi_app

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
