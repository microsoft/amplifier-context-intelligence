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


from context_intelligence_server.graph_backend import BackendHealth  # noqa: E402
from context_intelligence_server.main import app, registry  # noqa: E402
from context_intelligence_server.services import GraphState, HookStateService  # noqa: E402

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
# Shared GraphBackend test doubles.
#
# Post graph-backend-segregation (doc: PR #109 rework), a Neo4j driver is
# never visible outside neo4j_backend.Neo4jGraphBackend -- SessionRegistry
# and every route ask a bound GraphBackend for a GraphStore/QueryableStore
# instead. These doubles satisfy that same seam without a driver:
#
# * FakeQueryableStore -- a QueryableStore double for /cypher wiring tests.
#   Records exactly what POST /cypher forwarded to ``execute_query`` so
#   tests can assert wiring (query/params/workspace pass-through, error
#   mapping) without a real Neo4j session.
# * FakeGraphBackend    -- a GraphBackend double. ``session_store``/
#   ``admin_store`` hand out ``GraphState`` (the existing in-memory
#   GraphStore implementation from services.py -- a second, independent
#   port implementation, not a mock) unless a test needs something else;
#   ``query_store`` hands out a FakeQueryableStore. Lifecycle calls
#   (start/aclose/ensure_schema) are counted so lifespan-wiring tests can
#   assert ordering/arguments without touching a real connection pool.
# ---------------------------------------------------------------------------


class FakeQueryableStore:
    """QueryableStore test double for /cypher wiring tests.

    ``rows``/``exc`` control the (mutually exclusive) outcome of every call.
    Every call is recorded in ``calls`` so a test can assert on the exact
    query/params/dialect/workspace the endpoint forwarded.
    """

    supported_dialects = frozenset({"cypher"})

    def __init__(
        self,
        rows: list[dict[str, Any]] | None = None,
        exc: Exception | None = None,
    ) -> None:
        self._rows = list(rows or [])
        self._exc = exc
        self.calls: list[dict[str, Any]] = []

    async def execute_query(
        self,
        query: str,
        params: dict[str, Any] | None = None,
        dialect: str = "cypher",
        workspace: str | None = None,
    ) -> list[dict[str, Any]]:
        self.calls.append(
            {
                "query": query,
                "params": dict(params or {}),
                "dialect": dialect,
                "workspace": workspace,
            }
        )
        if self._exc is not None:
            raise self._exc
        return list(self._rows)


class FakeGraphBackend:
    """In-memory ``GraphBackend`` test double -- no Neo4j driver anywhere.

    ``session_store``/``admin_store`` return a fresh ``GraphState`` (an
    existing, fully-conforming ``GraphStore`` implementation) unless
    overridden; ``query_store`` returns the configured ``FakeQueryableStore``
    (a fresh empty one by default). Lifecycle methods count their own calls
    and can be made to raise, so tests can assert lifespan wiring (start
    before use, aclose exactly once, ensure_schema's kwargs, error
    propagation) without any real connection.
    """

    def __init__(
        self,
        *,
        query_store: Any = None,
        health: BackendHealth | None = None,
        raise_on_start: Exception | None = None,
        raise_on_ensure_schema: Exception | None = None,
        schema_established: bool = True,
    ) -> None:
        self.start_calls = 0
        self.aclose_calls = 0
        self.ensure_schema_calls: list[dict[str, Any]] = []
        self.started = False
        self._query_store = (
            query_store if query_store is not None else FakeQueryableStore()
        )
        self._health = health
        self._raise_on_start = raise_on_start
        self._raise_on_ensure_schema = raise_on_ensure_schema
        self._schema_established = schema_established

    async def start(self) -> None:
        self.start_calls += 1
        if self._raise_on_start is not None:
            raise self._raise_on_start
        self.started = True

    async def aclose(self) -> None:
        self.aclose_calls += 1
        self.started = False

    def session_store(self, *, workspace: str, created_by: str | None = None) -> Any:
        store = GraphState(workspace=workspace)
        store.created_by = created_by
        return store

    def admin_store(self) -> Any:
        return GraphState()

    def query_store(self) -> Any:
        return self._query_store

    async def ensure_schema(self, *, fail_on_data_conflict: bool = False) -> bool:
        self.ensure_schema_calls.append({"fail_on_data_conflict": fail_on_data_conflict})
        if self._raise_on_ensure_schema is not None:
            raise self._raise_on_ensure_schema
        return self._schema_established

    async def health(self) -> BackendHealth:
        if self._health is not None:
            return self._health
        return BackendHealth(
            write_connected=True,
            read_connected=True,
            url="bolt://fake:7687",
            browser_url="",
        )

    async def diagnose(self) -> dict[str, int]:
        return {}

    async def repair(self) -> dict[str, int]:
        return {}


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


@pytest.fixture(autouse=True)
def _default_graph_backend() -> Generator[None, None, None]:
    """Bind a fresh FakeGraphBackend for every test, on both seams that need one.

    ``SessionRegistry.get_or_create()`` raises ``RuntimeError`` unless a
    ``GraphBackend`` has been bound via ``set_graph_backend`` (see
    registry.py), and ``GET /status`` / ``POST /cypher`` read
    ``app.state.graph_backend`` directly -- neither is ever set by running
    the real ``lifespan()``, since most tests exercise routes straight
    through ``ASGITransport`` without it. Before graph-backend-segregation,
    the registry built its own driver lazily on first use and no such
    binding was needed; this fixture is the direct replacement, mirroring
    what ``lifespan()`` does at boot. Individual tests that care about
    specific backend behavior (custom health(), cypher rows/errors, etc.)
    override ``app.state.graph_backend`` and/or call
    ``registry.set_graph_backend(...)`` themselves.
    """
    backend = FakeGraphBackend()
    app.state.graph_backend = backend
    registry.set_graph_backend(backend)
    yield
    registry.set_graph_backend(None)
    if hasattr(app.state, "graph_backend"):
        del app.state.graph_backend


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
