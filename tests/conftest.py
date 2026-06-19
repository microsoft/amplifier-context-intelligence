"""Pytest configuration and shared fixtures for the test suite."""

from collections.abc import AsyncGenerator, Generator
from typing import Any

import httpx
import pytest


from context_intelligence_server.main import app, registry  # noqa: E402
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
    """Driver mock; delegates to a single MockNeo4jSession with the given config."""

    def __init__(
        self,
        rows: list[dict[str, Any]] | None = None,
        exc: Exception | None = None,
        captured: dict[str, Any] | None = None,
    ) -> None:
        self._rows = rows
        self._exc = exc
        self._captured = captured

    def session(self) -> MockNeo4jSession:
        return MockNeo4jSession(self._rows, self._exc, self._captured)


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture(autouse=True)
def safe_settings(tmp_path: Any) -> Generator[None, None, None]:
    from unittest.mock import patch
    from context_intelligence_server.config import Settings as _Settings

    _real = _Settings()

    class _SettingsProxy:
        blob_path: str = _real.blob_path
        queues_path: str = str(tmp_path / "queues")
        neo4j_url: str = _real.neo4j_url
        neo4j_user: str = _real.neo4j_user
        neo4j_password: str = _real.neo4j_password
        stale_session_timeout: float = _real.stale_session_timeout
        write_concurrency: int = _real.write_concurrency
        max_delivery_attempts: int = _real.max_delivery_attempts
        neo4j_flush_chunk_rows: int = _real.neo4j_flush_chunk_rows
        neo4j_flush_chunk_bytes: int = _real.neo4j_flush_chunk_bytes
        neo4j_lock_timeout: float = _real.neo4j_lock_timeout

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
    from context_intelligence_server.main import asgi_app  # noqa: PLC0415

    monkeypatch.setattr(asgi_app, "api_key", "test-secret")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=asgi_app),
        base_url="http://test",
    ) as c:
        yield c


@pytest.fixture
def services() -> HookStateService:
    """Return a fresh HookStateService bound to the test workspace."""
    return HookStateService(workspace="test-workspace")
