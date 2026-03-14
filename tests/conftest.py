"""Pytest configuration and shared fixtures for the test suite."""

from collections.abc import AsyncGenerator, Generator
from typing import Any

import pytest
import httpx

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
def reset_registry() -> Generator[None, None, None]:
    """Ensure each test starts with a clean session registry."""
    registry._workers.clear()
    if hasattr(registry, "_completed"):
        registry._completed.clear()
    yield
    # Explicitly cancel running drain tasks before clearing so teardown intent is clear
    for w in list(registry._workers.values()):
        if w.task and not w.task.done():
            w.task.cancel()
    registry._workers.clear()
    if hasattr(registry, "_completed"):
        registry._completed.clear()


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as c:
        yield c


@pytest.fixture
def services() -> HookStateService:
    """Return a fresh HookStateService bound to the test workspace."""
    return HookStateService(workspace="test-workspace")
