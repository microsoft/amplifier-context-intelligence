"""Tests for context_intelligence_server.pipeline — server-side event processing pipeline.

Test coverage:
- TERMINAL_EVENTS constant
- _find_handler: exact match, default fallback, wildcard, system events, first-match-wins
- process_event: ensure_session_node called, handler dispatch, error isolation,
  missing session_id handling, terminal event flush
- setup_handlers: structure, handler count, interface compliance
"""

from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock

from context_intelligence_server.protocol import HookResult


# ---------------------------------------------------------------------------
# Stub handlers used across tests (no real handler imports needed)
# ---------------------------------------------------------------------------


class _StubEntityHandler:
    """Minimal conforming EventHandler stub with call tracking."""

    def __init__(self, events: set[str], *, name: str = "stub") -> None:
        self.handled_events: frozenset[str] = frozenset(events)
        self.name = name
        self.services: MagicMock = MagicMock()
        self._mock_call: AsyncMock = AsyncMock(return_value=HookResult())

    async def __call__(self, event: str, data: dict) -> HookResult:
        return await self._mock_call(event, data)


class _StubDefaultHandler:
    """Minimal default handler stub (handled_events intentionally empty)."""

    def __init__(self) -> None:
        self.handled_events: frozenset[str] = frozenset()
        self.services: MagicMock = MagicMock()
        self._mock_call: AsyncMock = AsyncMock(return_value=HookResult())

    async def __call__(self, event: str, data: dict) -> HookResult:
        return await self._mock_call(event, data)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def default_handler() -> _StubDefaultHandler:
    return _StubDefaultHandler()


@pytest.fixture
def session_handler() -> _StubEntityHandler:
    return _StubEntityHandler(
        {"session:start", "session:fork", "session:end"}, name="session"
    )


@pytest.fixture
def system_handler() -> _StubEntityHandler:
    return _StubEntityHandler(
        {"context:compaction", "cancel:requested", "cancel:completed"}, name="system"
    )


@pytest.fixture
def step_handler() -> _StubEntityHandler:
    # includes wildcard pattern
    return _StubEntityHandler(
        {"provider:request", "llm:request", "llm:response", "content_block:*"},
        name="step",
    )


@pytest.fixture
def handlers(
    session_handler: _StubEntityHandler,
    step_handler: _StubEntityHandler,
    system_handler: _StubEntityHandler,
    default_handler: _StubDefaultHandler,
) -> dict:
    return {
        "entity": [session_handler, step_handler, system_handler],
        "default": default_handler,
    }


@pytest.fixture
def mock_worker() -> MagicMock:
    worker = MagicMock()
    worker.services = MagicMock()
    worker.services.ensure_session_node = AsyncMock()
    worker.services.graph = MagicMock()
    worker.services.graph.flush = AsyncMock()
    return worker


# ===========================================================================
# TERMINAL_EVENTS
# ===========================================================================


def test_terminal_events_is_frozenset() -> None:
    from context_intelligence_server.pipeline import TERMINAL_EVENTS

    assert isinstance(TERMINAL_EVENTS, frozenset)


def test_terminal_events_contains_session_end() -> None:
    from context_intelligence_server.pipeline import TERMINAL_EVENTS

    assert "session:end" in TERMINAL_EVENTS


def test_terminal_events_contains_execution_end() -> None:
    from context_intelligence_server.pipeline import TERMINAL_EVENTS

    assert "execution:end" in TERMINAL_EVENTS


def test_terminal_events_contains_orchestrator_complete() -> None:
    from context_intelligence_server.pipeline import TERMINAL_EVENTS

    assert "orchestrator:complete" in TERMINAL_EVENTS


# ===========================================================================
# _find_handler
# ===========================================================================


def test_find_handler_exact_match(
    session_handler: _StubEntityHandler, default_handler: _StubDefaultHandler
) -> None:
    from context_intelligence_server.pipeline import _find_handler

    handlers = {"entity": [session_handler], "default": default_handler}
    result = _find_handler("session:start", handlers)
    assert result is session_handler


def test_find_handler_returns_default_for_unclaimed(
    session_handler: _StubEntityHandler, default_handler: _StubDefaultHandler
) -> None:
    from context_intelligence_server.pipeline import _find_handler

    handlers = {"entity": [session_handler], "default": default_handler}
    result = _find_handler("unknown:event", handlers)
    assert result is default_handler


def test_find_handler_wildcard_matching(
    step_handler: _StubEntityHandler, default_handler: _StubDefaultHandler
) -> None:
    from context_intelligence_server.pipeline import _find_handler

    handlers = {"entity": [step_handler], "default": default_handler}
    result = _find_handler("content_block:start", handlers)
    assert result is step_handler


def test_find_handler_wildcard_suffix_matching(
    step_handler: _StubEntityHandler, default_handler: _StubDefaultHandler
) -> None:
    """content_block:delta also matches content_block:*."""
    from context_intelligence_server.pipeline import _find_handler

    handlers = {"entity": [step_handler], "default": default_handler}
    result = _find_handler("content_block:delta", handlers)
    assert result is step_handler


def test_find_handler_wildcard_does_not_match_unrelated(
    step_handler: _StubEntityHandler, default_handler: _StubDefaultHandler
) -> None:
    """Wildcard content_block:* must not absorb session:start."""
    from context_intelligence_server.pipeline import _find_handler

    handlers = {"entity": [step_handler], "default": default_handler}
    result = _find_handler("session:start", handlers)
    assert result is default_handler


def test_find_handler_system_event_claimed_not_default(
    system_handler: _StubEntityHandler, default_handler: _StubDefaultHandler
) -> None:
    """System events must be claimed by SystemEventHandler, not DefaultHandler."""
    from context_intelligence_server.pipeline import _find_handler

    handlers = {"entity": [system_handler], "default": default_handler}
    result = _find_handler("context:compaction", handlers)
    assert result is system_handler


def test_find_handler_first_match_wins() -> None:
    """When two entity handlers claim the same event, the first in list wins."""
    from context_intelligence_server.pipeline import _find_handler

    h1 = _StubEntityHandler({"some:event"}, name="first")
    h2 = _StubEntityHandler({"some:event"}, name="second")
    default = _StubDefaultHandler()
    handlers = {"entity": [h1, h2], "default": default}
    result = _find_handler("some:event", handlers)
    assert result is h1


def test_find_handler_empty_entity_list_returns_default(
    default_handler: _StubDefaultHandler,
) -> None:
    from context_intelligence_server.pipeline import _find_handler

    handlers = {"entity": [], "default": default_handler}
    result = _find_handler("any:event", handlers)
    assert result is default_handler


# ===========================================================================
# process_event
# ===========================================================================


async def test_process_event_calls_ensure_session_node(
    mock_worker: MagicMock, handlers: dict
) -> None:
    from context_intelligence_server.pipeline import process_event

    data = {"session_id": "sess-123", "timestamp": "2024-01-01T00:00:00Z"}
    await process_event(mock_worker, "session:start", data, handlers)
    mock_worker.services.ensure_session_node.assert_called_once_with("sess-123", data)


async def test_process_event_dispatches_to_matching_handler(
    session_handler: _StubEntityHandler, default_handler: _StubDefaultHandler
) -> None:
    from context_intelligence_server.pipeline import process_event

    worker = MagicMock()
    worker.services.ensure_session_node = AsyncMock()
    worker.services.graph.flush = AsyncMock()
    handlers = {"entity": [session_handler], "default": default_handler}
    data = {"session_id": "sess-123"}
    await process_event(worker, "session:start", data, handlers)
    session_handler._mock_call.assert_called_once_with("session:start", data)
    default_handler._mock_call.assert_not_called()


async def test_process_event_handler_exception_does_not_propagate(
    mock_worker: MagicMock, default_handler: _StubDefaultHandler
) -> None:
    """Handler exceptions must be swallowed so the drain loop continues."""
    from context_intelligence_server.pipeline import process_event

    broken = _StubEntityHandler({"some:event"})
    broken._mock_call = AsyncMock(side_effect=RuntimeError("boom!"))
    handlers = {"entity": [broken], "default": default_handler}

    # Must NOT raise
    await process_event(mock_worker, "some:event", {"session_id": "sess-123"}, handlers)


async def test_process_event_missing_session_id_skips_ensure_but_dispatches(
    default_handler: _StubDefaultHandler,
) -> None:
    """Events without session_id skip ensure_session_node but still dispatch."""
    from context_intelligence_server.pipeline import process_event

    worker = MagicMock()
    worker.services.ensure_session_node = AsyncMock()
    worker.services.graph.flush = AsyncMock()
    handlers = {"entity": [], "default": default_handler}
    data: dict = {}  # no session_id

    await process_event(worker, "some:event", data, handlers)

    worker.services.ensure_session_node.assert_not_called()
    default_handler._mock_call.assert_called_once()


async def test_process_event_session_end_triggers_flush(
    mock_worker: MagicMock, handlers: dict
) -> None:
    """session:end is a terminal event and must trigger graph.flush."""
    from context_intelligence_server.pipeline import process_event

    data = {"session_id": "sess-123"}
    await process_event(mock_worker, "session:end", data, handlers)
    mock_worker.services.graph.flush.assert_called()


async def test_process_event_execution_end_triggers_flush(
    mock_worker: MagicMock, handlers: dict
) -> None:
    """execution:end is a terminal event and must trigger graph.flush."""
    from context_intelligence_server.pipeline import process_event

    data = {"session_id": "sess-123"}
    await process_event(mock_worker, "execution:end", data, handlers)
    mock_worker.services.graph.flush.assert_called()


async def test_process_event_orchestrator_complete_triggers_flush(
    mock_worker: MagicMock, handlers: dict
) -> None:
    """orchestrator:complete is a terminal event and must trigger graph.flush."""
    from context_intelligence_server.pipeline import process_event

    data = {"session_id": "sess-123"}
    await process_event(mock_worker, "orchestrator:complete", data, handlers)
    mock_worker.services.graph.flush.assert_called()


async def test_process_event_non_terminal_does_not_flush(
    mock_worker: MagicMock, handlers: dict
) -> None:
    """Non-terminal events must NOT call graph.flush."""
    from context_intelligence_server.pipeline import process_event

    data = {"session_id": "sess-123"}
    await process_event(mock_worker, "session:start", data, handlers)
    mock_worker.services.graph.flush.assert_not_called()


async def test_process_event_flush_exception_does_not_propagate(
    mock_worker: MagicMock, handlers: dict
) -> None:
    """Even if graph.flush raises, process_event must not propagate the error."""
    from context_intelligence_server.pipeline import process_event

    mock_worker.services.graph.flush = AsyncMock(
        side_effect=RuntimeError("flush exploded")
    )
    data = {"session_id": "sess-123"}

    # Must NOT raise even though flush raises
    await process_event(mock_worker, "session:end", data, handlers)


async def test_process_event_ensure_session_exception_does_not_propagate(
    mock_worker: MagicMock, handlers: dict
) -> None:
    """ensure_session_node exceptions must be absorbed."""
    from context_intelligence_server.pipeline import process_event

    mock_worker.services.ensure_session_node = AsyncMock(
        side_effect=RuntimeError("db down")
    )
    data = {"session_id": "sess-123"}

    # Must NOT raise
    await process_event(mock_worker, "session:start", data, handlers)


# ===========================================================================
# setup_handlers
# ===========================================================================


def test_setup_handlers_returns_dict_with_entity_and_default_keys() -> None:
    from context_intelligence_server.pipeline import setup_handlers
    from context_intelligence_server.services import HookStateService

    services = HookStateService(workspace="test")
    result = setup_handlers(services)
    assert "entity" in result
    assert "default" in result


def test_setup_handlers_entity_is_list() -> None:
    from context_intelligence_server.pipeline import setup_handlers
    from context_intelligence_server.services import HookStateService

    services = HookStateService(workspace="test")
    result = setup_handlers(services)
    assert isinstance(result["entity"], list)


def test_setup_handlers_entity_has_six_handlers() -> None:
    """6 entity handlers: Session, OrchestratorRun, Step, Recipe, ToolExecution, SystemEvent."""
    from context_intelligence_server.pipeline import setup_handlers
    from context_intelligence_server.services import HookStateService

    services = HookStateService(workspace="test")
    result = setup_handlers(services)
    assert len(result["entity"]) == 6


def test_setup_handlers_all_entity_handlers_have_handled_events() -> None:
    from context_intelligence_server.pipeline import setup_handlers
    from context_intelligence_server.services import HookStateService

    services = HookStateService(workspace="test")
    result = setup_handlers(services)
    for handler in result["entity"]:
        assert hasattr(handler, "handled_events"), (
            f"{type(handler).__name__} missing handled_events"
        )


def test_setup_handlers_default_handler_has_services() -> None:
    from context_intelligence_server.pipeline import setup_handlers
    from context_intelligence_server.services import HookStateService

    services = HookStateService(workspace="test")
    result = setup_handlers(services)
    assert result["default"].services is services


def test_setup_handlers_entity_handlers_have_services() -> None:
    from context_intelligence_server.pipeline import setup_handlers
    from context_intelligence_server.services import HookStateService

    services = HookStateService(workspace="test")
    result = setup_handlers(services)
    for handler in result["entity"]:
        assert handler.services is services, (
            f"{type(handler).__name__}.services is not the injected services"
        )


def test_setup_handlers_handler_names_include_expected_types() -> None:
    """Verify the handler classes are the expected types by name."""
    from context_intelligence_server.pipeline import setup_handlers
    from context_intelligence_server.services import HookStateService

    services = HookStateService(workspace="test")
    result = setup_handlers(services)
    type_names = {type(h).__name__ for h in result["entity"]}
    assert "SessionHandler" in type_names
    assert "OrchestratorRunHandler" in type_names
    assert "StepHandler" in type_names
    assert "RecipeHandler" in type_names
    assert "ToolExecutionHandler" in type_names
    assert "SystemEventHandler" in type_names
    assert type(result["default"]).__name__ == "DefaultHandler"
