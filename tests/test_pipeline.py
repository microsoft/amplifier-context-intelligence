"""Tests for context_intelligence_server.pipeline — always-default + ordered enrichers model.

Test coverage:
- TERMINAL_EVENTS constant (is_frozenset, contains_only_session_end,
  does_not_contain_execution_end, does_not_contain_orchestrator_complete)
- PipelineHandlers/setup_handlers (returns_pipeline_handlers, has_default_handler,
  has_enrichers_list, enricher_count=2, enricher_order, all_enrichers_have_handled_events,
  enrichers_have_services)
- process_event (always_calls_default_handler, enricher_called_additionally,
  enricher_not_called_for_unclaimed_event, multiple_enrichers_both_called,
  calls_ensure_session_node, missing_session_id_skips_ensure_but_dispatches,
  session_end_triggers_flush, non_terminal_does_not_flush,
  handler_exception_does_not_propagate, flush_exception_does_not_propagate,
  ensure_session_exception_does_not_propagate)
- blob processing (blob_processing_called_when_all_conditions_met,
  blob_processing_skipped_without_timestamp, blob_processing_skipped_without_blob_store,
  blob_skip_missing_timestamp_logs_warning)
- touch_session call site (calls_touch_session, skips_touch_session_without_timestamp,
  skips_touch_session_without_session_id)
"""

from __future__ import annotations

import logging
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# NOTE: ToolCallHandler stub injection is performed in conftest.py so it
# fires before any test module loads, regardless of pytest collection order.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Stub classes used across tests
# ---------------------------------------------------------------------------


class _StubDefaultHandler:
    """Minimal default handler stub (no handled_events — always called)."""

    def __init__(self, services: Any = None) -> None:
        self.services = services or MagicMock()
        self._mock_call: AsyncMock = AsyncMock()

    async def __call__(self, event: str, data: dict[str, Any]) -> None:
        return await self._mock_call(event, data)


class _StubEnricher:
    """Enricher stub with configurable handled_events and call tracking."""

    def __init__(self, events: set[str], *, name: str = "stub") -> None:
        self.handled_events: frozenset[str] = frozenset(events)
        self.name = name
        self.services: MagicMock = MagicMock()
        self._mock_call: AsyncMock = AsyncMock()

    async def __call__(self, event: str, data: dict[str, Any]) -> None:
        return await self._mock_call(event, data)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def default_handler() -> _StubDefaultHandler:
    return _StubDefaultHandler()


@pytest.fixture
def mock_worker() -> MagicMock:
    worker = MagicMock()
    worker.services = MagicMock()
    worker.services.ensure_session_node = AsyncMock()
    worker.services.touch_session = AsyncMock()
    worker.services.graph = MagicMock()
    worker.services.graph.flush = AsyncMock()
    worker.services.graph.schedule_flush = MagicMock()
    worker.services.blob_store = None
    return worker


@pytest.fixture
def session_enricher() -> _StubEnricher:
    return _StubEnricher({"session:start", "session:end"}, name="session")


@pytest.fixture
def tool_enricher() -> _StubEnricher:
    return _StubEnricher({"tool_call:start", "tool_call:end"}, name="tool_call")


@pytest.fixture
def pipeline_handlers(
    default_handler: _StubDefaultHandler,
    session_enricher: _StubEnricher,
    tool_enricher: _StubEnricher,
) -> Any:
    from context_intelligence_server.pipeline import PipelineHandlers

    return PipelineHandlers(
        default=default_handler,  # type: ignore[arg-type]
        enrichers=[session_enricher, tool_enricher],
    )


# ===========================================================================
# TERMINAL_EVENTS
# ===========================================================================


def test_terminal_events_is_frozenset() -> None:
    from context_intelligence_server.pipeline import TERMINAL_EVENTS

    assert isinstance(TERMINAL_EVENTS, frozenset)


def test_terminal_events_contains_only_session_end() -> None:
    from context_intelligence_server.pipeline import TERMINAL_EVENTS

    assert TERMINAL_EVENTS == frozenset({"session:end"})


def test_terminal_events_does_not_contain_execution_end() -> None:
    from context_intelligence_server.pipeline import TERMINAL_EVENTS

    assert "execution:end" not in TERMINAL_EVENTS


def test_terminal_events_does_not_contain_orchestrator_complete() -> None:
    from context_intelligence_server.pipeline import TERMINAL_EVENTS

    assert "orchestrator:complete" not in TERMINAL_EVENTS


# ===========================================================================
# PipelineHandlers / setup_handlers
# ===========================================================================


def test_setup_handlers_returns_pipeline_handlers() -> None:
    from context_intelligence_server.pipeline import PipelineHandlers, setup_handlers
    from context_intelligence_server.services import HookStateService

    services = HookStateService(workspace="test")
    result = setup_handlers(services)
    assert isinstance(result, PipelineHandlers)


def test_setup_handlers_has_default_handler_with_services() -> None:
    from context_intelligence_server.pipeline import setup_handlers
    from context_intelligence_server.handlers.data_layer_1.default import DefaultHandler
    from context_intelligence_server.services import HookStateService

    services = HookStateService(workspace="test")
    result = setup_handlers(services)
    assert isinstance(result.default, DefaultHandler)
    assert result.default.services is services


def test_setup_handlers_has_enrichers_list() -> None:
    from context_intelligence_server.pipeline import setup_handlers
    from context_intelligence_server.services import HookStateService

    services = HookStateService(workspace="test")
    result = setup_handlers(services)
    assert isinstance(result.enrichers, list)


def test_setup_handlers_enricher_count() -> None:
    """setup_handlers must return exactly 12 enrichers (8 data_layer_2 + 4 data_layer_3)."""
    from context_intelligence_server.pipeline import setup_handlers
    from context_intelligence_server.services import HookStateService

    services = HookStateService(workspace="test")
    result = setup_handlers(services)
    assert len(result.enrichers) == 12


def test_setup_handlers_enricher_order() -> None:
    """Enrichers must be [SessionHandler, OrchestratorRunHandler, IterationHandler,
    ContentBlockHandler, ToolCallHandler] in that dispatch order."""
    from context_intelligence_server.pipeline import setup_handlers
    from context_intelligence_server.handlers.data_layer_2.session import SessionHandler
    from context_intelligence_server.handlers.data_layer_2.orchestrator_run import (
        OrchestratorRunHandler,
    )
    from context_intelligence_server.handlers.data_layer_2.iteration import (
        IterationHandler,
    )
    from context_intelligence_server.handlers.data_layer_2.content_block import (
        ContentBlockHandler,
    )
    from context_intelligence_server.services import HookStateService

    services = HookStateService(workspace="test")
    result = setup_handlers(services)
    assert isinstance(result.enrichers[0], SessionHandler)
    assert isinstance(result.enrichers[1], OrchestratorRunHandler)
    assert isinstance(result.enrichers[2], IterationHandler)
    assert isinstance(result.enrichers[3], ContentBlockHandler)
    assert type(result.enrichers[4]).__name__ == "ToolCallHandler"


def test_setup_handlers_l3_enricher_order() -> None:
    """Layer 3 enrichers must be appended after all Layer 2 enrichers in correct order:
    [DelegationHandler, SkillLoadHandler, RecipeRunHandler, RecipeStepHandler]."""
    from context_intelligence_server.pipeline import setup_handlers
    from context_intelligence_server.handlers.data_layer_3.delegation import (
        DelegationHandler,
    )
    from context_intelligence_server.handlers.data_layer_3.skill_load import (
        SkillLoadHandler,
    )
    from context_intelligence_server.handlers.data_layer_3.recipe_run import (
        RecipeRunHandler,
    )
    from context_intelligence_server.handlers.data_layer_3.recipe_step import (
        RecipeStepHandler,
    )
    from context_intelligence_server.services import HookStateService

    services = HookStateService(workspace="test")
    result = setup_handlers(services)
    assert isinstance(result.enrichers[8], DelegationHandler)
    assert isinstance(result.enrichers[9], SkillLoadHandler)
    assert isinstance(result.enrichers[10], RecipeRunHandler)
    assert isinstance(result.enrichers[11], RecipeStepHandler)


def test_setup_handlers_all_enrichers_have_handled_events() -> None:
    from context_intelligence_server.pipeline import setup_handlers
    from context_intelligence_server.services import HookStateService

    services = HookStateService(workspace="test")
    result = setup_handlers(services)
    for enricher in result.enrichers:
        assert hasattr(enricher, "handled_events"), (
            f"{type(enricher).__name__} missing handled_events"
        )


def test_setup_handlers_enrichers_have_services() -> None:
    from context_intelligence_server.pipeline import setup_handlers
    from context_intelligence_server.services import HookStateService

    services = HookStateService(workspace="test")
    result = setup_handlers(services)
    for enricher in result.enrichers:
        assert enricher.services is services, (
            f"{type(enricher).__name__}.services is not the injected services"
        )


# ===========================================================================
# process_event — default handler always called
# ===========================================================================


async def test_process_event_always_calls_default_handler(
    mock_worker: MagicMock,
    pipeline_handlers: Any,
) -> None:
    """Default handler is called for every event, even unclaimed ones."""
    from context_intelligence_server.pipeline import process_event

    data = {"session_id": "sess-123"}
    await process_event(mock_worker, "unknown:event", data, pipeline_handlers)
    pipeline_handlers.default._mock_call.assert_called_once()


async def test_process_event_always_calls_default_handler_for_enriched_event(
    mock_worker: MagicMock,
    pipeline_handlers: Any,
) -> None:
    """Default handler is still called even when an enricher also handles the event."""
    from context_intelligence_server.pipeline import process_event

    data = {"session_id": "sess-123"}
    await process_event(mock_worker, "session:start", data, pipeline_handlers)
    pipeline_handlers.default._mock_call.assert_called_once()


# ===========================================================================
# process_event — enricher dispatch
# ===========================================================================


async def test_process_event_enricher_called_additionally(
    mock_worker: MagicMock,
    pipeline_handlers: Any,
    session_enricher: _StubEnricher,
) -> None:
    """Enricher is called in addition to default handler for matching events."""
    from context_intelligence_server.pipeline import process_event

    data = {"session_id": "sess-123"}
    await process_event(mock_worker, "session:start", data, pipeline_handlers)
    session_enricher._mock_call.assert_called_once()


async def test_process_event_enricher_not_called_for_unclaimed_event(
    mock_worker: MagicMock,
    pipeline_handlers: Any,
    session_enricher: _StubEnricher,
) -> None:
    """Enricher is NOT called for events not in its handled_events."""
    from context_intelligence_server.pipeline import process_event

    data = {"session_id": "sess-123"}
    await process_event(mock_worker, "unknown:event", data, pipeline_handlers)
    session_enricher._mock_call.assert_not_called()


async def test_process_event_multiple_enrichers_both_called(
    mock_worker: MagicMock,
    default_handler: _StubDefaultHandler,
) -> None:
    """When multiple enrichers all handle the same event, all are called."""
    from context_intelligence_server.pipeline import PipelineHandlers, process_event

    enricher_a = _StubEnricher({"shared:event"}, name="enricher_a")
    enricher_b = _StubEnricher({"shared:event"}, name="enricher_b")
    handlers = PipelineHandlers(
        default=default_handler,  # type: ignore[arg-type]
        enrichers=[enricher_a, enricher_b],
    )

    data = {"session_id": "sess-123"}
    await process_event(mock_worker, "shared:event", data, handlers)

    enricher_a._mock_call.assert_called_once()
    enricher_b._mock_call.assert_called_once()


# ===========================================================================
# process_event — session node management
# ===========================================================================


async def test_process_event_calls_ensure_session_node(
    mock_worker: MagicMock,
    pipeline_handlers: Any,
) -> None:
    from context_intelligence_server.pipeline import process_event

    data = {"session_id": "sess-123"}
    await process_event(mock_worker, "session:start", data, pipeline_handlers)
    mock_worker.services.ensure_session_node.assert_called_once_with("sess-123", data)


async def test_process_event_missing_session_id_skips_ensure_but_dispatches(
    mock_worker: MagicMock,
    pipeline_handlers: Any,
) -> None:
    """Events without session_id skip ensure_session_node but default handler still runs."""
    from context_intelligence_server.pipeline import process_event

    data: dict[str, Any] = {}  # no session_id
    await process_event(mock_worker, "unknown:event", data, pipeline_handlers)

    mock_worker.services.ensure_session_node.assert_not_called()
    pipeline_handlers.default._mock_call.assert_called_once()


# ===========================================================================
# process_event — terminal flush
# ===========================================================================


async def test_process_event_session_end_triggers_flush(
    mock_worker: MagicMock,
    pipeline_handlers: Any,
) -> None:
    """session:end is a terminal event and must trigger graph.flush."""
    from context_intelligence_server.pipeline import process_event

    data = {"session_id": "sess-123"}
    await process_event(mock_worker, "session:end", data, pipeline_handlers)
    mock_worker.services.graph.flush.assert_called()


async def test_process_event_non_terminal_does_not_flush(
    mock_worker: MagicMock,
    pipeline_handlers: Any,
) -> None:
    """Non-terminal events must NOT call graph.flush."""
    from context_intelligence_server.pipeline import process_event

    data = {"session_id": "sess-123"}
    await process_event(mock_worker, "session:start", data, pipeline_handlers)
    mock_worker.services.graph.flush.assert_not_called()


async def test_process_event_non_terminal_calls_schedule_flush(
    mock_worker: MagicMock,
    pipeline_handlers: Any,
) -> None:
    """Non-terminal events must call graph.schedule_flush() exactly once."""
    from context_intelligence_server.pipeline import process_event

    data = {"session_id": "sess-123"}
    await process_event(mock_worker, "session:start", data, pipeline_handlers)
    mock_worker.services.graph.schedule_flush.assert_called_once()


async def test_process_event_terminal_calls_flush_not_schedule_flush(
    mock_worker: MagicMock,
    pipeline_handlers: Any,
) -> None:
    """session:end must call graph.flush() and must NOT call graph.schedule_flush()."""
    from context_intelligence_server.pipeline import process_event

    data = {"session_id": "sess-123"}
    await process_event(mock_worker, "session:end", data, pipeline_handlers)
    mock_worker.services.graph.flush.assert_called()
    mock_worker.services.graph.schedule_flush.assert_not_called()


# ===========================================================================
# process_event — error isolation
# ===========================================================================


async def test_process_event_handler_exception_does_not_propagate(
    mock_worker: MagicMock,
    default_handler: _StubDefaultHandler,
) -> None:
    """Handler exceptions must be swallowed so the drain loop continues."""
    from context_intelligence_server.pipeline import PipelineHandlers, process_event

    default_handler._mock_call = AsyncMock(side_effect=RuntimeError("boom!"))
    handlers = PipelineHandlers(default=default_handler, enrichers=[])  # type: ignore[arg-type]

    # Must NOT raise
    await process_event(mock_worker, "some:event", {"session_id": "sess-123"}, handlers)


async def test_process_event_flush_exception_does_not_propagate(
    mock_worker: MagicMock,
    pipeline_handlers: Any,
) -> None:
    """Even if graph.flush raises, process_event must not propagate the error."""
    from context_intelligence_server.pipeline import process_event

    mock_worker.services.graph.flush = AsyncMock(
        side_effect=RuntimeError("flush exploded")
    )
    data = {"session_id": "sess-123"}

    # Must NOT raise
    await process_event(mock_worker, "session:end", data, pipeline_handlers)


async def test_process_event_ensure_session_exception_does_not_propagate(
    mock_worker: MagicMock,
    pipeline_handlers: Any,
) -> None:
    """ensure_session_node exceptions must be absorbed."""
    from context_intelligence_server.pipeline import process_event

    mock_worker.services.ensure_session_node = AsyncMock(
        side_effect=RuntimeError("db down")
    )
    data = {"session_id": "sess-123"}

    # Must NOT raise
    await process_event(mock_worker, "session:start", data, pipeline_handlers)


# ===========================================================================
# Blob processing in process_event
# ===========================================================================


async def test_blob_processing_called_when_all_conditions_met(
    pipeline_handlers: Any,
) -> None:
    """process_event_data is called when session_id, timestamp, and blob_store are all truthy."""
    from context_intelligence_server.pipeline import process_event

    worker = MagicMock()
    worker.services.ensure_session_node = AsyncMock()
    worker.services.graph = MagicMock()
    worker.services.graph.flush = AsyncMock()
    worker.services.blob_store = MagicMock()  # truthy blob_store

    data = {
        "session_id": "sess-123",
        "timestamp": "2024-01-01T00:00:00Z",
        "raw": "big data",
    }

    with (
        patch(
            "context_intelligence_server.pipeline.process_event_data",
            new_callable=AsyncMock,
        ) as mock_process,
        patch(
            "context_intelligence_server.pipeline.make_node_id",
            return_value="test-node-id",
        ) as mock_node_id,
    ):
        await process_event(worker, "session:start", data, pipeline_handlers)
        mock_node_id.assert_called_once_with(
            "sess-123", "session:start", "2024-01-01T00:00:00Z"
        )
        mock_process.assert_called_once_with(
            data, worker.services.blob_store, "sess-123", "test-node-id"
        )


async def test_blob_processing_skipped_without_timestamp(
    pipeline_handlers: Any,
) -> None:
    """process_event_data is NOT called when timestamp is missing from data."""
    from context_intelligence_server.pipeline import process_event

    worker = MagicMock()
    worker.services.ensure_session_node = AsyncMock()
    worker.services.graph = MagicMock()
    worker.services.graph.flush = AsyncMock()
    worker.services.blob_store = MagicMock()  # truthy blob_store

    data = {"session_id": "sess-123"}  # No timestamp

    with patch(
        "context_intelligence_server.pipeline.process_event_data",
        new_callable=AsyncMock,
    ) as mock_process:
        await process_event(worker, "session:start", data, pipeline_handlers)
        mock_process.assert_not_called()


async def test_blob_processing_skipped_without_blob_store(
    pipeline_handlers: Any,
) -> None:
    """process_event_data is NOT called when blob_store is None/falsy."""
    from context_intelligence_server.pipeline import process_event

    worker = MagicMock()
    worker.services.ensure_session_node = AsyncMock()
    worker.services.graph = MagicMock()
    worker.services.graph.flush = AsyncMock()
    worker.services.blob_store = None  # falsy blob_store

    data = {"session_id": "sess-123", "timestamp": "2024-01-01T00:00:00Z"}

    with patch(
        "context_intelligence_server.pipeline.process_event_data",
        new_callable=AsyncMock,
    ) as mock_process:
        await process_event(worker, "session:start", data, pipeline_handlers)
        mock_process.assert_not_called()


async def test_blob_skip_missing_timestamp_logs_warning(
    pipeline_handlers: Any,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """When session_id and blob_store are present but timestamp is missing,
    blob processing is skipped AND a WARNING log is emitted."""
    from context_intelligence_server.pipeline import process_event

    worker = MagicMock()
    worker.services.ensure_session_node = AsyncMock()
    worker.services.graph = MagicMock()
    worker.services.graph.flush = AsyncMock()
    worker.services.blob_store = MagicMock()  # truthy blob_store

    data = {"session_id": "sess-123"}  # No timestamp

    with caplog.at_level(logging.WARNING):
        await process_event(worker, "tool_call", data, pipeline_handlers)

    assert "blob_processing_skipped" in caplog.text
    assert "sess-123" in caplog.text
    assert "tool_call" in caplog.text
    assert "missing timestamp" in caplog.text


# ===========================================================================
# process_event — touch_session call site
# ===========================================================================


async def test_process_event_calls_touch_session(
    mock_worker: MagicMock,
    pipeline_handlers: Any,
) -> None:
    """process_event must call touch_session with session_id and timestamp."""
    from context_intelligence_server.pipeline import process_event

    data = {"session_id": "sess-123", "timestamp": "2026-01-01T00:00:01Z"}
    await process_event(mock_worker, "session:start", data, pipeline_handlers)
    mock_worker.services.touch_session.assert_called_once_with(
        "sess-123", "2026-01-01T00:00:01Z"
    )


async def test_process_event_skips_touch_session_without_timestamp(
    mock_worker: MagicMock,
    pipeline_handlers: Any,
) -> None:
    """process_event must NOT call touch_session when timestamp is absent."""
    from context_intelligence_server.pipeline import process_event

    data = {"session_id": "sess-123"}  # no timestamp
    await process_event(mock_worker, "session:start", data, pipeline_handlers)
    mock_worker.services.touch_session.assert_not_called()


async def test_process_event_skips_touch_session_without_session_id(
    mock_worker: MagicMock,
    pipeline_handlers: Any,
) -> None:
    """process_event must NOT call touch_session when session_id is absent."""
    from context_intelligence_server.pipeline import process_event

    data = {"timestamp": "2026-01-01T00:00:01Z"}  # no session_id
    await process_event(mock_worker, "session:start", data, pipeline_handlers)
    mock_worker.services.touch_session.assert_not_called()
