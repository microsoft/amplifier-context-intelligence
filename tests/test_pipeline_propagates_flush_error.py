"""Tests for per-event error isolation in process_event.

Task 6 update: process_event no longer flushes (the drainer's gated
``_flush_barrier`` is the sole Neo4j-write trigger). The old
"flush failure propagates out of process_event" contract therefore no longer
exists here — flush-failure handling is now a drainer contract, covered by
tests/test_registry.py::TestDurableDrainLoop::test_offset_not_committed_when_flush_fails.

Phase B2 update (USER DECISION option a): per-event handler errors (steps 2-6)
are NO LONGER swallowed — they PROPAGATE so the drainer can dead-letter the
offending line instead of committing the offset past a never-persisted event.
process_event still never schedules a flush (the drainer owns the barrier).
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class _Graph:
    """Graph stub: flush() raises flush_exc if set; schedule_flush() marks scheduled."""

    def __init__(self, flush_exc: Exception | None = None) -> None:
        self.flush_exc = flush_exc
        self.scheduled = False

    async def flush(self) -> None:
        if self.flush_exc is not None:
            raise self.flush_exc

    def schedule_flush(self) -> None:
        self.scheduled = True


async def _noop(*args: Any, **kwargs: Any) -> None:
    return None


def _make_worker(flush_exc: Exception | None = None) -> SimpleNamespace:
    services = SimpleNamespace(
        graph=_Graph(flush_exc),
        blob_store=None,
        ensure_session_node=_noop,
        touch_session=_noop,
    )
    return SimpleNamespace(services=services)


class _OkHandler:
    """Default handler that does nothing."""

    async def __call__(self, event: str, data: dict[str, Any]) -> None:
        return None


class _BoomHandler:
    """Default handler that raises a benign per-event error."""

    async def __call__(self, event: str, data: dict[str, Any]) -> None:
        raise ValueError("benign handler boom")


def _handlers(default: Any = None) -> Any:
    from context_intelligence_server.pipeline import PipelineHandlers

    return PipelineHandlers(default=default or _OkHandler(), enrichers=[])  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


# NOTE (Task 6): test_flush_error_propagates_on_terminal_event was removed.
# process_event no longer flushes — the drainer's gated _flush_barrier is the
# sole Neo4j-write trigger. Flush-failure propagation is now a drainer contract
# covered by tests/test_registry.py::TestDurableDrainLoop
# ::test_offset_not_committed_when_flush_fails.


async def test_handler_error_propagates_on_non_terminal_event() -> None:
    """Phase B2 (USER DECISION option a): a per-event handler error must
    PROPAGATE (not be swallowed) so the drainer can dead-letter the line.
    process_event still never schedules a flush (the drainer owns the barrier)."""
    from context_intelligence_server.pipeline import process_event

    worker = _make_worker()  # no flush_exc
    data = {"session_id": "s1", "timestamp": "2026-06-11T12:00:00+00:00"}
    handlers = _handlers(default=_BoomHandler())

    # The handler error must propagate out of process_event.
    with pytest.raises(ValueError, match="benign handler boom"):
        await process_event(worker, "user:prompt", data, handlers)  # type: ignore[arg-type]

    # process_event never schedules a flush (the drainer owns the barrier).
    assert worker.services.graph.scheduled is False
