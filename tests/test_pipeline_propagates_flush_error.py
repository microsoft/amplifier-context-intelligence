"""Tests for the flush-failure propagation contract in process_event.

Phase A correctness fix: a Neo4j flush failure (step 7) must propagate to
registry._process_one so the dropped batch is no longer silently swallowed,
while benign per-event handler errors (steps 2-6) stay swallowed so the drain
loop survives.
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


async def test_flush_error_propagates_on_terminal_event() -> None:
    """A flush failure on session:end must propagate (not be swallowed)."""
    from context_intelligence_server.pipeline import process_event

    worker = _make_worker(RuntimeError("DeadlockDetected: retries exhausted"))
    data = {"session_id": "s1", "timestamp": "2026-06-11T12:00:00+00:00"}

    with pytest.raises(RuntimeError):
        await process_event(worker, "session:end", data, _handlers())  # type: ignore[arg-type]


async def test_benign_handler_error_is_swallowed_on_non_terminal_event() -> None:
    """A benign per-event handler error must NOT propagate and must skip flush."""
    from context_intelligence_server.pipeline import process_event

    worker = _make_worker()  # no flush_exc
    data = {"session_id": "s1", "timestamp": "2026-06-11T12:00:00+00:00"}
    handlers = _handlers(default=_BoomHandler())

    # Must NOT raise
    await process_event(worker, "user:prompt", data, handlers)  # type: ignore[arg-type]

    # Steps 1-6 errored, so step 7 flush was skipped.
    assert worker.services.graph.scheduled is False
