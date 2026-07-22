"""Confirms the 'zero new metrics' claim of the Phase A propagation fix.

When process_event raises (e.g. a Neo4j flush that exhausts deadlock retries),
the existing registry._process_one path must land the failure in the surfaces
/status already reads: it increments worker.error_count and appends a
ring-buffer EventRecord(result='error', error=<non-empty>). No new metrics are
introduced — propagation simply re-activates this existing failure surface.
"""

from __future__ import annotations

from typing import Any

import pytest

from context_intelligence_server import registry as registry_module
from context_intelligence_server.status import ring_buffer
from context_intelligence_server.registry import SessionRegistry, SessionWorker


async def test_propagated_flush_error_increments_error_count_and_ring_buffer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A raised process_event lands in error_count + ring-buffer result='error'."""
    # Real SessionWorker; services is unused because process_event is patched to raise.
    worker = SessionWorker(session_id="s1", workspace="ws1", services=None)  # type: ignore[arg-type]
    assert worker.error_count == 0

    async def _boom(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("DeadlockDetected")

    # registry.py imports process_event into its own namespace, so patch it there.
    monkeypatch.setattr(registry_module, "process_event", _boom)

    # Phase B2 (Task 7): _process_one now RE-RAISES after recording the failure
    # to its existing surfaces, so the drainer can dead-letter the line instead
    # of committing the offset past a never-persisted event. The recording in
    # the except/finally still happens; the raise just propagates afterwards.
    with pytest.raises(RuntimeError, match="DeadlockDetected"):
        await SessionRegistry()._process_one(
            worker, "session:end", {"session_id": "s1"}, handlers=None
        )

    # Existing failure surface #1: the worker's error counter.
    assert worker.error_count == 1

    # Existing failure surface #2: the newest-first ring buffer (/status reads this).
    latest = ring_buffer.recent()[0]
    assert latest.result == "error"
    assert latest.error  # non-empty error message
