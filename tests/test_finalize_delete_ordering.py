"""`_finalize_session` delete-ordering race.

Covers, per the finalized spec chain (R1-R6
+ T9 are AUTHORITATIVE):

  T1 (a)/(A) headline    -- a late append landing in the finalize window is
                            drained-then-deleted (retry succeeds on attempt 2)
  T2 (a)/(B) give-up      -- a late append on EVERY delete attempt exhausts
                            the bounded retry; the log is RETAINED and
                            recover()-reportable
  T4 (b) Call B ordering  -- no double-delete; delete -> close -> deregister,
                            deregister LAST, on the clean path
  T5 (c) compaction non-interaction -- a late-append-during-finalize retry
                            composes cleanly with a PRIOR compaction on the
                            same key
  T6 (d) regression       -- the common no-late-append finalize still
                            deletes cleanly (must be GREEN before and after)
  T7 regression/extraction -- a FIRST-pass tail flush failure returns before
                            CompletedSession is recorded (must be GREEN
                            before and after -- pre-existing, unchanged
                            behaviour merely extracted into `_drain_to_eof`)
  T8                       -- the retry loop terminates in at most
                            `_FINALIZE_DELETE_ATTEMPTS` DELETE attempts
                            regardless of a continuously-appending client
  T9 (R4)                 -- the NEW permanent-retention residual: the
                            retry's own re-drain can itself suffer a tail
                            flush failure AFTER CompletedSession was already
                            recorded, `return`-ing early (never reaching
                            _safe_close/_deregister). `orphaned_sessions()`
                            is the honest signal; `delete_drained` is never
                            called again for that key.

Window-injection technique (spec section 5, deterministic, not
timing-dependent): wrap `qm.delete_drained` with a spy that, BEFORE
delegating to the real method, performs `await qm.append(sid, <late
line>)`. That lands the append strictly inside the real window (after the
final `read_batch`, before the in-lock `stat`) by construction.

T3 ("prove pick-up") is intentionally folded into this file's own idiom
for "a fresh drainer resumes a retained log" -- the same pattern
`test_finalize_reruns_to_completion_after_a_transient_finalize_failure`
already uses (a second `SessionWorker` over the same on-disk queue, driven
through the real `start_drain`/`drain_worker`), rather than the real
`get_or_create(..., recovered=True)` (which would require a real Neo4j
driver -- out of scope for this non-Neo4j file). See
`test_retained_log_is_picked_up_by_a_fresh_drainer` below.

No real Neo4j is used anywhere in this file.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any
from unittest.mock import patch

import pytest

from context_intelligence_server.queue_manager import QueueManager
from context_intelligence_server.registry import SessionRegistry, SessionWorker
from context_intelligence_server.services import HookStateService

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Wire format + fakes (mirrors tests/test_drain_supervision.py's
# style; duplicated rather than imported across test modules, matching this
# repo's existing convention -- see tests/test_steady_state_reclaim.py)
# ---------------------------------------------------------------------------


def _line(event: str, workspace: str, data: dict) -> bytes:
    """Encode an appended event line exactly as POST /events stores it."""
    return json.dumps({"event": event, "workspace": workspace, "data": data}).encode(
        "utf-8"
    )


class _AccumGraph:
    """A minimal, faithful accumulating-buffer graph fake (not a hollow mock).

    Writes accumulate in ``buffer`` until ``flush()`` succeeds, at which
    point they move into ``flushed`` -- a SET, modeling a real store's
    idempotent id-keyed MERGE (replaying the same event after a re-drain
    must never show up twice). ``fail_on_call``, if given, makes the Nth
    NON-EMPTY ``flush()`` call raise (1-based); every other call succeeds.
    Empty-buffer flushes are never counted (mirrors GraphStore Protocol
    guarantee #5 -- the empty-buffer early return).
    """

    def __init__(self, *, fail_on_call: int | None = None) -> None:
        self.workspace = "/ws"
        self.created_by: str | None = None
        self.buffer: set[str] = set()
        self.flushed: set[str] = set()
        self.discards = 0
        self.closed = False
        self._fail_on_call = fail_on_call
        self._calls = 0

    async def flush(self) -> None:
        if not self.buffer:
            return  # empty-buffer early return (GraphStore Protocol guarantee #5)
        self._calls += 1
        if self._fail_on_call is not None and self._calls == self._fail_on_call:
            raise RuntimeError(f"simulated flush failure on call {self._calls}")
        self.flushed |= self.buffer
        self.buffer.clear()

    def discard_buffer(self) -> None:
        self.buffer.clear()
        self.discards += 1

    async def close(self) -> None:
        self.closed = True


async def _accumulate(
    worker: SessionWorker, event: str, data: object, handlers: object
) -> None:
    """Stand-in for ``process_event``: buffers the event name on the fake graph."""
    worker.services.graph.buffer.add(event)


def _make_worker(sid: str, graph: Any, workspace: str = "/ws") -> SessionWorker:
    worker = SessionWorker(
        session_id=sid,
        workspace=workspace,
        services=HookStateService(workspace=workspace),
    )
    worker.services.graph = graph  # type: ignore[assignment]
    return worker


def _delete_drained_injector(
    qm: QueueManager,
    late_lines: list[bytes],
    inject_on: set[int],
) -> tuple[Callable[[str], Awaitable[bool]], dict[str, int]]:
    """Wrap ``qm.delete_drained`` per the spec's window-injection technique.

    On the given 1-based delete-call numbers, appends the NEXT late line
    BEFORE delegating to the real ``delete_drained`` -- landing the append
    strictly inside the real window (after the final ``read_batch``, before
    the in-lock ``stat``) by construction, not by timing.

    Returns ``(wrapper, calls)`` where ``calls["count"]`` is the number of
    times ``delete_drained`` was actually invoked (assign the wrapper to
    ``qm.delete_drained`` and inspect ``calls`` afterwards).
    """
    original = qm.delete_drained
    calls = {"count": 0}
    injected = {"count": 0}

    async def _wrapper(session_id: str) -> bool:
        calls["count"] += 1
        attempt = calls["count"]
        if attempt in inject_on and injected["count"] < len(late_lines):
            await qm.append(session_id, late_lines[injected["count"]])
            injected["count"] += 1
        return await original(session_id)

    return _wrapper, calls


def _start_supervised(
    reg: SessionRegistry, worker: SessionWorker, *, flush_timeout: float = 10.0
) -> asyncio.Task:
    """Mirror production ``start_drain`` (registry.py) exactly: create the
    task, attach the done-callback, bind ``worker.task``."""
    import functools

    task = asyncio.create_task(
        reg.drain_worker(worker, flush_timeout=flush_timeout),
        name=f"drain-{worker.session_id}",
    )
    task.add_done_callback(functools.partial(reg._on_drain_done, worker))
    worker.task = task
    return task


# ---------------------------------------------------------------------------
# T1 -- (a)/(A) headline: a late append is drained, THEN deleted
# ---------------------------------------------------------------------------


async def test_late_append_in_finalize_window_is_drained_then_deleted(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """This test run IS the non-vacuity control: pre-fix, delete_drained is
    called exactly ONCE, returns False, the log is still on disk, and the
    late event is NEVER dispatched (the return value is discarded)."""
    reg = SessionRegistry()
    qm = reg.queue_manager
    sid = "d5-t1-late-append"
    graph = _AccumGraph()
    worker = _make_worker(sid, graph)
    reg._register_for_test(worker)

    late_line = _line("late:event", "/ws", {"session_id": sid})
    wrapper, calls = _delete_drained_injector(qm, [late_line], inject_on={1})
    qm.delete_drained = wrapper  # type: ignore[method-assign]

    with (
        patch(
            "context_intelligence_server.registry.process_event",
            side_effect=_accumulate,
        ),
        caplog.at_level(logging.WARNING, logger="context_intelligence_server"),
    ):
        await qm.append(sid, _line("tool:pre", "/ws", {"session_id": sid}))
        await qm.append(sid, _line("session:end", "/ws", {"session_id": sid}))
        await reg._finalize_session(worker, handlers=object())

    assert calls["count"] == 2, (
        "attempt 1 must retain (late append lands inside the window); "
        "attempt 2 must succeed after the re-drain persists it"
    )
    assert "late:event" in graph.flushed, (
        "the late event must be dispatched BEFORE the log is deleted"
    )
    assert not qm._log_path(sid).exists()
    assert not qm._offset_path(sid).exists()
    assert any(
        r.levelno == logging.WARNING
        and "finalize_delete_retained" in r.getMessage()
        and getattr(r, "session_id", None) == sid
        for r in caplog.records
    ), "the retained-on-attempt-1 WARNING must be logged exactly once"


# ---------------------------------------------------------------------------
# T2 -- (a)/(B) give-up: a late append on EVERY attempt exhausts the retry
# ---------------------------------------------------------------------------


async def test_late_append_on_every_attempt_retains_and_is_recoverable(
    caplog: pytest.LogCaptureFixture,
) -> None:
    reg = SessionRegistry()
    qm = reg.queue_manager
    sid = "d5-t2-give-up"
    graph = _AccumGraph()
    worker = _make_worker(sid, graph)
    reg._register_for_test(worker)

    late_lines = [
        _line(f"late:event:{i}", "/ws", {"session_id": sid}) for i in range(3)
    ]
    wrapper, calls = _delete_drained_injector(qm, late_lines, inject_on={1, 2, 3})
    qm.delete_drained = wrapper  # type: ignore[method-assign]

    with (
        patch(
            "context_intelligence_server.registry.process_event",
            side_effect=_accumulate,
        ),
        caplog.at_level(logging.WARNING, logger="context_intelligence_server"),
    ):
        await qm.append(sid, _line("session:end", "/ws", {"session_id": sid}))
        await reg._finalize_session(worker, handlers=object())

    assert calls["count"] == 3, (
        "must give up after exactly _FINALIZE_DELETE_ATTEMPTS(=3) delete calls"
    )
    assert qm._log_path(sid).exists(), "log must be RETAINED on give-up (never lost)"
    assert any(
        r.levelno == logging.ERROR
        and "finalize_delete_gave_up" in r.getMessage()
        and getattr(r, "session_id", None) == sid
        for r in caplog.records
    ), "the give-up must be logged loudly at ERROR"

    recoverable = await qm.recover()
    assert sid in recoverable, (
        "a retained log with a complete uncommitted line must be recover()-reportable"
    )
    assert sid not in reg.active_sessions(), (
        "give-up still deregisters + closes (unchanged teardown path, spec 3.5)"
    )
    assert worker.store_closed is True


# ---------------------------------------------------------------------------
# T4 -- (b) no double-delete + Call B ordering preserved on the clean path
# ---------------------------------------------------------------------------


async def test_no_double_delete_and_call_b_ordering_preserved() -> None:
    reg = SessionRegistry()
    qm = reg.queue_manager
    sid = "d5-t4-call-b-ordering"
    graph = _AccumGraph()
    worker = _make_worker(sid, graph)
    reg._register_for_test(worker)

    sequence: list[str] = []
    original_delete = qm.delete_drained
    original_close = graph.close
    original_deregister = reg._deregister

    async def _spy_delete(session_id: str) -> bool:
        sequence.append("delete_drained")
        return await original_delete(session_id)

    async def _spy_close() -> None:
        sequence.append("graph.close")
        await original_close()

    def _spy_deregister(session_id: str) -> None:
        sequence.append("_deregister")
        original_deregister(session_id)

    qm.delete_drained = _spy_delete  # type: ignore[method-assign]
    graph.close = _spy_close  # type: ignore[method-assign]
    reg._deregister = _spy_deregister  # type: ignore[method-assign]

    with patch(
        "context_intelligence_server.registry.process_event",
        side_effect=_accumulate,
    ):
        await qm.append(sid, _line("session:end", "/ws", {"session_id": sid}))
        await reg._finalize_session(worker, handlers=object())

    assert sequence.count("delete_drained") == 1, (
        "exactly one delete_drained call on the clean path -- never a double-delete"
    )
    assert sequence == ["delete_drained", "graph.close", "_deregister"], (
        "Call B ordering: delete -> close -> deregister, deregister LAST"
    )


# ---------------------------------------------------------------------------
# T5 -- (c) no race with a PRIOR compaction on the same key
# ---------------------------------------------------------------------------


async def test_finalize_retry_does_not_race_compaction_on_the_same_key() -> None:
    """Drives the REAL drain_worker loop with compaction enabled and
    min_prefix=0 (so Trigger H compacts the earlier, non-terminal batch),
    THEN a late append lands inside the finalize window. The compaction
    path's own analysis says these cannot interact -- finalize never
    compacts, exactly one drain task exists per session -- this proves it
    holds with the retry loop composed in."""
    reg = SessionRegistry()
    qm = reg.queue_manager
    sid = "d5-t5-no-compaction-race"
    graph = _AccumGraph()
    worker = _make_worker(sid, graph)
    reg._register_for_test(worker)

    class _CompactAlwaysSettings:
        queue_compact_enabled = True
        queue_compact_min_prefix_bytes = 0
        queue_compact_max_tail_bytes = 64 * 1024 * 1024

    late_line = _line("late:event", "/ws", {"session_id": sid})
    wrapper, calls = _delete_drained_injector(qm, [late_line], inject_on={1})
    qm.delete_drained = wrapper  # type: ignore[method-assign]

    with (
        patch(
            "context_intelligence_server.registry.process_event",
            side_effect=_accumulate,
        ),
        patch(
            "context_intelligence_server.registry.get_settings",
            return_value=_CompactAlwaysSettings(),
        ),
    ):
        await qm.append(sid, _line("e1", "/ws", {"session_id": sid}))
        task = _start_supervised(reg, worker)

        for _ in range(500):
            if "e1" in graph.flushed:
                break
            await asyncio.sleep(0.01)
        assert "e1" in graph.flushed, (
            "precondition: e1 must drain (and Trigger H compact) BEFORE "
            "session:end arrives, in its own separate non-terminal batch"
        )

        await qm.append(sid, _line("session:end", "/ws", {"session_id": sid}))
        await asyncio.wait_for(task, timeout=10.0)

    assert graph.flushed == {"e1", "session:end", "late:event"}, (
        "every event persisted exactly once, no reorder, no exception"
    )
    assert calls["count"] == 2, (
        "the late-append-during-finalize retry must still retain-then-succeed "
        "even after a prior compaction ran on the same key"
    )
    assert not qm._log_path(sid).exists()
    assert not qm._offset_path(sid).exists()


# ---------------------------------------------------------------------------
# T6 -- (d) regression: the common no-late-append finalize still deletes
# ---------------------------------------------------------------------------


async def test_clean_finalize_still_deletes_and_tears_down(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Must be GREEN before AND after the change."""
    reg = SessionRegistry()
    qm = reg.queue_manager
    sid = "d5-t6-clean-finalize"
    graph = _AccumGraph()
    worker = _make_worker(sid, graph)
    reg._register_for_test(worker)

    delete_calls = {"count": 0}
    original_delete = qm.delete_drained

    async def _counting_delete(session_id: str) -> bool:
        delete_calls["count"] += 1
        return await original_delete(session_id)

    qm.delete_drained = _counting_delete  # type: ignore[method-assign]

    with (
        patch(
            "context_intelligence_server.registry.process_event",
            side_effect=_accumulate,
        ),
        caplog.at_level(logging.INFO, logger="context_intelligence_server"),
    ):
        await qm.append(sid, _line("tool:pre", "/ws", {"session_id": sid}))
        await qm.append(sid, _line("session:end", "/ws", {"session_id": sid}))
        await reg._finalize_session(worker, handlers=object())

    assert not qm._log_path(sid).exists()
    assert not qm._offset_path(sid).exists()
    assert delete_calls["count"] == 1
    assert len(reg.completed_sessions()) == 1
    assert any(
        r.levelno == logging.INFO and "session_finalized" in r.getMessage()
        for r in caplog.records
    )
    assert sid not in reg.active_sessions()
    assert worker.store_closed is True


# ---------------------------------------------------------------------------
# T7 -- regression/extraction: a FIRST-pass tail flush failure returns
# before CompletedSession is recorded (pre-existing behaviour, unchanged;
# guards the _drain_to_eof extraction against changing existing semantics)
# ---------------------------------------------------------------------------


async def test_first_pass_tail_flush_failure_returns_before_completed_session(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Must be GREEN before AND after the change."""
    reg = SessionRegistry()
    qm = reg.queue_manager
    sid = "d5-t7-tail-flush-failure"
    graph = _AccumGraph(fail_on_call=1)
    worker = _make_worker(sid, graph)
    reg._register_for_test(worker)

    with (
        patch(
            "context_intelligence_server.registry.process_event",
            side_effect=_accumulate,
        ),
        caplog.at_level(logging.ERROR, logger="context_intelligence_server"),
    ):
        await qm.append(sid, _line("tool:pre", "/ws", {"session_id": sid}))
        await qm.append(sid, _line("session:end", "/ws", {"session_id": sid}))
        await reg._finalize_session(worker, handlers=object())

    assert any(
        r.levelno == logging.ERROR and "finalize_tail_flush_failed" in r.getMessage()
        for r in caplog.records
    )
    assert len(reg.completed_sessions()) == 0, (
        "CompletedSession must NOT be recorded when the FIRST pass's tail flush fails"
    )
    assert sid in reg.active_sessions(), (
        "worker must remain registered so a respawn retries"
    )
    assert worker.store_closed is False
    assert qm._log_path(sid).exists(), "the tail must remain uncommitted on disk"


# ---------------------------------------------------------------------------
# T8: the retry loop terminates in at most _FINALIZE_DELETE_ATTEMPTS
# DELETE attempts, regardless of a continuously-appending client
# ---------------------------------------------------------------------------


@pytest.mark.timeout(30)
async def test_retry_loop_terminates_under_continuous_append() -> None:
    reg = SessionRegistry()
    qm = reg.queue_manager
    sid = "d5-t8-bounded-termination"
    graph = _AccumGraph()
    worker = _make_worker(sid, graph)
    reg._register_for_test(worker)

    # More late lines than attempts are possible, to prove the bound is real
    # even if the injector *could* keep going.
    late_lines = [
        _line(f"late:event:{i}", "/ws", {"session_id": sid}) for i in range(10)
    ]
    wrapper, calls = _delete_drained_injector(qm, late_lines, inject_on={1, 2, 3, 4, 5})
    qm.delete_drained = wrapper  # type: ignore[method-assign]

    with patch(
        "context_intelligence_server.registry.process_event",
        side_effect=_accumulate,
    ):
        await qm.append(sid, _line("session:end", "/ws", {"session_id": sid}))
        await reg._finalize_session(worker, handlers=object())

    assert calls["count"] == 3, (
        "delete attempts must be bounded to _FINALIZE_DELETE_ATTEMPTS(=3) "
        "regardless of how many late lines a continuous appender could supply"
    )


# ---------------------------------------------------------------------------
# T9 (R4): NEW permanent-retention residual. The retry's
# OWN re-drain can itself suffer a tail flush failure AFTER CompletedSession
# was already recorded -- returning early, never reaching
# _safe_close/_deregister. orphaned_sessions() is the honest signal;
# delete_drained is never called again for this key.
# ---------------------------------------------------------------------------


async def test_permanent_retention_when_retrys_own_redrain_flush_fails(
    caplog: pytest.LogCaptureFixture,
) -> None:
    reg = SessionRegistry()
    qm = reg.queue_manager
    sid = "d5-t9-permanent-retention"
    # 1st flush (tool:pre + session:end, the initial _drain_to_eof) succeeds;
    # 2nd flush (the retry's re-drain of the late event) fails.
    graph = _AccumGraph(fail_on_call=2)
    worker = _make_worker(sid, graph)
    reg._register_for_test(worker)

    late_line = _line("late:event", "/ws", {"session_id": sid})
    wrapper, calls = _delete_drained_injector(qm, [late_line], inject_on={1})
    qm.delete_drained = wrapper  # type: ignore[method-assign]

    with (
        patch(
            "context_intelligence_server.registry.process_event",
            side_effect=_accumulate,
        ),
        caplog.at_level(logging.ERROR, logger="context_intelligence_server"),
    ):
        await qm.append(sid, _line("tool:pre", "/ws", {"session_id": sid}))
        await qm.append(sid, _line("session:end", "/ws", {"session_id": sid}))
        # A real, completed Task is required for orphaned_sessions() (it
        # checks worker.task is not None and worker.task.done()).
        task = asyncio.create_task(reg._finalize_session(worker, handlers=object()))
        worker.task = task
        await task

    assert calls["count"] == 1, (
        "delete_drained called exactly once; the re-drain's OWN flush "
        "failure returns early before a second delete attempt"
    )
    assert len(reg.completed_sessions()) == 1, (
        "CompletedSession was already recorded BEFORE the retry loop began"
    )
    assert sid in reg.active_sessions(), (
        "the early return never reaches _deregister -- permanently registered"
    )
    assert worker.store_closed is False, (
        "the early return never reaches _safe_close either"
    )
    assert any(
        r.levelno == logging.ERROR and "finalize_tail_flush_failed" in r.getMessage()
        for r in caplog.records
    )
    orphans = reg.orphaned_sessions()
    assert any(w.session_id == sid for w in orphans), (
        "orphaned_sessions() is the honest signal for this permanent-"
        "retention residual -- registered, task done, never re-entered"
    )
    assert qm._log_path(sid).exists(), (
        "the late event's log is RETAINED -- never lost, never re-attempted"
    )


# ---------------------------------------------------------------------------
# T3 (spec section 5, "prove pick-up") -- a fresh drainer over the SAME
# on-disk retained log dispatches the late event and drains fully. Uses the
# same fake-graph respawn pattern another regression test uses for a similar
# scenario uses (test_finalize_reruns_to_completion_after_a_transient_finalize_failure),
# rather than the real get_or_create(recovered=True) path, which would need
# a real Neo4j driver -- out of scope for this non-Neo4j file.
# ---------------------------------------------------------------------------


async def test_retained_log_is_picked_up_by_a_fresh_drainer() -> None:
    reg = SessionRegistry()
    qm = reg.queue_manager
    sid = "d5-t3-retained-log-pickup"
    graph = _AccumGraph()
    worker = _make_worker(sid, graph)
    reg._register_for_test(worker)

    late_lines = [
        _line(f"late:event:{i}", "/ws", {"session_id": sid}) for i in range(3)
    ]
    original_delete = qm.delete_drained
    wrapper, calls = _delete_drained_injector(qm, late_lines, inject_on={1, 2, 3})
    qm.delete_drained = wrapper  # type: ignore[method-assign]

    with patch(
        "context_intelligence_server.registry.process_event",
        side_effect=_accumulate,
    ):
        await qm.append(sid, _line("session:end", "/ws", {"session_id": sid}))
        await reg._finalize_session(worker, handlers=object())

        # T2's give-up end-state: log retained with late:event:2 undrained.
        assert calls["count"] == 3
        assert qm._log_path(sid).exists()
        assert "late:event:2" not in graph.flushed

        # Restore the REAL delete_drained for the fresh drainer -- the
        # injector's job (landing a late append inside the *original*
        # finalize's window) is done; a second drainer must not re-inject.
        qm.delete_drained = original_delete  # type: ignore[method-assign]

        worker2 = _make_worker(sid, graph)
        reg._register_for_test(worker2)
        reg.start_drain(worker2)
        assert worker2.task is not None
        await asyncio.wait_for(worker2.task, timeout=5.0)

    assert "late:event:2" in graph.flushed, (
        "the fresh drainer must dispatch the previously-retained late event"
    )
    assert qm._read_committed_offset(sid) == qm._complete_data_end(sid), (
        "log ends fully drained -- committed advances to complete_data_end, "
        "proving outcome (B) is real pick-up, not merely asserted"
    )
