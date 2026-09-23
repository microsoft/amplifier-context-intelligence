"""`_finalize_session` delete-ordering race: a late append landing in the
finalize window must be drained then deleted, or, if every attempt sees a
late append, the bounded retry gives up and the log is retained.

Uses a deterministic window-injection technique: wrap `qm.delete_drained`
with a spy that appends a late line before delegating to the real method.
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
from context_intelligence_server.graph_store import DurableCompletionReadError
from context_intelligence_server.registry import SessionRegistry, SessionWorker
from context_intelligence_server.services import HookStateService

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Wire format + fakes (mirrors tests/test_drain_supervision.py's style)
# ---------------------------------------------------------------------------


def _line(event: str, workspace: str, data: dict) -> bytes:
    """Encode an appended event line exactly as POST /events stores it."""
    return json.dumps({"event": event, "workspace": workspace, "data": data}).encode(
        "utf-8"
    )


class _AccumGraph:
    """Accumulating-buffer graph fake. Writes accumulate in ``buffer`` until
    ``flush()`` moves them into ``flushed`` (a SET, so a replayed event never
    shows up twice). ``fail_on_call``, if given, makes the Nth non-empty
    ``flush()`` call raise (1-based)."""

    def __init__(self, *, fail_on_call: int | None = None) -> None:
        self.workspace = "/ws"
        self.created_by: str | None = None
        self.buffer: set[str] = set()
        self.flushed: set[str] = set()
        self.discards = 0
        self.closed = False
        self._fail_on_call = fail_on_call
        self._calls = 0
        self.durably_completed = False
        self.completion_errors: list[Exception] = []
        self.steps: list[str] = []

    async def flush(self) -> None:
        if not self.buffer:
            return  # empty-buffer early return (GraphStore Protocol guarantee #5)
        self._calls += 1
        if self._fail_on_call is not None and self._calls == self._fail_on_call:
            raise RuntimeError(f"simulated flush failure on call {self._calls}")
        self.flushed |= self.buffer
        self.buffer.clear()
        self.steps.append("flush")

    def discard_buffer(self) -> None:
        self.buffer.clear()
        self.discards += 1

    async def is_session_durably_completed(self, session_id: str) -> bool:
        self.steps.append("read")
        if self.completion_errors:
            raise self.completion_errors.pop(0)
        return self.durably_completed

    async def close(self) -> None:
        self.closed = True


async def _accumulate(
    worker: SessionWorker,
    event: str,
    data: object,
    handlers: object,
    **_kw: object,
) -> None:
    """Stand-in for ``process_event``: buffers the event name on the fake graph."""
    worker.services.graph.buffer.add(event)
    if event == "session:end":
        worker.services.graph.durably_completed = True


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
    """Wrap ``qm.delete_drained`` so, on the given 1-based call numbers, it
    appends the next late line before delegating to the real method. Returns
    ``(wrapper, calls)`` where ``calls["count"]`` tracks invocations."""
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
# A late append is drained, then deleted
# ---------------------------------------------------------------------------


async def test_late_append_in_finalize_window_is_drained_then_deleted(
    caplog: pytest.LogCaptureFixture,
) -> None:
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
# A late append on every attempt exhausts the retry
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
# No double-delete + delete/close/deregister ordering preserved on the clean path
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
        "ordering: delete -> close -> deregister, deregister LAST"
    )


# ---------------------------------------------------------------------------
# Regression: the common no-late-append finalize still deletes
# ---------------------------------------------------------------------------


async def test_clean_finalize_still_deletes_and_tears_down(
    caplog: pytest.LogCaptureFixture,
) -> None:
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
# A first-pass tail flush failure returns before CompletedSession is recorded
# ---------------------------------------------------------------------------


async def test_first_pass_tail_flush_failure_returns_before_completed_session(
    caplog: pytest.LogCaptureFixture,
) -> None:
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
# The retry loop terminates in at most _FINALIZE_DELETE_ATTEMPTS DELETE
# attempts, regardless of a continuously-appending client
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
# Permanent retention: the retry's own re-drain can itself suffer a tail
# flush failure after CompletedSession was already recorded, returning early
# and never reaching _safe_close/_deregister. orphaned_sessions() is the
# honest signal.
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
# A fresh drainer over the same on-disk retained log dispatches the late
# event and drains fully.
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

        # Give-up end-state: log retained with late:event:2 undrained.
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


# ---------------------------------------------------------------------------
# A late cleanup after the original terminal worker has deregistered is a
# cleanup-only finalization only when durable graph state proves completion.
# ---------------------------------------------------------------------------


async def test_late_cleanup_after_terminal_deregistration_flushes_and_finishes() -> (
    None
):
    reg = SessionRegistry()
    qm = reg.queue_manager
    sid = "late-cleanup-after-end"
    original_graph = _AccumGraph()
    original = _make_worker(sid, original_graph)
    reg._register_for_test(original)

    with patch(
        "context_intelligence_server.registry.process_event", side_effect=_accumulate
    ):
        await qm.append(sid, _line("session:end", "/ws", {"session_id": sid}))
        original_task = _start_supervised(reg, original)
        await asyncio.wait_for(original_task, timeout=5)

        assert sid not in reg.active_sessions()
        prior_history = list(reg.completed_sessions())
        assert len(prior_history) == 1

        cleanup_graph = _AccumGraph()
        cleanup_graph.durably_completed = True
        cleanup = _make_worker(sid, cleanup_graph)
        reg._register_for_test(cleanup)
        await qm.append(sid, _line("cleanup:finally_end", "/ws", {"session_id": sid}))
        cleanup_task = _start_supervised(reg, cleanup)
        await asyncio.wait_for(cleanup_task, timeout=5)

    assert "cleanup:finally_end" in cleanup_graph.flushed
    assert not (await qm.read_batch(sid, max_items=10)).records
    assert sid not in reg.active_sessions()
    assert cleanup.store_closed is True
    assert reg.completed_sessions() == prior_history


async def test_cleanup_only_requires_durable_completed_session() -> None:
    reg = SessionRegistry()
    qm = reg.queue_manager
    sid = "early-cleanup"
    graph = _AccumGraph()
    worker = _make_worker(sid, graph)
    reg._register_for_test(worker)

    with patch(
        "context_intelligence_server.registry.process_event", side_effect=_accumulate
    ):
        await qm.append(sid, _line("cleanup:finally_end", "/ws", {"session_id": sid}))
        task = _start_supervised(reg, worker)
        for _ in range(50):
            await asyncio.sleep(0.01)
            if not (await qm.read_batch(sid, max_items=10)).records:
                break
        assert sid in reg.active_sessions()
        assert not worker.store_closed
        assert reg.completed_sessions() == []

        await qm.append(sid, _line("session:end", "/ws", {"session_id": sid}))
        await asyncio.wait_for(task, timeout=5)

    assert len(reg.completed_sessions()) == 1
    assert sid not in reg.active_sessions()


async def test_terminal_batch_with_cleanup_never_reads_durable_completion() -> None:
    reg = SessionRegistry()
    qm = reg.queue_manager
    sid = "cleanup-with-terminal"
    graph = _AccumGraph()
    graph.completion_errors = [AssertionError("terminal branch must not read")]
    worker = _make_worker(sid, graph)
    reg._register_for_test(worker)

    with patch(
        "context_intelligence_server.registry.process_event", side_effect=_accumulate
    ):
        await qm.append(sid, _line("cleanup:finally_end", "/ws", {"session_id": sid}))
        await qm.append(sid, _line("session:end", "/ws", {"session_id": sid}))
        await asyncio.wait_for(_start_supervised(reg, worker), timeout=5)

    assert graph.steps == ["flush", "flush"]
    assert len(reg.completed_sessions()) == 1
    assert sid not in reg.active_sessions()


async def test_cleanup_after_restart_finishes_without_creating_history() -> None:
    reg = SessionRegistry()
    qm = reg.queue_manager
    sid = "restart-cleanup"
    graph = _AccumGraph()
    graph.durably_completed = True
    worker = _make_worker(sid, graph)
    reg._register_for_test(worker)

    with patch(
        "context_intelligence_server.registry.process_event", side_effect=_accumulate
    ):
        await qm.append(sid, _line("cleanup:finally_end", "/ws", {"session_id": sid}))
        await asyncio.wait_for(_start_supervised(reg, worker), timeout=5)

    assert reg.completed_sessions() == []
    assert sid not in reg.active_sessions()


async def test_cleanup_finalizer_promotes_a_terminal_event_in_its_tail() -> None:
    reg = SessionRegistry()
    qm = reg.queue_manager
    sid = "cleanup-tail-terminal"
    graph = _AccumGraph()
    graph.durably_completed = True
    worker = _make_worker(sid, graph)
    reg._register_for_test(worker)
    original_delete = qm.delete_drained
    appended = False

    async def delete_drained(session_id: str) -> bool:
        nonlocal appended
        if not appended:
            appended = True
            await qm.append(sid, _line("session:end", "/ws", {"session_id": sid}))
        return await original_delete(session_id)

    qm.delete_drained = delete_drained  # type: ignore[method-assign]

    with patch(
        "context_intelligence_server.registry.process_event", side_effect=_accumulate
    ):
        await qm.append(sid, _line("cleanup:finally_end", "/ws", {"session_id": sid}))
        await asyncio.wait_for(_start_supervised(reg, worker), timeout=5)

    assert len(reg.completed_sessions()) == 1
    assert sid not in reg.active_sessions()


async def test_durable_completion_read_retries_without_poisoning_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reg = SessionRegistry()
    qm = reg.queue_manager
    sid = "cleanup-read-retry"
    graph = _AccumGraph()
    graph.durably_completed = True
    graph.completion_errors = [DurableCompletionReadError("read unavailable")] * 3
    worker = _make_worker(sid, graph)
    reg._register_for_test(worker)
    monkeypatch.setattr(
        "context_intelligence_server.registry._TRANSIENT_BACKOFF_CAP", 0.001
    )

    with patch(
        "context_intelligence_server.registry.process_event", side_effect=_accumulate
    ):
        await qm.append(sid, _line("cleanup:finally_end", "/ws", {"session_id": sid}))
        await asyncio.wait_for(_start_supervised(reg, worker), timeout=5)

    assert await qm.read_dead_letters(sid) == []
    assert not (await qm.read_batch(sid, max_items=10)).records
    assert reg.pipeline_counters()["write_retries_total"] >= 3
    assert sid not in reg.active_sessions()


async def test_isolation_fallback_commits_durably_completed_cleanup_in_order() -> None:
    reg = SessionRegistry()
    reg.is_recovery = True
    qm = reg.queue_manager
    reg._max_delivery_attempts = 1
    sid = "cleanup-isolation"
    graph = _AccumGraph(fail_on_call=1)
    graph.durably_completed = True
    worker = _make_worker(sid, graph)
    reg._register_for_test(worker)
    original_commit = qm.commit

    async def commit(session_id: str, offset: int) -> None:
        graph.steps.append("commit")
        await original_commit(session_id, offset)

    async def post_commit(_worker: SessionWorker, _records: list[object]) -> None:
        graph.steps.append("post")

    async def pre_flush() -> None:
        graph.steps.append("pre_flush")

    async def before_recovery_flush(
        _worker: SessionWorker, _records: list[object]
    ) -> None:
        graph.steps.append("recovery_pre_flush")

    async def before_commit(_worker: SessionWorker, _records: list[object]) -> None:
        graph.steps.append("pre_commit")

    qm.commit = commit  # type: ignore[method-assign]
    reg.pre_flush = pre_flush
    reg.before_recovery_flush = before_recovery_flush
    reg.on_batch_flushed = before_commit
    reg.on_batch_written = post_commit

    with patch(
        "context_intelligence_server.registry.process_event", side_effect=_accumulate
    ):
        await qm.append(sid, _line("cleanup:finally_end", "/ws", {"session_id": sid}))
        await asyncio.wait_for(_start_supervised(reg, worker), timeout=5)

    assert graph.steps[-7:] == [
        "pre_flush",
        "recovery_pre_flush",
        "flush",
        "read",
        "pre_commit",
        "commit",
        "post",
    ]
    assert await qm.read_dead_letters(sid) == []
    assert sid not in reg.active_sessions()
