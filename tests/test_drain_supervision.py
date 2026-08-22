"""Drain supervision + offset-ownership tests.

Verifies that an unexpected exception raised inside ``drain_worker`` is
never silent: ``add_done_callback``/``_on_drain_done`` ensures it is
logged, the store is closed, and the worker is deregistered so a fresh
one can be created. No real Neo4j is used anywhere in this file.

Determinism rule: no wall-clock sleeps as synchronisation -- every wait is
either a bounded poll on an observable condition, or an ``asyncio.Event``
the injected fake sets right before blocking.
"""

from __future__ import annotations

import asyncio
import contextlib
import errno
import functools
import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any
from unittest.mock import patch

import neo4j.exceptions as neo4j_exc
import pytest

from context_intelligence_server.queue_manager import QueueManager
from context_intelligence_server.registry import SessionRegistry, SessionWorker
from context_intelligence_server.services import HookStateService

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Wire format + fakes
# ---------------------------------------------------------------------------


def _line(event: str, workspace: str, data: dict) -> bytes:
    """Encode an appended event line exactly as POST /events stores it
    (mirrors ``tests/test_registry.py::_line``)."""
    return json.dumps({"event": event, "workspace": workspace, "data": data}).encode(
        "utf-8"
    )


class _FlakyGraph:
    """A faithful accumulating-buffer graph fake (NOT a hollow mock).

    Mirrors ``tests/test_registry.py::_AccumBufferGraph`` /
    ``tests/test_large_event_tail_drop.py::_FaultInjectableGraph``:
    writes accumulate in ``buffer`` until ``flush()`` succeeds, at which
    point they move into ``flushed`` -- a SET, modeling a real store's
    idempotent id-keyed MERGE (replaying the same event after a respawn
    must never show up twice). ``fail_when`` decides whether ``flush()``
    raises for the CURRENT buffer contents; the default rejects only
    multi-event batches (a single isolated line always succeeds), which is
    what drives a batch through retries -> exhaustion -> per-line isolation
    without every individual line being unprocessable.
    """

    def __init__(self, *, fail_when: Callable[[set[str]], bool] | None = None) -> None:
        self.workspace = "/ws"
        self.created_by: str | None = None
        self.buffer: set[str] = set()
        self.flushed: set[str] = set()
        self.discards = 0
        self.closed = False
        self._fail_when = fail_when or (lambda buf: len(buf) > 1)

    async def flush(self) -> None:
        if not self.buffer:
            return  # empty-buffer early return (mirrors neo4j_store.py:1501-1502)
        if self._fail_when(self.buffer):
            raise RuntimeError(f"flush rejected for buffer={sorted(self.buffer)}")
        self.flushed |= self.buffer
        self.buffer.clear()  # success clears

    def discard_buffer(self) -> None:
        self.buffer.clear()
        self.discards += 1

    async def close(self) -> None:
        self.closed = True


class _SequencedFlushGraph:
    """flush() raises each exception in ``sequence`` in order, then succeeds
    forever after. Used only for the real-neo4j-exception-type test."""

    def __init__(self, sequence: list[BaseException]) -> None:
        self.workspace = "/ws"
        self.created_by: str | None = None
        self.buffer: set[str] = set()
        self.flushed: set[str] = set()
        self.discards = 0
        self.closed = False
        self._sequence = list(sequence)
        self._calls = 0

    async def flush(self) -> None:
        if self._calls < len(self._sequence):
            exc = self._sequence[self._calls]
            self._calls += 1
            raise exc
        self._calls += 1
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
    """Stand-in for ``process_event``: buffers the event name on the fake
    graph, exactly like ``_FaultInjectableGraph``'s harness in the sibling
    large-event tail-drop test file."""
    worker.services.graph.buffer.add(event)


def _make_worker(sid: str, graph: Any, workspace: str = "/ws") -> SessionWorker:
    worker = SessionWorker(
        session_id=sid,
        workspace=workspace,
        services=HookStateService(workspace=workspace),
    )
    worker.services.graph = graph  # type: ignore[assignment]
    return worker


def _flaky(
    original: Callable[..., Awaitable[Any]],
    exc: BaseException,
    *,
    on_call: int = 1,
) -> Callable[..., Awaitable[Any]]:
    """Return an async wrapper around ``original`` that raises ``exc`` on
    its ``on_call``-th invocation (1-based) and delegates to ``original``
    for every other call. This is the fault-injection primitive used by
    every site test below -- always a REAL exception type, never a bare
    ``Exception()``, so a test can never pass by accident on an
    over-broad except clause."""
    state = {"n": 0}

    async def _wrapper(*args: Any, **kwargs: Any) -> Any:
        state["n"] += 1
        if state["n"] == on_call:
            raise exc
        return await original(*args, **kwargs)

    return _wrapper


def _start_supervised(
    reg: SessionRegistry, worker: SessionWorker, *, flush_timeout: float = 10.0
) -> asyncio.Task:
    """Mirror production ``start_drain`` (registry.py) EXACTLY: create the
    task, attach the done-callback, bind ``worker.task``. Needed because
    this file drives ``drain_worker`` directly (for injection control) the
    same way ``tests/test_large_event_tail_drop.py::_drive_drain_to_quiescence``
    does, and production's supervision is only real if the binding matches
    production's own ``start_drain``."""
    task = asyncio.create_task(
        reg.drain_worker(worker, flush_timeout=flush_timeout),
        name=f"drain-{worker.session_id}",
    )
    task.add_done_callback(functools.partial(reg._on_drain_done, worker))
    worker.task = task
    return task


async def _pump(n: int = 5) -> None:
    """Let ``n`` event-loop iterations pass -- long enough for a
    ``call_soon``-scheduled done-callback to actually run."""
    for _ in range(n):
        await asyncio.sleep(0)


async def _await_death(task: asyncio.Task) -> None:
    """Wait for ``task`` to finish (absorbing whatever it raised), then pump
    the loop so its done-callback has actually executed before we assert
    anything about its effects."""
    with contextlib.suppress(BaseException):
        await task
    await _pump()


async def _drain_until_idle(
    reg: SessionRegistry,
    qm: QueueManager,
    worker: SessionWorker,
    sid: str,
    *,
    max_polls: int = 400,
    poll_sleep: float = 0.01,
) -> asyncio.Task:
    """Poll (bounded, never a bare sleep as the only wait) until EITHER the
    task finishes on its own OR the queue is fully drained -- mirrors
    ``tests/test_large_event_tail_drop.py::_drive_drain_to_quiescence``."""
    task = worker.task
    assert task is not None
    for _ in range(max_polls):
        await asyncio.sleep(poll_sleep)
        if task.done():
            break
        if (await qm.read_batch(sid, 10)).lines == []:
            break
    return task


async def _cancel_and_await(task: asyncio.Task | None) -> None:
    assert task is not None
    if not task.done():
        task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


def _died_loudly(
    caplog: pytest.LogCaptureFixture, sid: str, exc_type: type[BaseException]
) -> logging.LogRecord:
    """Assert (a): a ``drain_worker_died`` ERROR was logged, carrying the
    session id (in both the message and ``extra``) and the injected
    exception's real type via ``exc_info``. Returns the matching record."""
    matches = [
        r
        for r in caplog.records
        if r.levelno == logging.ERROR
        and "drain_worker_died" in r.getMessage()
        and sid in r.getMessage()
        and getattr(r, "session_id", None) == sid
    ]
    assert matches, (
        f"expected a drain_worker_died ERROR with session_id={sid!r}; "
        f"caplog had: {[r.getMessage() for r in caplog.records]}"
    )
    rec = matches[0]
    assert rec.exc_info is not None, "drain_worker_died must carry exc_info"
    actual_type = rec.exc_info[0]
    assert actual_type is not None and issubclass(actual_type, exc_type), (
        f"expected exc_info type {exc_type}, got {actual_type}"
    )
    return rec


# ---------------------------------------------------------------------------
# Injection matrix: one test per unguarded failure site.
#
# Every test asserts:
#   (a) not silently dead   -- drain_worker_died ERROR w/ session id + exc_info
#   (b) store closed        -- worker.store_closed is True, fake.closed is True
#   (c) deregistered+respawn drains the exact remaining suffix, no gap/dup
#   (d) finalize re-runs to completion (terminal-path tests only)
#   (e) no second live drainer
# ---------------------------------------------------------------------------


class TestInjectionMatrix:
    async def test_read_batch_failure_is_supervised_and_respawns(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """qm.read_batch raises OSError(EIO) once."""
        reg = SessionRegistry()
        qm = reg.queue_manager
        sid = "d1-s1-read-batch"
        graph = _FlakyGraph()
        worker = _make_worker(sid, graph)
        reg._register_for_test(worker)

        qm.read_batch = _flaky(  # type: ignore[method-assign]
            qm.read_batch, OSError(errno.EIO, "Input/output error")
        )

        written_before = reg.pipeline_counters()["written_total"]

        with (
            patch(
                "context_intelligence_server.registry.process_event",
                side_effect=_accumulate,
            ),
            caplog.at_level(logging.ERROR, logger="context_intelligence_server"),
        ):
            await qm.append(sid, _line("e1", "/ws", {"session_id": sid}))
            task = _start_supervised(reg, worker)
            await _await_death(task)

            _died_loudly(caplog, sid, OSError)
            assert worker.store_closed is True
            assert graph.closed is True
            assert sid not in reg.active_sessions()

            # (c) respawn: a fresh worker over the SAME queue + SAME fake
            # (the fake models the accumulating write buffer; flushed is a
            # SET, so a replayed line can never show up twice).
            worker2 = _make_worker(sid, graph)
            reg._register_for_test(worker2)
            reg.start_drain(worker2)
            await _drain_until_idle(reg, qm, worker2, sid)
            await _cancel_and_await(worker2.task)

        assert graph.flushed == {"e1"}
        assert reg.pipeline_counters()["written_total"] == written_before + 1
        assert (await qm.read_batch(sid, 10)).lines == []
        assert worker2.task is not task

    async def test_commit_failure_is_supervised_and_respawns(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """qm.commit raises OSError(ESTALE) once. No duplicate node in
        fake.flushed after the replay (flushed is a set), and written_total
        counts the line exactly once."""
        reg = SessionRegistry()
        qm = reg.queue_manager
        sid = "d1-s2-commit"
        graph = _FlakyGraph()
        worker = _make_worker(sid, graph)
        reg._register_for_test(worker)

        qm.commit = _flaky(  # type: ignore[method-assign]
            qm.commit, OSError(errno.ESTALE, "Stale file handle")
        )

        written_before = reg.pipeline_counters()["written_total"]

        with (
            patch(
                "context_intelligence_server.registry.process_event",
                side_effect=_accumulate,
            ),
            caplog.at_level(logging.ERROR, logger="context_intelligence_server"),
        ):
            await qm.append(sid, _line("e1", "/ws", {"session_id": sid}))
            task = _start_supervised(reg, worker)
            await _await_death(task)

            _died_loudly(caplog, sid, OSError)
            assert worker.store_closed is True
            assert sid not in reg.active_sessions()

            worker2 = _make_worker(sid, graph)  # SAME fake -- dedup proof
            reg._register_for_test(worker2)
            reg.start_drain(worker2)
            await _drain_until_idle(reg, qm, worker2, sid)
            await _cancel_and_await(worker2.task)

        assert graph.flushed == {"e1"}, "no duplicate: flushed is a set"
        assert reg.pipeline_counters()["written_total"] == written_before + 1
        assert (await qm.read_batch(sid, 10)).lines == []

    async def test_dead_letter_failure_no_longer_kills_the_drainer(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """qm.dead_letter raises OSError(EIO) once, in the dead-letter
        except-clause. The poison line ends up in read_dead_letters and
        tail-1/tail-2 are persisted after respawn."""
        reg = SessionRegistry()
        qm = reg.queue_manager
        sid = "d1-s3-dead-letter"
        graph = _FlakyGraph(fail_when=lambda buf: "oversized" in buf)
        worker = _make_worker(sid, graph)
        reg._register_for_test(worker)

        qm.dead_letter = _flaky(  # type: ignore[method-assign]
            qm.dead_letter, OSError(errno.EIO, "disk unavailable")
        )

        with (
            patch(
                "context_intelligence_server.registry.process_event",
                side_effect=_accumulate,
            ),
            caplog.at_level(logging.ERROR, logger="context_intelligence_server"),
        ):
            await qm.append(sid, _line("small-1", "/ws", {"session_id": sid}))
            await qm.append(sid, _line("small-2", "/ws", {"session_id": sid}))
            await qm.append(sid, _line("oversized", "/ws", {"session_id": sid}))
            await qm.append(sid, _line("tail-1", "/ws", {"session_id": sid}))
            await qm.append(sid, _line("tail-2", "/ws", {"session_id": sid}))

            task = _start_supervised(reg, worker)
            await _await_death(task)

            _died_loudly(caplog, sid, OSError)
            assert worker.store_closed is True
            assert sid not in reg.active_sessions()

            worker2 = _make_worker(sid, graph)
            reg._register_for_test(worker2)
            reg.start_drain(worker2)
            await _drain_until_idle(reg, qm, worker2, sid)
            await _cancel_and_await(worker2.task)

        dead = await qm.read_dead_letters(sid)
        assert len(dead) == 1
        assert json.loads(dead[0]["payload"])["event"] == "oversized"
        assert graph.flushed == {"small-1", "small-2", "tail-1", "tail-2"}
        assert (await qm.read_batch(sid, 10)).lines == []

    async def test_isolation_commit_failure_is_supervised_and_respawns(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """qm.commit raises TimeoutError once on the first isolation-path
        commit call. No line is dead-lettered twice, none is lost."""
        reg = SessionRegistry()
        qm = reg.queue_manager
        sid = "d1-s4-isolation-commit"
        graph = _FlakyGraph()  # batch of 2 forces exhaustion -> isolation
        worker = _make_worker(sid, graph)
        reg._register_for_test(worker)

        qm.commit = _flaky(  # type: ignore[method-assign]
            qm.commit, TimeoutError("SMB operation timed out")
        )

        written_before = reg.pipeline_counters()["written_total"]

        with (
            patch(
                "context_intelligence_server.registry.process_event",
                side_effect=_accumulate,
            ),
            caplog.at_level(logging.ERROR, logger="context_intelligence_server"),
        ):
            await qm.append(sid, _line("g1", "/ws", {"session_id": sid}))
            await qm.append(sid, _line("g2", "/ws", {"session_id": sid}))

            task = _start_supervised(reg, worker)
            await _await_death(task)

            _died_loudly(caplog, sid, TimeoutError)
            assert worker.store_closed is True
            assert sid not in reg.active_sessions()

            worker2 = _make_worker(sid, graph)
            reg._register_for_test(worker2)
            reg.start_drain(worker2)
            await _drain_until_idle(reg, qm, worker2, sid)
            await _cancel_and_await(worker2.task)

        assert graph.flushed == {"g1", "g2"}
        assert reg.pipeline_counters()["written_total"] == written_before + 2
        dead = await qm.read_dead_letters(sid)
        assert dead == [], "neither line is poison -- nothing should be dead-lettered"
        assert (await qm.read_batch(sid, 10)).lines == []

    async def test_finalize_tail_read_failure_refinalizes_after_respawn(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """qm.read_batch raises OSError(ESTALE) once, on _finalize_session's
        own tail read (its second call). The terminal batch was already
        committed up to session:end, so finalize re-runs after respawn to
        full completion (assert (d))."""
        reg = SessionRegistry()
        qm = reg.queue_manager
        sid = "d1-s5-finalize-read"
        # A clean (always-succeeds) graph: this test targets read_batch, not
        # flush -- a batch-size-sensitive fake would force exhaustion on the
        # very first (2-record) batch and never reach the terminal batch's
        # commit at all.
        graph = _FlakyGraph(fail_when=lambda buf: False)
        worker = _make_worker(sid, graph)
        reg._register_for_test(worker)

        qm.read_batch = _flaky(  # type: ignore[method-assign]
            qm.read_batch, OSError(errno.ESTALE, "Stale file handle"), on_call=2
        )

        with (
            patch(
                "context_intelligence_server.registry.process_event",
                side_effect=_accumulate,
            ),
            caplog.at_level(logging.ERROR, logger="context_intelligence_server"),
        ):
            await qm.append(sid, _line("tool:pre", "/ws", {"session_id": sid}))
            await qm.append(sid, _line("session:end", "/ws", {"session_id": sid}))

            task = _start_supervised(reg, worker)
            await _await_death(task)

            _died_loudly(caplog, sid, OSError)
            assert worker.store_closed is True
            assert sid not in reg.active_sessions()

            worker2 = _make_worker(sid, graph)
            reg._register_for_test(worker2)
            reg.start_drain(worker2)
            assert worker2.task is not None
            await asyncio.wait_for(worker2.task, timeout=5.0)

        # (d) finalize re-ran to completion.
        assert len(reg.completed_sessions()) == 1
        assert graph.flushed == {"tool:pre", "session:end"}
        assert sid not in reg.active_sessions()

    async def test_finalize_tail_commit_failure_refinalizes_after_respawn(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """qm.commit raises OSError(ENOSPC) once, on _finalize_session's own
        tail commit (its second call, after the terminal batch's own
        up-to-session:end commit succeeds)."""
        reg = SessionRegistry()
        qm = reg.queue_manager
        sid = "d1-s6-finalize-commit"
        # Clean graph: this test targets commit, not flush -- same reasoning
        # as the previous test.
        graph = _FlakyGraph(fail_when=lambda buf: False)
        worker = _make_worker(sid, graph)
        reg._register_for_test(worker)

        qm.commit = _flaky(  # type: ignore[method-assign]
            qm.commit, OSError(errno.ENOSPC, "No space left on device"), on_call=2
        )

        with (
            patch(
                "context_intelligence_server.registry.process_event",
                side_effect=_accumulate,
            ),
            caplog.at_level(logging.ERROR, logger="context_intelligence_server"),
        ):
            await qm.append(sid, _line("tool:pre", "/ws", {"session_id": sid}))
            await qm.append(sid, _line("session:end", "/ws", {"session_id": sid}))

            task = _start_supervised(reg, worker)
            await _await_death(task)

            _died_loudly(caplog, sid, OSError)
            assert worker.store_closed is True
            assert sid not in reg.active_sessions()

            worker2 = _make_worker(sid, graph)
            reg._register_for_test(worker2)
            reg.start_drain(worker2)
            assert worker2.task is not None
            await asyncio.wait_for(worker2.task, timeout=5.0)

        assert len(reg.completed_sessions()) == 1
        assert graph.flushed == {"tool:pre", "session:end"}
        assert sid not in reg.active_sessions()

    async def test_delete_drained_failure_is_supervised_without_second_drainer(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """qm.delete_drained raises PermissionError once. CompletedSession
        was already recorded (appended before delete_drained runs); the
        callback deregisters + closes; no second drainer."""
        reg = SessionRegistry()
        qm = reg.queue_manager
        sid = "d1-s7-delete-drained"
        graph = _FlakyGraph()
        worker = _make_worker(sid, graph)
        reg._register_for_test(worker)

        qm.delete_drained = _flaky(  # type: ignore[method-assign]
            qm.delete_drained, PermissionError(errno.EACCES, "Permission denied")
        )

        with (
            patch(
                "context_intelligence_server.registry.process_event",
                side_effect=_accumulate,
            ),
            caplog.at_level(logging.ERROR, logger="context_intelligence_server"),
        ):
            await qm.append(sid, _line("session:end", "/ws", {"session_id": sid}))
            task = _start_supervised(reg, worker)
            await _await_death(task)

        _died_loudly(caplog, sid, PermissionError)
        assert worker.store_closed is True
        assert graph.closed is True
        assert sid not in reg.active_sessions()
        assert len(reg.completed_sessions()) == 1, (
            "CompletedSession is appended BEFORE delete_drained; it must "
            "survive delete_drained raising"
        )
        # (e): no second live drainer exists for this session.
        assert all(
            w.session_id != sid or (w.task is None or w.task.done())
            for w in reg.workers()
        )

    async def test_prologue_exception_is_supervised(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """setup_handlers raises before the while loop even starts. A later
        get_or_create-equivalent still builds a working worker."""
        reg = SessionRegistry()
        qm = reg.queue_manager
        sid = "d1-s8-prologue"
        graph = _FlakyGraph()
        worker = _make_worker(sid, graph)
        reg._register_for_test(worker)

        with (
            patch(
                "context_intelligence_server.registry.setup_handlers",
                side_effect=RuntimeError("handler wiring failed"),
            ),
            caplog.at_level(logging.ERROR, logger="context_intelligence_server"),
        ):
            task = _start_supervised(reg, worker)
            await _await_death(task)

        _died_loudly(caplog, sid, RuntimeError)
        assert worker.store_closed is True
        assert graph.closed is True
        assert sid not in reg.active_sessions()

        # A later worker (setup_handlers no longer patched) works normally.
        worker2 = _make_worker(sid, graph)
        reg._register_for_test(worker2)
        with patch(
            "context_intelligence_server.registry.process_event",
            side_effect=_accumulate,
        ):
            await qm.append(sid, _line("e1", "/ws", {"session_id": sid}))
            reg.start_drain(worker2)
            await _drain_until_idle(reg, qm, worker2, sid)
            await _cancel_and_await(worker2.task)

        assert graph.flushed == {"e1"}

    async def test_flush_failure_uses_real_neo4j_exception_types(self) -> None:
        """Not a crash scenario -- proves the inner retry path tolerates
        real neo4j driver exception types, not just a bare Exception."""
        reg = SessionRegistry()
        qm = reg.queue_manager
        sid = "d1-flush-real-exceptions"
        graph = _SequencedFlushGraph(
            [
                neo4j_exc.ServiceUnavailable("db unreachable"),
                neo4j_exc.TransientError("deadlock, retry"),
            ]
        )
        worker = _make_worker(sid, graph)
        reg._register_for_test(worker)

        with patch(
            "context_intelligence_server.registry.process_event",
            side_effect=_accumulate,
        ):
            await qm.append(sid, _line("e1", "/ws", {"session_id": sid}))
            reg.start_drain(worker)
            await _drain_until_idle(reg, qm, worker, sid)
            assert not worker.task.done(), (  # type: ignore[union-attr]
                "within-budget retries must not kill the task"
            )
            await _cancel_and_await(worker.task)  # type: ignore[arg-type]

        assert graph.flushed == {"e1"}
        assert reg.pipeline_counters()["write_retries_total"] >= 2
        assert (await qm.read_batch(sid, 10)).lines == []


# ---------------------------------------------------------------------------
# Mechanism-specific tests
# ---------------------------------------------------------------------------


class TestMechanismSpecific:
    async def test_committed_offset_freezes_AT_the_terminal_line(self) -> None:
        """After the terminal batch commits, the first pending
        record parses to session:end -- the offset is frozen AT the
        boundary, not past it. _finalize_session is stubbed out so we can
        inspect queue state before delete_drained would remove the log."""
        reg = SessionRegistry()
        qm = reg.queue_manager
        sid = "d1-committed-at-terminal"
        # Clean graph: this test is about the commit boundary, not flush
        # failure -- a batch-size-sensitive fake would force the 2-record
        # batch through poison isolation instead of a normal commit.
        graph = _FlakyGraph(fail_when=lambda buf: False)
        worker = _make_worker(sid, graph)
        reg._register_for_test(worker)

        with (
            patch(
                "context_intelligence_server.registry.process_event",
                side_effect=_accumulate,
            ),
            patch.object(reg, "_finalize_session", autospec=True) as mock_finalize,
        ):
            mock_finalize.return_value = None
            await qm.append(sid, _line("tool:pre", "/ws", {"session_id": sid}))
            await qm.append(sid, _line("session:end", "/ws", {"session_id": sid}))
            task = _start_supervised(reg, worker)
            await asyncio.wait_for(task, timeout=5.0)

        mock_finalize.assert_awaited_once()
        pending = await qm.read_batch(sid, 10)
        assert len(pending.records) == 1
        event, _ws, _data = reg._parse_line(pending.records[0].raw)
        assert event == "session:end"

    async def test_recover_reports_a_terminal_but_unfinalized_session(self) -> None:
        """recover() reports a session frozen at its terminal line as
        recoverable (committed < complete_data_end)."""
        reg = SessionRegistry()
        qm = reg.queue_manager
        sid = "d1-recover-terminal-unfinalized"
        graph = _FlakyGraph(fail_when=lambda buf: False)
        worker = _make_worker(sid, graph)
        reg._register_for_test(worker)

        with (
            patch(
                "context_intelligence_server.registry.process_event",
                side_effect=_accumulate,
            ),
            patch.object(reg, "_finalize_session", autospec=True) as mock_finalize,
        ):
            mock_finalize.return_value = None
            await qm.append(sid, _line("tool:pre", "/ws", {"session_id": sid}))
            await qm.append(sid, _line("session:end", "/ws", {"session_id": sid}))
            task = _start_supervised(reg, worker)
            await asyncio.wait_for(task, timeout=5.0)

        recoverable = await qm.recover()
        assert sid in recoverable

    async def test_finalize_reruns_to_completion_after_a_transient_finalize_failure(
        self,
    ) -> None:
        """After a transient finalize-tail read failure and a respawn, the
        session is fully finalized -- CompletedSession recorded,
        delete_drained ran, every line persisted exactly once."""
        reg = SessionRegistry()
        qm = reg.queue_manager
        sid = "d1-finalize-full-completion"
        graph = _FlakyGraph(fail_when=lambda buf: False)
        worker = _make_worker(sid, graph)
        reg._register_for_test(worker)

        qm.read_batch = _flaky(  # type: ignore[method-assign]
            qm.read_batch, OSError(errno.ESTALE, "Stale file handle"), on_call=2
        )

        with patch(
            "context_intelligence_server.registry.process_event",
            side_effect=_accumulate,
        ):
            await qm.append(sid, _line("tool:pre", "/ws", {"session_id": sid}))
            await qm.append(sid, _line("session:end", "/ws", {"session_id": sid}))
            task = _start_supervised(reg, worker)
            await _await_death(task)

            worker2 = _make_worker(sid, graph)
            reg._register_for_test(worker2)
            reg.start_drain(worker2)
            assert worker2.task is not None
            await asyncio.wait_for(worker2.task, timeout=5.0)

        assert len(reg.completed_sessions()) == 1
        assert graph.flushed == {"tool:pre", "session:end"}
        assert not qm._log_path(sid).exists(), "delete_drained must have run"
        assert not qm._offset_path(sid).exists()

    async def test_no_second_drainer_during_the_finalize_window(self) -> None:
        """While qm.delete_drained is parked, get_or_create must be a no-op
        (no new task, worker.task unchanged); finalization then completes."""
        reg = SessionRegistry()
        qm = reg.queue_manager
        sid = "d1-no-second-drainer"
        graph = _FlakyGraph()
        worker = _make_worker(sid, graph)
        reg._register_for_test(worker)

        entered_delete = asyncio.Event()
        release = asyncio.Event()
        original_delete = qm.delete_drained

        async def _parked_delete(session_id: str) -> bool:
            entered_delete.set()
            await release.wait()
            return await original_delete(session_id)

        qm.delete_drained = _parked_delete  # type: ignore[method-assign]

        with patch(
            "context_intelligence_server.registry.process_event",
            side_effect=_accumulate,
        ):
            await qm.append(sid, _line("session:end", "/ws", {"session_id": sid}))
            task = _start_supervised(reg, worker)

            await asyncio.wait_for(entered_delete.wait(), timeout=5.0)
            assert sid in reg.active_sessions(), (
                "worker must still be registered while delete_drained is parked "
                "(_deregister is the LAST act of finalization)"
            )

            pre_task = worker.task
            reg.get_or_create(sid, "/ws")
            assert worker.task is pre_task, (
                "a concurrent get_or_create during the finalize window must be "
                "a no-op: the live task is not done() yet"
            )

            release.set()
            await asyncio.wait_for(task, timeout=5.0)

        assert len(reg.completed_sessions()) == 1
        assert sid not in reg.active_sessions()


# ---------------------------------------------------------------------------
# Spent-worker guard: a cancelled worker must never be revived
# ---------------------------------------------------------------------------


class TestSpentWorkerGuard:
    async def test_cancelled_worker_is_not_revived_through_a_closed_store(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """task.cancel() -- drain_worker swallows CancelledError and returns
        cleanly, worker.store_closed is True, no drain_worker_died ERROR,
        and start_drain(worker) creates no new task: the same worker object
        must never be revived once its store is closed."""
        reg = SessionRegistry()
        qm = reg.queue_manager
        sid = "d1-cancelled-not-revived"
        graph = _FlakyGraph()
        worker = _make_worker(sid, graph)
        reg._register_for_test(worker)

        with (
            patch(
                "context_intelligence_server.registry.process_event",
                side_effect=_accumulate,
            ),
            caplog.at_level(logging.ERROR, logger="context_intelligence_server"),
        ):
            await qm.append(sid, _line("e1", "/ws", {"session_id": sid}))
            task = _start_supervised(reg, worker)
            await asyncio.sleep(0)  # let it actually start running
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            await _pump()

        assert task.cancelled() is False, (
            "drain_worker catches CancelledError and returns -- the task "
            "carries a clean result, not a cancellation"
        )
        assert graph.closed is True
        assert worker.store_closed is True
        assert sid not in reg.active_sessions(), (
            "the cancellation handler must deregister, or this worker "
            "is wedged (found by get_or_create, refused by start_drain, "
            "forever)"
        )
        assert not any("drain_worker_died" in r.getMessage() for r in caplog.records), (
            "a clean cancellation is not a crash -- no ERROR expected"
        )

        pre_task = worker.task
        reg.start_drain(worker)  # attempt to revive the SAME spent worker
        assert worker.task is pre_task, (
            "start_drain must refuse to revive a store_closed worker"
        )


class TestPoisonLineIsolation:
    async def test_poison_line_is_dead_lettered_and_advances_without_dying(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A merged/unparseable line is dead-lettered via the isolation
        path, the offset advances past it, the task stays alive, and no
        drain_worker_died fires."""
        reg = SessionRegistry()
        qm = reg.queue_manager
        sid = "d1-poison-isolated"
        graph = _FlakyGraph()
        worker = _make_worker(sid, graph)
        reg._register_for_test(worker)

        with (
            patch(
                "context_intelligence_server.registry.process_event",
                side_effect=_accumulate,
            ),
            caplog.at_level(logging.ERROR, logger="context_intelligence_server"),
        ):
            await qm.append(sid, _line("good-1", "/ws", {"session_id": sid}))
            await qm.append(sid, b"{ this is not valid json")
            await qm.append(sid, _line("good-2", "/ws", {"session_id": sid}))

            task = _start_supervised(reg, worker)
            await _drain_until_idle(reg, qm, worker, sid)
            assert not task.done(), "the drainer must stay alive after isolation"
            await _cancel_and_await(task)

        dead = await qm.read_dead_letters(sid)
        assert len(dead) == 1
        assert graph.flushed == {"good-1", "good-2"}
        assert not any("drain_worker_died" in r.getMessage() for r in caplog.records)


class TestNoDoubleCountOnReplay:
    async def test_no_double_count_on_replay(self) -> None:
        """Force the isolation-path commit to fail once. written_total after
        the replay equals the number of distinct committed lines."""
        reg = SessionRegistry()
        qm = reg.queue_manager
        sid = "d1-no-double-count"
        graph = _FlakyGraph()
        worker = _make_worker(sid, graph)
        reg._register_for_test(worker)

        qm.commit = _flaky(  # type: ignore[method-assign]
            qm.commit, TimeoutError("SMB operation timed out")
        )

        written_before = reg.pipeline_counters()["written_total"]

        with patch(
            "context_intelligence_server.registry.process_event",
            side_effect=_accumulate,
        ):
            await qm.append(sid, _line("g1", "/ws", {"session_id": sid}))
            await qm.append(sid, _line("g2", "/ws", {"session_id": sid}))

            task = _start_supervised(reg, worker)
            await _await_death(task)

            worker2 = _make_worker(sid, graph)
            reg._register_for_test(worker2)
            reg.start_drain(worker2)
            await _drain_until_idle(reg, qm, worker2, sid)
            await _cancel_and_await(worker2.task)

        assert reg.pipeline_counters()["written_total"] == written_before + 2, (
            "each of the 2 distinct lines must be counted exactly once"
        )


class TestCloseTaskReferenced:
    async def test_close_task_is_referenced_until_it_completes(self) -> None:
        """registry._close_tasks holds the fire-and-forget close task while
        it is pending, and is empty once it finishes -- without this,
        asyncio's weak reference could let it be garbage-collected mid-close."""
        reg = SessionRegistry()
        qm = reg.queue_manager
        sid = "d1-close-task-referenced"
        graph = _FlakyGraph()
        worker = _make_worker(sid, graph)
        reg._register_for_test(worker)

        close_started = asyncio.Event()
        close_release = asyncio.Event()

        async def _slow_close() -> None:
            close_started.set()
            await close_release.wait()
            graph.closed = True

        graph.close = _slow_close  # type: ignore[method-assign]

        qm.read_batch = _flaky(  # type: ignore[method-assign]
            qm.read_batch, OSError(errno.EIO, "boom")
        )

        with patch(
            "context_intelligence_server.registry.process_event",
            side_effect=_accumulate,
        ):
            await qm.append(sid, _line("e1", "/ws", {"session_id": sid}))
            task = _start_supervised(reg, worker)
            with contextlib.suppress(BaseException):
                await task

            await asyncio.wait_for(close_started.wait(), timeout=5.0)
            assert len(reg._close_tasks) == 1, (
                "the close task must be referenced while pending"
            )

            close_release.set()
            for _ in range(200):
                await asyncio.sleep(0.005)
                if not reg._close_tasks:
                    break

        assert reg._close_tasks == set()
        assert graph.closed is True


class TestTerminalBatchFlushExhaustion:
    async def test_terminal_batch_flush_exhaustion_loses_finalization(self) -> None:
        """Known limitation: force the terminal batch itself through
        _handle_exhausted_batch (flush always fails). The committed offset
        ends up past session:end, no CompletedSession is recorded, and
        recover() does not report the session."""
        reg = SessionRegistry()
        qm = reg.queue_manager
        sid = "d1-terminal-exhaustion"
        graph = _FlakyGraph(fail_when=lambda buf: True)  # flush ALWAYS fails
        worker = _make_worker(sid, graph)
        reg._register_for_test(worker)

        with patch(
            "context_intelligence_server.registry.process_event",
            side_effect=_accumulate,
        ):
            await qm.append(sid, _line("session:end", "/ws", {"session_id": sid}))
            task = _start_supervised(reg, worker)
            await _drain_until_idle(reg, qm, worker, sid)
            if not task.done():
                await _cancel_and_await(task)

        # Committed PAST session:end (the whole, only, line): EOF.
        assert (await qm.read_batch(sid, 10)).lines == []
        assert reg.completed_sessions() == []
        recoverable = await qm.recover()
        assert sid not in recoverable
        dead = await qm.read_dead_letters(sid)
        assert len(dead) == 1, (
            "session:end itself is dead-lettered (flush never succeeds)"
        )
