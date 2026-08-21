"""Reproduction / characterization test for a silent tail-drop under load.

Root cause:

- POST /events is fire-and-forget: it durably appends to a per-session
  on-disk log and returns 202 BEFORE any Neo4j write (main.py:875-919).
  Persistence happens later in a per-session asyncio.Task ``drain_worker``
  (registry.py). PRE-FIX, that task was spawned by ``start_drain`` with no
  await, no done-callback, and no supervisor restart, so any unexpected
  exception killed it SILENTLY.

- POST-FIX: ``start_drain`` now attaches a
  done-callback (``_on_drain_done``) to every drain task. An unexpected
  exception no longer dies silently -- it propagates out of ``drain_worker``
  (unchanged control flow otherwise) to that callback, which logs an ERROR
  (``drain_worker_died``, with the session id and traceback), closes the
  graph store, and deregisters the worker so the next event (or a boot
  ``recover()``) respawns it. This file's Case B test below is exactly the
  scenario that callback exists for: it STILL reproduces the underlying
  fault (the unguarded ``dead_letter`` seam inside
  ``_handle_exhausted_batch``), but now proves the death is LOUD and
  self-healing rather than silent and terminal.

- ``drain_worker``'s outer ``while True:`` catches ONLY
  ``asyncio.CancelledError``. The inner ``try/except Exception`` retries a
  failed batch flush up to ``_max_delivery_attempts`` (default 5,
  config.py:811) times, then calls ``_handle_exhausted_batch``. Inside that
  method, the per-record ``qm.dead_letter`` call and the unconditional
  per-record ``qm.commit`` sit OUTSIDE the guard that isolates
  dispatch/flush failures. If ``dead_letter`` raises a plain ``Exception``
  while handling a poison record's flush failure, that new exception
  propagates out of the ``except`` block, out of ``_handle_exhausted_batch``,
  out of the ONLY try/except in ``drain_worker`` that could have swallowed
  it (the outer one only catches ``asyncio.CancelledError``) -- killing the
  drain task (loudly, post-fix; silently, pre-fix).

- Offsets only advance after a successful commit, so a mid-stream fatal
  escape leaves the persisted set a clean ordered PREFIX and the TAIL is
  never persisted, never dead-lettered, and never retried by THIS task --
  it sits on disk, uncommitted, while the client already received 202 for
  every event (main.py's accept path is fully decoupled from this failure).
  Post-fix, the task's done-callback deregisters the worker so the NEXT
  event (or a boot ``recover()``) respawns a fresh drainer that resumes
  exactly at the stranded tail -- see the strengthened Case B assertions
  below.

- Large (>1MB) events are where this is reached in production:
  one-row-floor chunking (neo4j_store.py:924-982) gives an oversized event
  its own slow solo transaction, which is more likely to exhaust the
  ``_max_delivery_attempts`` retry budget and drive the batch into this
  unguarded exhausted-batch region.

This file pins BOTH the designed resilience path (Case A, CONTROL) and the
actual defect (Case B, DEFECT) by driving the REAL ``drain_worker`` loop
(not a reimplementation) against a real, tmp_path-backed ``QueueManager``,
with only the graph store's ``flush``/``discard_buffer``/``close`` and (in
Case B) the queue manager's ``dead_letter`` replaced by test doubles. No
real Neo4j is required.
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import json
import logging
from unittest.mock import AsyncMock, patch

import pytest

from context_intelligence_server.queue_manager import QueueManager
from context_intelligence_server.registry import SessionRegistry, SessionWorker
from context_intelligence_server.services import HookStateService

pytestmark = pytest.mark.integration


def _line(event: str, workspace: str, data: dict) -> bytes:
    """Encode an appended event line exactly as POST /events stores it.

    Mirrors ``tests/test_registry.py::_line`` -- the on-disk wire format the
    durable queue actually persists (queue_manager.py's ``append``/
    ``read_batch`` treat the log as opaque bytes; the JSON shape is imposed
    by ``registry._parse_line``, registry.py:424-427).
    """
    return json.dumps({"event": event, "workspace": workspace, "data": data}).encode(
        "utf-8"
    )


class _FaultInjectableGraph:
    """FAITHFUL model of a real store's accumulating write buffer.

    Mirrors ``tests/test_registry.py::_AccumBufferGraph``: writes ACCUMULATE
    in a buffer; ``flush()`` fails while the designated "oversized" event is
    RESIDENT in that buffer (modeling a Neo4j write rejection/timeout on an
    oversized event's own solo transaction -- neo4j_store.py:924-982's
    one-row-floor chunking is what gives a large event that isolated, slower
    transaction in production); a SUCCESSFUL flush clears the buffer;
    ``discard_buffer()`` clears it without flushing (the COE-blocker
    mechanism at registry.py:463 and :483 that prevents poison residue from
    contaminating the next line).
    """

    def __init__(self, poison_event: str) -> None:
        self.workspace = "/ws"
        self.poison_event = poison_event
        self.buffer: set[str] = set()
        self.flushed: list[str] = []
        self.discards = 0
        self.closed = False

    async def flush(self) -> None:
        if not self.buffer:
            return  # empty-buffer early return (mirrors neo4j_store.py:656-657)
        if self.poison_event in self.buffer:
            raise RuntimeError(
                f"neo4j write rejected for {self.poison_event!r} "
                "(oversized solo transaction exhausted retries)"
            )
        self.flushed.extend(sorted(self.buffer))
        self.buffer.clear()  # success clears

    def discard_buffer(self) -> None:
        self.buffer.clear()
        self.discards += 1

    async def close(self) -> None:
        self.closed = True


async def _drive_drain_to_quiescence(
    reg: SessionRegistry,
    qm: QueueManager,
    worker: SessionWorker,
    sid: str,
    *,
    flush_timeout: float = 10.0,
    max_polls: int = 400,
    poll_sleep: float = 0.01,
) -> asyncio.Task:
    """Start the REAL drain_worker as a background task and poll (never a
    bare sleep) until EITHER the task finishes on its own (the defect case:
    an unguarded exception kills it) OR the on-disk queue is fully drained
    (the control case: it keeps idle-polling forever and must be cancelled
    by the caller). Bounded by max_polls * poll_sleep, never unbounded.

    Supervision binding: production's ``start_drain`` attaches a
    done-callback (``_on_drain_done``) to every drain task, alongside
    binding ``worker.task``. This helper attaches it too, so Case B below
    can observe the real contract (loud death + self-heal) rather than a
    silent kill.
    """
    task = asyncio.create_task(
        reg.drain_worker(worker, flush_timeout=flush_timeout), name=f"drain-{sid}"
    )
    # Mirror start_drain EXACTLY: bind worker.task AND attach the
    # done-callback. We build the task manually here (rather than calling
    # start_drain itself) so the caller controls flush_timeout, but the
    # binding must match production so a later "did anything supervise /
    # replace this task" check is meaningful.
    task.add_done_callback(functools.partial(reg._on_drain_done, worker))
    worker.task = task
    for _ in range(max_polls):
        await asyncio.sleep(poll_sleep)
        if task.done():
            break
        if (await qm.read_batch(sid, 10)).lines == []:
            break
    return task


async def _cancel_and_await(task: asyncio.Task) -> None:
    if not task.done():
        task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


# ---------------------------------------------------------------------------
# Case A -- CONTROL: the designed resilience path. Proves the isolation +
# dead-letter mechanism WORKS when the unguarded seam (dead_letter/commit in
# _handle_exhausted_batch) does not itself fail: the oversized line is
# isolated and dead-lettered, and BOTH the small prefix before it AND the
# small tail after it persist. This is "the flow being handled" as designed.
# ---------------------------------------------------------------------------


class TestOversizedEventControlPathIsolatesAndContinues:
    async def test_prefix_and_tail_persist_oversized_dead_lettered(self) -> None:
        """Batch [small-1, small-2, OVERSIZED, tail-1, tail-2] against a store
        whose flush() only rejects the oversized event: normal-path flush
        fails and retries exhaust (registry.py:361-391), driving isolation
        (_handle_exhausted_batch, registry.py:444-485) with dead_letter and
        commit both healthy. Expected (the INTENDED behavior):

        - small-1, small-2, tail-1, tail-2 all persist (fake.flushed).
        - the oversized event is dead-lettered, not silently dropped.
        - the committed offset advances past ALL 5 lines (no stranded tail).
        - the drain task is still healthy (no unhandled exception) --
          contrasts directly with Case B below.
        """
        reg = SessionRegistry()
        qm = reg.queue_manager
        sid = "large-event-control"

        fake = _FaultInjectableGraph(poison_event="oversized")
        worker = SessionWorker(
            session_id=sid,
            workspace="/ws",
            services=HookStateService(workspace="/ws"),
        )
        worker.services.graph = fake  # type: ignore[assignment]
        reg._register_for_test(worker)

        async def _process(w: object, event: str, data: object, h: object) -> None:
            fake.buffer.add(event)

        with patch(
            "context_intelligence_server.registry.process_event", side_effect=_process
        ):
            await qm.append(sid, _line("small-1", "/ws", {"session_id": sid}))
            await qm.append(sid, _line("small-2", "/ws", {"session_id": sid}))
            await qm.append(sid, _line("oversized", "/ws", {"session_id": sid}))
            await qm.append(sid, _line("tail-1", "/ws", {"session_id": sid}))
            await qm.append(sid, _line("tail-2", "/ws", {"session_id": sid}))

            task = await _drive_drain_to_quiescence(reg, qm, worker, sid)
            await _cancel_and_await(task)

        # --- pin: full prefix AND tail persisted (no drop, no truncation) ---
        assert fake.flushed == ["small-1", "small-2", "tail-1", "tail-2"]

        # --- pin: oversized event isolated + dead-lettered, not lost ---
        dead = await qm.read_dead_letters(sid)
        assert len(dead) == 1
        assert json.loads(dead[0]["payload"])["event"] == "oversized"

        # --- pin: offset advanced past the WHOLE batch (registry.py:484) ---
        assert (await qm.read_batch(sid, 10)).lines == []

        # --- pin: the drain task is healthy -- no unhandled exception ---
        assert task.cancelled() or task.exception() is None


# ---------------------------------------------------------------------------
# Case B -- DEFECT: silent tail-drop reproduction. Same enqueue shape, but
# the UNGUARDED seam inside _handle_exhausted_batch's except-clause
# (queue_manager.dead_letter) raises while handling the
# oversized line's flush failure. Pins the exact silent-tail-drop symptom.
# ---------------------------------------------------------------------------


class TestOversizedEventDefectSilentlyDropsTail:
    async def test_tail_after_oversized_event_is_silently_dropped(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Same batch shape as Case A: [small-1, small-2, OVERSIZED, tail-1,
        tail-2]. This time ``queue_manager.dead_letter`` raises when the
        isolation loop tries to dead-letter the oversized line after its
        flush fails. Because that raise happens INSIDE the
        ``except Exception as exc:`` clause, it is not caught by that same
        try -- it escapes ``_handle_exhausted_batch`` entirely, past the
        ONLY guard in ``drain_worker`` that pre-dates this fix (which catches
        solely ``asyncio.CancelledError``) -- killing the drain task exactly
        per the original silent-tail-drop root cause.

        Reproduced symptom -- items 1-5 are UNCHANGED by this fix, byte-identical injection:

        1. The small PREFIX (small-1, small-2) persisted -- isolation
           committed past them one at a time before reaching the oversized
           line.
        2. The TAIL (tail-1, tail-2) after the oversized event did NOT
           persist during THIS task's run -- clean truncation, not
           corruption.
        3. The committed offset is stuck exactly at the prefix boundary
           (2 lines committed) while the tail bytes remain durably on disk
           in the ``.log`` (queue_manager never deletes/rewrites it).
        4. ``worker.task`` is ``done()`` with an exception attached.

        The fix STRENGTHENS what happens next -- this
        is the new post-fix contract, proven here rather than asserted:

        6. The death is LOUD: a ``drain_worker_died`` ERROR is logged with
           this session's id (previously: nothing -- "no supervisor exists"
           is no longer true, ``_on_drain_done`` is that supervisor).
        7. ``worker.store_closed`` is True -- the graph store was closed by
           the callback, so the driver is not leaked.
        8. The worker is DEREGISTERED (``sid not in reg.active_sessions()``)
           -- a fresh ``get_or_create`` builds a NEW worker rather than being
           refused by the spent-worker guard.
        9. That fresh respawn drains the EXACT stranded suffix --
           oversized (now dead-lettered successfully), tail-1, tail-2 -- with
           no gap and no duplicate. The "fourth state" (item 5 below) is
           only ever true of the CRASHED task's own run; it does not survive
           the respawn.
        5. The oversized event was never dead-lettered by the CRASHED task
           either (the dead_letter call itself is what failed on that run)
           -- it is not accounted for anywhere BY THAT TASK: not persisted,
           not queued, not dead. This is the "fourth state" the design
           docstring explicitly says must never happen -- and per point 9,
           it is resolved by the respawn, not left standing.
        """
        reg = SessionRegistry()
        qm = reg.queue_manager
        sid = "large-event-defect"

        fake = _FaultInjectableGraph(poison_event="oversized")
        worker = SessionWorker(
            session_id=sid,
            workspace="/ws",
            services=HookStateService(workspace="/ws"),
        )
        worker.services.graph = fake  # type: ignore[assignment]
        reg._register_for_test(worker)

        # THE INJECTED DEFECT: the unguarded seam inside
        # _handle_exhausted_batch's except-clause (queue_manager.dead_letter). A real
        # I/O failure while writing the dead-letter record (disk full,
        # transient FS error under the write pressure a large-payload retry
        # storm creates) raises here, escaping the only guard that could
        # have caught a plain Exception. Saved so it can be RESTORED before
        # the respawn (point 9 below) -- the injected fault models a
        # TRANSIENT failure on this one attempt, not a permanently broken
        # dead_letter primitive.
        _real_dead_letter = qm.dead_letter
        qm.dead_letter = AsyncMock(  # type: ignore[method-assign]
            side_effect=OSError("disk unavailable while writing dead-letter record")
        )

        async def _process(w: object, event: str, data: object, h: object) -> None:
            fake.buffer.add(event)

        with (
            patch(
                "context_intelligence_server.registry.process_event",
                side_effect=_process,
            ),
            caplog.at_level(logging.ERROR, logger="context_intelligence_server"),
        ):
            # Fire-and-forget accept semantics: every event is durably
            # appended (this IS the 202 the client would have received --
            # main.py:875-919 returns before any Neo4j write happens) before
            # the drain loop ever touches Neo4j.
            await qm.append(sid, _line("small-1", "/ws", {"session_id": sid}))
            await qm.append(sid, _line("small-2", "/ws", {"session_id": sid}))
            await qm.append(sid, _line("oversized", "/ws", {"session_id": sid}))
            await qm.append(sid, _line("tail-1", "/ws", {"session_id": sid}))
            await qm.append(sid, _line("tail-2", "/ws", {"session_id": sid}))
            assert len((await qm.read_batch(sid, 10)).lines) == 5, (
                "all 5 events durably accepted before any Neo4j write is attempted"
            )

            task = await _drive_drain_to_quiescence(reg, qm, worker, sid)
            # Do NOT cancel: the defect kills the task on its own. If the
            # task were somehow still alive, that itself falsifies the
            # reproduction, so make that failure explicit rather than
            # masking it with an unconditional cancel.
            if not task.done():
                await _cancel_and_await(task)
                pytest.fail(
                    "drain task did not die on its own -- the injected "
                    "dead_letter failure did not escape the loop as the "
                    "root-cause analysis predicted; investigate before "
                    "trusting this reproduction"
                )
            # Let the done-callback (scheduled via call_soon when the task
            # completed) actually run before asserting its effects.
            for _ in range(5):
                await asyncio.sleep(0)

        # (1) The small PREFIX persisted.
        assert fake.flushed == ["small-1", "small-2"], (
            "expected exactly the prefix before the oversized event to have "
            "flushed via the per-line isolation path"
        )

        # (2) The TAIL after the oversized event did NOT persist.
        assert "tail-1" not in fake.flushed
        assert "tail-2" not in fake.flushed

        # (3) Committed offset stuck at the prefix boundary; the tail bytes
        # (oversized + tail-1 + tail-2) remain durably on disk, uncommitted.
        remaining = await qm.read_batch(sid, 10)
        remaining_events = [json.loads(raw)["event"] for raw in remaining.lines]
        assert remaining_events == ["oversized", "tail-1", "tail-2"], (
            "the tail must remain stranded, uncommitted, on disk -- this is "
            "the silent data-loss window this defect produces (bytes are not "
            "gone, but nothing will ever drain them without a fix)"
        )

        # (4) The drain task is done() with a silent, unretrieved exception
        # -- and nothing restarted it (start_drain has no supervisor,
        # registry.py:543-547; a truly silent kill in production would never
        # have this .exception() call made against it at all).
        assert task.done()
        assert not task.cancelled()
        exc = task.exception()
        assert exc is not None, (
            "the drain task must have died from an unhandled exception, "
            "escaping past drain_worker's sole guard (asyncio.CancelledError "
            "only, registry.py:419)"
        )
        assert isinstance(exc, OSError)

        # (5) The oversized event was never dead-lettered BY THE CRASHED
        # TASK either (the dead_letter call is exactly what failed on that
        # run) -- unaccounted for by that task: not persisted, not
        # dead-lettered. (Point 9 below shows the respawn resolves this.)
        dead = await qm.read_dead_letters(sid)
        assert dead == []

        # --- STRENGTHENING: the death is loud and self-healing ---

        # (6) drain_worker_died was logged at ERROR with this session's id.
        died_records = [
            r
            for r in caplog.records
            if r.levelno == logging.ERROR
            and "drain_worker_died" in r.getMessage()
            and getattr(r, "session_id", None) == sid
        ]
        assert died_records, (
            "expected a drain_worker_died ERROR with session_id="
            f"{sid!r} -- start_drain's done-callback (_on_drain_done) is "
            "the supervisor that makes this death loud instead of silent"
        )
        assert died_records[0].exc_info is not None
        assert died_records[0].exc_info[0] is OSError

        # (7) The store was closed by the callback -- no driver leak.
        assert worker.store_closed is True
        assert fake.closed is True

        # (8) The worker was deregistered -- it is no longer "the" worker
        # for this session; a fresh get_or_create-equivalent can revive it.
        assert sid not in reg.active_sessions(), (
            "the crashed worker must be deregistered by _on_drain_done, or "
            "it is a spent worker (store_closed) that start_drain would "
            "refuse forever"
        )

        # (9) A fresh respawn (register_for_test + start_drain, mirroring
        # what get_or_create's else-branch now does) drains
        # the EXACT stranded suffix with no gap and no duplicate: oversized
        # is STILL genuinely poison (its own solo flush always rejects it,
        # same as Case A), but dead_letter is RESTORED to the real
        # implementation -- the injected fault was transient, not a
        # permanently broken dead_letter primitive -- so this time it
        # succeeds, isolates oversized, and tail-1/tail-2 persist normally.
        qm.dead_letter = _real_dead_letter  # type: ignore[method-assign]
        fake2 = _FaultInjectableGraph(poison_event="oversized")
        worker2 = SessionWorker(
            session_id=sid,
            workspace="/ws",
            services=HookStateService(workspace="/ws"),
        )
        worker2.services.graph = fake2  # type: ignore[assignment]
        reg._register_for_test(worker2)

        async def _process2(
            w: SessionWorker, event: str, data: object, h: object
        ) -> None:
            # Route to the RESPAWNED worker's own graph, not the crashed
            # worker's -- unlike `_process` above (which only ever needed
            # one fake for the whole test), the respawn uses a fresh store.
            fake2.buffer.add(event)

        with patch(
            "context_intelligence_server.registry.process_event",
            side_effect=_process2,
        ):
            task2 = await _drive_drain_to_quiescence(reg, qm, worker2, sid)
            await _cancel_and_await(task2)

        assert fake2.flushed == ["tail-1", "tail-2"]
        dead2 = await qm.read_dead_letters(sid)
        assert len(dead2) == 1
        assert json.loads(dead2[0]["payload"])["event"] == "oversized"
        assert (await qm.read_batch(sid, 10)).lines == []
        assert task2 is not task
