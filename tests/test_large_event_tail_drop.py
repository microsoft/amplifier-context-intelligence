"""Tail-drop under load: POST /events durably appends and returns before any
Neo4j write; a per-session `drain_worker` task later flushes. An unguarded
`dead_letter` call inside `_handle_exhausted_batch`'s except-clause can
raise past `drain_worker`'s only guard (`asyncio.CancelledError` only),
killing the task and stranding the uncommitted tail on disk. `start_drain`'s
done-callback makes that death loud (`drain_worker_died`) and self-healing:
it deregisters the worker so the next event or a boot recovery respawns a
fresh drainer that resumes at the stranded tail. Large (>1MB) events reach
this more often since their own slow solo transaction is more likely to
exhaust the retry budget. No real Neo4j is required.
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
    """Encode an appended event line exactly as POST /events stores it."""
    return json.dumps({"event": event, "workspace": workspace, "data": data}).encode(
        "utf-8"
    )


class _FaultInjectableGraph:
    """Models a real store's accumulating write buffer: writes accumulate;
    `flush()` fails while the designated poison event is resident (modeling
    a Neo4j write rejection on an oversized event's own solo transaction); a
    successful flush clears the buffer; `discard_buffer()` clears it without
    flushing, so poison residue can't contaminate the next line."""

    def __init__(self, poison_event: str) -> None:
        self.workspace = "/ws"
        self.poison_event = poison_event
        self.buffer: set[str] = set()
        self.flushed: list[str] = []
        self.discards = 0
        self.closed = False

    async def flush(self) -> None:
        if not self.buffer:
            return  # empty-buffer early return, mirroring the real store
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
    """Start the real drain_worker as a background task and poll (never a
    bare sleep) until either it finishes on its own (defect: an unguarded
    exception kills it) or the queue drains (control: idle-polls forever,
    must be cancelled). Attaches the same done-callback `start_drain` does,
    so Case B can observe the real loud-death + self-heal contract."""
    task = asyncio.create_task(
        reg.drain_worker(worker, flush_timeout=flush_timeout), name=f"drain-{sid}"
    )
    # mirror start_drain's own bindings so a later supervision check is meaningful
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
# Case A -- CONTROL: the designed resilience path. Proves isolation +
# dead-letter isolates an oversized line while the prefix and tail persist.
# ---------------------------------------------------------------------------


class TestOversizedEventControlPathIsolatesAndContinues:
    async def test_prefix_and_tail_persist_oversized_dead_lettered(self) -> None:
        """Batch [small-1, small-2, OVERSIZED, tail-1, tail-2] against a store
        whose flush() only rejects the oversized event: retries exhaust,
        driving isolation with dead_letter and commit both healthy. All four
        small events persist, the oversized event is dead-lettered (not
        dropped), the offset advances past all 5 lines, and the drain task
        stays healthy -- contrasts directly with Case B below.
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

        # --- pin: offset advanced past the whole batch ---
        assert (await qm.read_batch(sid, 10)).lines == []

        # --- pin: the drain task is healthy -- no unhandled exception ---
        assert task.cancelled() or task.exception() is None


# ---------------------------------------------------------------------------
# Case B -- DEFECT: same enqueue shape, but the unguarded seam inside
# _handle_exhausted_batch's except-clause (dead_letter) raises, reproducing
# the silent tail-drop symptom.
# ---------------------------------------------------------------------------


class TestOversizedEventDefectSilentlyDropsTail:
    async def test_tail_after_oversized_event_is_silently_dropped(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Same batch shape as Case A, but `dead_letter` itself raises while
        isolating the oversized line, escaping past drain_worker's only
        guard and killing the task. The prefix persists, the tail (oversized
        + 2 events) is stranded uncommitted on disk, and the task dies with
        an exception. The done-callback then logs it loud, closes the store,
        and deregisters the worker so a respawn drains the exact stranded
        suffix with no gap or duplicate.
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

        # the injected defect: dead_letter raises inside the unguarded
        # except-clause seam; saved so it can be restored before the respawn
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
            # fire-and-forget accept semantics: every event is durably
            # appended before the drain loop ever touches Neo4j
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
            # let the done-callback (scheduled via call_soon) run before asserting
            for _ in range(5):
                await asyncio.sleep(0)

        # the small prefix persisted
        assert fake.flushed == ["small-1", "small-2"], (
            "expected exactly the prefix before the oversized event to have "
            "flushed via the per-line isolation path"
        )

        # the tail after the oversized event did not persist
        assert "tail-1" not in fake.flushed
        assert "tail-2" not in fake.flushed

        # committed offset stuck at the prefix boundary; the tail bytes
        # remain durably on disk, uncommitted
        remaining = await qm.read_batch(sid, 10)
        remaining_events = [json.loads(raw)["event"] for raw in remaining.lines]
        assert remaining_events == ["oversized", "tail-1", "tail-2"], (
            "the tail must remain stranded, uncommitted, on disk -- this is "
            "the silent data-loss window this defect produces (bytes are not "
            "gone, but nothing will ever drain them without a fix)"
        )

        # the drain task is done() with a silent, unretrieved exception
        assert task.done()
        assert not task.cancelled()
        exc = task.exception()
        assert exc is not None, (
            "the drain task must have died from an unhandled exception, "
            "escaping past drain_worker's sole guard (asyncio.CancelledError "
            "only)"
        )
        assert isinstance(exc, OSError)

        # the oversized event was never dead-lettered by the crashed task
        # either -- resolved once the respawn below runs
        dead = await qm.read_dead_letters(sid)
        assert dead == []

        # --- the death is loud and self-healing ---

        # drain_worker_died was logged at ERROR with this session's id
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

        # the store was closed by the callback -- no driver leak
        assert worker.store_closed is True
        assert fake.closed is True

        # the worker was deregistered -- a fresh get_or_create can revive it
        assert sid not in reg.active_sessions(), (
            "the crashed worker must be deregistered by _on_drain_done, or "
            "it is a spent worker (store_closed) that start_drain would "
            "refuse forever"
        )

        # a fresh respawn drains the exact stranded suffix with no gap or
        # duplicate: oversized is still genuinely poison, but dead_letter is
        # restored to the real implementation, so this time it succeeds
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
            # route to the respawned worker's own fresh graph, not the crashed one's
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
