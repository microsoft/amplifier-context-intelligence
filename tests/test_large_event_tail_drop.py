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


# The failure-mode counterpart (dead_letter raising mid-drain -> loud task
# death, respawn drains the stranded tail, oversized event dead-lettered) is
# proven by test_drain_supervision.py::
# test_dead_letter_failure_no_longer_kills_the_drainer over this same event
# shape; not duplicated here.
