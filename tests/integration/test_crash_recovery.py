"""Integration — durable no-loss across a simulated crash, and the
commit-never-advances-over-undurable-data invariant (panel finding #2)."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

from context_intelligence_server import registry as registry_module
from context_intelligence_server.queue_manager import QueueManager
from context_intelligence_server.registry import SessionRegistry, SessionWorker
from context_intelligence_server.services import HookStateService


def _line(event: str, ws: str, data: dict) -> bytes:
    return json.dumps({"event": event, "workspace": ws, "data": data}).encode("utf-8")


async def test_no_loss_after_crash_mid_drain() -> None:
    settings = (
        registry_module.get_settings()
    )  # queues_path patched to tmp_path by safe_settings
    sid = "crash-sess"
    qm = QueueManager(queues_dir=Path(settings.queues_path))

    K = 5
    for i in range(K):
        await qm.append(sid, _line(f"e{i}", "/ws", {"session_id": sid}))

    reg1 = SessionRegistry()
    processed_first: list[str] = []

    async def _proc1(w: object, event: str, data: object, h: object) -> None:
        processed_first.append(event)
        await asyncio.sleep(0.01)  # slow enough to be interrupted mid-drain

    w1 = SessionWorker(
        session_id=sid, workspace="/ws", services=HookStateService(workspace="/ws")
    )
    w1.services.graph.flush = AsyncMock()  # type: ignore[method-assign]
    w1.services.graph.close = AsyncMock()  # type: ignore[method-assign]
    reg1._register_for_test(w1)
    with patch(
        "context_intelligence_server.registry.process_event", side_effect=_proc1
    ):
        t1 = asyncio.create_task(reg1.drain_worker(w1, flush_timeout=10.0))
        await asyncio.sleep(0.015)  # let it start, not finish
        t1.cancel()
        try:
            await t1
        except asyncio.CancelledError:
            pass

    # Fresh registry over the SAME dir: recover + drain to completion.
    reg2 = SessionRegistry()
    processed_second: list[str] = []

    async def _proc2(w: object, event: str, data: object, h: object) -> None:
        processed_second.append(event)

    recovered = await reg2.queue_manager.recover()
    assert sid in recovered

    w2 = SessionWorker(
        session_id=sid, workspace="/ws", services=HookStateService(workspace="/ws")
    )
    w2.services.graph.flush = AsyncMock()  # type: ignore[method-assign]
    w2.services.graph.close = AsyncMock()  # type: ignore[method-assign]
    reg2._register_for_test(w2)
    with patch(
        "context_intelligence_server.registry.process_event", side_effect=_proc2
    ):
        t2 = asyncio.create_task(reg2.drain_worker(w2, flush_timeout=10.0))
        for _ in range(300):
            await asyncio.sleep(0.01)
            if (await reg2.queue_manager.read_batch(sid, 10)).lines == []:
                break
        t2.cancel()
        try:
            await t2
        except asyncio.CancelledError:
            pass

    all_seen = set(processed_first) | set(processed_second)
    assert {f"e{i}" for i in range(K)} <= all_seen  # no loss
    assert (await reg2.queue_manager.read_batch(sid, 10)).lines == []  # offset == EOF


async def test_offset_never_advances_over_undurable_data() -> None:
    """Panel finding #2: a flush that FAILS does not advance the offset; a
    later successful flush re-persists the same data. Relies on neo4j_store's
    buffer-restore-on-failure (neo4j_store.py:686-696)."""
    settings = registry_module.get_settings()
    sid = "flush-fail-sess"
    qm = QueueManager(queues_dir=Path(settings.queues_path))
    await qm.append(sid, _line("e0", "/ws", {"session_id": sid}))

    reg = SessionRegistry()
    flush_calls = {"n": 0}

    async def _flush() -> None:
        flush_calls["n"] += 1
        if flush_calls["n"] == 1:
            raise RuntimeError("DeadlockDetected")  # first flush fails

    w = SessionWorker(
        session_id=sid, workspace="/ws", services=HookStateService(workspace="/ws")
    )
    w.services.graph.flush = AsyncMock(side_effect=_flush)  # type: ignore[method-assign]
    w.services.graph.close = AsyncMock()  # type: ignore[method-assign]
    reg._register_for_test(w)

    with patch(
        "context_intelligence_server.registry.process_event", new_callable=AsyncMock
    ):
        t = asyncio.create_task(reg.drain_worker(w, flush_timeout=10.0))
        # After the first (failed) flush, the offset must NOT have advanced.
        await asyncio.sleep(0.05)
        assert (await qm.read_batch(sid, 10)).lines != []  # still pending
        # The retry (2nd flush) succeeds; the line is then committed.
        for _ in range(200):
            await asyncio.sleep(0.01)
            if (await qm.read_batch(sid, 10)).lines == []:
                break
        t.cancel()
        try:
            await t
        except asyncio.CancelledError:
            pass

    assert flush_calls["n"] >= 2  # re-flushed after failure
    assert (await qm.read_batch(sid, 10)).lines == []  # eventually committed
