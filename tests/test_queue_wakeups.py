"""Focused queue-owned append-wakeup tests."""

from __future__ import annotations

import asyncio
import threading
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from context_intelligence_server.queue_manager import QueueManager
from context_intelligence_server.registry import (
    LiveFirstFlushGate,
    SessionRegistry,
    SessionWorker,
)
from context_intelligence_server.services import HookStateService


@pytest.fixture
def qm(tmp_path) -> QueueManager:
    return QueueManager(tmp_path / "queues")


async def test_append_between_clear_and_read_is_not_lost(qm: QueueManager) -> None:
    """An append before the read is found by that read without waiting."""
    with qm.append_waiter("session") as waiter:
        waiter.clear_before_read()
        await qm.append("session", b"first")
        batch = await qm.read_batch("session", max_items=1)

    assert batch.lines == [b"first"]


async def test_append_between_empty_read_and_wait_is_not_lost(qm: QueueManager) -> None:
    """A generation change makes a post-read append visible without a timeout."""
    with qm.append_waiter("session") as waiter:
        generation = waiter.clear_before_read()
        assert not (await qm.read_batch("session", max_items=1)).records
        await qm.append("session", b"first")
        assert await waiter.wait_for_change(generation, timeout=60.0)


async def test_failed_append_after_write_wakes_waiter(
    qm: QueueManager, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed append still wakes, because its partial-line repair may be readable."""
    original = qm._write_record

    def write_then_fail(guard, path, line) -> None:
        original(guard, path, line)
        raise OSError("simulated post-write failure")

    monkeypatch.setattr(qm, "_write_record", write_then_fail)

    with qm.append_waiter("session") as waiter:
        generation = waiter.clear_before_read()
        with pytest.raises(OSError, match="post-write"):
            await qm.append("session", b"first")
        assert await waiter.wait_for_change(generation, timeout=0.1)

    assert (await qm.read_batch("session", max_items=1)).lines == [b"first"]


async def test_cancelled_append_after_write_wakes_waiter(
    qm: QueueManager, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cancellation is delivered after the settled write has notified waiters."""
    original = qm._write_record
    wrote = threading.Event()
    release = threading.Event()

    def write_then_block(guard, path, line) -> None:
        original(guard, path, line)
        wrote.set()
        assert release.wait(timeout=3.0)

    monkeypatch.setattr(qm, "_write_record", write_then_block)

    with qm.append_waiter("session") as waiter:
        generation = waiter.clear_before_read()
        append = asyncio.create_task(qm.append("session", b"first"))
        await asyncio.wait_for(asyncio.to_thread(wrote.wait), timeout=1.0)
        append.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await append
        assert await waiter.wait_for_change(generation, timeout=0.1)

    assert (await qm.read_batch("session", max_items=1)).lines == [b"first"]


async def test_trim_defers_guard_removal_until_waiter_releases_and_late_append_reuses_it(
    qm: QueueManager,
) -> None:
    """A held waiter prevents ABA guard replacement across trim and late append."""
    await qm.append("session", b"committed")
    batch = await qm.read_batch("session", max_items=1)
    await qm.commit("session", batch.end_offset)
    original_guard = qm._guards["session"]

    with qm.append_waiter("session"):
        assert await qm.delete_drained("session")
        assert qm._guards["session"] is original_guard
        await qm.append("session", b"late")
        assert qm._guards["session"] is original_guard

    late = await qm.read_batch("session", max_items=1)
    assert late.lines == [b"late"]
    await qm.commit("session", late.end_offset)
    assert await qm.delete_drained("session")
    assert "session" not in qm._guards


@pytest.mark.parametrize("delete_method", ["delete_drained", "delete_session"])
async def test_cancelled_settled_delete_defers_removal_until_last_holder_releases(
    qm: QueueManager, monkeypatch: pytest.MonkeyPatch, delete_method: str
) -> None:
    """A cancellation after unlink cannot strand a guard in the map."""
    await qm.append("session", b"committed")
    batch = await qm.read_batch("session", max_items=1)
    await qm.commit("session", batch.end_offset)
    guard = qm._guards["session"]
    offset = qm._offset_path("session")
    unlinked_offset = threading.Event()
    release_delete = threading.Event()
    original_unlink = Path.unlink

    def unlink_then_park(path: Path, *args, **kwargs) -> None:
        original_unlink(path, *args, **kwargs)
        if path == offset:
            unlinked_offset.set()
            assert release_delete.wait(timeout=3.0)

    monkeypatch.setattr(Path, "unlink", unlink_then_park)

    with qm.append_waiter("session"):
        delete = asyncio.create_task(getattr(qm, delete_method)("session"))
        await asyncio.wait_for(asyncio.to_thread(unlinked_offset.wait), timeout=1.0)
        delete.cancel()
        release_delete.set()
        with pytest.raises(asyncio.CancelledError):
            await delete
        assert qm._guards["session"] is guard
        assert guard.deferred_removal

    assert "session" not in qm._guards


async def test_retained_delete_never_marks_guard_for_removal(qm: QueueManager) -> None:
    """A non-drained log keeps its guard after all temporary holders leave."""
    await qm.append("session", b"pending")
    guard = qm._guards["session"]

    with qm.append_waiter("session"):
        assert not await qm.delete_drained("session")
        assert not guard.deferred_removal

    assert qm._guards["session"] is guard


def _worker(session_id: str) -> SessionWorker:
    worker = SessionWorker(
        session_id=session_id,
        workspace="/workspace",
        services=HookStateService(workspace="/workspace"),
    )
    worker.services.graph.flush = AsyncMock()  # type: ignore[method-assign]
    return worker


async def test_idle_workers_read_once_then_append_wakes_before_maintenance(
    qm: QueueManager,
) -> None:
    """Idle workers wait on their own queue instead of repeatedly reading files."""
    registry = SessionRegistry()
    registry._queue_manager = qm
    registry._write_semaphore = asyncio.Semaphore(1)
    registry._max_delivery_attempts = 5
    workers = [_worker(f"session-{index}") for index in range(3)]
    for worker in workers:
        registry._register_for_test(worker)

    original_read_batch = qm.read_batch
    reads_by_session: dict[str, int] = {}
    initial_reads = asyncio.Event()
    second_read = asyncio.Event()
    processed = asyncio.Event()
    processed_sessions: list[str] = []

    async def counted_read_batch(session_id: str, max_items: int):
        reads_by_session[session_id] = reads_by_session.get(session_id, 0) + 1
        if len(reads_by_session) == len(workers):
            initial_reads.set()
        if reads_by_session[session_id] == 2:
            second_read.set()
        return await original_read_batch(session_id, max_items)

    async def capture_process(worker: SessionWorker, *_args, **_kwargs) -> None:
        processed_sessions.append(worker.session_id)
        processed.set()

    qm.read_batch = counted_read_batch  # type: ignore[method-assign]
    with patch(
        "context_intelligence_server.registry.process_event",
        side_effect=capture_process,
    ):
        tasks = [
            asyncio.create_task(registry.drain_worker(worker, flush_timeout=60.0))
            for worker in workers
        ]
        try:
            await asyncio.wait_for(initial_reads.wait(), timeout=1.0)
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(second_read.wait(), timeout=0.2)

            await qm.append(
                "session-1",
                b'{"event":"tool:pre","workspace":"/workspace","data":{}}',
            )
            # The worker has already been started; append wakes it rather than
            # waiting for its 60-second maintenance deadline.
            await asyncio.wait_for(processed.wait(), timeout=1.0)
            assert processed_sessions == ["session-1"]
            assert reads_by_session["session-0"] == reads_by_session["session-2"] == 1
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.parametrize("is_recovery", [False, True])
async def test_empty_read_to_wait_race_wakes_live_and_recovery_drainers(
    qm: QueueManager, is_recovery: bool
) -> None:
    """An append after an empty read but before wait is never missed."""
    registry = SessionRegistry()
    registry._queue_manager = qm
    registry._write_semaphore = asyncio.Semaphore(1)
    registry._max_delivery_attempts = 5
    registry.is_recovery = is_recovery
    if is_recovery:
        registry.drain_batch_size = 1
        registry.flush_gate = LiveFirstFlushGate(1)
    worker = _worker("session")
    registry._register_for_test(worker)
    seen: list[str] = []
    empty_read = asyncio.Event()
    processed = asyncio.Event()
    original_read_batch = qm.read_batch
    injected = False

    async def capture_process(_worker, event, _data, _handlers, **_kwargs) -> None:
        seen.append(event)
        processed.set()

    async def append_after_empty_read(session_id: str, max_items: int):
        nonlocal injected
        batch = await original_read_batch(session_id, max_items)
        if not injected and not batch.records:
            injected = True
            empty_read.set()
            await qm.append(
                session_id,
                b'{"event":"raced","workspace":"/workspace","data":{}}',
            )
        return batch

    qm.read_batch = append_after_empty_read  # type: ignore[method-assign]
    with patch(
        "context_intelligence_server.registry.process_event",
        side_effect=capture_process,
    ):
        task = asyncio.create_task(registry.drain_worker(worker, flush_timeout=60.0))
        try:
            await asyncio.wait_for(empty_read.wait(), timeout=1.0)
            await asyncio.wait_for(processed.wait(), timeout=1.0)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    assert seen == ["raced"]
