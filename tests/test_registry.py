"""Tests for SessionRegistry and SessionWorker."""

import asyncio
import dataclasses
import time
from collections import deque
from pathlib import Path
from unittest.mock import ANY, AsyncMock, patch

import pytest

from context_intelligence_server.blob_store import AsyncDiskBlobStore
from context_intelligence_server.config import get_settings
from context_intelligence_server.registry import (
    CompletedSession,
    SessionRegistry,
    SessionWorker,
)
from context_intelligence_server.services import HookStateService


# ---------------------------------------------------------------------------
# Factory helper
# ---------------------------------------------------------------------------


def make_completed(
    session_id: str = "session-x",
    workspace: str = "/workspace/test",
    started_at: float | None = None,
    ended_at: float | None = None,
    events_processed: int = 5,
    error_count: int = 0,
    duration_seconds: float = 1.0,
) -> CompletedSession:
    """Factory for CompletedSession test instances."""
    now = time.time()
    return CompletedSession(
        session_id=session_id,
        workspace=workspace,
        started_at=started_at if started_at is not None else now - duration_seconds,
        ended_at=ended_at if ended_at is not None else now,
        events_processed=events_processed,
        error_count=error_count,
        duration_seconds=duration_seconds,
    )


# ---------------------------------------------------------------------------
# TestCompletedSession
# ---------------------------------------------------------------------------


class TestCompletedSession:
    def test_completed_session_is_dataclass(self) -> None:
        """CompletedSession must be a dataclass with all 7 required fields."""
        assert dataclasses.is_dataclass(CompletedSession)
        field_names = {f.name for f in dataclasses.fields(CompletedSession)}
        expected = {
            "session_id",
            "workspace",
            "started_at",
            "ended_at",
            "events_processed",
            "error_count",
            "duration_seconds",
        }
        assert expected == field_names

    def test_registry_has_completed_deque(self) -> None:
        """SessionRegistry._completed is a deque with maxlen=100."""
        reg = SessionRegistry()
        assert hasattr(reg, "_completed")
        assert isinstance(reg._completed, deque)
        assert reg._completed.maxlen == 100

    def test_completed_sessions_returns_list(self) -> None:
        """completed_sessions() returns a list (not a deque)."""
        reg = SessionRegistry()
        result = reg.completed_sessions()
        assert isinstance(result, list)

    def test_completed_ring_overflow(self) -> None:
        """Ring buffer retains at most 100 entries; oldest entry is evicted at 101."""
        reg = SessionRegistry()

        # Fill with 101 entries; first entry has session_id "session-0"
        for i in range(101):
            reg._completed.append(make_completed(session_id=f"session-{i}"))

        sessions = reg.completed_sessions()
        assert len(sessions) == 100
        # Oldest entry (session-0) must have been evicted
        ids = [s.session_id for s in sessions]
        assert "session-0" not in ids
        assert "session-1" in ids
        assert "session-100" in ids

    def test_session_worker_has_started_at_field(self) -> None:
        """SessionWorker must have a started_at field defaulting to time.time()."""
        field_names = {f.name for f in dataclasses.fields(SessionWorker)}
        assert "started_at" in field_names
        before = time.time()
        worker = SessionWorker(
            session_id="test",
            workspace="/ws",
            services=HookStateService(workspace="/ws"),
        )
        after = time.time()
        assert before <= worker.started_at <= after

    def test_session_worker_has_error_count_field(self) -> None:
        """SessionWorker must have an error_count field defaulting to 0."""
        field_names = {f.name for f in dataclasses.fields(SessionWorker)}
        assert "error_count" in field_names
        worker = SessionWorker(
            session_id="test",
            workspace="/ws",
            services=HookStateService(workspace="/ws"),
        )
        assert worker.error_count == 0


@pytest.fixture
def registry() -> SessionRegistry:
    return SessionRegistry()


@pytest.mark.asyncio
async def test_get_or_create_new_worker(registry: SessionRegistry) -> None:
    worker = registry.get_or_create("session-1", "/workspace/a")

    assert isinstance(worker, SessionWorker)
    assert worker.session_id == "session-1"
    assert worker.workspace == "/workspace/a"
    assert isinstance(worker.queue, asyncio.Queue)
    assert worker.queue.empty()
    assert isinstance(worker.task, asyncio.Task)


@pytest.mark.asyncio
async def test_get_or_create_returns_same_worker(registry: SessionRegistry) -> None:
    worker_a = registry.get_or_create("session-1", "/workspace/a")
    worker_b = registry.get_or_create("session-1", "/workspace/a")

    assert worker_a is worker_b


@pytest.mark.asyncio
async def test_active_count(registry: SessionRegistry) -> None:
    assert registry.active_count() == 0

    registry.get_or_create("session-1", "/workspace/a")
    assert registry.active_count() == 1

    registry.get_or_create("session-2", "/workspace/b")
    assert registry.active_count() == 2


@pytest.mark.asyncio
async def test_active_sessions(registry: SessionRegistry) -> None:
    registry.get_or_create("session-b", "/workspace/b")
    registry.get_or_create("session-a", "/workspace/a")

    sessions = registry.active_sessions()
    assert sorted(sessions) == sessions
    assert sessions == ["session-a", "session-b"]


@pytest.mark.asyncio
async def test_remove(registry: SessionRegistry) -> None:
    registry.get_or_create("session-1", "/workspace/a")
    assert registry.active_count() == 1

    registry.remove("session-1")
    assert registry.active_count() == 0


@pytest.mark.asyncio
async def test_remove_nonexistent_is_noop(registry: SessionRegistry) -> None:
    # Should not raise any exception
    registry.remove("nonexistent-session")
    assert registry.active_count() == 0


@pytest.mark.asyncio
async def test_queue_put_get(registry: SessionRegistry) -> None:
    worker = registry.get_or_create("session-1", "/workspace/a")

    event = ("tool_call", {"tool": "bash", "result": "ok"})
    await worker.queue.put(event)

    # get() on a non-empty queue returns without yielding — drain task does not interpose
    retrieved = await worker.queue.get()
    assert retrieved == event


class TestSessionWorkerHasServices:
    def test_worker_has_services_attribute(self) -> None:
        field_names = {f.name for f in dataclasses.fields(SessionWorker)}
        assert "services" in field_names

    @pytest.mark.asyncio
    async def test_worker_services_is_hook_state_service(
        self, registry: SessionRegistry
    ) -> None:
        worker = registry.get_or_create("session-1", "/workspace/test")

        assert hasattr(worker, "services")
        assert isinstance(worker.services, HookStateService)

    @pytest.mark.asyncio
    async def test_worker_services_graph_workspace_matches_passed_workspace(
        self,
        registry: SessionRegistry,
    ) -> None:
        workspace = "/workspace/my-project"
        worker = registry.get_or_create("session-1", workspace)

        assert worker.services.graph.workspace == workspace

    @pytest.mark.asyncio
    # confirms workspace field added in this task (complements test_get_or_create_new_worker)
    async def test_worker_has_workspace_attribute(
        self, registry: SessionRegistry
    ) -> None:
        workspace = "/workspace/foo"
        worker = registry.get_or_create("session-1", workspace)

        assert worker.workspace == workspace

    @pytest.mark.asyncio
    async def test_worker_services_blob_store_is_async_disk_blob_store(
        self, registry: SessionRegistry
    ) -> None:
        """worker.services.blob_store is an AsyncDiskBlobStore instance."""
        worker = registry.get_or_create("session-1", "/workspace/test")

        assert isinstance(worker.services.blob_store, AsyncDiskBlobStore)

    @pytest.mark.asyncio
    async def test_worker_services_blob_store_root_matches_settings_blob_path(
        self, registry: SessionRegistry
    ) -> None:
        """worker.services.blob_store._root matches Settings.blob_path."""
        worker = registry.get_or_create("session-1", "/workspace/test")

        settings = get_settings()
        blob_store = worker.services.blob_store
        assert isinstance(blob_store, AsyncDiskBlobStore)
        assert blob_store._root == Path(settings.blob_path)


class TestDrainLoopCallsProcessEvent:
    """drain_worker calls process_event for each dequeued item."""

    @pytest.mark.asyncio
    async def test_queued_event_is_processed(self) -> None:
        """process_event is called with (worker, event, data, handlers) when an event is enqueued."""
        reg = SessionRegistry()
        worker = SessionWorker(
            session_id="test-session",
            workspace="/workspace/test",
            services=HookStateService(workspace="/workspace/test"),
        )

        event = "tool_call"
        workspace = "/workspace/test"
        data: dict[str, object] = {"session_id": "test-session", "tool": "bash"}

        with patch(
            "context_intelligence_server.registry.process_event",
            new_callable=AsyncMock,
        ) as mock_process:
            task = asyncio.create_task(reg.drain_worker(worker, flush_timeout=10.0))

            # Enqueue the event tuple
            await worker.queue.put((event, workspace, data))

            # Yield control so the drain loop can process the item
            await asyncio.sleep(0.05)

            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

            mock_process.assert_called_once_with(worker, event, data, ANY)

    @pytest.mark.asyncio
    async def test_drain_worker_is_method_on_registry(self) -> None:
        """drain_worker must be an instance method on SessionRegistry."""
        import inspect

        assert hasattr(SessionRegistry, "drain_worker")
        assert inspect.iscoroutinefunction(SessionRegistry.drain_worker)

    @pytest.mark.asyncio
    async def test_shutdown_flush_on_cancelled_error(self) -> None:
        """graph.close is called when the task is cancelled (shutdown close)."""
        reg = SessionRegistry()
        worker = SessionWorker(
            session_id="test-session",
            workspace="/workspace/test",
            services=HookStateService(workspace="/workspace/test"),
        )
        worker.services.graph.close = AsyncMock()  # type: ignore[method-assign]

        task = asyncio.create_task(reg.drain_worker(worker, flush_timeout=10.0))
        await asyncio.sleep(0.02)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        worker.services.graph.close.assert_awaited_once()


class TestPeriodicFlush:
    """drain_worker calls graph.flush periodically when no events arrive."""

    @pytest.mark.asyncio
    async def test_timeout_triggers_flush(self) -> None:
        """graph.flush is called after flush_timeout elapses with no events."""
        reg = SessionRegistry()
        worker = SessionWorker(
            session_id="test-session",
            workspace="/workspace/test",
            services=HookStateService(workspace="/workspace/test"),
        )
        worker.services.graph.flush = AsyncMock()  # type: ignore[method-assign]

        # Very short timeout so the flush fires quickly
        task = asyncio.create_task(reg.drain_worker(worker, flush_timeout=0.05))

        # Wait longer than the timeout to ensure at least one flush cycle fires
        await asyncio.sleep(0.2)

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        assert worker.services.graph.flush.call_count >= 1

    @pytest.mark.asyncio
    async def test_flush_timeout_default_is_30s(self) -> None:
        """drain_worker default flush_timeout is 30 seconds."""
        import inspect

        sig = inspect.signature(SessionRegistry.drain_worker)
        assert sig.parameters["flush_timeout"].default == 30.0

    @pytest.mark.asyncio
    async def test_flush_exception_does_not_kill_drain_worker(self) -> None:
        """An exception from graph.flush must not kill the drain worker coroutine.

        If flush() raises (e.g. OSError("disk full")), the drain loop should
        catch the exception, log it, and continue processing events.  The
        session:end event enqueued after the failing flush must still be
        consumed and the task must complete normally.
        """
        reg = SessionRegistry()
        worker = SessionWorker(
            session_id="test-session",
            workspace="/workspace/test",
            services=HookStateService(workspace="/workspace/test"),
        )
        # First flush call raises, second succeeds
        worker.services.graph.flush = AsyncMock(  # type: ignore[method-assign]
            side_effect=[OSError("disk full"), None]
        )
        worker.services.graph.close = AsyncMock()  # type: ignore[method-assign]

        with patch(
            "context_intelligence_server.registry.process_event",
            new_callable=AsyncMock,
        ):
            task = asyncio.create_task(reg.drain_worker(worker, flush_timeout=0.05))

            # Wait long enough for the flush timeout cycle to fire (the raising one)
            await asyncio.sleep(0.15)

            # Enqueue session:end — if the drain loop survived it will process this
            await worker.queue.put(
                ("session:end", "/workspace/test", {"session_id": "test-session"})
            )

            # If the drain loop was killed by the OSError, wait_for will timeout
            try:
                await asyncio.wait_for(task, timeout=2.0)
            except asyncio.TimeoutError:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                pytest.fail(
                    "drain_worker was killed by the OSError raised from flush(); "
                    "the exception must be caught inside the TimeoutError handler."
                )


class TestWorkerActivityTracking:
    """SessionWorker tracks activity: last_event, last_event_time, events_processed."""

    def test_worker_tracking_fields_initialized(self) -> None:
        """New SessionWorker has zeroed tracking fields."""
        worker = SessionWorker(
            session_id="test-session",
            workspace="/workspace/test",
            services=HookStateService(workspace="/workspace/test"),
        )

        assert worker.last_event == ""
        assert worker.last_event_time == 0.0
        assert worker.events_processed == 0

    @pytest.mark.asyncio
    async def test_worker_tracking_updated_after_drain(self) -> None:
        """Fields are updated after drain_worker processes an event."""
        reg = SessionRegistry()
        worker = SessionWorker(
            session_id="test-session",
            workspace="/workspace/test",
            services=HookStateService(workspace="/workspace/test"),
        )

        event = "tool_call"
        workspace = "/workspace/test"
        data: dict[str, object] = {"session_id": "test-session", "tool": "bash"}

        with patch(
            "context_intelligence_server.registry.process_event",
            new_callable=AsyncMock,
        ):
            task = asyncio.create_task(reg.drain_worker(worker, flush_timeout=10.0))

            # Enqueue the event tuple
            await worker.queue.put((event, workspace, data))

            # Yield control so the drain loop can process the item
            await asyncio.sleep(0.05)

            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        assert worker.last_event == event
        assert worker.last_event_time > 0.0
        assert worker.events_processed == 1

    @pytest.mark.asyncio
    async def test_worker_events_processed_increments(self) -> None:
        """events_processed counter increments once per event."""
        reg = SessionRegistry()
        worker = SessionWorker(
            session_id="test-session",
            workspace="/workspace/test",
            services=HookStateService(workspace="/workspace/test"),
        )

        event = "tool_call"
        workspace = "/workspace/test"
        data: dict[str, object] = {"session_id": "test-session", "tool": "bash"}

        with patch(
            "context_intelligence_server.registry.process_event",
            new_callable=AsyncMock,
        ):
            task = asyncio.create_task(reg.drain_worker(worker, flush_timeout=10.0))

            # Enqueue three events
            await worker.queue.put((event, workspace, data))
            await worker.queue.put((event, workspace, data))
            await worker.queue.put((event, workspace, data))

            # Yield control so the drain loop can process all items
            await asyncio.sleep(0.1)

            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        assert worker.events_processed == 3


class TestRingBufferEmission:
    """drain_worker emits an EventRecord to ring_buffer after each processed event."""

    @pytest.mark.asyncio
    async def test_ring_buffer_receives_record_after_event(self) -> None:
        """After drain processes an event, ring_buffer contains an EventRecord
        with the correct event, session_id, workspace, and result='ok'."""
        from context_intelligence_server.dashboard import EventRingBuffer

        reg = SessionRegistry()
        worker = SessionWorker(
            session_id="test-session",
            workspace="/workspace/test",
            services=HookStateService(workspace="/workspace/test"),
        )

        event = "tool_call"
        workspace = "/workspace/test"
        data: dict[str, object] = {"session_id": "test-session", "tool": "bash"}

        fresh_buffer = EventRingBuffer()

        with (
            patch(
                "context_intelligence_server.registry.process_event",
                new_callable=AsyncMock,
            ),
            patch(
                "context_intelligence_server.registry.ring_buffer",
                fresh_buffer,
            ),
        ):
            task = asyncio.create_task(reg.drain_worker(worker, flush_timeout=10.0))

            await worker.queue.put((event, workspace, data))
            await asyncio.sleep(0.05)

            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        records = fresh_buffer.recent()
        assert len(records) == 1
        record = records[0]
        assert record.event == event
        assert record.session_id == "test-session"
        assert record.workspace == "/workspace/test"
        assert record.result == "ok"


class TestDeregister:
    """_deregister removes the worker WITHOUT cancelling its asyncio task."""

    @pytest.mark.asyncio
    async def test_deregister_removes_from_workers(
        self, registry: SessionRegistry
    ) -> None:
        """_deregister removes session from _workers; active_count goes to 0."""
        worker = registry.get_or_create("session-1", "/workspace/a")
        task = worker.task
        assert registry.active_count() == 1

        registry._deregister("session-1")
        assert registry.active_count() == 0

        # Cleanup orphaned task
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    @pytest.mark.asyncio
    async def test_deregister_does_not_cancel_task(
        self, registry: SessionRegistry
    ) -> None:
        """Task is still running after _deregister (not cancelled)."""
        worker = registry.get_or_create("session-1", "/workspace/a")
        task = worker.task
        assert task is not None

        registry._deregister("session-1")

        # Task should still be running — not done, no cancel request issued
        assert not task.done()
        assert task.cancelling() == 0

        # Cleanup: cancel the orphaned task
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    @pytest.mark.asyncio
    async def test_deregister_nonexistent_is_noop(
        self, registry: SessionRegistry
    ) -> None:
        """_deregister on a nonexistent session_id does not raise."""
        # Should not raise
        registry._deregister("nonexistent-session")
        assert registry.active_count() == 0

    @pytest.mark.asyncio
    async def test_remove_cancels_task_but_deregister_does_not(
        self, registry: SessionRegistry
    ) -> None:
        """remove() cancels the task; _deregister() leaves the task running."""
        worker_a = registry.get_or_create("session-a", "/workspace/a")
        worker_b = registry.get_or_create("session-b", "/workspace/b")
        task_a = worker_a.task
        task_b = worker_b.task
        assert task_a is not None
        assert task_b is not None

        # remove() should issue a cancel request to the task
        registry.remove("session-a")
        assert task_a.cancelling() > 0

        # _deregister() should NOT cancel the task
        registry._deregister("session-b")
        assert not task_b.done()
        assert task_b.cancelling() == 0

        # Cleanup: await task_a (already cancelling via remove — cancel() is idempotent)
        task_a.cancel()
        try:
            await task_a
        except asyncio.CancelledError:
            pass
        # Cleanup: cancel the orphaned task_b
        task_b.cancel()
        try:
            await task_b
        except asyncio.CancelledError:
            pass


class TestWorkerSelfTermination:
    """drain_worker self-terminates after processing session:end."""

    @pytest.mark.asyncio
    async def test_session_end_removes_worker_from_registry(self) -> None:
        """After session:end, worker is removed from _workers without task cancellation."""
        reg = SessionRegistry()
        worker = SessionWorker(
            session_id="test-session",
            workspace="/workspace/test",
            services=HookStateService(workspace="/workspace/test"),
        )
        reg._workers["test-session"] = worker

        with patch(
            "context_intelligence_server.registry.process_event",
            new_callable=AsyncMock,
        ):
            task = asyncio.create_task(reg.drain_worker(worker, flush_timeout=10.0))
            await worker.queue.put(
                ("session:end", "/workspace/test", {"session_id": "test-session"})
            )
            # drain_worker should self-terminate after session:end
            await asyncio.wait_for(task, timeout=2.0)

        assert "test-session" not in reg._workers

    @pytest.mark.asyncio
    async def test_session_end_writes_completed_session(self) -> None:
        """After tool_call + session:end, CompletedSession is written with correct fields."""
        reg = SessionRegistry()
        worker = SessionWorker(
            session_id="test-session",
            workspace="/workspace/test",
            services=HookStateService(workspace="/workspace/test"),
        )
        reg._workers["test-session"] = worker

        with patch(
            "context_intelligence_server.registry.process_event",
            new_callable=AsyncMock,
        ):
            task = asyncio.create_task(reg.drain_worker(worker, flush_timeout=10.0))
            await worker.queue.put(
                ("tool_call", "/workspace/test", {"session_id": "test-session"})
            )
            await worker.queue.put(
                ("session:end", "/workspace/test", {"session_id": "test-session"})
            )
            await asyncio.wait_for(task, timeout=2.0)

        assert len(reg._completed) == 1
        cs = reg._completed[0]
        assert cs.session_id == "test-session"
        assert cs.workspace == "/workspace/test"
        assert cs.events_processed == 2
        assert cs.error_count == 0
        assert cs.ended_at > 0.0
        assert cs.duration_seconds >= 0.0
        assert cs.started_at <= cs.ended_at

    @pytest.mark.asyncio
    async def test_session_end_calls_graph_close(self) -> None:
        """graph.close is awaited exactly once on session:end."""
        reg = SessionRegistry()
        worker = SessionWorker(
            session_id="test-session",
            workspace="/workspace/test",
            services=HookStateService(workspace="/workspace/test"),
        )
        reg._workers["test-session"] = worker
        worker.services.graph.close = AsyncMock()  # type: ignore[method-assign]

        with patch(
            "context_intelligence_server.registry.process_event",
            new_callable=AsyncMock,
        ):
            task = asyncio.create_task(reg.drain_worker(worker, flush_timeout=10.0))
            await worker.queue.put(
                ("session:end", "/workspace/test", {"session_id": "test-session"})
            )
            await asyncio.wait_for(task, timeout=2.0)

        worker.services.graph.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_session_end_drains_tail_events(self) -> None:
        """Events already in the queue after session:end are also processed."""
        reg = SessionRegistry()
        worker = SessionWorker(
            session_id="test-session",
            workspace="/workspace/test",
            services=HookStateService(workspace="/workspace/test"),
        )
        reg._workers["test-session"] = worker

        processed_events: list[str] = []

        async def mock_process(
            w: object, event: str, data: object, handlers: object
        ) -> None:
            processed_events.append(event)

        with patch(
            "context_intelligence_server.registry.process_event",
            side_effect=mock_process,
        ):
            # Pre-fill queue: session:end followed by a tail event
            await worker.queue.put(
                ("session:end", "/workspace/test", {"session_id": "test-session"})
            )
            await worker.queue.put(
                ("tail_event", "/workspace/test", {"session_id": "test-session"})
            )
            task = asyncio.create_task(reg.drain_worker(worker, flush_timeout=10.0))
            await asyncio.wait_for(task, timeout=2.0)

        assert "session:end" in processed_events
        assert "tail_event" in processed_events

    @pytest.mark.asyncio
    async def test_session_end_graph_close_error_still_deregisters(self) -> None:
        """If graph.close raises RuntimeError, worker is still deregistered and CompletedSession written."""
        reg = SessionRegistry()
        worker = SessionWorker(
            session_id="test-session",
            workspace="/workspace/test",
            services=HookStateService(workspace="/workspace/test"),
        )
        reg._workers["test-session"] = worker
        worker.services.graph.close = AsyncMock(  # type: ignore[method-assign]
            side_effect=RuntimeError("close failed")
        )

        with patch(
            "context_intelligence_server.registry.process_event",
            new_callable=AsyncMock,
        ):
            task = asyncio.create_task(reg.drain_worker(worker, flush_timeout=10.0))
            await worker.queue.put(
                ("session:end", "/workspace/test", {"session_id": "test-session"})
            )
            await asyncio.wait_for(task, timeout=2.0)

        assert "test-session" not in reg._workers
        assert len(reg._completed) == 1

    @pytest.mark.asyncio
    async def test_error_count_incremented_on_process_event_failure(self) -> None:
        """error_count increments when process_event raises; CompletedSession records it."""
        reg = SessionRegistry()
        worker = SessionWorker(
            session_id="test-session",
            workspace="/workspace/test",
            services=HookStateService(workspace="/workspace/test"),
        )
        reg._workers["test-session"] = worker

        call_count = 0

        async def mock_process(
            w: object, event: str, data: object, handlers: object
        ) -> None:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ValueError("processing error")

        with patch(
            "context_intelligence_server.registry.process_event",
            side_effect=mock_process,
        ):
            task = asyncio.create_task(reg.drain_worker(worker, flush_timeout=10.0))
            await worker.queue.put(
                ("tool_call", "/workspace/test", {"session_id": "test-session"})
            )
            await worker.queue.put(
                ("session:end", "/workspace/test", {"session_id": "test-session"})
            )
            await asyncio.wait_for(task, timeout=2.0)

        assert worker.error_count == 1
        assert len(reg._completed) == 1
        assert reg._completed[0].error_count == 1


class TestStaleSessionReaping:
    """Stale sessions are reaped when idle > stale_session_timeout."""

    @pytest.mark.asyncio
    async def test_stale_worker_reaped_after_timeout(self) -> None:
        """Worker with last_event_time ~5.8 days ago gets graph.close called
        and is deregistered."""
        from unittest.mock import MagicMock

        reg = SessionRegistry()
        worker = SessionWorker(
            session_id="stale-session",
            workspace="/workspace/test",
            services=HookStateService(workspace="/workspace/test"),
        )
        # 5.8 days ago > default stale_session_timeout of 5 days (432000 s)
        worker.last_event_time = time.time() - (5.8 * 24 * 3600)
        reg._register_for_test(worker)
        worker.services.graph.close = AsyncMock()  # type: ignore[method-assign]

        mock_settings = MagicMock()
        mock_settings.stale_session_timeout = 432000.0  # 5 days

        with patch(
            "context_intelligence_server.registry.get_settings",
            return_value=mock_settings,
        ):
            task = asyncio.create_task(reg.drain_worker(worker, flush_timeout=0.05))
            await asyncio.wait_for(task, timeout=2.0)

        assert "stale-session" not in reg._workers
        worker.services.graph.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_stale_session_not_added_to_completed(self) -> None:
        """Reaped stale sessions are NOT added to the _completed deque."""
        from unittest.mock import MagicMock

        reg = SessionRegistry()
        worker = SessionWorker(
            session_id="stale-session",
            workspace="/workspace/test",
            services=HookStateService(workspace="/workspace/test"),
        )
        worker.last_event_time = time.time() - (5.8 * 24 * 3600)
        reg._register_for_test(worker)
        worker.services.graph.close = AsyncMock()  # type: ignore[method-assign]

        mock_settings = MagicMock()
        mock_settings.stale_session_timeout = 432000.0

        with patch(
            "context_intelligence_server.registry.get_settings",
            return_value=mock_settings,
        ):
            task = asyncio.create_task(reg.drain_worker(worker, flush_timeout=0.05))
            await asyncio.wait_for(task, timeout=2.0)

        assert len(reg._completed) == 0

    @pytest.mark.asyncio
    async def test_stale_reap_graph_close_error_still_deregisters(self) -> None:
        """If graph.close raises during stale reaping, worker is still deregistered."""
        from unittest.mock import MagicMock

        reg = SessionRegistry()
        worker = SessionWorker(
            session_id="stale-session",
            workspace="/workspace/test",
            services=HookStateService(workspace="/workspace/test"),
        )
        worker.last_event_time = time.time() - (5.8 * 24 * 3600)
        reg._register_for_test(worker)
        worker.services.graph.close = AsyncMock(  # type: ignore[method-assign]
            side_effect=RuntimeError("close failed")
        )

        mock_settings = MagicMock()
        mock_settings.stale_session_timeout = 432000.0  # 5 days

        with patch(
            "context_intelligence_server.registry.get_settings",
            return_value=mock_settings,
        ):
            task = asyncio.create_task(reg.drain_worker(worker, flush_timeout=0.05))
            await asyncio.wait_for(task, timeout=2.0)

        assert "stale-session" not in reg._workers


class TestCancelledErrorCallsClose:
    """CancelledError causes graph.close() (not just flush) to be called."""

    @pytest.mark.asyncio
    async def test_cancelled_error_calls_close(self) -> None:
        """graph.close is called when the drain task is cancelled."""
        reg = SessionRegistry()
        worker = SessionWorker(
            session_id="test-session",
            workspace="/workspace/test",
            services=HookStateService(workspace="/workspace/test"),
        )
        worker.services.graph.close = AsyncMock()  # type: ignore[method-assign]

        task = asyncio.create_task(reg.drain_worker(worker, flush_timeout=10.0))
        await asyncio.sleep(0.02)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        worker.services.graph.close.assert_awaited_once()


class TestGetOrCreate:
    """SessionRegistry get_or_create creates workers correctly."""

    @pytest.mark.asyncio
    async def test_get_or_create_creates_worker(self) -> None:
        """get_or_create creates a new worker for an unknown session."""
        reg = SessionRegistry()
        worker = reg.get_or_create("session-restore", "/ws")

        assert worker is not None
        assert worker.session_id == "session-restore"
        assert isinstance(worker.services, HookStateService)


class TestProcessOneLogsException:
    """R-1: _process_one must log at ERROR level when process_event raises."""

    @pytest.mark.asyncio
    async def test_process_one_exception_logged(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """When process_event raises, _process_one must emit a logger.exception call
        containing the session_id and event name."""
        import logging

        reg = SessionRegistry()
        worker = SessionWorker(
            session_id="test-session",
            workspace="/workspace/test",
            services=HookStateService(workspace="/workspace/test"),
        )

        async def mock_process(w, event, data, handlers):
            raise ValueError("handler exploded")

        with (
            patch(
                "context_intelligence_server.registry.process_event",
                side_effect=mock_process,
            ),
            caplog.at_level(logging.ERROR, logger="context_intelligence_server"),
        ):
            task = asyncio.create_task(reg.drain_worker(worker, flush_timeout=10.0))
            await worker.queue.put(
                ("tool_call", "/workspace/test", {"session_id": "test-session"})
            )
            await asyncio.sleep(0.05)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        assert "process_one_failed" in caplog.text
        assert "test-session" in caplog.text
        assert "tool_call" in caplog.text


class TestCursorPersistenceRemoved:
    """Verify that all cursor persistence methods have been removed from SessionRegistry."""

    def test_no_persist_cursors_sync_method(self) -> None:
        """SessionRegistry must not have _persist_cursors_sync method."""
        assert not hasattr(SessionRegistry, "_persist_cursors_sync")

    def test_no_load_persisted_cursors_method(self) -> None:
        """SessionRegistry must not have _load_persisted_cursors method."""
        assert not hasattr(SessionRegistry, "_load_persisted_cursors")

    def test_no_delete_persisted_cursors_method(self) -> None:
        """SessionRegistry must not have _delete_persisted_cursors method."""
        assert not hasattr(SessionRegistry, "_delete_persisted_cursors")

    def test_no_purge_session_cursors_method(self) -> None:
        """SessionRegistry must not have purge_session_cursors method."""
        assert not hasattr(SessionRegistry, "purge_session_cursors")

    def test_no_purge_all_cursors_method(self) -> None:
        """SessionRegistry must not have purge_all_cursors method."""
        assert not hasattr(SessionRegistry, "purge_all_cursors")

    def test_no_path_import_in_registry_module(self) -> None:
        """registry module must not expose Path (from pathlib)."""
        import context_intelligence_server.registry as registry_mod

        assert not hasattr(registry_mod, "Path")

    def test_get_or_create_has_no_replay_parameter(self) -> None:
        """get_or_create must not accept a replay parameter (dead code removed)."""
        import inspect

        sig = inspect.signature(SessionRegistry.get_or_create)
        assert "replay" not in sig.parameters
