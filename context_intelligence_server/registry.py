"""Session registry — per-session worker management."""

import asyncio
import logging
from dataclasses import dataclass, field

from context_intelligence_server.blob_store import AsyncDiskBlobStore
from context_intelligence_server.config import get_settings
from context_intelligence_server.pipeline import process_event, setup_handlers
from context_intelligence_server.services import HookStateService

logger = logging.getLogger("context_intelligence_server")


@dataclass
class SessionWorker:
    session_id: str
    workspace: str
    services: HookStateService
    queue: asyncio.Queue = field(default_factory=asyncio.Queue)
    task: asyncio.Task | None = None


class SessionRegistry:
    def __init__(self) -> None:
        self._workers: dict[str, SessionWorker] = {}

    async def drain_worker(
        self, worker: SessionWorker, flush_timeout: float = 30.0
    ) -> None:
        """Background coroutine that drains the session's event queue.

        Initializes handlers once, then loops:
        - Dequeues events with a timeout of *flush_timeout* seconds.
        - Dispatches each event via process_event.
        - On TimeoutError (no events for flush_timeout seconds): calls graph.flush
          as a periodic fallback for disconnected sessions.
        - On CancelledError (shutdown): flushes once then exits cleanly.
        """
        handlers = setup_handlers(worker.services)

        while True:
            try:
                event_tuple = await asyncio.wait_for(
                    worker.queue.get(), timeout=flush_timeout
                )
                event, _workspace, data = event_tuple
                try:
                    await process_event(worker, event, data, handlers)
                finally:
                    worker.queue.task_done()

            except asyncio.TimeoutError:
                # Periodic fallback flush for disconnected sessions
                await worker.services.graph.flush()

            except asyncio.CancelledError:
                # Shutdown: flush any buffered writes before exiting
                await worker.services.graph.flush()
                break

    def start_drain(self, worker: SessionWorker) -> None:
        if worker.task is None or worker.task.done():
            worker.task = asyncio.create_task(
                self.drain_worker(worker), name=f"drain-{worker.session_id}"
            )

    def get_or_create(self, session_id: str, workspace: str) -> SessionWorker:
        if session_id not in self._workers:
            settings = get_settings()
            blob_store = AsyncDiskBlobStore(root=settings.blob_path)
            self._workers[session_id] = SessionWorker(
                session_id=session_id,
                workspace=workspace,
                services=HookStateService(workspace=workspace, blob_store=blob_store),
            )
            self.start_drain(self._workers[session_id])
        return self._workers[session_id]

    def remove(self, session_id: str) -> None:
        worker = self._workers.pop(session_id, None)
        if worker and worker.task and not worker.task.done():
            worker.task.cancel()

    def active_count(self) -> int:
        return len(self._workers)

    def active_sessions(self) -> list[str]:
        return sorted(self._workers.keys())
