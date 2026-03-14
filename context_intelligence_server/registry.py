"""Session registry — per-session worker management."""

import asyncio
import logging
from dataclasses import dataclass, field

logger = logging.getLogger("context_intelligence_server")


@dataclass
class SessionWorker:
    session_id: str
    workspace: str
    queue: asyncio.Queue = field(default_factory=asyncio.Queue)
    task: asyncio.Task | None = None


async def drain_worker(worker: SessionWorker) -> None:
    """Background coroutine that drains the session's event queue."""
    while True:
        event_tuple = await worker.queue.get()
        try:
            event, workspace, data = event_tuple
            session_id = data.get("session_id", "") if isinstance(data, dict) else ""
            logger.info(
                "drain_worker: event=%s session_id=%s workspace=%s",
                event,
                session_id,
                workspace,
            )
        except Exception:
            logger.exception("drain_worker: error processing event")
        finally:
            worker.queue.task_done()


class SessionRegistry:
    def __init__(self) -> None:
        self._workers: dict[str, SessionWorker] = {}

    def start_drain(self, worker: SessionWorker) -> None:
        if worker.task is None or worker.task.done():
            worker.task = asyncio.create_task(
                drain_worker(worker), name=f"drain-{worker.session_id}"
            )

    def get_or_create(self, session_id: str, workspace: str) -> SessionWorker:
        if session_id not in self._workers:
            self._workers[session_id] = SessionWorker(
                session_id=session_id,
                workspace=workspace,
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
