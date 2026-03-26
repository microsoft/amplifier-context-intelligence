"""Session registry — per-session worker management."""

import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from context_intelligence_server.blob_store import AsyncDiskBlobStore
from context_intelligence_server.config import get_settings
from context_intelligence_server.dashboard import EventRecord, ring_buffer
from context_intelligence_server.neo4j_store import Neo4jGraphStore
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
    last_event: str = ""
    last_event_time: float = 0.0
    events_processed: int = 0
    started_at: float = field(default_factory=time.time)
    error_count: int = 0


@dataclass
class CompletedSession:
    """Snapshot of a finished session stored in the ring buffer."""

    session_id: str
    workspace: str
    started_at: float
    ended_at: float
    events_processed: int
    error_count: int
    duration_seconds: float


class SessionRegistry:
    def __init__(self) -> None:
        self._workers: dict[str, SessionWorker] = {}
        self._completed: deque[CompletedSession] = deque(maxlen=100)

    async def _process_one(
        self,
        worker: SessionWorker,
        event: str,
        data: dict[str, Any],
        handlers: dict[str, Any],
    ) -> None:
        """Dispatch one event, update worker stats, and record to the ring buffer."""
        result = "ok"
        error = ""
        try:
            await process_event(worker, event, data, handlers)
            worker.last_event = event
            worker.last_event_time = time.time()
            worker.events_processed += 1
        except Exception as exc:
            logger.exception(
                "process_one_failed session=%s event=%s", worker.session_id, event
            )
            result = "error"
            error = str(exc)
            worker.error_count += 1
        finally:
            ring_buffer.add(
                EventRecord(
                    timestamp=time.time(),
                    event=event,
                    session_id=data.get("session_id", ""),
                    workspace=worker.workspace,
                    result=result,
                    error=error,
                )
            )
            worker.queue.task_done()

    async def drain_worker(
        self, worker: SessionWorker, flush_timeout: float = 30.0
    ) -> None:
        """Background coroutine that drains the session's event queue.

        Initializes handlers once, then loops:
        - Dequeues events with a timeout of *flush_timeout* seconds.
        - Dispatches each event via process_event.
        - On TimeoutError (no events for flush_timeout seconds): calls graph.flush
          as a periodic fallback for disconnected sessions.
        - On CancelledError (shutdown): calls graph.close, then exits cleanly.
        - On session:end: drains tail events, creates CompletedSession,
          calls graph.close, deregisters the worker, then exits.
        """
        handlers = setup_handlers(worker.services)

        while True:
            try:
                event_tuple = await asyncio.wait_for(
                    worker.queue.get(), timeout=flush_timeout
                )
                event, _workspace, data = event_tuple
                await self._process_one(worker, event, data, handlers)

                if event == "session:end":
                    # Drain any tail events already in the queue
                    while True:
                        try:
                            tail_event, _tail_ws, tail_data = worker.queue.get_nowait()
                            await self._process_one(
                                worker, tail_event, tail_data, handlers
                            )
                        except asyncio.QueueEmpty:
                            break

                    # Record the completed session
                    ended_at = time.time()
                    self._completed.append(
                        CompletedSession(
                            session_id=worker.session_id,
                            workspace=worker.workspace,
                            started_at=worker.started_at,
                            ended_at=ended_at,
                            events_processed=worker.events_processed,
                            error_count=worker.error_count,
                            duration_seconds=ended_at - worker.started_at,
                        )
                    )

                    try:
                        await worker.services.graph.close()
                    except Exception:
                        logger.exception(
                            "graph.close failed for session %s", worker.session_id
                        )

                    self._deregister(worker.session_id)
                    break

            except asyncio.TimeoutError:
                # Periodic fallback flush for disconnected sessions
                try:
                    await worker.services.graph.flush()
                except Exception:
                    logger.exception(
                        "periodic_flush_failed for session %s", worker.session_id
                    )
                # Stale session reaping
                settings = get_settings()
                if (
                    worker.last_event_time > 0
                    and time.time() - worker.last_event_time
                    > settings.stale_session_timeout
                ):
                    logger.info(
                        "Reaping stale session %s (idle > %s seconds)",
                        worker.session_id,
                        settings.stale_session_timeout,
                    )
                    try:
                        await worker.services.graph.close()
                    except Exception:
                        logger.exception(
                            "graph.close failed for stale session %s",
                            worker.session_id,
                        )
                    self._deregister(worker.session_id)
                    break

            except asyncio.CancelledError:
                try:
                    await worker.services.graph.close()
                except Exception:
                    logger.exception(
                        "graph.close failed on cancel for session %s",
                        worker.session_id,
                    )
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
            neo4j_auth = (
                (settings.neo4j_user, settings.neo4j_password)
                if settings.neo4j_password
                else None
            )
            neo4j_store = Neo4jGraphStore(
                uri=settings.neo4j_url,
                auth=neo4j_auth,
            )
            self._workers[session_id] = SessionWorker(
                session_id=session_id,
                workspace=workspace,
                services=HookStateService(
                    workspace=workspace,
                    blob_store=blob_store,
                    graph_store=neo4j_store,
                ),
            )
            self.start_drain(self._workers[session_id])
        return self._workers[session_id]

    def remove(self, session_id: str) -> None:
        worker = self._workers.pop(session_id, None)
        if worker and worker.task and not worker.task.done():
            worker.task.cancel()

    def _deregister(self, session_id: str) -> None:
        """Remove worker from registry WITHOUT cancelling its asyncio task."""
        self._workers.pop(session_id, None)

    def _register_for_test(self, worker: SessionWorker) -> None:
        """Insert a pre-built worker into the registry — for use in tests only.

        Avoids direct access to the private ``_workers`` dict in test helpers
        while keeping the public API uncluttered.
        """
        self._workers[worker.session_id] = worker

    def completed_sessions(self) -> list[CompletedSession]:
        """Return completed sessions sorted by most recently ended first."""
        return sorted(self._completed, key=lambda s: s.ended_at, reverse=True)

    def workers(self) -> list[SessionWorker]:
        """Return the list of all active SessionWorker objects."""
        return list(self._workers.values())

    def active_count(self) -> int:
        return len(self._workers)

    def active_sessions(self) -> list[str]:
        return sorted(self._workers.keys())
