"""Session registry — per-session worker management."""

import asyncio
import json
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from context_intelligence_server.blob_store import AsyncDiskBlobStore
from context_intelligence_server.config import get_settings
from context_intelligence_server.dashboard import EventRecord, ring_buffer
from context_intelligence_server.neo4j_store import Neo4jGraphStore
from context_intelligence_server.pipeline import process_event, setup_handlers
from context_intelligence_server.queue_manager import Batch, QueueManager
from context_intelligence_server.services import HookStateService

logger = logging.getLogger("context_intelligence_server")

_DRAIN_MAX_BATCH = 100
_DRAIN_POLL_INTERVAL = 0.05  # idle poll cadence; bounded by flush_timeout


@dataclass
class SessionWorker:
    session_id: str
    workspace: str
    services: HookStateService
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
        # Durable-ingest infrastructure, built lazily on first use. The
        # module-level registry singleton is constructed at import time,
        # before the per-test settings patch applies, so we cannot read
        # settings here — see _ensure_infra().
        self._queue_manager: QueueManager | None = None
        self._write_semaphore: asyncio.Semaphore | None = None
        self._max_delivery_attempts: int = 0

    def _ensure_infra(self) -> None:
        """Build the shared QueueManager + write semaphore on first use.

        Lazy: reads get_settings() at call time (not at __init__) so that the
        infrastructure is rooted at the settings in effect when first accessed.
        Idempotent: only the first call constructs; subsequent calls are no-ops.
        """
        if self._queue_manager is None:
            settings = get_settings()
            self._queue_manager = QueueManager(queues_dir=Path(settings.queues_path))
            self._write_semaphore = asyncio.Semaphore(settings.write_concurrency)
            self._max_delivery_attempts = settings.max_delivery_attempts

    @property
    def queue_manager(self) -> QueueManager:
        """The single shared on-disk QueueManager owned by this registry."""
        self._ensure_infra()
        assert self._queue_manager is not None
        return self._queue_manager

    @property
    def write_semaphore(self) -> asyncio.Semaphore:
        """The single shared global cap on concurrent Neo4j-write flushes."""
        self._ensure_infra()
        assert self._write_semaphore is not None
        return self._write_semaphore

    async def _process_one(
        self,
        worker: SessionWorker,
        event: str,
        data: dict[str, Any],
        handlers: Any,
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
            raise  # Phase B2: propagate so the drainer dead-letters this line
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

    async def _flush_barrier(self, worker: SessionWorker) -> None:
        """The ONE Neo4j-write boundary: a semaphore-gated, awaited flush.

        Acquiring self.write_semaphore caps the number of concurrent Neo4j
        write transactions across ALL session drainers (the starvation guard).
        The offset must only ever advance AFTER this returns successfully.

        Correctness of commit-after-flush depends on neo4j_store._flush_body
        snapshotting+clearing the buffer under _flush_lock and RESTORING it on
        failure (neo4j_store.py:686-696), plus the empty-buffer early return
        (:656-657). We do not modify that file; we rely on it here.
        """
        async with self.write_semaphore:
            await worker.services.graph.flush()

    async def drain_worker(
        self, worker: SessionWorker, flush_timeout: float = 30.0
    ) -> None:
        """Durable drain loop for one session.

        Reads the next batch after the committed offset, dispatches each line
        through process_event, then runs the single semaphore-gated flush
        barrier and commits the offset only on success (the "ack"). A batch
        that exhausts its retry budget — or that raises during dispatch — is
        isolated ONE LINE AT A TIME and dead-lettered (never silently dropped).
        When the log is idle the drainer polls and reaps the session if it has
        been idle past the stale timeout. The drainer is the SOLE flush trigger
        (process_event no longer self-flushes, Task 6).
        """
        handlers = setup_handlers(worker.services)
        qm = self.queue_manager
        session_id = worker.session_id
        poll_interval = min(flush_timeout, _DRAIN_POLL_INTERVAL)
        idle_elapsed = 0.0
        attempts = 0

        while True:
            try:
                batch = await qm.read_batch(session_id, max_items=_DRAIN_MAX_BATCH)

                if not batch.lines:
                    await asyncio.sleep(poll_interval)
                    idle_elapsed += poll_interval
                    if idle_elapsed >= flush_timeout:
                        idle_elapsed = 0.0
                        settings = get_settings()
                        if (
                            worker.last_event_time > 0
                            and time.time() - worker.last_event_time
                            > settings.stale_session_timeout
                        ):
                            logger.info(
                                "Reaping stale session %s (idle > %s seconds)",
                                session_id,
                                settings.stale_session_timeout,
                            )
                            await self._safe_close(worker)
                            self._deregister(session_id)
                            return
                    continue

                idle_elapsed = 0.0

                # --- dispatch + durable write barrier, one error path ---
                try:
                    saw_terminal = await self._process_batch(worker, batch, handlers)
                    await self._flush_barrier(worker)
                except asyncio.CancelledError:
                    await self._safe_close(worker)
                    return
                except Exception:
                    attempts += 1
                    logger.exception(
                        "drain_batch_failed session=%s attempt=%d",
                        session_id,
                        attempts,
                    )
                    if attempts >= self._max_delivery_attempts:
                        # Budget spent -> isolate the batch ONE LINE AT A TIME,
                        # dead-letter the offending line(s), advance past all.
                        await self._handle_exhausted_batch(worker, batch, handlers)
                        attempts = 0
                        continue
                    # Budget NOT yet spent: back off one poll interval before
                    # re-reading the SAME offset (offset is not committed; the
                    # idempotent MERGE makes the replay a no-op). The backoff
                    # avoids a tight Neo4j-hammering retry loop on a transient
                    # deadlock and keeps retries on the loop's poll cadence.
                    await asyncio.sleep(poll_interval)
                    continue

                attempts = 0
                await qm.commit(session_id, batch.end_offset)

                if saw_terminal:
                    await self._finalize_session(worker, handlers)
                    return

            except asyncio.CancelledError:
                await self._safe_close(worker)
                return

    @staticmethod
    def _parse_line(raw: bytes) -> tuple[str, str, dict[str, Any]]:
        """Decode an appended event line (raw EventRequest JSON)."""
        obj = json.loads(raw.decode("utf-8"))
        return obj["event"], obj.get("workspace", ""), obj.get("data", {})

    async def _process_batch(
        self, worker: SessionWorker, batch: Batch, handlers: Any
    ) -> bool:
        """Dispatch each line in the batch; return True if it contained a
        terminal (session:end) event."""
        from context_intelligence_server.pipeline import TERMINAL_EVENTS  # noqa: PLC0415

        saw_terminal = False
        for raw in batch.lines:
            event, _workspace, data = self._parse_line(raw)
            await self._process_one(worker, event, data, handlers)
            if event in TERMINAL_EVENTS:
                saw_terminal = True
        return saw_terminal

    async def _handle_exhausted_batch(
        self, worker: SessionWorker, batch: Batch, handlers: Any
    ) -> None:
        """Reprocess a poison batch ONE LINE AT A TIME (linear isolation).

        Each line is dispatched + flushed individually under the write
        semaphore. A line that still fails (parse error, handler error, or
        repeated flush failure) is dead-lettered with its error AND its write
        residue is discarded from the store buffer (COE blocker, decision #13);
        good lines flush normally. Every line advances the offset past itself
        (commit), so the whole batch is accounted for. No silent loss, no
        binary shrink, no cross-line contamination.
        """
        qm = self.queue_manager
        session_id = worker.session_id
        # The failed BATCH flush left its writes resident in the store buffer
        # (_flush_body restores on failure, neo4j_store.py:686-696). Discard that
        # accumulated residue so the FIRST isolated line flushes from a clean
        # buffer — otherwise the poison line's residue contaminates line 1.
        worker.services.graph.discard_buffer()
        offset = batch.start_offset
        for raw in batch.lines:
            line_end = offset + len(raw) + 1  # +1 for the newline read_batch strips
            try:
                event, _ws, data = self._parse_line(raw)
                await self._process_one(worker, event, data, handlers)
                await self._flush_barrier(worker)
            except Exception as exc:
                await qm.dead_letter(session_id, raw + b"\n", str(exc))
                # COE blocker (decision #13): drop the failed line's residue so
                # it cannot contaminate the NEXT line's flush. A successful flush
                # clears the buffer itself; only the failure path needs this.
                worker.services.graph.discard_buffer()
            await qm.commit(session_id, line_end)
            offset = line_end

    async def _finalize_session(self, worker: SessionWorker, handlers: Any) -> None:
        """session:end seen: drain any tail lines read-to-EOF, then record the
        CompletedSession, close the graph, deregister, and DELETE the drained
        logs. Panel finding #7: if a tail flush fails, do NOT finalize — return
        without recording/closing so the drainer retries (no tail loss)."""
        qm = self.queue_manager
        session_id = worker.session_id
        while True:
            tail = await qm.read_batch(session_id, max_items=_DRAIN_MAX_BATCH)
            if not tail.lines:
                break
            try:
                await self._process_batch(worker, tail, handlers)
                await self._flush_barrier(worker)
            except Exception:
                logger.exception("finalize_tail_flush_failed session=%s", session_id)
                return  # NOT finalized: keep worker alive, leave tail uncommitted
            await qm.commit(session_id, tail.end_offset)

        ended_at = time.time()
        self._completed.append(
            CompletedSession(
                session_id=session_id,
                workspace=worker.workspace,
                started_at=worker.started_at,
                ended_at=ended_at,
                events_processed=worker.events_processed,
                error_count=worker.error_count,
                duration_seconds=ended_at - worker.started_at,
            )
        )
        await self._safe_close(worker)
        self._deregister(session_id)
        # Panel finding #5: reclaim disk — a fully drained, finalized session no
        # longer needs its .log/.offset. Keep .dead.jsonl (retained dead-letter).
        await qm.delete_drained(session_id)

    async def _safe_close(self, worker: SessionWorker) -> None:
        try:
            await worker.services.graph.close()
        except Exception:
            logger.exception("graph.close failed for session %s", worker.session_id)

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
