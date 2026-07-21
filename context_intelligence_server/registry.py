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
from context_intelligence_server.status import EventRecord, ring_buffer
from context_intelligence_server.neo4j_store import Neo4jGraphStore
from context_intelligence_server.pipeline import process_event, setup_handlers
from context_intelligence_server.queue_manager import Batch, QueueManager
from context_intelligence_server.services import HookStateService

logger = logging.getLogger("context_intelligence_server")

_DRAIN_MAX_BATCH = 100
_DRAIN_POLL_INTERVAL = 0.05  # idle poll cadence; bounded by flush_timeout

# A positive residual must PERSIST this long before it is called degraded.
# Must exceed the worst-case transient-skew window: the derive_all_stats
# cache TTL (1.0s) plus the /status poll cadence (~3s). 15s is >10x the cache
# TTL, so any in-flight two-clock skew clears well before it trips degraded,
# while a genuine (monotonic, non-clearing) silent drop still trips it.
_RESIDUAL_DEGRADED_GRACE = 15.0


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
    # Phase 2 (#278): liveness timestamp — when the flush boundary last
    # completed for this worker. Defaults to creation time (NOT 0.0) so a
    # brand-new worker reads as fresh, not ancient. Stamped in _flush_barrier.
    last_successful_flush: float = field(default_factory=time.time)


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
        # Live pipeline-conservation counters (D2): make silently-dropped
        # events observable via /status. accepted = events admitted to the
        # log; written = events persisted to Neo4j; replayed = events
        # re-driven from the log on recovery; write_retries = transient
        # write retries attempted by the drainer.
        self._accepted_total: int = 0
        self._written_total: int = 0
        self._replayed_total: int = 0
        self._write_retries_total: int = 0
        # FIX B: monotonic timestamp when the residual first went positive and
        # stayed unexplained. None means "clean". Gates the degraded flag so a
        # transient two-clock skew never latches; only a sustained positive
        # residual (real silent drop) does.
        self._residual_positive_since: float | None = None

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

    def record_accepted(self, n: int = 1) -> None:
        """Count events admitted to the durable log (ingest accepted)."""
        self._accepted_total += n

    def record_written(self, n: int) -> None:
        """Count events successfully persisted to Neo4j."""
        self._written_total += n

    def record_replayed(self, n: int) -> None:
        """Count events re-driven from the log during recovery."""
        self._replayed_total += n

    def record_purged(self, n: int) -> None:
        """Remove n purged dead-letters from the accepted total (conservation).

        A bare dead-letter purge unlinks the .dead.jsonl file, dropping `dead`
        by n. Those lines were counted in `accepted` at ingest but never
        `written`; discarding them from disk must also discard them from
        `accepted`, or the residual latches at +n forever. Symmetric to
        record_replayed, which moves lines dead -> in_queue and therefore must
        NOT touch accepted.

        Clamp: accepted can never fall below written. Under the single-writer
        guarantee the clamp can never legitimately engage (a dead line is
        accepted-but-not-written, so n <= accepted - written); if it does, log
        a warning as an accounting-drift signal rather than silently masking it.
        """
        if n <= 0:
            return
        target = self._accepted_total - n
        floored = max(self._written_total, target)
        if floored != target:
            logger.warning(
                "record_purged clamp engaged accepted=%d written=%d purge=%d",
                self._accepted_total,
                self._written_total,
                n,
            )
        self._accepted_total = floored

    def record_write_retry(self) -> None:
        """Count a single transient Neo4j-write retry attempt."""
        self._write_retries_total += 1

    def seed_counters(self, accepted: int, written: int) -> None:
        """ADD a crash-recovery baseline to the accepted/written counters.

        On startup the server reconstructs how many events were already
        accepted/written before the crash and seeds those totals so the live
        conservation snapshot stays correct across restarts. This ADDS to the
        running counters rather than replacing them.
        """
        self._accepted_total += accepted
        self._written_total += written

    def pipeline_counters(self) -> dict[str, int]:
        """Snapshot of the live conservation counters (sync, no disk I/O)."""
        return {
            "accepted_total": self._accepted_total,
            "written_total": self._written_total,
            "replayed_total": self._replayed_total,
            "write_retries_total": self._write_retries_total,
        }

    async def pipeline_metrics(self) -> dict[str, Any]:
        """Assemble the pipeline-conservation health block for /status (D2/D3).

        Combines the live in-memory counters (pipeline_counters) with the
        disk-derived queue/dead aggregate (queue_manager.derive_all_stats) into
        a single conservation view. The residual is the count of accepted
        events that are neither persisted, nor still queued, nor dead-lettered:

            residual = accepted - written - in_queue - dead

        ``degraded`` is True whenever ``dead > 0`` (an accounted-for loss, no
        grace period) OR the residual is POSITIVE and has stayed positive for
        at least ``_RESIDUAL_DEGRADED_GRACE`` seconds. A negative residual is
        never degraded (it is benign two-clock skew between the live counters
        and the cached disk snapshot, clamped to a ``lost`` value of zero), and
        a positive residual that clears before the grace window elapses is
        treated as the same transient skew rather than real loss.

        IMPORTANT caveats:
        - This is a LIVE per-process measure, not an all-time audit. Finalized
          session logs are deleted by ``delete_drained``, so their accepted /
          written / in_queue contributions leave the disk-derived aggregate.
          The in-memory accepted/written counters persist, so the residual
          stays conserved for the lifetime of the process (seeded across
          restarts via ``seed_counters``).
        - It is only valid under the single-worker (single-process) guarantee:
          one writer owns the counters and the on-disk queues.
        - ``write_retries_total`` is the transient/deadlock proxy — the closest
          observable signal for retried (e.g. DeadlockDetected) writes.
        - ``deadlock_detected_total`` and ``events_failed_total`` are
          intentionally omitted: neither is cleanly trackable at this layer.
        - ``oldest_unflushed_age`` is DEFERRED to C2 and is intentionally
          absent from this block.
        """
        agg = await self.queue_manager.derive_all_stats()
        counters = self.pipeline_counters()
        in_queue = agg["in_queue_total"]
        dead = agg["dead_total"]
        residual = (
            counters["accepted_total"] - counters["written_total"] - in_queue - dead
        )
        # A NEGATIVE residual is never data loss: written+in_queue+dead cannot
        # legitimately exceed accepted, so residual<0 is purely a sampling skew
        # between the fresh counters and the cached disk snapshot. Clamp the
        # loss signal at zero -- only a positive residual can mean real loss.
        lost = max(0, residual)
        now = time.monotonic()
        if lost > 0:
            if self._residual_positive_since is None:
                self._residual_positive_since = now
            sustained = (
                now - self._residual_positive_since
            ) >= _RESIDUAL_DEGRADED_GRACE
        else:
            self._residual_positive_since = None
            sustained = False
        # dead>0 is an accounted-for loss and is degraded immediately (no grace).
        # A positive residual is degraded only once it has PERSISTED past the
        # grace window -- transient in-flight skew clears before then.
        degraded = dead > 0 or sustained
        return {
            "accepted_total": counters["accepted_total"],
            "written_total": counters["written_total"],
            "replayed_total": counters["replayed_total"],
            "write_retries_total": counters["write_retries_total"],
            "in_queue_total": in_queue,
            "dead_letter_total": dead,
            "residual": residual,
            "degraded": degraded,
        }

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
            # Phase 2 (#278): stamp liveness at the SINGLE flush boundary all
            # three success paths funnel through. Marks completion of the flush
            # barrier (advances even on an empty-buffer flush = liveness proof
            # that the drainer reached and finished the write barrier).
            worker.last_successful_flush = time.time()

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
                    self.record_write_retry()
                    # Throttle the failure log off the local attempts counter
                    # (resets to 0 on commit and after exhaustion): the first
                    # failure gets ONE traceback (WARNING), middle attempts are
                    # DEBUG, and budget exhaustion gets a single ERROR (no
                    # per-attempt traceback storm).
                    if attempts == 1:
                        logger.warning(
                            "drain_batch_failed session=%s attempt=%d",
                            session_id,
                            attempts,
                            exc_info=True,
                            extra={"session_id": session_id},
                        )
                    elif attempts >= self._max_delivery_attempts:
                        logger.error(
                            "drain_batch_exhausted session=%s attempts=%d",
                            session_id,
                            attempts,
                            extra={"session_id": session_id},
                        )
                    else:
                        logger.debug(
                            "drain_batch_failed session=%s attempt=%d",
                            session_id,
                            attempts,
                            extra={"session_id": session_id},
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
                self.record_written(len(batch.lines))
                logger.debug(
                    "batch_committed events=%d offset=%d",
                    len(batch.lines),
                    batch.end_offset,
                    extra={"session_id": session_id},
                )

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
                self.record_written(1)
            except Exception as exc:
                await qm.dead_letter(session_id, raw + b"\n", str(exc))
                logger.warning(
                    "dead_letter session=%s error=%s",
                    session_id,
                    exc,
                    extra={"session_id": session_id},
                )
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
            self.record_written(len(tail.lines))
            logger.debug(
                "batch_committed events=%d offset=%d",
                len(tail.lines),
                tail.end_offset,
                extra={"session_id": session_id},
            )

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
        logger.info(
            "session_finalized session=%s events=%d",
            session_id,
            worker.events_processed,
            extra={"session_id": session_id},
        )

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

    def get_or_create(
        self,
        session_id: str,
        workspace: str,
        created_by: str | None = None,
    ) -> SessionWorker:
        if session_id not in self._workers:
            settings = get_settings()
            blob_store = AsyncDiskBlobStore(root=settings.blob_path)
            _admin = settings.resolve_neo4j_admin()
            neo4j_store = Neo4jGraphStore(
                uri=_admin.url,
                auth=_admin.auth,
                flush_chunk_rows=settings.neo4j_flush_chunk_rows,
                flush_chunk_bytes=settings.neo4j_flush_chunk_bytes,
                neo4j_lock_timeout=settings.neo4j_lock_timeout,
            )
            self._workers[session_id] = SessionWorker(
                session_id=session_id,
                workspace=workspace,
                services=HookStateService(
                    workspace=workspace,
                    created_by=created_by,
                    blob_store=blob_store,
                    graph_store=neo4j_store,
                ),
            )
            self.start_drain(self._workers[session_id])
            logger.info(
                "drainer_spawned session=%s",
                session_id,
                extra={"session_id": session_id},
            )
        else:
            # Session-ownership invariant: each session_id is owned by exactly one
            # contributor; the bound created_by (set once at creation) is load-bearing
            # for provenance.  Log at ERROR — not WARNING — so monitoring surfaces a
            # violation observably; preserve the bound id and don't crash live ingest.
            if created_by is not None:
                bound = getattr(
                    self._workers[session_id].services.graph, "created_by", None
                )
                if bound is not None and bound != created_by:
                    logger.error(
                        "session_ownership_invariant_violation session=%s "
                        "bound_contributor=%s conflicting_contributor=%s",
                        session_id,
                        bound,
                        created_by,
                        extra={
                            "session_id": session_id,
                            "bound_contributor": bound,
                            "conflicting_contributor": created_by,
                        },
                    )
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

    def orphaned_sessions(self) -> list[SessionWorker]:
        """Return workers that are still registered but whose drain task has
        finished — the silent-stall signal for #278.

        A worker is orphaned iff it is in _workers AND its task has completed
        (task.done()). This catches the finalization-path orphan (a tail flush
        failure returns early without deregistering, so the task completes but
        the worker is never removed) and any unhandled exception that escapes
        the drain loop. Deterministic and instant — no timer, no threshold.
        """
        return [
            worker
            for worker in self._workers.values()
            if worker.task is not None and worker.task.done()
        ]

    def active_count(self) -> int:
        return len(self._workers)

    def active_sessions(self) -> list[str]:
        return sorted(self._workers.keys())
