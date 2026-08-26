"""Session registry — per-session worker management."""

import asyncio
import functools
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

# Bounded retry count for the finalize delete-drained loop; not operator-tunable.
# No backoff between attempts -- sleeping would widen the race window this closes.
_FINALIZE_DELETE_ATTEMPTS = 3

# Grace period before a positive residual is flagged degraded -- must exceed
# the stats-cache TTL + poll cadence to avoid false positives from clock skew.
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
    # Timestamp when the flush boundary last completed; defaults to creation
    # time (not 0.0) so a brand-new worker reads as fresh. Set in _flush_barrier.
    last_successful_flush: float = field(default_factory=time.time)
    # Set True by _safe_close, as its FIRST statement. A worker
    # whose store has been closed is never revived — see start_drain.
    store_closed: bool = False


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
        # Strong refs to fire-and-forget close tasks -- asyncio only holds a
        # weak ref, so without this a close can be GC'd mid-execution. Self-discards on done.
        self._close_tasks: set[asyncio.Task] = set()
        # Durable-ingest infra, built lazily (see _ensure_infra) since the
        # module-level singleton is constructed before test settings patches apply.
        self._queue_manager: QueueManager | None = None
        self._write_semaphore: asyncio.Semaphore | None = None
        self._max_delivery_attempts: int = 0
        # Live conservation counters surfaced via /status (accepted/written/
        # replayed/write_retries) so silently-dropped events are observable.
        self._accepted_total: int = 0
        self._written_total: int = 0
        self._replayed_total: int = 0
        self._write_retries_total: int = 0
        # Monotonic time the residual first went positive (None = clean).
        # Gates `degraded` so transient clock skew doesn't latch it.
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

        A dead-letter purge drops `dead` by n without ever having been
        `written`, so `accepted` must drop too or the residual latches at +n
        forever. Clamped so accepted never falls below written; an engaged
        clamp logs a warning as an accounting-drift signal.
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
        """Assemble the pipeline-conservation health block for /status.

        Combines live counters with the disk-derived queue/dead aggregate.
        residual = accepted - written - in_queue - dead. `degraded` is True
        when dead > 0, or when a positive residual persists past
        `_RESIDUAL_DEGRADED_GRACE` seconds (transient clock skew clears
        before then). Live per-process only: finalized session logs are
        deleted, but the in-memory counters persist across restarts via
        `seed_counters`.
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
            raise  # Propagate so the drainer dead-letters this line
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
        """The one Neo4j-write boundary: a semaphore-gated, awaited flush.

        The semaphore caps concurrent write transactions across all session
        drainers. The offset must only advance after this returns successfully;
        correctness relies on the GraphStore protocol's flush-failure isolation.
        """
        async with self.write_semaphore:
            await worker.services.graph.flush()
            # Stamped here (the one flush boundary) as liveness proof the
            # drainer reached and finished the write barrier.
            worker.last_successful_flush = time.time()

    async def drain_worker(
        self, worker: SessionWorker, flush_timeout: float = 30.0
    ) -> None:
        """Durable drain loop for one session.

        Reads batches after the committed offset, dispatches each event, runs
        the flush barrier, and commits only on success; an exhausted retry
        budget dead-letters the batch line-by-line.

        Any other exception propagates -- `_on_drain_done` is the sole
        supervision point (logs, closes, deregisters so a respawn or boot
        `recover()` picks it up). A terminal `session:end` record is left
        uncommitted so a later drain re-enters `_finalize_session`.

        The queue owns all byte-position math; this registry only chooses
        which offset to commit via `qm.commit`/`qm.dead_letter`. When idle,
        the drainer polls and reaps the session past the stale timeout.
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

                if not batch.records:
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
                                "session_reaped_stale session=%s idle_seconds=%s",
                                session_id,
                                settings.stale_session_timeout,
                                extra={"session_id": session_id},
                            )
                            await self._safe_close(worker)
                            self._deregister(session_id)
                            return
                    continue

                idle_elapsed = 0.0

                # --- dispatch + durable write barrier, one error path ---
                try:
                    safe_count, terminal_at = await self._process_batch(
                        worker, batch, handlers
                    )
                    await self._flush_barrier(worker)
                except asyncio.CancelledError:
                    # INFO not ERROR: a cancel here is normally deliberate
                    # (shutdown, idle reap, test teardown), not a failure.
                    logger.info(
                        "drain_worker_cancelled session=%s site=%s",
                        session_id,
                        "dispatch",
                        extra={"session_id": session_id},
                    )
                    await self._safe_close(worker)
                    # Must deregister so get_or_create builds a fresh worker --
                    # else start_drain's store_closed guard refuses it forever.
                    self._deregister(session_id)
                    return
                except Exception:
                    attempts += 1
                    self.record_write_retry()
                    # First failure: WARNING w/ traceback. Middle attempts: DEBUG.
                    # Exhaustion: single ERROR. Avoids a per-attempt traceback storm.
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
                        # Budget spent: isolate the batch line-by-line and dead-letter.
                        terminal_seen = await self._handle_exhausted_batch(
                            worker, batch, handlers
                        )
                        if terminal_seen:
                            # Mirror the normal terminal branch below: the
                            # session:end record was left uncommitted, so
                            # finalize instead of resuming the drain loop.
                            await self._finalize_session(worker, handlers)
                            return
                        attempts = 0
                        continue
                    # Not yet exhausted: back off before re-reading the same
                    # offset (idempotent MERGE makes the replay a no-op).
                    await asyncio.sleep(poll_interval)
                    continue

                attempts = 0
                # Commit only up to session:end -- leaving it uncommitted makes
                # "ended but not finalized" durable across a respawn/recover().
                commit_to = batch.end_offset if terminal_at is None else terminal_at
                await qm.commit(session_id, commit_to)
                counted = len(batch.records) if terminal_at is None else safe_count
                self.record_written(counted)
                logger.debug(
                    "batch_committed events=%d offset=%d",
                    counted,
                    commit_to,
                    extra={"session_id": session_id},
                )

                if terminal_at is not None:
                    await self._finalize_session(worker, handlers)
                    return

            except asyncio.CancelledError:
                # Cancelled while reading/idle (outer site; never reaches the inner try).
                logger.info(
                    "drain_worker_cancelled session=%s site=%s",
                    session_id,
                    "loop",
                    extra={"session_id": session_id},
                )
                await self._safe_close(worker)
                self._deregister(session_id)  # See the note above.
                return

    @staticmethod
    def _parse_line(raw: bytes) -> tuple[str, str, dict[str, Any]]:
        """Decode an appended event line (raw EventRequest JSON)."""
        obj = json.loads(raw.decode("utf-8"))
        return obj["event"], obj.get("workspace", ""), obj.get("data", {})

    async def _process_batch(
        self, worker: SessionWorker, batch: Batch, handlers: Any
    ) -> tuple[int, int | None]:
        """Dispatch every record; report the first terminal boundary.

        Returns ``(safe_count, terminal_at)``: ``terminal_at`` is the queue-
        produced start offset of the first ``session:end`` record (or None),
        and ``safe_count`` is how many records precede it. Every record is
        still dispatched -- a failed terminal dispatch still goes through
        the retry/isolation path.
        """
        from context_intelligence_server.pipeline import (
            TERMINAL_EVENTS,
        )

        terminal_at: int | None = None
        safe_count = 0
        for rec in batch.records:
            event, _workspace, data = self._parse_line(rec.raw)
            await self._process_one(worker, event, data, handlers)
            if terminal_at is None:
                if event in TERMINAL_EVENTS:
                    terminal_at = rec.start
                else:
                    safe_count += 1
        return safe_count, terminal_at

    async def _handle_exhausted_batch(
        self, worker: SessionWorker, batch: Batch, handlers: Any
    ) -> bool:
        """Reprocess a poison batch one line at a time (linear isolation).

        Each record is dispatched and flushed individually. A record that
        fails is dead-lettered and its buffer residue discarded so it can't
        contaminate later records. Every non-terminal record advances the
        offset to its own queue-produced end, so it is fully accounted for.

        A record that successfully parses as a terminal ``session:end``
        record is NOT dispatched or committed here -- isolation stops
        immediately and returns True, leaving that record (and anything
        after it) uncommitted, mirroring the normal drain loop's terminal
        semantics (see ``drain_worker``/``_process_batch``). The caller must
        then call ``_finalize_session`` instead of resuming the drain loop,
        exactly like the non-exhausted terminal path: ``_finalize_session``'s
        own ``_drain_to_eof`` re-reads and re-dispatches the terminal record.
        A record whose bytes fail to parse is NOT terminal -- it is
        dead-lettered and committed past like any other poison line.

        Returns False when the whole batch is isolated without ever
        reaching a terminal record (unchanged behavior: no finalization).
        """
        qm = self.queue_manager
        session_id = worker.session_id
        # The failed batch flush left writes resident in the store buffer --
        # discard so the first isolated record flushes from a clean buffer.
        worker.services.graph.discard_buffer()
        for rec in batch.records:
            try:
                event, _ws, data = self._parse_line(rec.raw)
            except Exception as exc:
                # Unparseable: can't be a terminal record -- poison as before.
                await qm.dead_letter(session_id, rec.raw, str(exc))  # no re-framing
                logger.warning(
                    "dead_letter session=%s error=%s",
                    session_id,
                    exc,
                    exc_info=exc,
                    extra={"session_id": session_id},
                )
                worker.services.graph.discard_buffer()
                await qm.commit(session_id, rec.end)  # queue-produced offset
                continue

            from context_intelligence_server.pipeline import TERMINAL_EVENTS

            if event in TERMINAL_EVENTS:
                return True

            wrote = False
            try:
                await self._process_one(worker, event, data, handlers)
                await self._flush_barrier(worker)
                wrote = True
            except Exception as exc:
                await qm.dead_letter(session_id, rec.raw, str(exc))  # no re-framing
                logger.warning(
                    "dead_letter session=%s error=%s",
                    session_id,
                    exc,
                    exc_info=exc,
                    extra={"session_id": session_id},
                )
                # Drop the failed record's residue so it cannot contaminate
                # the NEXT record's flush. A successful flush clears the
                # buffer itself; only the failure path needs this.
                worker.services.graph.discard_buffer()
            await qm.commit(session_id, rec.end)  # queue-produced offset
            if wrote:
                self.record_written(1)
        return False

    async def _drain_to_eof(self, worker: SessionWorker, handlers: Any) -> bool:
        """Drain every remaining record for this session up to EOF.

        Returns True when fully drained. Returns False when a tail flush
        failed -- nothing was committed, and the caller must not finalize.
        """
        qm = self.queue_manager
        session_id = worker.session_id
        while True:
            tail = await qm.read_batch(session_id, max_items=_DRAIN_MAX_BATCH)
            if not tail.records:
                return True
            try:
                await self._process_batch(worker, tail, handlers)
                await self._flush_barrier(worker)
            except Exception:
                logger.exception("finalize_tail_flush_failed session=%s", session_id)
                return False  # NOT finalized: keep worker alive, tail uncommitted
            await qm.commit(session_id, tail.end_offset)
            self.record_written(len(tail.records))
            logger.debug(
                "batch_committed events=%d offset=%d",
                len(tail.records),
                tail.end_offset,
                extra={"session_id": session_id},
            )

    async def _finalize_session(self, worker: SessionWorker, handlers: Any) -> None:
        """session:end seen: drain to EOF, record CompletedSession, delete
        the drained log, close the graph, then deregister -- in that order.

        If the tail flush fails, finalization is aborted (no record/close)
        so a respawn retries. ``delete_drained`` returning False means an
        append landed after drain -- retried up to
        ``_FINALIZE_DELETE_ATTEMPTS`` times, re-draining each time; a
        persistent failure retains the log as a bounded, non-lossy residual.
        """
        qm = self.queue_manager
        session_id = worker.session_id
        if not await self._drain_to_eof(worker, handlers):
            # Orphan: still registered, task about to finish. Recoverable --
            # a respawn or boot recover() re-enters _finalize_session. Stays
            # registered so orphaned_sessions() surfaces it on /status.
            logger.warning(
                "finalize_orphan session=%s reason=tail_flush_failed "
                "recoverable=respawn",
                session_id,
                extra={"session_id": session_id},
            )
            return

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
        # Reclaim disk (keep .dead.jsonl). delete_drained's return is
        # load-bearing: False means an append landed after drain -- retry below.
        for attempt in range(1, _FINALIZE_DELETE_ATTEMPTS + 1):
            if await qm.delete_drained(session_id):
                break
            logger.warning(
                "finalize_delete_retained session=%s attempt=%d/%d",
                session_id,
                attempt,
                _FINALIZE_DELETE_ATTEMPTS,
                extra={"session_id": session_id},
            )
            if attempt == _FINALIZE_DELETE_ATTEMPTS:
                # Give up: log retained, picked up by recover() or the sweep.
                logger.error(
                    "finalize_delete_gave_up session=%s retained_log=true "
                    "pickup=recover_sweep",
                    session_id,
                    extra={"session_id": session_id},
                )
                break
            if not await self._drain_to_eof(worker, handlers):
                # Permanent orphan: this session can never re-enter
                # _finalize_session, so nothing will retry it on its own.
                # Stays registered so orphaned_sessions() surfaces it on /status.
                logger.error(
                    "finalize_orphan session=%s reason=delete_retry_exhausted "
                    "permanent=true",
                    session_id,
                    extra={"session_id": session_id},
                )
                return  # late-tail flush failed: same semantics as the first pass
        await self._safe_close(worker)
        self._deregister(session_id)  # the LAST act -- no await after this
        logger.info(
            "session_finalized session=%s events=%d",
            session_id,
            worker.events_processed,
            extra={"session_id": session_id},
        )

    async def _safe_close(self, worker: SessionWorker) -> None:
        """Close the graph store. A worker whose store has been closed is
        never revived (see ``start_drain``'s guard) -- mark it FIRST, before
        the await, so there is no suspension point between "we began
        closing" and "it is marked"."""
        worker.store_closed = True
        try:
            await worker.services.graph.close()
        except Exception:
            logger.exception("graph.close failed for session %s", worker.session_id)

    @staticmethod
    def _task_failure(task: asyncio.Task) -> BaseException | None:
        """The exception a finished task died with, else None.

        None for a task that is still running, was cancelled, or returned
        cleanly. Checking ``cancelled()`` first is mandatory: ``task.exception()``
        RAISES ``CancelledError`` on a cancelled task.
        """
        if not task.done() or task.cancelled():
            return None
        return task.exception()

    def _on_drain_done(self, worker: SessionWorker, task: asyncio.Task) -> None:
        """The ONE supervision point for a finished drain task.

        Synchronous by asyncio contract, invoked via ``call_soon`` exactly
        once per task, and only ever AFTER the task is done -- so it can
        never race a live drainer.
        """
        exc = self._task_failure(task)
        if exc is None:
            return  # cancelled, or a clean return
        session_id = worker.session_id
        try:
            logger.error(
                "drain_worker_died session=%s",
                session_id,
                exc_info=exc,
                extra={"session_id": session_id},
            )
        finally:
            # Teardown must happen even if logging itself failed.
            self._deregister(session_id)  # sync; first, so revival unblocks
            try:
                close_task = asyncio.get_running_loop().create_task(
                    self._safe_close(worker), name=f"close-{session_id}"
                )
            except RuntimeError:  # loop already closing at shutdown
                logger.warning(
                    "drain_worker_died_close_skipped session=%s",
                    session_id,
                    extra={"session_id": session_id},
                )
            else:
                # Hold a strong ref -- asyncio only keeps a weak one, so
                # without this the close task can be GC'd mid-execution.
                self._close_tasks.add(close_task)
                close_task.add_done_callback(self._close_tasks.discard)

    def start_drain(self, worker: SessionWorker) -> None:
        if worker.store_closed:
            # Spent store: draining through it would dead-letter good events.
            # The closer MUST also deregister, or this refuses the worker forever.
            return
        task = worker.task
        if task is not None:
            if not task.done():
                return  # live drainer -- nothing to do
            if self._task_failure(task) is not None:
                # Crashed; the done-callback owns teardown and will deregister.
                return
        # A previous, cleanly-finished task means this is a respawn, distinct
        # from a brand-new worker (task is None, logged by get_or_create).
        respawn = task is not None
        new_task = asyncio.create_task(
            self.drain_worker(worker), name=f"drain-{worker.session_id}"
        )
        new_task.add_done_callback(functools.partial(self._on_drain_done, worker))
        worker.task = new_task
        if respawn:
            logger.info(
                "drainer_respawned session=%s",
                worker.session_id,
                extra={"session_id": worker.session_id},
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
            # Respawn on every repeat event so a deregistered-but-not-yet-
            # revived worker comes back the moment traffic resumes.
            self.start_drain(self._workers[session_id])
            # Each session_id is owned by exactly one contributor. ERROR (not
            # WARNING) so monitoring surfaces a violation; ingest still proceeds.
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
            # Forced removal of a still-live task; the normal path is a graceful finalize.
            logger.info(
                "drain_worker_remove session=%s had_live_task=%s",
                session_id,
                True,
                extra={"session_id": session_id},
            )
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
        """Return workers still registered whose drain task has finished.

        Orphaned iff in ``_workers`` AND ``task.done()`` -- catches a tail-
        flush failure that returns early without deregistering, and any
        unhandled exception escaping the drain loop. Deterministic, no timer.
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
