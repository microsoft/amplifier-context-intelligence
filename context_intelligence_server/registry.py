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
from context_intelligence_server.neo4j_store import Neo4jGraphStore
from context_intelligence_server.pipeline import process_event, setup_handlers
from context_intelligence_server.queue_manager import Batch, QueueManager
from context_intelligence_server.services import HookStateService
from context_intelligence_server.status import EventRecord, ring_buffer

logger = logging.getLogger("context_intelligence_server")

_DRAIN_MAX_BATCH = 100
_DRAIN_POLL_INTERVAL = 0.05  # idle poll cadence; bounded by flush_timeout

# Bounded retry count for _finalize_session's
# delete-drained loop. A module constant, not a config knob -- this is not
# policy an operator would ever tune (KERNEL_PHILOSOPHY section 11); it only
# guarantees termination against an adversarial continuously-appending
# client. No backoff between attempts: sleeping would WIDEN the window this
# retry is trying to close.
_FINALIZE_DELETE_ATTEMPTS = 3

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
    # Set True by _safe_close, as its FIRST statement. A worker
    # whose store has been closed is never revived — see start_drain.
    store_closed: bool = False
    # True unless this worker was built
    # on the CRASH-RECOVERY path (get_or_create(..., recovered=True)). The
    # DEFAULT IS DELIBERATELY True — the fail-safe value that does NOT
    # auto-exit — so a directly-constructed worker (tests, future call
    # sites, any live POST) keeps today's behaviour; only an explicitly-
    # recovered worker becomes exit-eligible via drain_worker's dry-exit
    # block. Distinct from (and NOT derivable from) last_event_time: that
    # field is stamped on EVERY processed record including replayed legacy
    # ones, so it cannot tell "never saw a live POST" from "drained its
    # recovered backlog" — the exact leaked population.
    live_event_seen: bool = True
    # SCHEDULING state, not
    # byte state -- "a commit has landed for this session since the last
    # compaction attempt." Set True unconditionally after a non-terminal
    # commit (Trigger H) and by the exhausted-batch call site; cleared by
    # Trigger I immediately before it calls the queue. Exists solely so an
    # idle drainer does not re-attempt a no-op compaction every poll tick --
    # the registry never reads a byte value to set or clear it (the
    # queue-owns-all-byte-math boundary rule).
    compact_pending: bool = False


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
        # Strong references to fire-and-forget close-{sid} tasks
        # spawned by _on_drain_done. asyncio holds only a WEAK reference to a
        # running task; without this the close can be garbage-collected
        # mid-execution and the driver leaks anyway. Self-discards on done.
        self._close_tasks: set[asyncio.Task] = set()
        # Durable-ingest infrastructure, built lazily on first use. The
        # module-level registry singleton is constructed at import time,
        # before the per-test settings patch applies, so we cannot read
        # settings here — see _ensure_infra().
        self._queue_manager: QueueManager | None = None
        self._write_semaphore: asyncio.Semaphore | None = None
        self._max_delivery_attempts: int = 0
        # Live pipeline-conservation counters: make silently-dropped
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
    def queues_dir_path(self) -> Path:
        """The queue directory PATH, resolved WITHOUT constructing anything.

        Unlike ``queue_manager``, this NEVER calls ``_ensure_infra`` -- no
        QueueManager, no ``asyncio.Semaphore``, no ``mkdir``, no syscall of
        any kind. For observers (the writer-lease detector) that need to
        place a sibling artifact next to the queues but must not become the
        thing that builds the queue infrastructure: doing so off the event
        loop can race ``_ensure_infra``'s own unlocked check-then-construct
        and build TWO ``QueueManager`` instances for one directory -- the
        same torn/merged-line append corruption through a second door.

        Once a ``QueueManager`` exists, this returns the OWNER's own
        directory (never recomputed) so it can never diverge from what a
        test or operator live-reload has actually pointed the registry at.
        Before one exists, it evaluates the identical expression
        ``_ensure_infra`` will itself use to construct -- so the two can
        never disagree, and this converges on the owner's directory the
        moment a QueueManager is built by someone else.
        """
        if self._queue_manager is not None:
            return self._queue_manager.queues_dir
        return Path(get_settings().queues_path)

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
        """Assemble the pipeline-conservation health block for /status.

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

        Correctness of commit-after-flush depends on the GraphStore Protocol's
        flush-failure-isolation guarantee (graph_store.py guarantee #6) and
        the buffer being restored on failure (implementations' own
        responsibility, not this module's) plus the empty-buffer early return
        (guarantee #5). We rely on the Protocol contract here, not on any
        other module's private line numbers.
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
        that exhausts its retry budget is isolated ONE LINE AT A TIME by the
        EXISTING _handle_exhausted_batch path and dead-lettered (never
        silently dropped) -- Rule 1: poison lines and infra failures are two
        disjoint mechanisms, neither reachable from the other.

        Any OTHER unexpected exception PROPAGATES out of this coroutine. It
        is not swallowed, retried, counted, or backed off in-loop: the task
        dies loudly, and ``_on_drain_done`` (the task's done-callback,
        attached by ``start_drain``) is the SOLE supervision point -- it logs
        an ERROR (with the session id and traceback), closes the store, and
        deregisters the worker so the next event (or a boot ``recover()``)
        respawns it. Cancellation is handled locally at each site: the store
        is closed and the worker deregistered so a spent worker over a
        closed store is never revived (see ``start_drain``'s guard).

        A batch containing a terminal (``session:end``) record commits only
        UP TO that record (see ``_process_batch``'s ``terminal_at``), never
        past it -- the terminal record stays uncommitted so ANY later drain
        (a respawn, or a fresh boot's ``recover()``) re-reads it and
        re-enters ``_finalize_session``. "Ended but not finalized" is
        therefore a durable, re-derivable fact, not a fragile in-memory flag.

        The ONE rule this whole module enforces: the queue produces offsets
        (``Record.start``/``Record.end``); this registry only ever CHOOSES
        among them (which offset to commit to) and hands them back via
        ``qm.commit``/``qm.dead_letter`` -- it never computes a byte position
        itself. Every queue-side decision (``delete_drained``, ``recover()``,
        retain) keys off ``committed`` and needs to know nothing else. See
        the ``GraphStore`` Protocol (graph_store.py) for the store-side
        guarantees this loop depends on.

        When the log is idle the drainer polls and reaps the session if it
        has been idle past the stale timeout. The drainer is the SOLE flush
        trigger (process_event no longer self-flushes, Task 6).
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
                    # Trigger I (idle): placed BEFORE the dry-exit block below
                    # (not after it) so a BOOT-RECOVERED, no-terminal
                    # drainer compacts its committed prefix BEFORE it takes
                    # that exit -- otherwise a recovered drainer that never
                    # sees a live POST leaks its full log forever (the most
                    # operationally common shape of FINDING 1). This sits
                    # OUTSIDE the D-1 recheck-to-_deregister window (that
                    # window starts at the cheap pre-filter just below, not
                    # here), so it does not touch D-1's safety argument.
                    # `compact_pending` is cleared BEFORE the call -- a
                    # compaction that errors is not
                    # retried until the next commit re-arms it.
                    if worker.compact_pending:
                        worker.compact_pending = False
                        settings = get_settings()
                        if settings.queue_compact_enabled:
                            await qm.compact_committed_prefix(
                                session_id,
                                0,
                                settings.queue_compact_max_tail_bytes,
                            )
                    # Dry-exit: a
                    # recovered drainer over an already-drained log (no
                    # terminal record, so it can never reach the normal
                    # finalize path) must exit promptly instead of leaking
                    # forever -- this is what actually bounds the LIVE
                    # recovered-drainer population; the respawn ceiling only
                    # bounds DISPATCHES. Cheap pre-filter first (skips the
                    # extra read_batch on the overwhelming majority of live
                    # sessions); the RE-READ after the await is the
                    # load-bearing part (D-1): a POST landing during the
                    # await's suspension sets live_event_seen=True via
                    # get_or_create BEFORE append, so re-testing the flag
                    # AFTER the recheck -- with no await between the re-read
                    # and _deregister -- means a live client can never have
                    # the drainer pulled out from under it.
                    if not worker.live_event_seen:
                        recheck = await qm.read_batch(session_id, max_items=1)
                        if not recheck.records and not worker.live_event_seen:
                            await self._safe_close(worker)
                            self._deregister(session_id)
                            logger.info(
                                "recovered_drainer_exited session=%s reason=drained",
                                session_id,
                                extra={"session_id": session_id},
                            )
                            return
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
                            # key=value form (was "Reaping stale
                            # session %s (idle > %s seconds)") -- greppable,
                            # matches the dominant registry.py convention.
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
                    # A cancelled drain was previously invisible
                    # end-to-end -- INFO, not ERROR, since a cancel is
                    # normally deliberate (shutdown, idle reap of a peer,
                    # test teardown), not a failure.
                    logger.info(
                        "drain_worker_cancelled session=%s site=%s",
                        session_id,
                        "dispatch",
                        extra={"session_id": session_id},
                    )
                    await self._safe_close(worker)
                    self._deregister(session_id)  # C13/R-B: spent worker must
                    # be deregistered so get_or_create builds a FRESH one --
                    # else start_drain's store_closed guard refuses it forever.
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
                # Terminal batch: commit only UP TO session:end. Leaving that
                # record uncommitted is what makes "ended but not finalized"
                # DURABLE -- any later drain (respawn or boot recover()) re-
                # reads it and re-enters finalization. See spec section 3.
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

                if terminal_at is None:
                    # Trigger H (hot): placed AFTER record_written (not right
                    # after commit) so there is never an await between the
                    # offset advancing and the write being counted -- no
                    # transient positive-residual window. Guarded on
                    # `terminal_at is None`: a terminal batch goes straight
                    # to _finalize_session -> delete_drained, which removes
                    # the whole file, so compacting first is pure waste.
                    # `compact_pending` is set UNCONDITIONALLY -- it is
                    # scheduling state ("a commit landed"), not a byte
                    # comparison; the registry never inspects the queue's
                    # return value.
                    worker.compact_pending = True
                    settings = get_settings()
                    if settings.queue_compact_enabled:
                        await qm.compact_committed_prefix(
                            session_id,
                            settings.queue_compact_min_prefix_bytes,
                            settings.queue_compact_max_tail_bytes,
                        )

                if terminal_at is not None:
                    await self._finalize_session(worker, handlers)
                    return

            except asyncio.CancelledError:
                # The OUTER site -- cancelled while reading/idle
                # (never reaches the inner dispatch/flush try above).
                logger.info(
                    "drain_worker_cancelled session=%s site=%s",
                    session_id,
                    "loop",
                    extra={"session_id": session_id},
                )
                await self._safe_close(worker)
                self._deregister(session_id)  # C13/R-B: see above
                return

    @staticmethod
    def _parse_line(raw: bytes) -> tuple[str, str, dict[str, Any]]:
        """Decode an appended event line (raw EventRequest JSON)."""
        obj = json.loads(raw.decode("utf-8"))
        return obj["event"], obj.get("workspace", ""), obj.get("data", {})

    async def _process_batch(
        self, worker: SessionWorker, batch: Batch, handlers: Any
    ) -> tuple[int, int | None]:
        """Dispatch EVERY record in the batch; report the first terminal boundary.

        Returns ``(safe_count, terminal_at)``:
          * ``terminal_at`` -- the QUEUE-PRODUCED start offset of the FIRST
            terminal (session:end) record, or None when there was none.
          * ``safe_count``  -- how many records precede that boundary, i.e.
            how many the caller may count as written when it commits
            ``terminal_at``. Equals ``len(batch.records)`` when
            ``terminal_at`` is None.

        Every record is still DISPATCHED, so a
        session:end whose DISPATCH fails keeps going down the existing
        retry -> _handle_exhausted_batch isolation path (spec section 3.A.4).

        No byte position is computed here: offsets come from the queue
        (Record.start) and are only ever handed back to it (spec section 1.3).
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
    ) -> None:
        """Reprocess a poison batch ONE LINE AT A TIME (linear isolation).

        Each record is dispatched + flushed individually under the write
        semaphore. A record that still fails (parse error, handler error, or
        repeated flush failure) is dead-lettered with its error AND its write
        residue is discarded from the store buffer (COE blocker, decision #13);
        good records flush normally. Every record advances the offset to its
        own queue-produced end (commit), so the whole batch is accounted for.
        No silent loss, no binary shrink, no cross-line contamination.

        R2: ``wrote`` is set True only when this record's OWN flush actually
        succeeded, and ``record_written`` is only ever called when it is --
        the commit (which may itself fail and kill this coroutine, per Rule
        2) always happens outside the try/except, so a replay after a crash
        here can never double-count a record that was already counted.
        """
        qm = self.queue_manager
        session_id = worker.session_id
        # The failed BATCH flush left its writes resident in the store buffer
        # (GraphStore Protocol guarantee #6). Discard that accumulated residue
        # so the FIRST isolated record flushes from a clean buffer — otherwise
        # the poison record's residue contaminates record 1.
        worker.services.graph.discard_buffer()
        for rec in batch.records:
            wrote = False
            try:
                event, _ws, data = self._parse_line(rec.raw)
                await self._process_one(worker, event, data, handlers)
                await self._flush_barrier(worker)
                wrote = True
            except Exception as exc:
                await qm.dead_letter(session_id, rec.raw, str(exc))  # no re-framing
                # Cheap tightening: attach the traceback (was
                # message-only) so a repeating poison-line cause is visible.
                logger.warning(
                    "dead_letter session=%s error=%s",
                    session_id,
                    exc,
                    exc_info=exc,
                    extra={"session_id": session_id},
                )
                # COE blocker (decision #13): drop the failed record's residue
                # so it cannot contaminate the NEXT record's flush. A
                # successful flush clears the buffer itself; only the
                # failure path needs this.
                worker.services.graph.discard_buffer()
            await qm.commit(session_id, rec.end)  # queue-produced offset
            if wrote:
                self.record_written(1)

    async def _drain_to_eof(self, worker: SessionWorker, handlers: Any) -> bool:
        """Drain every remaining record for this session up to EOF.

        Returns True when the log is drained (committed == the last complete
        line). Returns False when a tail flush FAILED -- nothing was committed
        for that batch, and the caller must NOT finalize (panel finding #7:
        no tail loss).

        Extracted VERBATIM from
        _finalize_session so the finalize delete-retry can re-drain a late
        append without duplicating this logic and without re-recording the
        CompletedSession.
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
        """session:end seen: drain any tail records read-to-EOF, then record
        the CompletedSession, DELETE the drained logs, close the graph, and
        deregister -- in that order (Call B). Panel finding #7: if a tail
        flush fails, do NOT finalize — return without recording/closing so
        the drainer retries (no tail loss).

        Call B reorder (spec section 4): ``delete_drained`` runs BEFORE
        ``_safe_close``/``_deregister``, and ``_deregister`` is the LAST
        statement before the final log line, with NO ``await`` after it.
        Throughout ``delete_drained`` and ``_safe_close`` the worker is
        STILL REGISTERED, so a concurrent ``get_or_create`` takes the
        ``else:`` branch and ``start_drain`` no-ops against the still-live
        task -- a second drainer over the same log is structurally
        unreachable, not merely unlikely. ``delete_drained`` is gated on the
        QUEUE's own committed offset (queue_manager.py), which only ever
        advances after a successful flush -- the graph store is never
        consulted for this ordering to be correct.

        ``delete_drained``'s return value is
        LOAD-BEARING, not advisory. False means the in-lock guard
        (queue_manager.py:821-828) found uncommitted bytes -- an append
        landed in the window between the drain-to-EOF above and the unlink.
        The log is retained (never lost), but the late event is undrained.
        Re-drain it HERE, on this still-live drainer over this still-open
        store, and retry the delete, up to ``_FINALIZE_DELETE_ATTEMPTS``
        times. This SHRINKS the window (single-hit -> N-consecutive-hit); it
        does not eliminate it -- the non-loss guarantee comes from
        the in-lock guard, not from this retry.

        Two distinct residuals, both non-lossy:
          - Give-up after N consecutive window-hits (the loop's own
            ``break``): falls through to the unchanged ``_safe_close`` +
            ``_deregister`` teardown below. The retained log is
            recover()-reportable (only if the uncommitted tail
            is a COMPLETE line -- a torn tail heals at next boot instead) and
            is picked up by the next POST or the <=60s crash-recovery sweep.
          - A re-drain's OWN tail flush failure: fires
            AFTER CompletedSession was already appended, so this session can
            never re-enter _finalize_session (its committed offset is past
            session:end). ``return``-ing here (mirroring the FIRST pass's
            semantics) never reaches _safe_close/_deregister, so the worker
            stays registered with a now-completed task --
            ``orphaned_sessions()`` is the honest signal. delete_drained is
            never called again for this key: a bounded, PERMANENT-retention
            residual, non-lossy (the late event is still on disk,
            compaction can shrink the file toward ~0 bytes but never unlinks
            it), not a leak of un-persisted data.
        """
        qm = self.queue_manager
        session_id = worker.session_id
        if not await self._drain_to_eof(worker, handlers):
            # NAMES the orphan state this return leaves behind
            # (still registered, task about to finish) -- the underlying
            # flush failure itself is already logged inside _drain_to_eof.
            # Recoverable: a respawn (or a fresh boot's recover()) re-enters
            # _finalize_session and retries from here.
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
        # Panel finding #5: reclaim disk — a fully drained, finalized session no
        # longer needs its .log/.offset. Keep .dead.jsonl (retained dead-letter).
        # MOVED UP (Call B): still registered here, so a concurrent
        # get_or_create's start_drain no-ops against the still-live task.
        #
        # delete_drained's return value is LOAD-BEARING, not advisory.
        # False means the in-lock guard (queue_manager.py:821-828) found
        # uncommitted bytes -- an append landed in the window between the
        # drain-to-EOF above and the unlink. The log is retained (never lost),
        # but the late event is undrained. Re-drain it HERE, on this still-live
        # drainer over this still-open store, and retry the delete. The worker
        # stays REGISTERED throughout (Call B), so a concurrent
        # get_or_create's start_drain still no-ops against this live task --
        # a second drainer over the same log remains structurally unreachable.
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
                # Bounded give-up (docstring above): the log is RETAINED with a
                # complete uncommitted line, so recover() reports it and the
                # <=60s crash-recovery sweep (or the next POST for this
                # session) drains it. Non-lossy, but loud -- we are leaving a
                # file behind.
                logger.error(
                    "finalize_delete_gave_up session=%s retained_log=true "
                    "pickup=recover_sweep",
                    session_id,
                    extra={"session_id": session_id},
                )
                break
            if not await self._drain_to_eof(worker, handlers):
                # NAMES the PERMANENT-retention orphan
                # -- this session can never re-enter _finalize_session (its
                # committed offset is already past session:end), so unlike
                # the give-up-after-N-attempts path above, this residual is
                # never picked up by a respawn. ERROR,
                # not WARNING: nothing will retry this on its own.
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
        """The exception a FINISHED task died with, else None.

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
                # V-6: hold a strong reference until it finishes. asyncio only
                # keeps a WEAK reference to a running task; without this the
                # close can be garbage-collected mid-execution and the driver
                # leaks anyway -- defeating the point of closing it at all.
                self._close_tasks.add(close_task)
                close_task.add_done_callback(self._close_tasks.discard)

    def start_drain(self, worker: SessionWorker) -> None:
        if worker.store_closed:
            # Spent: the store was closed (cancellation, the idle reap, or
            # finalization). Draining through a closed store would
            # spuriously dead-letter good events.
            #
            # NB (R-B): for this claim to hold, the spent worker MUST also be
            # deregistered by whoever closed it -- otherwise get_or_create
            # takes the else: branch, finds this same spent worker, and
            # start_drain refuses it forever (session wedged, no drainer
            # until next boot, visible as orphaned:true). The done-callback
            # (_on_drain_done) deregisters; the TWO CancelledError handlers
            # in drain_worker MUST call self._deregister(session_id) beside
            # _safe_close (change C13) so the next get_or_create builds a
            # FRESH worker.
            return
        task = worker.task
        if task is not None:
            if not task.done():
                return  # live drainer -- nothing to do
            if self._task_failure(task) is not None:
                # Crashed; the done-callback owns teardown and will deregister.
                return
        # ``task is not None`` here means a PREVIOUS task existed
        # and finished (done, not crashed -- cancelled or a clean return) --
        # this is a genuine RESPAWN of an already-registered worker, distinct
        # from the brand-new-worker case (task is None), which get_or_create
        # already logs as ``drainer_spawned`` at its own call site.
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
        *,
        recovered: bool = False,
    ) -> SessionWorker:
        """Get-or-create the sticky drainer for ``session_id``.

        ``recovered`` (keyword-only, additive -- no existing call site
        changes) is the ONE place ``SessionWorker.live_event_seen`` is set.
        Pass ``recovered=True`` from the crash-recovery / sweep dispatch path
        so a worker that has NEVER seen a live POST can dry-exit once its
        recovered backlog drains (see ``drain_worker``). A live POST always
        omits it (default False), which marks the worker non-exit-eligible
        BEFORE its bytes exist on disk -- the strongest ordering for the
        drain loop's own race window.
        """
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
                live_event_seen=not recovered,
            )
            self.start_drain(self._workers[session_id])
            logger.info(
                "drainer_spawned session=%s",
                session_id,
                extra={"session_id": session_id},
            )
        else:
            # C12 (R-C): respawn on every repeat event, not just ones that
            # happen to carry a contributor id -- this is what lets a
            # deregistered-but-not-yet-revived worker (crash, cancellation,
            # idle-reap) come back to life the moment traffic resumes.
            # start_drain itself is the no-op guard when a drain is already
            # live or the store is closed and unrevivable.
            self.start_drain(self._workers[session_id])
            # Guarded by the PARAMETER, not the branch -- the sweep calls
            # get_or_create repeatedly for the SAME recovered session
            # (idempotent respawn). An unguarded assignment here would mark
            # every re-dispatched recovered worker as live. Only a call that
            # omits recovered (i.e. a real live POST) ever flips this True.
            if not recovered:
                self._workers[session_id].live_event_seen = True
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
            # A forced removal of a still-live drain task is
            # an extraordinary event (the normal path is a graceful
            # finalize) -- previously invisible end-to-end.
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

    def has_worker(self, session_id: str) -> bool:
        """The public read for boot-reclaim's Gate 1.

        Lets ``main._boot_reclaim`` check live ownership WITHOUT reaching
        into ``_workers`` and without paying O(n) per key over ``workers()``.
        """
        return session_id in self._workers

    def orphaned_sessions(self) -> list[SessionWorker]:
        """Return workers that are still registered but whose drain task has
        finished — the silent-stall signal for #278.

        A worker is orphaned iff it is in _workers AND its task has completed
        (task.done()). This catches the finalization-path orphan (a tail flush
        failure returns early without deregistering, so the task completes but
        the worker is never removed) and any unhandled exception that escapes
        the drain loop. Deterministic and instant — no timer, no threshold.

        A finalize-orphan can now ALSO
        arise from a late-tail flush failure inside the delete-retry loop,
        AFTER the CompletedSession was already recorded. Because that session
        can never re-enter _finalize_session (its committed offset is past
        session:end), this is a bounded PERMANENT-retention residual — this
        is the honest signal for it, distinct from the give-up-after-N-
        attempts path (which still deregisters normally and is picked up by
        recover()'s <=60s sweep instead).
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
