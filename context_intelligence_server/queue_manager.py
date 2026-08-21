"""On-disk durable queue manager for the event-write pipeline.

Disk layout (one set of files per session, keyed by ``session_id``):

- ``<session_id>.log`` -- append-only, newline-terminated, opaque ``bytes``.
  Each line is one enqueued record. The log is REWRITTEN only via
  ``compact_committed_prefix``: the committed prefix is
  dropped and the log rebased to hold just its undrained tail. This is
  ALWAYS an atomic tmp + ``os.replace`` -- the log is never rewritten IN
  PLACE, and no path ever seeks-and-overwrites live bytes.
- ``<session_id>.offset`` -- a single integer: the byte position in the log
  that has been durably processed (committed). A missing offset file means 0.
- ``<session_id>.dead.jsonl`` -- append-only dead-letter records for batches
  that could not be processed after exhausting retries. Expired
  once log-less and older than ``dead_letter_retention_seconds``.
- ``<session_id>.log.compact.tmp`` -- transient tmp used ONLY by
  ``compact_committed_prefix``'s tail copy. Inert to every other reader,
  classifier and recovery pass (matches no other glob); a stray left by a
  crash mid-compaction is reaped by ``reclaim_orphans`` as
  ``orphan_compact_tmp``.

Durability note:
    Appends use a plain ``write()`` (no ``fsync``): bytes reach the OS page
    cache and survive a process crash, but not a power loss.

session_id contract:
    Every public method validates ``session_id`` and raises ``ValueError`` if
    it is empty or contains a path separator (``/`` or ``\\``) or a null byte.
    The ``session_id`` is used raw as the filename stem, so it must be a safe,
    single path component.

Framing invariant:
    One event == one contiguous byte range in the ``.log``, terminated by
    exactly one ``\\n``, for any payload size, under any concurrency, under
    cancellation, on any filesystem -- AS LONG AS EXACTLY ONE PROCESS WRITES
    THE QUEUE DIRECTORY. That precondition is not enforced by this module
    (see ``main.py``'s single-worker startup guard); a violation is a
    cross-process race no in-process lock can see.

    A second, narrower precondition this invariant depends on (v2.1 P1):
    a record's bytes contain no raw ``0x0A`` except the terminator this
    module appends. That is true today because every caller serializes with
    ``json.dumps(..., ensure_ascii=True)`` (the default) -- see
    ``main.py``'s event-ingest handler and this module's own
    ``dead_letter``. A future caller that violates P1 must revisit this
    module's torn-tail reasoning (queue_manager tests assert P1 on real
    record bytes).

    Correctness here does NOT depend on filesystem write atomicity
    (``O_APPEND`` is kept as defence in depth, not relied upon): each key's
    ``_KeyGuard.file_lock`` is a ``threading.Lock`` acquired and released by
    the worker thread that performs the write, so no event-loop event --
    cancellation included -- can release it mid-write.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import logging
import os
import threading
import time
from collections.abc import Callable, Coroutine, Iterator
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, TypeVar

from context_intelligence_server.config import get_settings

logger = logging.getLogger(__name__)

_T = TypeVar("_T")

# Fixed buffer size for streaming scans over a session ``.log`` (last-newline
# search and newline counting). Bounds boot-time and /status memory to O(chunk)
# instead of O(file): a durable log can be multi-GB (4.9 GB in the incident),
# and loading one into RAM just to count newlines is what drove ~44 GB RSS at
# startup. 1 MiB balances syscall count against per-scan memory.
_SCAN_CHUNK_BYTES = 1 << 20


@dataclass(frozen=True)
class Record:
    """One log record and the byte range the QUEUE assigned it.

    ``start``/``end`` are opaque cursor values PRODUCED BY THE QUEUE and only
    ever handed back to it (``commit``). Callers MUST NOT compute them and
    MUST NOT assume ``end - start == len(raw) + 1`` -- that relationship is
    the queue's private framing invariant (module docstring), not a public
    contract.
    """

    raw: bytes  # WITHOUT the terminator, exactly as ``lines`` is today
    start: int
    end: int


@dataclass(frozen=True)
class Batch:
    """A contiguous batch of log records read from a session's append-only log.

    Attributes:
        session_id: The session the records belong to.
        records: Queue-produced ``Record``s -- each carries its own opaque
            ``start``/``end`` cursor. The queue produces these offsets; a
            caller (the registry) only ever hands them back via ``commit``.
        start_offset: Byte position in the log where this batch begins.
        end_offset: Byte position in the log AFTER the last returned record.
            This is the value passed to ``commit``. When no complete records
            are available, ``end_offset == start_offset``.
    """

    session_id: str
    records: list[Record]
    start_offset: int
    end_offset: int

    @property
    def lines(self) -> list[bytes]:
        """Raw record payloads, terminator-stripped -- the pre-Record view.

        Derived from ``records`` so the two can never disagree. Retained
        because ~90 call sites across main.py and 12 test files read it.
        """
        return [r.raw for r in self.records]


class Verdict(str, Enum):
    """Boot-safety classifier verdict.

    RESUMABLE     -- keep; a drainer can/should be dispatched for this key.
    UNRESUMABLE   -- delete: the `.log` cannot reach a drainer at all, or has
                     nothing left to persist that a reset wouldn't re-derive.
    DRAINED       -- delete: fully committed, nothing left to persist
                     (`fully_drained`).
    RESET_OFFSET  -- delete the `.offset` ONLY; the `.log` re-drains from
                     byte 0 (bounded re-drain, gated on size + an empty
                     `.dead.jsonl`).
    KEEP          -- keep, counted (not resumed, not deleted): either an
                     inert-but-harmless bucket (`bad_offset_with_dead`,
                     `unclassifiable`) or a genuinely unowned decision left
                     to a later pass.
    UNREADABLE    -- keep, counted as `failed`: a transient FS error, NOT a
                     corruption finding, and must never be laundered into a
                     deletion.
    """

    RESUMABLE = "resumable"
    UNRESUMABLE = "unresumable"
    DRAINED = "drained"
    RESET_OFFSET = "reset_offset"
    KEEP = "keep"
    UNREADABLE = "unreadable"


@dataclass(frozen=True)
class Classification:
    """One `classify_session` verdict. Produced with NO side effects.

    ``size`` is the `.log` `st_size` AT CLASSIFY TIME (0 when the log is
    missing/vanished) -- ``reclaim`` re-`stat()`s inside its guarded body and
    refuses to apply if the live size has drifted (the real check is size
    DRIFT, not ``size > committed``, because a RESET_OFFSET's `.offset` is
    corrupt by definition and cannot be read to produce a `committed` to
    compare against).

    ``dead_empty`` is whether `.dead.jsonl` was absent/0-byte at classify
    time. ``fallback_source`` is ``"byte0"`` or
    ``"sentinel"`` ONLY when ``reason == "fallback_workspace"``, else
    ``None`` -- kept separate from ``reason`` so the two
    fallback populations are counted distinctly on `/status`.
    """

    key: str
    verdict: Verdict
    reason: str  # one token from a closed vocabulary; "" for plain RESUMABLE
    size: int
    dead_empty: bool
    fallback_source: str | None = None


# A bad-offset log at/below this many
# bytes is RESET (re-drained from byte 0) rather than deleted outright.
# Read from Settings so it stays a single, operator-overridable config knob
# rather than a second hardcoded constant.
def _reclaim_redrain_max_bytes() -> int:
    return get_settings().reclaim_redrain_max_bytes


@dataclass
class _KeyGuard:
    """Everything that serializes access to ONE worker key's files.

    Two locks and one counter, three DIFFERENT jobs -- do not collapse them:

    ``file_lock`` is the CORRECTNESS lock for the BYTES. It is acquired and
    released by the WORKER THREAD that performs the write, in a ``finally``
    in that same thread. No event-loop event can release it. This is what
    makes the framing invariant true under cancellation: a ``CancelledError``
    unwinds coroutine frames on the event-loop thread, and none of those
    frames hold this lock.

    ``admission`` is a RESOURCE lock, not a correctness lock. It caps the
    number of worker threads this process will dispatch for one key at 1, so
    a burst on the busiest key cannot occupy the whole default executor
    (which ``read_batch``/``commit``/``spool_stats`` also share). If it were
    removed, the file would still never tear -- writes would just queue as
    blocked threads instead of as parked coroutines.

    ``waiters`` is the CORRECTNESS counter for the OBJECT'S LIFETIME. IT IS
    LOAD-BEARING -- ``delete_drained`` branches on it (v2.1 G1). It is the
    exact number of coroutines that currently hold a reference to this guard,
    including any parked on ``admission``. Removing a guard while a coroutine
    still references it lets that coroutine write under THIS ``file_lock``
    while a newcomer writes the SAME file under a fresh one -- two disjoint
    locks on one file, which reproduces the torn/merged-line corruption. It is
    incremented and decremented ONLY by ``_guard``, only on the event-loop
    thread, and never across an ``await`` boundary; on a single event loop it
    is therefore exact, not a heuristic. It doubles as the non-vacuous
    contention assertion in T2.
    """

    admission: asyncio.Lock
    file_lock: threading.Lock
    waiters: int = 0


async def _await_uninterrupted(coro: Coroutine[Any, Any, _T]) -> _T:
    """Await ``coro`` to completion even if THIS coroutine is cancelled.

    ``asyncio.to_thread`` cannot interrupt the OS thread it dispatched. If we
    returned as soon as a ``CancelledError`` arrived, ``append`` would return
    while its bytes were still in flight -- the caller would be told nothing
    definite, and the admission gate would open for the next writer.

    So: absorb cancellations, keep waiting, and re-raise the cancellation
    once the write has definitively succeeded or failed.

    NOTE ON RISK: this helper is resource hygiene and caller honesty. It is
    NOT what makes the framing invariant true -- ``_KeyGuard.file_lock`` is.
    If this helper were removed entirely, throughput and accounting would
    degrade; the file still would not tear.
    """
    task = asyncio.ensure_future(coro)
    cancelled: asyncio.CancelledError | None = None
    while True:
        try:
            result = await asyncio.shield(task)
            break
        except asyncio.CancelledError as exc:
            if task.done():
                raise  # the TASK was cancelled, not us
            cancelled = exc  # ours: remember it, keep waiting
        except BaseException:
            if cancelled is not None:
                raise cancelled from None  # teardown wins over the write's error
            raise
    if cancelled is not None:
        raise cancelled
    return result


class QueueManager:
    """Manages per-session append-only queues on disk."""

    def __init__(self, queues_dir: Path):
        self._dir = Path(queues_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._stats_cache: dict[str, Any] | None = None
        self._stats_cache_at: float = 0.0
        self._stats_cache_ttl: float = 1.0
        # Separate cache for spool_stats() (Change 2 / /status spool block).
        # A longer TTL than _stats_cache_ttl is fine here: spool_stats() is an
        # operator-facing "is the backlog growing" signal, not a
        # correctness-sensitive value, so a few extra seconds of staleness is
        # an acceptable trade for fewer directory scans under frequent
        # /status polling.
        self._spool_cache: dict[str, int] | None = None
        self._spool_cache_at: float = 0.0
        self._spool_cache_ttl: float = 5.0
        # One _KeyGuard per worker key that has been appended to and
        # not yet finalized-and-deleted. Created lazily by _guard(); removed
        # ONLY by delete_drained, under the admission lock, gated on identity
        # AND waiters == 1 (see _guard / delete_drained). No sweeper, no
        # timer, no refcount map, no eviction on the hot path.
        self._guards: dict[str, _KeyGuard] = {}

    @property
    def queues_dir(self) -> Path:
        """The queue directory this manager owns (for main._boot_reclaim's
        `*.log` glob, so it never needs to recompute the path from settings --
        it reads it from the SAME QueueManager instance registry.queue_manager
        already resolved, avoiding drift from a test/instance that points the
        registry's queue manager at a different directory)."""
        return self._dir

    def _log_path(self, session_id: str) -> Path:
        return self._dir / f"{session_id}.log"

    def _offset_path(self, session_id: str) -> Path:
        return self._dir / f"{session_id}.offset"

    def _dead_path(self, session_id: str) -> Path:
        return self._dir / f"{session_id}.dead.jsonl"

    def _compact_tmp_path(self, session_id: str) -> Path:
        """The tmp used by ``compact_committed_prefix``'s tail copy.

        Matches no glob any existing pass uses (``*.log``, ``*.offset``,
        ``*.offset.tmp``, ``*.dead.jsonl``, ``*.log.torn-*.bin``) -- a stray
        left by a crash between steps 3-5 is inert until ``reclaim_orphans``
        (``orphan_compact_tmp``) reaps it.
        """
        return self._dir / f"{session_id}.log.compact.tmp"

    def _read_committed_offset(self, session_id: str) -> int:
        try:
            text = self._offset_path(session_id).read_text("utf-8")
        except FileNotFoundError:
            return 0
        text = text.strip()
        return int(text) if text else 0

    @staticmethod
    def _last_complete_end(path: Path) -> int:
        """Byte position after the last complete line in ``path`` (0 if none).

        A torn trailing fragment (bytes after the final newline) is ignored:
        the returned offset is one past the last ``\\n``, or 0 when the file
        is missing or contains no complete line.

        Streams BACKWARD from EOF in fixed chunks to find the last ``\\n`` --
        O(tail) memory and I/O, never O(file). Path-based (not
        session-id-based) so it serves both ``.log`` files (via
        ``_complete_data_end``, a pure delegating refactor) and
        ``.dead.jsonl`` files (``heal_torn_tails``).
        """
        try:
            with open(path, "rb") as f:
                f.seek(0, os.SEEK_END)
                pos = f.tell()
                while pos > 0:
                    read_size = min(_SCAN_CHUNK_BYTES, pos)
                    pos -= read_size
                    f.seek(pos)
                    buf = f.read(read_size)
                    idx = buf.rfind(b"\n")
                    if idx != -1:
                        return pos + idx + 1
            return 0
        except FileNotFoundError:
            return 0

    def _complete_data_end(self, session_id: str) -> int:
        """Byte position after the last complete line of a session's ``.log``.

        Pure delegation to ``_last_complete_end`` -- no behaviour change from
        the pre-refactor inline version.
        """
        return self._last_complete_end(self._log_path(session_id))

    @staticmethod
    def _stream_newlines(path: Path, start: int = 0, end: int | None = None) -> int:
        """Count ``\\n`` bytes in ``path``'s byte range ``[start, end)`` -- streamed.

        ``end=None`` counts to EOF. Reads the range in fixed-size chunks
        (O(chunk) memory) instead of materialising the whole file (or a slice
        copy of it) in RAM, which is what ``read_bytes()`` +
        ``data[a:b].count(b"\\n")`` did on multi-GB spool files. Numerically
        identical to that slice-count for any range; a missing file counts 0.

        Path-based (not session-id-based) so it serves both ``.log`` scans
        (``_count_newlines``) and the whole-file ``.dead.jsonl`` count
        (``_count_dead``).
        """
        if end is not None and end <= start:
            return 0
        try:
            with open(path, "rb") as f:
                f.seek(start)
                remaining = None if end is None else end - start
                count = 0
                while True:
                    to_read = (
                        _SCAN_CHUNK_BYTES
                        if remaining is None
                        else min(_SCAN_CHUNK_BYTES, remaining)
                    )
                    if to_read <= 0:
                        break
                    buf = f.read(to_read)
                    if not buf:
                        break
                    count += buf.count(b"\n")
                    if remaining is not None:
                        remaining -= len(buf)
                return count
        except FileNotFoundError:
            return 0

    def _count_newlines(
        self, session_id: str, start: int = 0, end: int | None = None
    ) -> int:
        """Streamed newline count over a session ``.log``'s ``[start, end)``."""
        return self._stream_newlines(self._log_path(session_id), start, end)

    @staticmethod
    def _validate_session_id(session_id: str) -> None:
        if (
            not session_id
            or "/" in session_id
            or "\\" in session_id
            or "\0" in session_id
        ):
            raise ValueError(f"Invalid session_id: {session_id!r}")

    @contextlib.contextmanager
    def _guard(self, worker_key: str) -> Iterator[_KeyGuard]:
        """Get-or-create this key's guard AND register this coroutine as a holder.

        Get-or-create and the ``waiters`` increment are fused into ONE
        synchronous step with NO ``await`` between them, so an UNCOUNTED
        reference is not merely forbidden -- it is unwritable. The
        ``finally`` makes the decrement unforgettable. This is a race
        DELETED, not a race guarded against.

        CORRECTNESS-ORDERING REQUIREMENT (not a convenience): the increment
        MUST happen before the caller's first ``await``. Adding any yield
        point between the ``self._guards`` lookup and ``waiters += 1``
        reintroduces this race. Both statements are synchronous; keep them
        that way.

        Every guarded operation -- append, dead_letter, delete_drained,
        purge_dead_letters -- uses this. There are no exceptions.
        """
        guard = self._guards.get(worker_key)
        if guard is None:
            guard = _KeyGuard(asyncio.Lock(), threading.Lock())
            self._guards[worker_key] = guard
        guard.waiters += 1
        try:
            yield guard
        finally:
            guard.waiters -= 1

    @staticmethod
    def _write_all(fd: int, data: bytes) -> None:
        """Write ALL of ``data`` to ``fd``, looping over short writes.

        ``os.write`` may write fewer bytes than requested -- which is
        precisely what a network filesystem does with a multi-hundred-KB
        buffer -- so one call is an ATTEMPT, not a write. This loop is the
        code taking responsibility for what the storage layer does not
        promise.
        """
        view = memoryview(data)
        written = 0
        while written < len(view):
            n = os.write(fd, view[written:])
            if n == 0:  # never observed, but a 0 would spin forever
                raise OSError("os.write returned 0; refusing to spin")
            written += n

    @staticmethod
    def _discard_partial(fd: int, start: int, path: Path) -> None:
        """Undo a record whose write failed part-way. NEVER fails silently.

        Preferred: ``ftruncate`` the partial bytes away entirely -- the file
        returns to a clean line boundary and NOTHING downstream ever sees the
        failed record.

        Fallback, only if the truncate itself fails: terminate the fragment
        with a newline via the same short-write-safe loop, so at minimum the
        NEXT record cannot merge into it. Each failure is LOGGED at ERROR --
        this is the failure path of a corruption fix; a silent suppress here
        is the exact anti-pattern this whole issue is about.

        NOTE on the fallback: the newline-terminated ``<partial>\\n`` is a
        SYNTACTICALLY COMPLETE line, not a torn tail -- ``read_batch`` accepts
        it and ``heal_torn_tails`` will NOT quarantine it. That is safe: the
        write that produced it already raised ``OSError`` out to append()'s
        caller, so the event was never acknowledged; on the next drain the
        malformed JSON fails ``_parse_line`` and is correctly DEAD-LETTERED
        (offset advanced past it). Reached only on a compound OS failure
        (ftruncate fails AND the subsequent write succeeds).

        Final backstop if BOTH the truncate and the newline write fail: a
        genuine torn TAIL (no trailing newline), which every reader ignores
        and ``heal_torn_tails`` removes at next boot.
        """
        try:
            os.ftruncate(fd, start)
            return
        except OSError:
            logger.exception(
                "append_partial_truncate_failed path=%s start=%d",
                path,
                start,
            )
        try:
            QueueManager._write_all(fd, b"\n")
        except OSError:
            logger.exception(
                "append_partial_terminate_failed path=%s start=%d "
                "(torn tail left; heal_torn_tails will remove it at next boot)",
                path,
                start,
            )

    def _write_record(self, guard: _KeyGuard, path: Path, line: bytes) -> None:
        """Append ONE newline-terminated record as a contiguous byte range.

        RUNS IN A WORKER THREAD. Acquires ``guard.file_lock`` ITSELF -- the
        caller must NOT hold it, and must not try to release it. That is the
        whole point: the lock's lifetime is the thread's write, not the
        coroutine's await.

        ``O_APPEND`` is KEPT. The guard is what removes the RELIANCE on its
        atomicity; ``O_APPEND`` stays as defence in depth, because if a
        second writer ever appears it still positions every op at
        server-side EOF whereas seek-then-write would race on a stale
        position and OVERWRITE data.

        No ``fsync`` is issued: appends survive a process crash but not a
        power loss.
        """
        with guard.file_lock:
            flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND | getattr(os, "O_BINARY", 0)
            fd = os.open(path, flags, 0o644)
            try:
                start = os.fstat(fd).st_size  # sole writer: size cannot move under us
                try:
                    self._write_all(fd, line)
                except OSError:
                    self._discard_partial(fd, start, path)
                    raise
            finally:
                os.close(fd)

    @staticmethod
    def _heal_one(path: Path) -> tuple[int, bool]:
        """Heal one torn tail. Returns ``(bytes_discarded, healed)``.

        ORDERING IS LOAD-BEARING: copy the torn bytes to a quarantine
        sidecar, VERIFY the quarantine is byte-complete, THEN truncate --
        never the reverse, and never a truncate after a copy that raised or
        came up short. A partial or failed quarantine means the file is LEFT
        EXACTLY AS IT WAS: the torn tail stays, every reader already skips
        it, and the next boot retries. Truncating on an incomplete
        quarantine would destroy the only remaining copy of those bytes --
        strictly worse than leaving a tail that harms nobody.

        Raises ``OSError`` on ANY failure (quarantine write, verification
        mismatch, or truncate) -- the caller (``heal_torn_tails``) catches it
        per file so one bad file cannot fail the whole pass.
        """
        end = QueueManager._last_complete_end(path)
        size = path.stat().st_size
        if size <= end:
            return 0, False
        torn_bytes = size - end
        quarantine = path.with_name(f"{path.name}.torn-{time.time_ns()}.bin")

        with open(path, "rb") as src:
            src.seek(end)
            data = src.read()
        if len(data) != torn_bytes:
            raise OSError(
                f"short read quarantining {path}: expected {torn_bytes} bytes, "
                f"got {len(data)}"
            )

        flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_BINARY", 0)
        fd = os.open(quarantine, flags, 0o644)
        try:
            QueueManager._write_all(fd, data)
        finally:
            os.close(fd)

        written = quarantine.stat().st_size
        if written != torn_bytes:
            raise OSError(
                f"quarantine incomplete for {path}: wrote {written} of {torn_bytes} bytes"
            )

        # ONLY now, with a verified-complete quarantine on disk, is it safe
        # to shorten the original file.
        os.truncate(path, end)
        return torn_bytes, True

    async def heal_torn_tails(self) -> dict[str, int]:
        """ONE-TIME, BOUNDED startup pass: truncate every queue file back to
        its last complete line, quarantining the removed bytes.

        Runs ONCE per boot, BEFORE ``recovery_reconcile_dead``, BEFORE
        ``recovery_seed_counts``, BEFORE ``recover()`` respawns any drainer --
        so no reader and no writer is live while it runs. It is the ONLY
        place a queue file is ever shortened.

        For each ``*.log`` and ``*.dead.jsonl``, INDEPENDENTLY and
        FAULT-ISOLATED (v2.1 G3): a per-file failure is logged at ERROR and
        the file is skipped -- healing continues for the rest. THIS METHOD
        MUST NOT RAISE. Its caller is a lifespan hook, and a raise there is a
        restart loop on the very share being healed. Even the directory scan
        itself is guarded the same way, so an unreadable queue directory
        degrades to "healed nothing" rather than a boot loop.

        ``size == 0``, or a file with no newline at all (``end == 0``), are
        handled by the same arithmetic -- a 0-byte file is never touched
        (``0 > 0`` is false), and a newline-free file truncates to empty
        (correct: it contains no complete record). There is NO end-relative
        seek anywhere in this pass.

        Returns ``{"files_healed": n, "bytes_discarded": b, "files_failed":
        f}`` for the boot log. BOOT PROCEEDS EITHER WAY.
        """

        def _heal_all() -> dict[str, int]:
            files_healed = 0
            bytes_discarded = 0
            files_failed = 0
            try:
                paths = sorted(self._dir.glob("*.log")) + sorted(
                    self._dir.glob("*.dead.jsonl")
                )
            except OSError:
                logger.exception("heal_torn_tails_scan_failed dir=%s", self._dir)
                return {
                    "files_healed": 0,
                    "bytes_discarded": 0,
                    "files_failed": 0,
                }
            for path in paths:
                try:
                    discarded, healed = self._heal_one(path)
                except OSError:
                    files_failed += 1
                    logger.exception("torn_tail_heal_failed path=%s", path)
                    continue
                if healed:
                    files_healed += 1
                    bytes_discarded += discarded
                    logger.warning(
                        "torn_tail_healed path=%s discarded=%d",
                        path,
                        discarded,
                    )
            result = {
                "files_healed": files_healed,
                "bytes_discarded": bytes_discarded,
                "files_failed": files_failed,
            }
            logger.info("heal_torn_tails result=%s", result)
            if files_failed:
                logger.error("heal_torn_tails_incomplete files_failed=%d", files_failed)
            return result

        return await asyncio.to_thread(_heal_all)

    async def append(self, session_id: str, raw: bytes) -> None:
        """Durably append one record to ``session_id``'s ``.log``.

        Framing invariant (module docstring) holds under any concurrency,
        under cancellation, and regardless of filesystem write atomicity --
        see ``_KeyGuard.file_lock`` and ``_write_record``.
        """
        self._validate_session_id(session_id)
        line = raw if raw.endswith(b"\n") else raw + b"\n"  # unchanged (was :186)
        path = self._log_path(session_id)
        # ``_guard`` registers this coroutine as a reference-holder
        # SYNCHRONOUSLY, before the first await. That ordering is
        # load-bearing (v2.1 G1) -- ``delete_drained`` reads ``waiters`` to
        # decide whether the guard may be discarded, and a reference taken
        # after an await would be invisible to it.
        with self._guard(session_id) as guard:
            async with guard.admission:
                await _await_uninterrupted(
                    asyncio.to_thread(self._write_record, guard, path, line)
                )

    async def read_batch(self, session_id: str, max_items: int) -> Batch:
        self._validate_session_id(session_id)
        path = self._log_path(session_id)

        def _read() -> Batch:
            start = self._read_committed_offset(session_id)
            records: list[Record] = []
            consumed = 0
            try:
                with open(path, "rb") as f:
                    f.seek(start)
                    while len(records) < max_items:
                        raw = f.readline()
                        if not raw or not raw.endswith(b"\n"):
                            # EOF, or a torn trailing line with no newline yet:
                            # ignore the partial line and stop on a line boundary.
                            break
                        rec_start = start + consumed
                        consumed += len(raw)
                        rec_end = start + consumed
                        records.append(Record(raw[:-1], rec_start, rec_end))
            except FileNotFoundError:
                pass
            return Batch(session_id, records, start, start + consumed)

        return await asyncio.to_thread(_read)

    async def commit(self, session_id: str, new_offset: int) -> None:
        """Atomically and durably persist ``new_offset`` (the ack).

        Writes the offset to a temp file and uses ``os.replace`` for an atomic
        rename, so a reader never observes a torn or partial offset file. No
        ``fsync`` is issued: the offset survives a process crash but not a
        power loss.
        """
        self._validate_session_id(session_id)
        final = self._offset_path(session_id)
        tmp = self._dir / f"{session_id}.offset.tmp"

        def _commit() -> None:
            tmp.write_text(str(new_offset), encoding="utf-8")
            os.replace(tmp, final)

        await asyncio.to_thread(_commit)

    async def dead_letter(self, session_id: str, raw: bytes, error: str) -> None:
        """Append one dead-letter record for an unprocessable batch line.

        The original line is stored under ``payload`` as a UTF-8 string when it
        decodes cleanly; otherwise the raw bytes are stored base64-encoded under
        ``payload_b64`` (so non-UTF-8 payloads are never silently dropped). Each
        record also carries a ``ts`` (epoch seconds) and the ``error`` string.

        This is the dead-letter PRIMITIVE only. The poison-isolation POLICY
        (deciding WHEN to dead-letter a line) lives in the caller. The main
        ``.log`` and ``.offset`` files are untouched.

        Guarded by the SAME per-key ``_KeyGuard`` as ``append``: an
        unguarded write here is exactly as dangerous as an
        unguarded ``.log`` write, and is the trigger named for the b87417ef
        "fourth state".
        """
        self._validate_session_id(session_id)
        payload = raw[:-1] if raw.endswith(b"\n") else raw
        record: dict = {"ts": time.time(), "error": error}
        try:
            record["payload"] = payload.decode("utf-8")
        except UnicodeDecodeError:
            record["payload_b64"] = base64.b64encode(payload).decode("ascii")
        line = (json.dumps(record) + "\n").encode("utf-8")
        path = self._dead_path(session_id)
        with self._guard(session_id) as guard:
            async with guard.admission:
                try:
                    await _await_uninterrupted(
                        asyncio.to_thread(self._write_record, guard, path, line)
                    )
                except OSError:
                    # Previously ZERO logging here -- an OSError
                    # killed the drainer and showed only as a generic
                    # drain_worker_died with no hint the FAILING write was a
                    # dead-letter. LOGGING ONLY: re-raise unchanged so
                    # propagation behavior is identical to before this line.
                    # (traceback carries the exception; exc is not repeated.)
                    logger.exception("dead_letter_write_failed session=%s", session_id)
                    raise

    async def delete_drained(self, session_id: str) -> bool:
        """Remove the drained .log and .offset for a fully-finalized session.

        GUARDED and CONDITIONAL. Returns True if the files were removed (or
        were already both absent).

        Guarded: an unlink racing an in-flight append is how an event gets
        "appended successfully, then vanishes forever" -- worse than a
        dead-letter, because nothing detects the loss. The thread body takes
        ``guard.file_lock`` before unlinking, so it can never run while a
        write thread owns the fd.

        Conditional: even perfectly serialized, a naive caller could drain to
        EOF, THEN delete -- an append that arrives in between would be lost
        with no trace. The guard cannot fix that ordering by itself, so this
        method REFUSES to delete a log with uncommitted bytes and logs it.
        The caller (``_finalize_session``,
        registry.py) now RE-DRAINS on a False return and retries the delete,
        up to a bounded number of attempts, on this still-live drainer over
        this still-open store -- shrinking (not eliminating) the window. On
        give-up the session's retained files are found by ``recover()``
        (typically within the <=60s crash-recovery sweep, or the next POST
        for this session -- not merely "at next boot") and drained normally.
        Trade: a retained file (visible in spool_stats.pending_sessions)
        instead of a silent loss.

        A missing ``.log`` still unlinks a stale ``.offset``:
        leaving one behind would make a later-recreated log (the
        finalize-retry window above)
        start reading past its own end -- a silent, clean-looking loss.

        The .dead.jsonl is still intentionally KEPT. Idempotent as before.

        Guard removal (v2.1 G1): after the unlink, the entry is removed from
        the guard map ONLY if ``waiters == 1`` (this call is the only
        coroutine referencing the guard) AND the identity check passes.
        Otherwise another coroutine is holding a reference -- e.g. parked on
        ``admission`` -- and removing the entry would let it later write
        under a stale ``file_lock`` while a newcomer creates a fresh one:
        two disjoint locks on one file, the exact merged-line defect this
        whole change removes. Skipping removal in that case is safe and
        correct: the surviving coroutine simply reuses the same guard.
        """
        self._validate_session_id(session_id)
        log = self._log_path(session_id)
        offset = self._offset_path(session_id)

        def _delete(guard: _KeyGuard) -> bool:
            # Under guard.file_lock: this can never run while a write thread
            # for this key owns the fd. Acquired by THIS thread, not
            # the coroutine -- same discipline as _write_record.
            with guard.file_lock:
                try:
                    size = log.stat().st_size
                except FileNotFoundError:
                    # No log, but a stale .offset must not be left behind
                    # -- it would make a log recreated later
                    # start reading past its own end.
                    try:
                        offset.unlink()
                    except FileNotFoundError:
                        pass
                    return True

                committed = self._read_committed_offset(session_id)
                if size > committed:
                    logger.warning(
                        "delete_drained_retained session=%s uncommitted_bytes=%d",
                        session_id,
                        size - committed,
                    )
                    return False

                try:
                    log.unlink()
                except FileNotFoundError:
                    pass
                try:
                    offset.unlink()
                except FileNotFoundError:
                    pass
                return True

        with self._guard(session_id) as guard:
            async with guard.admission:
                ok = await _await_uninterrupted(asyncio.to_thread(_delete, guard))
                # Still holding admission: apply the three-part removal
                # condition. waiters == 1 is THIS call itself;
                # anything higher means another coroutine holds the guard
                # and removal must be skipped.
                if ok and guard.waiters == 1 and self._guards.get(session_id) is guard:
                    del self._guards[session_id]
        return ok

    async def compact_committed_prefix(
        self, session_id: str, min_prefix_bytes: int = 0, max_tail_bytes: int = 0
    ) -> int:
        """Rewrite ``<session_id>.log`` to hold only its undrained TAIL.

        The queue is a
        transient buffer, not an archive -- a still-OPEN session that has
        fully drained data must not keep the whole history on disk. This
        reclaims the already-committed PREFIX (bytes ``[0, C)``) while the
        session stays live, unlike ``delete_drained`` (which only ever
        removes the WHOLE file, and only at finalize).

        Returns the number of bytes reclaimed FROM THE FRONT of the log --
        i.e. ``C``, the committed prefix -- NOT the tail kept. ``0`` means
        nothing was reclaimed (nothing to do, the tail was too large, or the
        attempt failed). NEVER RAISES: the entire body is guarded so an
        ``OSError`` here can never kill a healthy drain loop.

        ``max_tail_bytes <= 0`` means no cap (explicit opt-out). Above 0, a
        tail larger than this is SKIPPED (not compacted) -- the threshold
        bounds how long ``guard.file_lock`` is held for one rewrite, not how
        often a rewrite happens (that is ``min_prefix_bytes``'s job).

        Same acquisition order as ``append``/``dead_letter``/
        ``delete_drained``/``reclaim``: ``_guard`` -> ``admission`` ->
        ``asyncio.to_thread`` -> ``guard.file_lock``. Guard-map removal is
        NOT performed here (contrast ``delete_drained``): the ``.log`` still
        exists and the session is live, so the guard must survive.

        Crash/failure ordering:
          1. Rebase ``.offset`` to ``0`` FIRST (tmp + ``os.replace`` --
             identical primitive to ``commit``). This is the point of no
             return: every crash window from here degrades to a bounded
             RE-DRIVE, never a loss.
          2. THEN ``os.replace`` the verified tail tmp over the ``.log``.
          3. If step 2 raises, RESTORE ``.offset := C`` (R3) -- a pure
             no-op, not an in-process re-drive (which would double-count
             ``written`` and drive the residual negative). If the restore
             itself also fails, log the honest ``redrive_expected=true``
             fallback rather than claim success.
        """
        self._validate_session_id(session_id)
        log = self._log_path(session_id)
        offset = self._offset_path(session_id)
        offset_tmp = self._dir / f"{session_id}.offset.tmp"
        tmp = self._compact_tmp_path(session_id)

        def _compact(_guard: _KeyGuard) -> int:
            with _guard.file_lock:
                try:
                    c = self._read_committed_offset(session_id)
                except (OSError, ValueError):
                    return 0
                try:
                    e = log.stat().st_size
                except OSError:
                    return 0

                # Step 2: bail (return 0) unless
                # C >= min_prefix_bytes and 0 < C <= E and the tail fits
                # under max_tail_bytes (<=0 means no cap).
                if not (c >= min_prefix_bytes and 0 < c <= e):
                    return 0
                tail = e - c
                if max_tail_bytes > 0 and tail > max_tail_bytes:
                    logger.debug(
                        "compact_skipped session=%s reason=tail_too_large "
                        "tail=%d limit=%d",
                        session_id,
                        tail,
                        max_tail_bytes,
                    )
                    return 0

                # Step 3: copy [C, E) into a fresh tmp. O_TRUNC so a stray
                # tmp from a previous attempt is never appended to.
                flags = (
                    os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_BINARY", 0)
                )
                try:
                    with open(log, "rb") as src:
                        src.seek(c)
                        fd = os.open(tmp, flags, 0o644)
                        try:
                            remaining = tail
                            while remaining > 0:
                                chunk = src.read(min(_SCAN_CHUNK_BYTES, remaining))
                                if not chunk:
                                    break
                                self._write_all(fd, chunk)
                                remaining -= len(chunk)
                        finally:
                            os.close(fd)
                except OSError:
                    logger.exception("compact_copy_failed session=%s", session_id)
                    with contextlib.suppress(OSError):
                        tmp.unlink()
                    return 0

                # Step 4: verify byte-complete BEFORE destroying anything --
                # _heal_one's ordering (quarantine, verify, only then act).
                try:
                    written = tmp.stat().st_size
                except OSError:
                    written = -1
                if written != tail:
                    with contextlib.suppress(OSError):
                        tmp.unlink()
                    logger.error(
                        "compact_aborted session=%s cause=short_copy "
                        "expected=%d got=%d",
                        session_id,
                        tail,
                        written,
                    )
                    return 0

                # Step 5: rebase the offset to 0 FIRST -- the point of no
                # return.
                try:
                    offset_tmp.write_text("0", encoding="utf-8")
                    os.replace(offset_tmp, offset)
                except OSError:
                    logger.exception(
                        "compact_offset_rebase_failed session=%s", session_id
                    )
                    with contextlib.suppress(OSError):
                        tmp.unlink()
                    return 0

                # Step 6: replace the log with the verified tail copy.
                try:
                    os.replace(tmp, log)
                except OSError:
                    # R3: restore the offset to C so this becomes a PURE
                    # NO-OP -- never an in-process re-drive (which would
                    # double-count `written` and drive the residual
                    # negative).
                    try:
                        offset_tmp.write_text(str(c), encoding="utf-8")
                        os.replace(offset_tmp, offset)
                        logger.error(
                            "compact_replace_failed session=%s committed=%d "
                            "action=offset_restored",
                            session_id,
                            c,
                        )
                    except OSError:
                        logger.error(
                            "compact_restore_failed session=%s committed=%d "
                            "redrive_expected=true",
                            session_id,
                            c,
                        )
                    with contextlib.suppress(OSError):
                        tmp.unlink()
                    return 0

                logger.info(
                    "queue_compacted session=%s reclaimed=%d tail=%d",
                    session_id,
                    c,
                    tail,
                )
                return c

        try:
            with self._guard(session_id) as guard:
                async with guard.admission:
                    reclaimed = await _await_uninterrupted(
                        asyncio.to_thread(_compact, guard)
                    )
        except Exception:
            # Precision 1: a blanket catch so an OSError mid-copy can never
            # escape into drain_worker and kill a healthy drainer.
            # asyncio.CancelledError derives from BaseException, so it still
            # propagates -- required, or the drainer's cancellation paths
            # would break.
            logger.exception("compact_failed session=%s", session_id)
            return 0

        if reclaimed:
            self._stats_cache = None
            self._spool_cache = None
        return reclaimed

    async def read_dead_letters(self, session_id: str) -> list[dict]:
        """Return all dead-letter records for ``session_id`` in append order.

        Returns an empty list when no dead-letter file exists. A malformed
        line is skipped (logged once, not per line) rather than raising --
        reached by ``GET /queues/dead-letter/{key}`` and the replay path, and
        a malformed record must not 500 an operator endpoint or abort a
        replay.
        """
        self._validate_session_id(session_id)

        def _read() -> list[dict]:
            try:
                text = self._dead_path(session_id).read_text(encoding="utf-8")
            except FileNotFoundError:
                return []
            records: list[dict] = []
            skipped = 0
            for ln in text.splitlines():
                if not ln.strip():
                    continue
                try:
                    records.append(json.loads(ln))
                except (
                    json.JSONDecodeError,
                    UnicodeDecodeError,
                    ValueError,
                    TypeError,
                ):
                    skipped += 1
                    continue
            if skipped:
                logger.warning(
                    "dead_letter_unparseable key=%s skipped=%d", session_id, skipped
                )
            return records

        return await asyncio.to_thread(_read)

    @staticmethod
    def _is_parseable_line(raw: bytes) -> bool:
        """Total: True iff ``raw`` is valid JSON. Never raises."""
        try:
            json.loads(raw)
        except (ValueError, TypeError):
            return False
        return True

    async def read_first_line(self, key: str) -> bytes | None:
        """Return the FIRST (byte-0) line of ``key``'s `.log`, or None.

        One `open` + one `readline` at offset 0 -- bounded, cheap. Returns
        None when the file is missing, empty, or its first line is not yet
        newline-terminated (torn/incomplete). Never raises (Q-13):
        this feeds the boot-path workspace-fallback resolution
        (``main._recover_one_session``), which must be a total function.
        """
        self._validate_session_id(key)
        path = self._log_path(key)

        def _read() -> bytes | None:
            try:
                with open(path, "rb") as f:
                    raw = f.readline()
            except OSError:
                return None
            if not raw or not raw.endswith(b"\n"):
                return None
            return raw[:-1]

        return await asyncio.to_thread(_read)

    async def classify_session(
        self,
        key: str,
        head_is_resumable: Callable[[bytes], bool],
    ) -> Classification:
        """Boot-safety classifier.

        Side-effect-free: reads only (never writes, truncates, or unlinks).
        Bounded I/O: `stat()` + the backward last-complete-end scan (already
        implemented) + one head `readline()` + (rarely) a byte-0 `readline()`
        + a bounded forward scan of at most `_SCAN_CHUNK_BYTES` -- never a
        whole-log read. MUST NOT RAISE: any OSError/ValueError this method
        cannot attribute to a specific, closed-set reason becomes
        ``Verdict.UNREADABLE`` -- its caller is a boot hook, and a raise
        there is a restart loop on the very share being read.

        ``key`` is a `.log` STEM: a log-less key (e.g. one with
        only a `.dead.jsonl`) is never passed here -- that population is
        ``reclaim_orphans``'s to own.
        """
        self._validate_session_id(key)
        log_path = self._log_path(key)
        dead_path = self._dead_path(key)
        threshold = _reclaim_redrain_max_bytes()

        def _dead_empty() -> bool:
            try:
                return dead_path.stat().st_size == 0
            except FileNotFoundError:
                return True  # absent .dead.jsonl counts as empty
            except OSError:
                # Cannot prove empty -> the conservative, non-destructive
                # answer is "not empty" (refuse RESET_OFFSET, fall to KEEP).
                return False

        def _bad_offset(reason: str, size: int, dead_empty: bool) -> Classification:
            if size <= threshold:
                if dead_empty:
                    return Classification(
                        key, Verdict.RESET_OFFSET, reason, size, dead_empty
                    )
                return Classification(
                    key, Verdict.KEEP, "bad_offset_with_dead", size, dead_empty
                )
            return Classification(key, Verdict.UNRESUMABLE, reason, size, dead_empty)

        def _classify() -> Classification:
            try:
                size = log_path.stat().st_size
            except FileNotFoundError:
                # A classify-time race (unlinked between the
                # `*.log` glob and this call) -- benign, not corruption.
                return Classification(key, Verdict.UNREADABLE, "log_vanished", 0, True)
            except OSError:
                return Classification(
                    key, Verdict.UNREADABLE, "unreadable_offset", 0, _dead_empty()
                )

            dead_empty = _dead_empty()

            try:
                committed = self._read_committed_offset(key)
            except OSError:
                return Classification(
                    key, Verdict.UNREADABLE, "unreadable_offset", size, dead_empty
                )
            except ValueError:
                return _bad_offset("unparseable_offset", size, dead_empty)

            if committed < 0:
                return _bad_offset("negative_offset", size, dead_empty)
            if committed > size:
                return _bad_offset("offset_past_eof", size, dead_empty)
            if size == 0:
                return Classification(
                    key, Verdict.UNRESUMABLE, "empty_log", size, dead_empty
                )

            complete_end = self._complete_data_end(key)
            if committed >= complete_end == size:
                return Classification(
                    key, Verdict.DRAINED, "fully_drained", size, dead_empty
                )
            if committed >= complete_end < size:
                # Un-newline-terminated remainder heal didn't reach this
                # boot (files_failed > 0) -- not our data to delete; heal
                # retries next boot. Not resumable YET by recover()'s own
                # predicate, but classify must not judge it un-resumable.
                return Classification(key, Verdict.RESUMABLE, "", size, dead_empty)

            # Head-record check (the common, 99% case): the first
            # UNCOMMITTED line, exactly what `_recover_one_session` parses.
            try:
                with open(log_path, "rb") as f:
                    f.seek(committed)
                    head_raw = f.readline()
            except OSError:
                return Classification(
                    key, Verdict.UNREADABLE, "unreadable_offset", size, dead_empty
                )
            if head_raw.endswith(b"\n") and head_is_resumable(head_raw[:-1]):
                return Classification(key, Verdict.RESUMABLE, "", size, dead_empty)

            # The head is unparseable/torn/lacks a workspace -- resume
            # with a FALLBACK workspace instead of deleting.
            # Step 10: byte 0 of the SAME file.
            try:
                with open(log_path, "rb") as f:
                    byte0_raw = f.readline()
            except OSError:
                byte0_raw = b""
            if byte0_raw.endswith(b"\n") and head_is_resumable(byte0_raw[:-1]):
                return Classification(
                    key,
                    Verdict.RESUMABLE,
                    "fallback_workspace",
                    size,
                    dead_empty,
                    fallback_source="byte0",
                )

            # Step 11: any parseable line within the first _SCAN_CHUNK_BYTES
            # from byte 0 (bounded -- never a whole-log read).
            window = min(size, _SCAN_CHUNK_BYTES)
            try:
                with open(log_path, "rb") as f:
                    buf = f.read(window)
            except OSError:
                buf = b""
            found_parseable = any(
                self._is_parseable_line(line) for line in buf.split(b"\n")[:-1]
            )
            if found_parseable:
                return Classification(
                    key,
                    Verdict.RESUMABLE,
                    "fallback_workspace",
                    size,
                    dead_empty,
                    fallback_source="sentinel",
                )

            # Step 12/13: bounding decides DELETE vs KEEP -- DELETE only when
            # the probe window provably covered the WHOLE file.
            if size <= _SCAN_CHUNK_BYTES:
                # `main._recover_one_session` is MORE LENIENT than
                # this classifier was -- it unconditionally sentinel-
                # dispatches whenever byte 0 is a COMPLETE (newline-
                # terminated) line, regardless of JSON parseability, because
                # the drainer dead-letters the unparseable head and drains
                # everything behind it. Deleting here would destroy data the
                # recovery path would have kept. DELETE is reserved for the
                # genuinely-unrecoverable case: no complete line at byte 0
                # at all (a torn-from-the-start log -- heal's territory, not
                # ours) -- which cannot co-occur with `complete_end >
                # committed >= 0` (already established above) but is kept as
                # a defensive, provably-safe fallback rather than assumed.
                if byte0_raw.endswith(b"\n"):
                    return Classification(
                        key,
                        Verdict.RESUMABLE,
                        "fallback_workspace",
                        size,
                        dead_empty,
                        fallback_source="sentinel",
                    )
                return Classification(
                    key, Verdict.UNRESUMABLE, "no_parseable_line", size, dead_empty
                )
            return Classification(key, Verdict.KEEP, "unclassifiable", size, dead_empty)

        return await asyncio.to_thread(_classify)

    async def reclaim(
        self,
        c: Classification,
        is_owned: Callable[[], bool],
    ) -> bool:
        """Apply a `classify_session` verdict: log-then-delete, or reset.

        Log-then-delete, IN THAT ORDER -- load-bearing: the
        `boot_reclaimed` audit line is emitted BEFORE any unlink, so a crash
        mid-unlink still leaves a record of the intent.

        Re-verifies, INSIDE the guarded body, immediately before applying
        (the server is concurrent with this
        pass, so a live event or a live drain can invalidate a classify-time
        snapshot before this runs):
          1. Ownership -- ``is_owned()`` is still False.
          2. Size DRIFT -- the `.log`'s live size still matches ``c.size``
             (NOT ``size > committed``: on the RESET_OFFSET path the
             `.offset` is corrupt by definition, so `committed` is
             uncomputable).
          3. For RESET_OFFSET only: `.dead.jsonl` is STILL empty.
        Any failure -> apply NOTHING, log `boot_reclaim_skipped_changed`,
        return False (re-classified next boot).

        MUST NOT RAISE: an OSError is logged as `boot_reclaim_failed` and
        returns False; the caller's pass continues to the next key.

        Only ``Verdict.UNRESUMABLE``/``Verdict.DRAINED`` (delete) and
        ``Verdict.RESET_OFFSET`` (reset) are actionable here; any other
        verdict is a caller error (nothing to reclaim) and returns False
        without touching disk.
        """
        self._validate_session_id(c.key)
        if c.verdict not in (
            Verdict.UNRESUMABLE,
            Verdict.DRAINED,
            Verdict.RESET_OFFSET,
        ):
            return False

        log = self._log_path(c.key)
        offset = self._offset_path(c.key)
        offset_tmp = self._dir / f"{c.key}.offset.tmp"
        dead = self._dead_path(c.key)
        action = "reset_offset" if c.verdict is Verdict.RESET_OFFSET else "delete"

        def _apply(guard: _KeyGuard) -> bool:
            with guard.file_lock:
                # Ownership, re-checked FRESH inside the guarded
                # body -- the registry owns live sessions; a session that acquired a
                # live worker after classify-time must never be reclaimed.
                if is_owned():
                    logger.warning(
                        "boot_reclaim_skipped_changed session=%s reason=%s cause=owned",
                        c.key,
                        c.reason,
                    )
                    return False
                try:
                    live_size = log.stat().st_size
                except FileNotFoundError:
                    live_size = 0
                except OSError:
                    logger.error(
                        "boot_reclaim_failed reason=%s path=%s session=%s error=stat_failed",
                        c.reason,
                        log,
                        c.key,
                    )
                    return False
                if live_size != c.size:
                    logger.warning(
                        "boot_reclaim_skipped_changed session=%s reason=%s cause=size_drift",
                        c.key,
                        c.reason,
                    )
                    return False
                if c.verdict is Verdict.RESET_OFFSET:
                    try:
                        dead_empty_now = dead.stat().st_size == 0
                    except FileNotFoundError:
                        dead_empty_now = True
                    except OSError:
                        dead_empty_now = False
                    if not dead_empty_now:
                        logger.warning(
                            "boot_reclaim_skipped_changed session=%s reason=%s "
                            "cause=dead_nonempty",
                            c.key,
                            c.reason,
                        )
                        return False

                logger.warning(
                    "boot_reclaimed reason=%s path=%s session=%s bytes=%d action=%s",
                    c.reason,
                    log,
                    c.key,
                    c.size,
                    action,
                )
                try:
                    if c.verdict is Verdict.RESET_OFFSET:
                        # Unlink ONLY the offset (+ any stray .offset.tmp) --
                        # the .log stays; the next drain re-reads from 0.
                        try:
                            offset.unlink()
                        except FileNotFoundError:
                            pass
                        try:
                            offset_tmp.unlink()
                        except FileNotFoundError:
                            pass
                    else:
                        # Unlink order is load-bearing: .log -> .offset ->
                        # .offset.tmp. A crash after the .log unlink leaves
                        # an orphan .offset, self-healed by the NEXT boot's
                        # reclaim_orphans; the reverse order would leave a
                        # .log with no .offset (committed==0) -- a full
                        # replay of data just judged un-resumable. Never.
                        try:
                            log.unlink()
                        except FileNotFoundError:
                            pass
                        try:
                            offset.unlink()
                        except FileNotFoundError:
                            pass
                        try:
                            offset_tmp.unlink()
                        except FileNotFoundError:
                            pass
                except OSError:
                    logger.exception(
                        "boot_reclaim_failed reason=%s path=%s session=%s",
                        c.reason,
                        log,
                        c.key,
                    )
                    return False
                return True

        with self._guard(c.key) as guard:
            async with guard.admission:
                ok = await _await_uninterrupted(asyncio.to_thread(_apply, guard))
                if (
                    ok
                    and c.verdict is not Verdict.RESET_OFFSET
                    and guard.waiters == 1
                    and self._guards.get(c.key) is guard
                ):
                    # The guard-removal condition, applied identically
                    # here: only when this call is the SOLE holder of the
                    # guard AND the identity check passes. A RESET_OFFSET
                    # leaves the .log in place, so its guard stays live.
                    del self._guards[c.key]
        if ok:
            self._stats_cache = None
            self._spool_cache = None
        return ok

    async def reclaim_orphans(
        self, before_ts: float, enabled: bool = True
    ) -> dict[str, int]:
        r"""One directory pass over log-less artifacts. `stat()`-only.

        Classifies `<key>.offset` / `<key>.offset.tmp` with NO `<key>.log`
        (`orphan_offset` / `orphan_offset_tmp`), `*.torn-*.bin` sidecars
        OLDER than ``before_ts`` (`torn_sidecar`) -- sidecars created by
        THIS boot's ``heal_torn_tails`` are kept, so a single boot never
        both quarantines bytes and destroys the quarantine -- and
        ``*.log.compact.tmp`` stragglers OLDER than
        ``before_ts`` (`orphan_compact_tmp`). UNLIKE the ``.offset``
        classes, a compact tmp is NOT gated on `<key>.log` being absent --
        a crash mid-compaction leaves the tmp beside a very much still-live
        ``.log``, so requiring a log-less key would never reap it. Age
        (``before_ts``) alone is the gate, same as ``torn_sidecar``: this
        pass runs before any drainer exists (phase ``reclaim``, before
        ``topup``), so a tmp from THIS boot's own compaction can never
        exist here in the first place -- the age check is defensive
        symmetry, not load-bearing.

        ``enabled`` mirrors the per-key ``reclaim`` path's own
        ``reclaim_enabled`` gate -- when False, EVERY candidate is only
        classified and LOGGED as a would-delete (``action=dry_run``); NO
        path is unlinked. When True, the candidate is unlinked for real,
        logged (``action=delete``), and counted. ``reclaim_orphans`` must
        never ignore ``reclaim_enabled`` and unlink unconditionally -- that
        would be an invisible delete every boot even under the safety
        default (``reclaim_enabled=False``). A real deletion must never be
        invisible: ``reclaimed``/``reclaimed_bytes`` only ever reflect
        actual unlinks, never dry-run candidates.

        MUST NOT RAISE (mirrors ``heal_torn_tails``'s directory-scan guard).
        """

        def _scan() -> dict[str, int]:
            reclaimed = 0
            reclaimed_bytes = 0
            failed = 0
            action = "delete" if enabled else "dry_run"
            try:
                offset_paths = sorted(self._dir.glob("*.offset"))
                tmp_paths = sorted(self._dir.glob("*.offset.tmp"))
                torn_paths = sorted(self._dir.glob("*.log.torn-*.bin"))
                compact_tmp_paths = sorted(self._dir.glob("*.log.compact.tmp"))
            except OSError:
                logger.exception("reclaim_orphans_scan_failed dir=%s", self._dir)
                return {
                    "reclaimed": 0,
                    "reclaimed_bytes": 0,
                    "failed": 0,
                }
            for path, reason in [
                *((p, "orphan_offset") for p in offset_paths),
                *((p, "orphan_offset_tmp") for p in tmp_paths),
            ]:
                stem = path.name[
                    : -len(".offset.tmp" if reason.endswith("tmp") else ".offset")
                ]
                if self._log_path(stem).exists():
                    continue
                try:
                    size = path.stat().st_size
                except OSError:
                    failed += 1
                    logger.exception(
                        "boot_reclaim_failed reason=%s path=%s", reason, path
                    )
                    continue
                logger.warning(
                    "boot_reclaimed reason=%s path=%s session=%s bytes=%d action=%s",
                    reason,
                    path,
                    stem,
                    size,
                    action,
                )
                if not enabled:
                    continue
                try:
                    path.unlink()
                    reclaimed += 1
                    reclaimed_bytes += size
                except OSError:
                    failed += 1
                    logger.exception(
                        "boot_reclaim_failed reason=%s path=%s", reason, path
                    )
            for path in torn_paths:
                try:
                    mtime = path.stat().st_mtime
                except OSError:
                    # Previously silent -- matches the
                    # neighbouring boot_reclaim_failed shape (the size-stat
                    # and unlink failures just below already log this way).
                    failed += 1
                    logger.exception(
                        "boot_reclaim_failed reason=torn_sidecar path=%s (mtime stat)",
                        path,
                    )
                    continue
                if mtime >= before_ts:
                    continue  # created by THIS boot's heal -- keep it
                try:
                    size = path.stat().st_size
                except OSError:
                    failed += 1
                    logger.exception(
                        "boot_reclaim_failed reason=torn_sidecar path=%s", path
                    )
                    continue
                logger.warning(
                    "boot_reclaimed reason=torn_sidecar path=%s session=%s "
                    "bytes=%d action=%s",
                    path,
                    path.name.split(".log.torn-")[0],
                    size,
                    action,
                )
                if not enabled:
                    continue
                try:
                    path.unlink()
                    reclaimed += 1
                    reclaimed_bytes += size
                except OSError:
                    failed += 1
                    logger.exception(
                        "boot_reclaim_failed reason=torn_sidecar path=%s", path
                    )
            for path in compact_tmp_paths:
                # NOT gated on
                # log-absence -- a stray tmp coexists with a very much
                # still-live `.log`. Its OWN suffix strip
                # (".log.compact.tmp", NOT the shorter ".offset.tmp" slice
                # length) so the audit line names the right session.
                try:
                    mtime = path.stat().st_mtime
                except OSError:
                    # Previously silent -- matches the
                    # neighbouring boot_reclaim_failed shape.
                    failed += 1
                    logger.exception(
                        "boot_reclaim_failed reason=orphan_compact_tmp "
                        "path=%s (mtime stat)",
                        path,
                    )
                    continue
                if mtime >= before_ts:
                    continue  # would-be THIS boot's own compaction -- keep it
                stem = path.name[: -len(".log.compact.tmp")]
                try:
                    size = path.stat().st_size
                except OSError:
                    failed += 1
                    logger.exception(
                        "boot_reclaim_failed reason=orphan_compact_tmp path=%s",
                        path,
                    )
                    continue
                logger.warning(
                    "boot_reclaimed reason=orphan_compact_tmp path=%s session=%s "
                    "bytes=%d action=%s",
                    path,
                    stem,
                    size,
                    action,
                )
                if not enabled:
                    continue
                try:
                    path.unlink()
                    reclaimed += 1
                    reclaimed_bytes += size
                except OSError:
                    failed += 1
                    logger.exception(
                        "boot_reclaim_failed reason=orphan_compact_tmp path=%s",
                        path,
                    )
            return {
                "reclaimed": reclaimed,
                "reclaimed_bytes": reclaimed_bytes,
                "failed": failed,
            }

        result = await asyncio.to_thread(_scan)
        if result["reclaimed"]:
            self._stats_cache = None
            self._spool_cache = None
        return result

    async def active_sessions(self) -> list[str]:
        """Return sorted session_ids with undrained data.

        A session is "active" when its committed offset is strictly less than
        the byte length of its ``.log`` file (i.e. there are appended bytes
        that have not yet been committed). Fully-committed sessions are
        excluded. The result is sorted by session_id.
        """

        def _scan() -> list[str]:
            result: list[str] = []
            for log in sorted(self._dir.glob("*.log")):
                session_id = log.stem
                # Fault-isolate PER KEY -- this is reachable
                # from an authenticated route (routers/queues.py), not just
                # boot, so the same corrupt-.offset asymmetry the boot paths
                # guard against applies here too.
                try:
                    if self._read_committed_offset(session_id) < log.stat().st_size:
                        result.append(session_id)
                except (OSError, ValueError):
                    logger.error("active_sessions_key_failed session=%s", session_id)
            return result

        return await asyncio.to_thread(_scan)

    async def recover(self) -> list[str]:
        """Return sorted session_ids that have a complete unprocessed line.

        A session is recoverable when its committed offset is strictly less
        than the end of its complete (newline-terminated) data, i.e. at least
        one whole line remains to be processed. A torn trailing line (bytes
        after the final newline) is ignored, so a session whose only remaining
        data is a partial line is NOT reported.

        This method is idempotent, safe on an empty directory, and performs no
        drainer logic; respawning drainers for the reported sessions is Phase
        B2.
        """

        def _scan() -> list[str]:
            result: list[str] = []
            for log in sorted(self._dir.glob("*.log")):
                session_id = log.stem
                # Fault-isolate PER KEY -- a corrupt/unreadable
                # `.offset` for one session (NUL-filled, negative,
                # non-numeric) must not raise out of a boot-path scan and
                # crash-loop the container. Skip just that key, log once.
                try:
                    committed = self._read_committed_offset(session_id)
                    if committed < self._complete_data_end(session_id):
                        result.append(session_id)
                except (OSError, ValueError):
                    # Cheap tightening: attach the traceback (was
                    # message-only) so a repeating corrupt-offset cause is
                    # visible on a boot-path scan.
                    logger.exception("recover_key_failed session=%s", session_id)
            return result

        return await asyncio.to_thread(_scan)

    def _count_dead(self, worker_key: str) -> int:
        """Count complete (newline-terminated) dead-letter lines for a key.

        Returns 0 when no dead-letter file exists. Dead-letter records are
        always written newline-terminated, so counting newlines yields the
        number of complete records.

        Streamed (bounded memory), not ``read_bytes()``: a .dead.jsonl is
        usually small but is NOT bounded -- a systematically-failing session
        dead-letters every line -- and this is called on the same boot and
        polled-/status paths as the .log scans.
        """
        return self._stream_newlines(self._dead_path(worker_key))

    def _all_worker_keys(self) -> list[str]:
        """Return the sorted union of ``.log`` and ``.dead.jsonl`` stems.

        ``Path.stem`` only strips the final suffix, so for ``s1.dead.jsonl`` it
        returns ``s1.dead``; the ``.dead.jsonl`` suffix is sliced explicitly to
        recover the bare worker key.
        """
        keys: set[str] = set()
        for log in self._dir.glob("*.log"):
            keys.add(log.stem)
        for dead in self._dir.glob("*.dead.jsonl"):
            keys.add(dead.name[: -len(".dead.jsonl")])
        return sorted(keys)

    async def derive_all_stats(self) -> dict[str, Any]:
        """Derive live queue stats purely from disk, with a short TTL cache.

        Returns an aggregate of per-worker ``in_queue`` (complete, uncommitted
        log lines) and ``dead`` (dead-letter records), plus ``in_queue_total``
        and ``dead_total``. No counters are stored: every value is derived from
        the files on disk.

        ``in_queue`` is computed with a TAIL READ -- seek to the committed
        offset and read only committed->EOF, then count newlines up to the last
        ``\\n`` (a torn trailing line has no newline and is not counted). The
        whole-file is never read. Results are cached for ``_stats_cache_ttl``
        seconds (monotonic clock) because ``/status`` polls every ~3s; the tail
        read plus the cache keep that path cheap under load.

        ``oldest_unflushed_age`` is deferred to C2 and is intentionally NOT
        computed or returned here.
        """
        now = time.monotonic()
        if (
            self._stats_cache is not None
            and (now - self._stats_cache_at) < self._stats_cache_ttl
        ):
            return self._stats_cache

        def _all() -> dict[str, Any]:
            per_key: list[dict[str, Any]] = []
            in_queue_total = 0
            dead_total = 0
            for worker_key in self._all_worker_keys():
                try:
                    committed = self._read_committed_offset(worker_key)
                except (OSError, ValueError):
                    # /status calls this (via pipeline_metrics); a corrupt or
                    # transiently-unreadable .offset must NOT 500 the health
                    # probe. Degrade to 0 for this key's stats -- mirroring the
                    # existing missing-file->0 convention in
                    # _read_committed_offset, and tending the conservation
                    # residual negative (benign, never a false `degraded`).
                    # Deliberately NO logging here: /status is polled, and a
                    # per-scan warning on a persistently-corrupt offset would
                    # flood the hot path. The visibility signal is the aggregate
                    # `spool.corrupt_offsets` field (see spool_stats()).
                    committed = 0
                # Streamed count of complete lines from committed -> EOF.
                # Equivalent to the old f.read() + data[:last_nl+1].count(b"\n")
                # (every b"\n" lies at or before the last one), but without
                # materialising the undrained tail -- which can be gigabytes
                # under a large backlog on this (polled) /status path.
                in_queue = self._count_newlines(worker_key, committed)
                dead = self._count_dead(worker_key)
                per_key.append(
                    {"worker_key": worker_key, "in_queue": in_queue, "dead": dead}
                )
                in_queue_total += in_queue
                dead_total += dead
            return {
                "per_key": per_key,
                "in_queue_total": in_queue_total,
                "dead_total": dead_total,
            }

        stats = await asyncio.to_thread(_all)
        self._stats_cache = stats
        self._stats_cache_at = now
        return stats

    async def spool_stats(self) -> dict[str, int]:
        """Cheap, aggregate-only spool footprint for the unauthenticated /status.

        Incident context: a durable spool silently grew to 38 GB across 583
        files (largest single file 4.9 GB) with ZERO signal anywhere that it
        was happening -- the only symptom was a graph that had stopped
        updating. This method exists so that number is always one field away.

        Returns exactly two aggregate integers:

        - ``pending_sessions``: count of worker keys with a ``.log`` file
          whose committed offset is strictly less than the file's size, i.e.
          there is unconsumed data (mirrors ``active_sessions()``'s
          definition, but via ``stat()`` instead of a full scan-and-compare
          pass, so it is safe to call on every /status hit).
        - ``spool_bytes_total``: total bytes on disk across EVERY file in the
          queue directory (``.log`` + ``.offset`` + ``.dead.jsonl``) -- the
          same number an operator would get from ``du`` on the spool
          directory, without shelling out.

        CHEAP BY CONSTRUCTION for the byte-total: this walks the directory
        and calls ``stat()`` on each entry to size ``spool_bytes_total`` --
        O(file count), NEVER O(file bytes). This is deliberately how a
        38 GB spool can be SIZED on every /status poll without walking
        38 GB of content.

        HONEST CAVEAT (D-3'): ``pending_sessions``/``corrupt_offsets``
        are NOT zero-cost -- computing them opens ONE ``.offset`` file per
        ``.log`` via ``_read_committed_offset`` (below). That is still
        O(files) opens of ~16 bytes each, never O(file CONTENT bytes) --
        unlike ``derive_all_stats()``, which tail-reads each log's undrained
        remainder to count pending lines -- but it is not the zero-``open()``
        claim an earlier revision of this docstring made. This keeps
        the method whole (does not split it) because it stays off the
        boot-window request path entirely (``/status`` is phase-gated, see
        ``main.get_status``); the honest bound is recorded here instead.
        On top of that, results are cached for ``_spool_cache_ttl`` seconds
        (monotonic clock) so a deployment with a very large number of spool
        files (thousands of sessions) still does not pay a full directory
        scan on every request.

        Per the /status aggregate-only contract: NO session ids, NO
        workspace names, and NO per-key table are returned or computable from
        this result -- two integers only.

        HEALTH-ENDPOINT SAFE: /status is the unauthenticated health probe (the
        ACA liveness surface). This method therefore MUST NOT be able to raise
        out to the /status handler -- an uncaught exception there becomes a 500,
        a failed health probe, and a container restart loop. Two degradation
        rules make that impossible:

        - A directory-level failure (the queue dir missing/unavailable -- e.g.
          an Azure Files SMB remount -- or any transient OS error while
          scanning) returns the degraded sentinel ``{-1, -1}`` instead of
          raising. Unlike every sibling reader, which uses ``glob()`` (empty on
          a missing dir), this scan uses ``iterdir()`` (raises on a missing
          dir), so the guard is mandatory, not cosmetic. The sentinel is NOT
          cached, so the very next poll re-scans and recovers the real numbers
          the moment the filesystem is healthy again.
        - A per-file failure (a raced delete, or a corrupt/unreadable
          ``.offset``) skips just that entry rather than failing the whole
          aggregate.

        A ``-1`` in either field is the operator-visible "spool footprint
        temporarily unavailable" signal -- distinct from a real ``0`` -- and
        never leaks any identifier.
        """
        now = time.monotonic()
        if (
            self._spool_cache is not None
            and (now - self._spool_cache_at) < self._spool_cache_ttl
        ):
            return self._spool_cache

        def _scan() -> dict[str, int]:
            spool_bytes_total = 0
            pending_sessions = 0
            corrupt_offsets = 0
            for entry in self._dir.iterdir():
                if not entry.is_file():
                    continue
                try:
                    size = entry.stat().st_size
                except FileNotFoundError:
                    # Raced with a concurrent delete_drained()/purge; the
                    # entry no longer exists -- simply exclude it, don't fail
                    # a cheap, best-effort aggregate over a live directory.
                    continue
                spool_bytes_total += size
                if entry.suffix == ".log":
                    try:
                        committed = self._read_committed_offset(entry.stem)
                    except ValueError:
                        # The .offset exists but is not a valid integer -- a
                        # GENUINELY corrupt offset. This is the one visibility
                        # signal for it (no logging anywhere, to avoid flooding
                        # the polled health path): surface it as an aggregate
                        # count on /status so `spool.corrupt_offsets > 0` is the
                        # operator's alarm. Count this file's bytes; skip its
                        # pending calc.
                        corrupt_offsets += 1
                        continue
                    except OSError:
                        # A transient/racing FS error reading the offset (NOT
                        # corruption): count bytes, skip pending calc, and do
                        # NOT inflate corrupt_offsets with a non-corruption cause.
                        continue
                    if committed < size:
                        pending_sessions += 1
            return {
                "pending_sessions": pending_sessions,
                "spool_bytes_total": spool_bytes_total,
                "corrupt_offsets": corrupt_offsets,
            }

        try:
            stats = await asyncio.to_thread(_scan)
        except (OSError, ValueError):
            # Queue dir missing/unavailable (e.g. Azure Files SMB remount) or a
            # transient FS error mid-scan. /status is the health probe and MUST
            # return 200 -- degrade to a sentinel and DO NOT cache it, so the
            # next poll retries immediately once the filesystem recovers. All
            # three fields are -1 = "temporarily unavailable" (distinct from a
            # real 0, and from a real corrupt_offsets count).
            return {
                "pending_sessions": -1,
                "spool_bytes_total": -1,
                "corrupt_offsets": -1,
            }

        self._spool_cache = stats
        self._spool_cache_at = now
        return stats

    async def dead_letter_keys(self) -> list[str]:
        """Return sorted worker keys that have a ``.dead.jsonl`` file.

        Keys with only main-log data (no dead-letter file) are excluded.
        ``Path.name`` is sliced by the ``.dead.jsonl`` suffix to recover the
        bare worker key (``Path.stem`` would only strip ``.jsonl``).
        """

        def _scan() -> list[str]:
            return sorted(
                dead.name[: -len(".dead.jsonl")]
                for dead in self._dir.glob("*.dead.jsonl")
            )

        return await asyncio.to_thread(_scan)

    async def purge_dead_letters(self, worker_key: str) -> int:
        """Delete the dead-letter file for ``worker_key`` and return the count.

        Counts the dead-letter records via ``_count_dead``, then unlinks the
        ``.dead.jsonl`` file. Returns the number of records removed (0 when no
        dead-letter file exists). Raises ``ValueError`` for an unsafe key.

        Deletion is routed exclusively through this method: callers must never
        touch the filesystem directly.

        Guarded by the key's ``_KeyGuard``: an unlink racing a
        ``dead_letter`` append from the drainer is the same class of hazard
        ``delete_drained`` guards against for the ``.log`` file.
        """
        self._validate_session_id(worker_key)
        path = self._dead_path(worker_key)

        def _purge() -> int:
            with guard.file_lock:
                count = self._count_dead(worker_key)
                try:
                    path.unlink()
                except FileNotFoundError:
                    pass
                return count

        with self._guard(worker_key) as guard:
            async with guard.admission:
                return await _await_uninterrupted(asyncio.to_thread(_purge))

    async def expire_dead_letters(
        self, now: float, retention_seconds: float, enabled: bool
    ) -> dict[str, int]:
        r"""Expire LOG-LESS dead-letter files older than ``retention_seconds``.

        A log-less ``.dead.jsonl`` is never auto-reclaimed by any other
        path. A key is expired iff:

          (a) ``<key>.log`` does NOT exist, AND
          (b) ``now - mtime(<key>.dead.jsonl) > retention_seconds``

        The log-less condition is a STRUCTURAL PROOF, not a heuristic: both
        boot mechanisms that consult ``.dead.jsonl``
        (``classify_session``'s ``_dead_empty()`` gate and
        ``recovery_reconcile_dead``) are only ever reachable for a key that
        STILL HAS a ``.log`` -- so a log-less dead file can never be one
        either depends on.

        Whole-file mtime (not per-line ``ts``) is used because ``dead_letter``
        only ever APPENDS -- mtime is the newest record's timestamp, so a
        file whose mtime is older than the window has EVERY line older than
        the window. A session that dead-letters one new line per hour never
        expires -- that is a property (an actively-failing session must stay
        visible), not a gap.

        ``retention_seconds <= 0`` disables expiry outright (returns zeros,
        no scan). ``enabled=False`` runs the FULL classification and emits
        the SAME audit line with ``action=dry_run``, but unlinks nothing --
        mirroring ``reclaim_orphans``'s contract: a real deletion must never
        be invisible, so ``expired_keys``/``expired_records``/
        ``expired_bytes`` only ever reflect ACTUAL unlinks, never dry-run
        candidates.

        Deletion routes exclusively through ``purge_dead_letters`` (never a
        direct filesystem call), per that method's own contract.

        MUST NOT RAISE (mirrors ``reclaim_orphans``'s directory-scan guard):
        this runs on the boot path and the periodic sweep tick, neither of
        which may crash-loop the server on a bad scan.
        """
        zeros = {
            "expired_keys": 0,
            "expired_records": 0,
            "expired_bytes": 0,
            "failed": 0,
        }
        if retention_seconds <= 0:
            return zeros

        def _scan() -> list[str]:
            candidates: list[str] = []
            for dead_path in sorted(self._dir.glob("*.dead.jsonl")):
                key = dead_path.name[: -len(".dead.jsonl")]
                if self._log_path(key).exists():
                    continue  # Never touch a key with a live .log
                try:
                    mtime = dead_path.stat().st_mtime
                except OSError:
                    continue
                if now - mtime > retention_seconds:
                    candidates.append(key)
            return candidates

        try:
            candidates = await asyncio.to_thread(_scan)
        except OSError:
            logger.exception("dead_letter_expire_scan_failed dir=%s", self._dir)
            return dict(zeros, failed=1)

        expired_keys = 0
        expired_records = 0
        expired_bytes = 0
        failed = 0
        action = "delete" if enabled else "dry_run"
        for key in candidates:
            dead_path = self._dead_path(key)
            try:
                size = dead_path.stat().st_size
                records = self._count_dead(key)
            except OSError:
                failed += 1
                logger.exception("dead_letter_expire_stat_failed key=%s", key)
                continue
            age_seconds = now - dead_path.stat().st_mtime
            logger.warning(
                "dead_letter_expired key=%s records=%d bytes=%d "
                "age_seconds=%.0f action=%s",
                key,
                records,
                size,
                age_seconds,
                action,
            )
            if not enabled:
                continue
            try:
                await self.purge_dead_letters(key)
            except (OSError, ValueError):
                failed += 1
                logger.exception("dead_letter_expire_purge_failed key=%s", key)
                continue
            expired_keys += 1
            expired_records += records
            expired_bytes += size

        if expired_keys:
            self._stats_cache = None
            self._spool_cache = None
        return {
            "expired_keys": expired_keys,
            "expired_records": expired_records,
            "expired_bytes": expired_bytes,
            "failed": failed,
        }

    async def recovery_seed_counts(self) -> tuple[int, int]:
        """Seed the conservation counters so residual == 0 by construction.

        Returns ``(accepted_seed, written_seed)`` to re-initialise the
        accepted/written conservation counters after a crash. Derived purely
        from disk so the invariant ``accepted == written + in_queue + dead``
        holds with a zero residual the instant the counters are seeded.

        Per worker key, from disk:

        - ``C`` = complete lines below the committed offset
        - ``P`` = complete lines between the committed offset and the end of
          complete data (== ``in_queue``)
        - ``D`` = dead-letter records

        Formula::

            written_seed  = max(0, C - D)
            accepted_seed = written_seed + P + D

        The ``max(0, ...)`` clamp is load-bearing. In a crash/replay window a
        dead-but-pending line (dead-lettered, but whose commit has not yet
        advanced past it) makes ``C - D`` go negative. The naive formula
        ``accepted = C + P`` / ``written = C - D`` yields a negative written
        count -- residual ``-1``, a false DEGRADED. Clamping written to zero
        and counting the line in BOTH ``P`` and ``D`` absorbs it into
        ``accepted_seed`` so the residual stays exactly zero.

        Ordering is load-bearing: this MUST run AFTER ``recovery_reconcile_dead``
        in the lifespan so the dead-letter counts it reads are already settled.
        """

        def _seed() -> tuple[int, int]:
            accepted = 0
            written = 0
            for key in self._all_worker_keys():
                # Fault-isolate PER KEY -- a corrupt `.offset` must
                # not crash the whole seed pass (which runs on every boot,
                # BEFORE drainers respawn). A skipped key contributes 0/0.
                # The WHOLE per-key body is guarded, not just the offset
                # read: a numerically-valid-but-corrupt offset (e.g.
                # negative) does not raise when READ, only later when used
                # as a seek() position in _count_newlines.
                try:
                    committed = self._read_committed_offset(key)
                    complete_end = self._complete_data_end(key)
                    dead = self._count_dead(key)
                    # Streamed newline counts over byte ranges -- numerically
                    # identical to the old data[:committed].count(b"\n") /
                    # data[committed:complete_end].count(b"\n"), but without
                    # loading the whole (possibly multi-GB) log at boot.
                    before = self._count_newlines(key, 0, committed)
                    pending = self._count_newlines(key, committed, complete_end)
                except (OSError, ValueError):
                    logger.error("recovery_seed_counts_key_failed key=%s", key)
                    continue
                written_seed = max(0, before - dead)
                accepted += written_seed + pending + dead
                written += written_seed
            return accepted, written

        return await asyncio.to_thread(_seed)

    def _dead_payload_set(self, worker_key: str) -> set[bytes]:
        """Return the set of original raw line bytes recorded as dead-letters.

        Each dead-letter record stores the original line (sans trailing
        newline) either as ``payload`` (a UTF-8 string) or ``payload_b64``
        (base64 of non-UTF-8 bytes). This mirrors ``dead_letter`` and rebuilds
        the raw line bytes so a reconcile pass can match them against pending
        log lines. Returns an empty set when no dead-letter file exists.

        A malformed line is skipped (not raised) -- this runs at startup via
        ``recovery_reconcile_dead``, and one bad line must not crash-loop the
        container.
        """
        try:
            text = self._dead_path(worker_key).read_text(encoding="utf-8")
        except FileNotFoundError:
            return set()
        payloads: set[bytes] = set()
        skipped = 0
        for ln in text.splitlines():
            if not ln.strip():
                continue
            try:
                record = json.loads(ln)
                if "payload" in record:
                    payloads.add(record["payload"].encode("utf-8"))
                elif "payload_b64" in record:
                    payloads.add(base64.b64decode(record["payload_b64"]))
            except (
                json.JSONDecodeError,
                UnicodeDecodeError,
                ValueError,
                TypeError,
                AttributeError,  # Q-5: a non-string `payload` (e.g. {"payload": 123})
                # raises AttributeError on `.encode()` -- this is a boot-path
                # total function; one bad record must not crash-loop the
                # container.
            ):
                skipped += 1
                continue
        if skipped:
            logger.warning(
                "dead_letter_unparseable key=%s skipped=%d", worker_key, skipped
            )
        return payloads

    async def recovery_reconcile_dead(self) -> int:
        """Advance committed offsets past leading already-dead pending lines.

        Closes the dead_letter->commit crash window. When the process
        crashes after a poison line was dead-lettered but before the commit
        advanced past it, the line remains pending in the ``.log``. A naively
        respawned drainer would re-read it, re-dead-letter it, and permanently
        corrupt the dead count. This pass steps the committed offset over each
        LEADING pending line whose raw bytes already appear in the dead-letter
        file, stopping at the first non-dead pending line.

        Per worker key with a ``.log`` and a non-empty dead-payload set, walk
        from the committed offset toward the end of complete data: for each
        leading line whose raw bytes are in the dead-payload set, advance past
        it (``skipped += 1``); stop at the first non-dead pending line. If the
        offset advanced, persist it atomically (tmp + ``os.replace``, mirroring
        ``commit``). Returns the total number of lines skipped across all keys.

        Covers both the crash window (dead_letter then crash before commit) and
        the replay window (re-append then crash before purge).

        Ordering is load-bearing: this MUST run ONCE at startup, BEFORE
        ``recovery_seed_counts`` and BEFORE drainers respawn.
        """

        def _reconcile() -> int:
            total_skipped = 0
            for key in self._all_worker_keys():
                # Q-6: check `.log` existence BEFORE reading the whole
                # `.dead.jsonl` into RAM (+ a payload set at ~3.6x its size).
                # A key with only a `.dead.jsonl` (the common shape left by
                # `delete_drained`) previously paid that read for nothing,
                # every boot, forever. Free fix.
                log_path = self._log_path(key)
                if not log_path.exists():
                    continue
                # Fault-isolate this key -- a corrupt/unreadable
                # dead-payload set or offset for ONE key must not abort the
                # reconcile pass for every other key. The boot
                # hook this feeds must never crash-loop the share it reads.
                try:
                    dead_payloads = self._dead_payload_set(key)
                    if not dead_payloads:
                        continue
                    committed = self._read_committed_offset(key)
                    complete_end = self._complete_data_end(key)
                    pos = committed
                    with open(log_path, "rb") as f:
                        f.seek(committed)
                        while pos < complete_end:
                            raw = f.readline()
                            if not raw or not raw.endswith(b"\n"):
                                break
                            if raw[:-1] not in dead_payloads:
                                break
                            pos += len(raw)
                            total_skipped += 1
                    if pos > committed:
                        final = self._offset_path(key)
                        tmp = self._dir / f"{key}.offset.tmp"
                        tmp.write_text(str(pos), encoding="utf-8")
                        os.replace(tmp, final)
                except (OSError, ValueError):
                    logger.exception("recovery_reconcile_dead_key_failed key=%s", key)
                    continue
            self._stats_cache = None
            return total_skipped

        return await asyncio.to_thread(_reconcile)
