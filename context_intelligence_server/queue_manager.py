"""On-disk durable queue for the event-write pipeline.

Per session ``<id>``: ``.log`` (append-only, ``\\n``-terminated records),
``.offset`` (committed byte position, missing == 0), ``.dead.jsonl`` (dead
letters).

Framing (one event == one ``\\n``-terminated byte range) holds only while a
single process writes the directory; each key's ``file_lock`` (a
``threading.Lock`` held on the writing thread) serialises its writes. Records
must contain no raw ``0x0A`` except the terminator. ``session_id`` is the raw
filename stem and is rejected if empty or containing a separator or null byte.
Appends are not ``fsync``ed: crash-durable, not power-loss-durable.
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
from collections.abc import Coroutine, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypeVar

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


@dataclass
class _KeyGuard:
    """Serializes access to one worker key's files.

    ``file_lock`` (``threading.Lock``): correctness lock for the bytes, held on
    the writing thread so no coroutine cancellation can release it mid-write.
    ``admission`` (``Semaphore(1)``): caps dispatched threads per key so one
    key cannot occupy the shared executor; not a correctness lock.
    ``waiters``: exact count of coroutines referencing this guard.
    ``delete_drained`` refuses to drop the guard while any remain, else two
    coroutines could lock the same file under different guards and tear it.
    """

    admission: asyncio.Lock
    file_lock: threading.Lock
    waiters: int = 0


async def _await_uninterrupted(coro: Coroutine[Any, Any, _T]) -> _T:
    """Await ``coro`` to completion even if this coroutine is cancelled.

    ``asyncio.to_thread`` cannot interrupt the OS thread it dispatched, so a
    cancellation is absorbed and re-raised only once the write has definitively
    succeeded or failed -- otherwise ``append`` would return with bytes still in
    flight. This is resource hygiene, not the framing guarantee (that is
    ``_KeyGuard.file_lock``).
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
        # Separate cache for spool_stats(). A longer TTL than _stats_cache_ttl
        # is fine here: spool_stats() is an
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

    def _log_path(self, session_id: str) -> Path:
        return self._dir / f"{session_id}.log"

    def _offset_path(self, session_id: str) -> Path:
        return self._dir / f"{session_id}.offset"

    def _dead_path(self, session_id: str) -> Path:
        return self._dir / f"{session_id}.dead.jsonl"

    def _read_committed_offset(self, session_id: str) -> int:
        """Committed byte offset. A missing or empty ``.offset`` reads 0."""
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
        ``_complete_data_end``) and ``.dead.jsonl`` files.
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
        """Byte position after the last complete line of a session's ``.log``."""
        return self._last_complete_end(self._log_path(session_id))

    @staticmethod
    def _stream_newlines(path: Path, start: int = 0, end: int | None = None) -> int:
        """Count ``\\n`` bytes in ``path``'s byte range ``[start, end)`` -- streamed.

        ``end=None`` counts to EOF. Reads in fixed-size chunks (O(chunk)
        memory, never O(file)); numerically identical to a full slice-count
        for any range. A missing file counts 0.

        Path-based (not session-id-based): serves both ``.log`` scans
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
        """Get-or-create this key's guard and register this coroutine as a holder.

        The lookup and the ``waiters`` increment are one synchronous step with
        no ``await`` between them, so an uncounted reference is impossible; the
        ``finally`` decrements. Keep both statements synchronous -- a yield
        point between them reintroduces the race. Every guarded operation uses
        this.
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
        """Newline-terminate a partial write; never truncates -- queue bytes are never removed."""
        try:
            QueueManager._write_all(fd, b"\n")
        except OSError:
            logger.exception(
                "append_partial_terminate_failed path=%s start=%d "
                "(torn fragment left unterminated; readers skip it, and the "
                "next append merges it into one poison line that the drainer "
                "dead-letters -- bytes are never silently removed)",
                path,
                start,
            )

    def _write_record(self, guard: _KeyGuard, path: Path, line: bytes) -> None:
        """Append one newline-terminated record as a contiguous byte range.

        Runs in a worker thread and acquires ``guard.file_lock`` itself, so the
        lock's lifetime is the thread's write, not the coroutine's await; the
        caller must not hold it. ``O_APPEND`` is kept as defence in depth (it
        positions every op at server-side EOF), but correctness rests on the
        guard, not on its atomicity. Not ``fsync``ed.
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

    async def append(self, session_id: str, raw: bytes) -> None:
        """Durably append one record to ``session_id``'s ``.log``.

        Framing invariant (module docstring) holds under any concurrency,
        under cancellation, and regardless of filesystem write atomicity --
        see ``_KeyGuard.file_lock`` and ``_write_record``.
        """
        self._validate_session_id(session_id)
        line = raw if raw.endswith(b"\n") else raw + b"\n"
        path = self._log_path(session_id)
        # ``_guard`` registers this coroutine as a reference-holder
        # SYNCHRONOUSLY, before the first await. That ordering is
        # load-bearing -- ``delete_drained`` reads ``waiters`` to
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

        Stores the line under ``payload`` (UTF-8) or ``payload_b64`` (raw
        bytes), plus ``ts`` and ``error``. Primitive only -- the caller decides
        when to dead-letter. Guarded by the same per-key ``_KeyGuard`` as
        ``append`` (an unguarded write here is as dangerous as an unguarded
        ``.log`` write); the ``.log``/``.offset`` files are untouched.
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
                    # LOGGING ONLY: re-raise unchanged so propagation is
                    # unaffected. Without this, a failed dead-letter write
                    # kills the drainer and surfaces only as a generic
                    # drain_worker_died, with no hint that the dead-letter
                    # write itself was the failing operation.
                    logger.exception("dead_letter_write_failed session=%s", session_id)
                    raise

    async def delete_drained(self, session_id: str) -> bool:
        """Remove the drained ``.log``/``.offset`` for a finalized session.

        Returns True if removed (or already both absent). Takes
        ``guard.file_lock`` before unlinking, so it can never race an in-flight
        append. Refuses (returns False) if the log still has uncommitted bytes;
        the caller re-drains and retries a bounded number of times, and
        ``recover()`` picks up any give-up. A missing ``.log`` still unlinks a
        stale ``.offset`` (else a recreated log reads past its own end). Keeps
        ``.dead.jsonl``. Idempotent.

        The guard-map entry is dropped only when ``waiters == 1`` and identity
        matches; otherwise a still-referencing coroutine could later lock a
        fresh guard over the same file and tear it.
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

        Aggregates per-worker ``in_queue`` (complete uncommitted lines) and
        ``dead`` (dead-letter records). ``in_queue`` is a tail read from the
        committed offset to EOF (the whole file is never read); results are
        cached for ``_stats_cache_ttl`` seconds since ``/status`` polls often.
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
                    # Must not 500 the health probe: degrade to 0, mirroring
                    # the missing-file->0 convention in
                    # _read_committed_offset. No logging here -- /status is
                    # polled; a persistently-corrupt offset would flood the
                    # hot path. Visible instead via the aggregate
                    # `spool.corrupt_offsets` field (see spool_stats()).
                    committed = 0
                # Streamed count of complete lines from committed -> EOF:
                # numerically equivalent to a full-file count, without
                # materialising a possibly multi-GB undrained tail.
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

        Returns two integers: ``pending_sessions`` (keys whose committed offset
        is below the ``.log`` size) and ``spool_bytes_total`` (bytes across all
        queue files). Sized via ``stat()`` per file -- O(file count), never
        O(bytes) -- and cached for ``_spool_cache_ttl`` seconds. No identifiers
        are returned or derivable.

        Must not raise (/status is the unauthenticated health probe): a
        directory-level failure returns the uncached sentinel ``{-1, -1}`` and a
        per-file failure skips that entry. ``-1`` means "temporarily
        unavailable", distinct from a real ``0``.
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
                        # Genuinely corrupt offset (not a valid int). No
                        # logging (polled health path); surfaced instead via
                        # the aggregate `spool.corrupt_offsets` count. Count
                        # this file's bytes; skip its pending calc.
                        corrupt_offsets += 1
                        continue
                    except OSError:
                        # Transient/racing FS error, not corruption: count
                        # bytes, skip pending calc, don't inflate
                        # corrupt_offsets.
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
            # Queue dir missing/unavailable, or a transient FS error mid-scan.
            # /status must return 200: degrade to an uncached sentinel so the
            # next poll retries once the filesystem recovers. -1 means
            # "temporarily unavailable", distinct from a real 0.
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

    async def recovery_seed_counts(self) -> tuple[int, int]:
        """Seed the conservation counters from disk so residual == 0.

        Returns ``(accepted_seed, written_seed)`` re-derived from disk so
        ``accepted == written + in_queue + dead`` holds immediately. Per key,
        with C=committed lines, P=pending lines, D=dead records:
        ``written_seed = max(0, C - D)``, ``accepted_seed = written_seed + P +
        D``. The clamp absorbs a dead-but-not-yet-committed line that would
        otherwise drive written negative (a false DEGRADED). Must run after
        ``recovery_reconcile_dead`` so the dead counts are settled.
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
                AttributeError,  # a non-string `payload` (e.g. {"payload": 123})
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

        Closes the dead-letter->commit crash window: a line dead-lettered but
        not yet committed past would otherwise be re-read and re-dead-lettered
        by a respawned drainer. Per key, steps the committed offset over each
        leading pending line whose bytes are already in the dead-letter file,
        stopping at the first non-dead line, and persists it atomically.
        Returns the total lines skipped. Must run once at startup, before
        ``recovery_seed_counts`` and before drainers respawn.
        """

        def _reconcile() -> int:
            total_skipped = 0
            for key in self._all_worker_keys():
                # Check `.log` existence BEFORE reading the whole
                # `.dead.jsonl` into RAM (+ a payload set at ~3.6x its size).
                # A key with only a `.dead.jsonl` (the common shape left by
                # `delete_drained`) has no log to reconcile, so skip the
                # expensive read entirely.
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
