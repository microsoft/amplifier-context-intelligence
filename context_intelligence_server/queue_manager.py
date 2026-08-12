"""On-disk durable queue manager for the event-write pipeline.

Disk layout (one set of files per session, keyed by ``session_id``):

- ``<session_id>.log`` — append-only, newline-terminated, opaque ``bytes``.
  Each line is one enqueued record. The log is never rewritten in place.
- ``<session_id>.offset`` — a single JSON record, written atomically:
  ``{"v": 1, "offset": <int>, "cursor": <dict | null>}``. ``offset`` is the
  byte position in the log that has been durably processed (committed);
  ``cursor`` is an opaque snapshot of cross-handler session state
  (``HookStateService.snapshot_cursor()``), persisted so a worker rebuild
  (crash restart or stale-session reap) restores its in-memory cursor
  instead of resetting it. The cursor is written INSIDE the same atomic
  ``os.replace`` as the offset — never as a separate file — so the two can
  never skew relative to each other: a crash mid-write loses both together,
  and a successful write always carries a cursor consistent with its offset.
  Legacy files whose content is a bare integer (pre-upgrade shape) are still
  accepted on read and yield ``cursor = None``; the next commit rewrites the
  file in the new JSON form. A missing offset file means offset 0, cursor
  ``None``.
- ``<session_id>.dead.jsonl`` — append-only dead-letter records for batches
  that could not be processed after exhausting retries.

Durability note:
    Appends use a plain durable ``write()``. This gives PROCESS-crash
    durability (the bytes are handed to the OS page cache and survive a
    process crash). POWER-LOSS durability via ``fsync`` is deliberately
    deferred to Phase B3 (fsync group-commit). This applies equally to the
    offset+cursor record: no ``fsync`` is issued on that write either.

session_id contract:
    Every public method validates ``session_id`` and raises ``ValueError`` if
    it is empty or contains a path separator (``/`` or ``\\``) or a null byte.
    The ``session_id`` is used raw as the filename stem, so it must be a safe,
    single path component.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# Fixed buffer size for streaming scans over a session ``.log`` (last-newline
# search and newline counting). Bounds boot-time and /status memory to O(chunk)
# instead of O(file): a durable log can be multi-GB (4.9 GB in the incident),
# and loading one into RAM just to count newlines is what drove ~44 GB RSS at
# startup. 1 MiB balances syscall count against per-scan memory.
_SCAN_CHUNK_BYTES = 1 << 20


@dataclass(frozen=True)
class Batch:
    """A contiguous batch of log lines read from a session's append-only log.

    Attributes:
        session_id: The session the lines belong to.
        lines: Raw, complete log lines WITHOUT their trailing newline.
        start_offset: Byte position in the log where this batch begins.
        end_offset: Byte position in the log AFTER the last returned line.
            This is the value passed to ``commit``. When no complete lines
            are available, ``end_offset == start_offset``.
    """

    session_id: str
    lines: list[bytes]
    start_offset: int
    end_offset: int


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

    def _log_path(self, session_id: str) -> Path:
        return self._dir / f"{session_id}.log"

    def _offset_path(self, session_id: str) -> Path:
        return self._dir / f"{session_id}.offset"

    def _dead_path(self, session_id: str) -> Path:
        return self._dir / f"{session_id}.dead.jsonl"

    def _write_offset_record(
        self, key: str, offset: int, cursor: dict[str, Any] | None
    ) -> None:
        """Sole writer of the ``.offset`` file — a single atomic JSON record.

        Writes ``{"v": 1, "offset": offset, "cursor": cursor}`` to a temp file
        and ``os.replace``s it into place, so a reader never observes a torn
        or partial record. Folding the cursor into the same record as the
        offset (rather than a sidecar file) is what guarantees the two can
        never skew: a crash loses both together, never one without the other.
        No ``fsync`` — same process-crash-durable, not-power-durable contract
        as the rest of this module.
        """
        final = self._offset_path(key)
        tmp = self._dir / f"{key}.offset.tmp"
        record = {"v": 1, "offset": offset, "cursor": cursor}
        tmp.write_text(json.dumps(record, separators=(",", ":")), encoding="utf-8")
        os.replace(tmp, final)

    def _read_offset_record(self, key: str) -> tuple[int, dict[str, Any] | None]:
        """Read the ``.offset`` file, returning ``(offset, cursor)``.

        Accepts both the current JSON record shape and the legacy bare-integer
        shape (pre-upgrade). A missing or empty file yields ``(0, None)``.

        Cursor unreadability degrades to ``None`` (D5: never crash boot over a
        corrupt/unknown cursor) — an unknown ``v`` or non-dict ``cursor``
        silently discards the cursor while still honoring the offset. Offset
        unreadability is NOT degraded: a malformed record (bad JSON, or an
        ``offset`` that isn't an int) raises ``ValueError`` loudly, exactly as
        the legacy ``int(text)`` parse did — degrading a corrupt offset to 0
        would replay the entire log and manufacture duplicate nodes, which is
        a worse and quieter failure than a loud boot error.
        """
        try:
            text = self._offset_path(key).read_text("utf-8")
        except FileNotFoundError:
            return 0, None
        text = text.strip()
        if not text:
            return 0, None
        if text.startswith("{"):
            try:
                rec = json.loads(text)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Malformed offset record for {key!r}: {exc}") from exc
            offset = rec.get("offset")
            if not isinstance(offset, int):
                raise ValueError(
                    f"Malformed offset record for {key!r}: offset is not an int"
                )
            cursor = rec.get("cursor")
            if rec.get("v") == 1 and isinstance(cursor, dict):
                return offset, cursor
            return offset, None
        # Legacy bare-integer shape.
        return int(text), None

    def _read_committed_offset(self, session_id: str) -> int:
        return self._read_offset_record(session_id)[0]

    def _complete_data_end(self, session_id: str) -> int:
        """Byte position after the last complete (newline-terminated) line.

        A torn trailing line (bytes after the final newline) is ignored: the
        returned offset is one past the last ``\\n``, or 0 when the log is
        missing or contains no complete line.

        Streams BACKWARD from EOF in fixed chunks to find the last ``\\n`` --
        O(tail) memory and I/O, never O(file). This log can be multi-GB (the
        durable spool grew to a 4.9 GB single file in the incident); reading
        the whole thing into RAM just to find the final newline is exactly the
        boot-time memory blowup this avoids.
        """
        path = self._log_path(session_id)
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

    async def append(self, session_id: str, raw: bytes) -> None:
        self._validate_session_id(session_id)
        line = raw if raw.endswith(b"\n") else raw + b"\n"
        path = self._log_path(session_id)

        def _append() -> None:
            with open(path, "ab") as f:
                f.write(line)

        await asyncio.to_thread(_append)

    async def read_batch(self, session_id: str, max_items: int) -> Batch:
        self._validate_session_id(session_id)
        path = self._log_path(session_id)

        def _read() -> Batch:
            start = self._read_committed_offset(session_id)
            lines: list[bytes] = []
            consumed = 0
            try:
                with open(path, "rb") as f:
                    f.seek(start)
                    while len(lines) < max_items:
                        raw = f.readline()
                        if not raw or not raw.endswith(b"\n"):
                            # EOF, or a torn trailing line with no newline yet:
                            # ignore the partial line and stop on a line boundary.
                            break
                        lines.append(raw[:-1])
                        consumed += len(raw)
            except FileNotFoundError:
                pass
            return Batch(session_id, lines, start, start + consumed)

        return await asyncio.to_thread(_read)

    async def commit(
        self,
        session_id: str,
        new_offset: int,
        cursor: dict[str, Any] | None,
    ) -> None:
        """Atomically and durably persist ``new_offset`` (the ack) + ``cursor``.

        ``cursor`` has NO default: every call site must spell it explicitly
        (a deliberate footgun-avoidance choice — see spec §10.4) so it is
        never accidentally omitted at a commit site.

        Writes the offset+cursor record to a temp file and uses ``os.replace``
        for an atomic rename, so a reader never observes a torn or partial
        offset file. No ``fsync`` is issued here: this gives process-crash
        durability, while power-loss durability is deferred to Phase B3 (fsync
        group-commit). Folding the cursor into this same atomic write (D1) is
        what makes offset and cursor always advance together — never skewed.
        """
        self._validate_session_id(session_id)
        await asyncio.to_thread(
            self._write_offset_record, session_id, new_offset, cursor
        )

    async def dead_letter(self, session_id: str, raw: bytes, error: str) -> None:
        """Append one dead-letter record for an unprocessable batch line.

        The original line is stored under ``payload`` as a UTF-8 string when it
        decodes cleanly; otherwise the raw bytes are stored base64-encoded under
        ``payload_b64`` (so non-UTF-8 payloads are never silently dropped). Each
        record also carries a ``ts`` (epoch seconds) and the ``error`` string.

        This is the dead-letter PRIMITIVE only. The poison-isolation POLICY
        (deciding WHEN to dead-letter a line) is Phase B2. The main ``.log`` and
        ``.offset`` files are untouched.
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

        def _append() -> None:
            with open(path, "ab") as f:
                f.write(line)

        await asyncio.to_thread(_append)

    async def delete_drained(self, session_id: str) -> None:
        """Remove the drained .log and .offset for a fully-finalized session.

        The .dead.jsonl (if any) is intentionally KEPT — dead-letters are
        retained for later inspection/replay (Phase C). Idempotent: missing
        files are ignored.
        """
        self._validate_session_id(session_id)

        def _delete() -> None:
            for p in (self._log_path(session_id), self._offset_path(session_id)):
                try:
                    p.unlink()
                except FileNotFoundError:
                    pass

        await asyncio.to_thread(_delete)

    async def read_cursor(self, session_id: str) -> dict[str, Any] | None:
        """Return the persisted cursor for ``session_id``, or ``None``.

        ``None`` covers: no offset file, a legacy bare-integer offset file, or
        a JSON record whose cursor is absent/unreadable (D5 safe-degrade —
        see ``_read_offset_record``).
        """
        self._validate_session_id(session_id)
        return await asyncio.to_thread(lambda: self._read_offset_record(session_id)[1])

    async def read_dead_letters(self, session_id: str) -> list[dict]:
        """Return all dead-letter records for ``session_id`` in append order.

        Returns an empty list when no dead-letter file exists.
        """
        self._validate_session_id(session_id)

        def _read() -> list[dict]:
            try:
                text = self._dead_path(session_id).read_text(encoding="utf-8")
            except FileNotFoundError:
                return []
            return [json.loads(ln) for ln in text.splitlines() if ln.strip()]

        return await asyncio.to_thread(_read)

    async def is_fully_drained(self, session_id: str) -> bool:
        """Return True iff *session_id* has no undrained (unprocessed) log data.

        Durable-state predicate (B3 of the blob-reclaim design, `docs/plans/
        2026-08-12-blob-reclaim-endpoint-spec.md`): mirrors the per-session
        check inside :meth:`recover` (committed offset >= complete-data end
        means fully drained). Derived purely from the ``.log``/``.offset``
        files on disk, so it is independent of in-memory worker liveness and
        survives a `kill -9` + restart -- a crashed process's undrained queue
        still reads as NOT drained here even though no worker is registered
        for it.

        A session with no ``.log`` file at all reads as fully drained
        (``0 >= 0``), which is correct: it never had any queued data.
        """
        self._validate_session_id(session_id)

        def _check() -> bool:
            return self._read_committed_offset(session_id) >= self._complete_data_end(
                session_id
            )

        return await asyncio.to_thread(_check)

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
                if self._read_committed_offset(session_id) < log.stat().st_size:
                    result.append(session_id)
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
                committed = self._read_committed_offset(session_id)
                if committed < self._complete_data_end(session_id):
                    result.append(session_id)
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

        CHEAP BY CONSTRUCTION: this walks the directory and calls ``stat()``
        on each entry -- O(file count), NEVER O(file bytes). No file content
        is read (unlike ``derive_all_stats()``, which tail-reads each log to
        count pending lines). This is deliberately how a 38 GB spool can be
        sized on every /status poll without walking 38 GB of content.
        On top of that, results are cached for ``_spool_cache_ttl`` seconds
        (monotonic clock) so a deployment with a very large number of spool
        files (thousands of sessions) still does not pay a full directory
        scan on every request.

        Per the /status aggregate-only contract (D3): NO session ids, NO
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
        """
        self._validate_session_id(worker_key)

        def _purge() -> int:
            count = self._count_dead(worker_key)
            try:
                self._dead_path(worker_key).unlink()
            except FileNotFoundError:
                pass
            return count

        return await asyncio.to_thread(_purge)

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
                committed = self._read_committed_offset(key)
                complete_end = self._complete_data_end(key)
                dead = self._count_dead(key)
                # Streamed newline counts over byte ranges -- numerically
                # identical to the old data[:committed].count(b"\n") /
                # data[committed:complete_end].count(b"\n"), but without loading
                # the whole (possibly multi-GB) log or its slice copies at boot.
                before = self._count_newlines(key, 0, committed)
                pending = self._count_newlines(key, committed, complete_end)
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
        """
        try:
            text = self._dead_path(worker_key).read_text(encoding="utf-8")
        except FileNotFoundError:
            return set()
        payloads: set[bytes] = set()
        for ln in text.splitlines():
            if not ln.strip():
                continue
            record = json.loads(ln)
            if "payload" in record:
                payloads.add(record["payload"].encode("utf-8"))
            elif "payload_b64" in record:
                payloads.add(base64.b64decode(record["payload_b64"]))
        return payloads

    async def recovery_reconcile_dead(self) -> int:
        """Advance committed offsets past leading already-dead pending lines.

        Closes the dead_letter->commit crash window (D2). When the process
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
                dead_payloads = self._dead_payload_set(key)
                if not dead_payloads:
                    continue
                log_path = self._log_path(key)
                if not log_path.exists():
                    continue
                committed, cursor = self._read_offset_record(key)
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
                    # Lines skipped here were dead-lettered and therefore never
                    # dispatched to handlers, so the cursor is unchanged and
                    # MUST be carried through unmodified (spec §5.1, R2):
                    # dropping it here would silently wipe cross-handler state
                    # on the recovery path.
                    self._write_offset_record(key, pos, cursor)
            self._stats_cache = None
            return total_skipped

        return await asyncio.to_thread(_reconcile)
