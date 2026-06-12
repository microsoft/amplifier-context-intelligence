"""On-disk durable queue manager for the event-write pipeline.

Disk layout (one set of files per session, keyed by ``session_id``):

- ``<session_id>.log`` — append-only, newline-terminated, opaque ``bytes``.
  Each line is one enqueued record. The log is never rewritten in place.
- ``<session_id>.offset`` — a single integer: the byte position in the log
  that has been durably processed (committed). A missing offset file means 0.
- ``<session_id>.dead.jsonl`` — append-only dead-letter records for batches
  that could not be processed after exhausting retries.

Durability note:
    Appends use a plain durable ``write()``. This gives PROCESS-crash
    durability (the bytes are handed to the OS page cache and survive a
    process crash). POWER-LOSS durability via ``fsync`` is deliberately
    deferred to Phase B3 (fsync group-commit).

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

    def _log_path(self, session_id: str) -> Path:
        return self._dir / f"{session_id}.log"

    def _offset_path(self, session_id: str) -> Path:
        return self._dir / f"{session_id}.offset"

    def _dead_path(self, session_id: str) -> Path:
        return self._dir / f"{session_id}.dead.jsonl"

    def _read_committed_offset(self, session_id: str) -> int:
        try:
            text = self._offset_path(session_id).read_text("utf-8")
        except FileNotFoundError:
            return 0
        text = text.strip()
        return int(text) if text else 0

    def _complete_data_end(self, session_id: str) -> int:
        """Byte position after the last complete (newline-terminated) line.

        A torn trailing line (bytes after the final newline) is ignored: the
        returned offset is one past the last ``\\n``, or 0 when the log is
        missing or contains no complete line.
        """
        try:
            data = self._log_path(session_id).read_bytes()
        except FileNotFoundError:
            return 0
        last_nl = data.rfind(b"\n")
        return last_nl + 1 if last_nl != -1 else 0

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

    async def commit(self, session_id: str, new_offset: int) -> None:
        """Atomically and durably persist ``new_offset`` (the ack).

        Writes the offset to a temp file and uses ``os.replace`` for an atomic
        rename, so a reader never observes a torn or partial offset file. No
        ``fsync`` is issued here: this gives process-crash durability, while
        power-loss durability is deferred to Phase B3 (fsync group-commit).
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
        """
        try:
            data = self._dead_path(worker_key).read_bytes()
        except FileNotFoundError:
            return 0
        return data.count(b"\n")

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
                committed = self._read_committed_offset(worker_key)
                in_queue = 0
                try:
                    with open(self._log_path(worker_key), "rb") as f:
                        f.seek(committed)
                        data = f.read()
                    last_nl = data.rfind(b"\n")
                    if last_nl != -1:
                        in_queue = data[: last_nl + 1].count(b"\n")
                except FileNotFoundError:
                    in_queue = 0
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
                try:
                    data = self._log_path(key).read_bytes()
                except FileNotFoundError:
                    data = b""
                before = data[:committed].count(b"\n")
                pending = data[committed:complete_end].count(b"\n")
                written_seed = max(0, before - dead)
                accepted += written_seed + pending + dead
                written += written_seed
            return accepted, written

        return await asyncio.to_thread(_seed)
