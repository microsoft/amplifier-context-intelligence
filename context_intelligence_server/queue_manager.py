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
import os
from dataclasses import dataclass
from pathlib import Path


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

    def _log_path(self, session_id: str) -> Path:
        return self._dir / f"{session_id}.log"

    def _offset_path(self, session_id: str) -> Path:
        return self._dir / f"{session_id}.offset"

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
            try:
                with open(path, "rb") as f:
                    f.seek(start)
                    data = f.read()
            except FileNotFoundError:
                data = b""
            lines: list[bytes] = []
            pos = 0
            while len(lines) < max_items:
                nl = data.find(b"\n", pos)
                if nl == -1:
                    break
                lines.append(data[pos:nl])
                pos = nl + 1
            return Batch(session_id, lines, start, start + pos)

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
