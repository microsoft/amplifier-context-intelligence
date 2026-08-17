"""QueueManager Protocol — the backend-neutral seam.

A ``QueueManager`` manages a durable, per-session append-only queue for the
event-write pipeline: events are appended as opaque ``bytes`` lines, read back
in batches, and the committed offset advances only once a batch has been
durably processed (the "ack"). Lines that cannot be processed after
exhausting retries are dead-lettered rather than silently dropped.

No ``Path``, on-disk layout, or ``os.*`` detail appears here or in any value
the Protocol returns — that is private to a concrete backend
(:class:`~context_intelligence_server.queue_manager.filesystem.FileSystemQueueManager`
and, later, an Azure equivalent).

This module is backend-neutral by construction: it imports nothing from
``os``, ``pathlib``, or any filesystem library.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

# ---------------------------------------------------------------------------
# Batch — a contiguous slice of a session's durable log
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# QueueManager protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class QueueManager(Protocol):
    """Protocol for a durable, per-session append-only queue.

    100% backend-neutral: no ``Path``, no on-disk layout, no ``os.*`` — ever.
    ``session_id`` (and, where relevant, ``worker_key``) is the only identity
    that crosses the boundary.
    """

    async def append(self, session_id: str, raw: bytes) -> None:
        """Append one raw record to *session_id*'s durable log."""
        ...

    async def read_batch(self, session_id: str, max_items: int) -> Batch:
        """Return up to *max_items* complete, uncommitted lines for *session_id*."""
        ...

    async def commit(self, session_id: str, new_offset: int) -> None:
        """Durably persist *new_offset* as the committed (acked) position."""
        ...

    async def dead_letter(self, session_id: str, raw: bytes, error: str) -> None:
        """Record one unprocessable line for *session_id*, with its *error*."""
        ...

    async def delete_drained(self, session_id: str) -> None:
        """Remove the drained log/offset for a fully-finalized *session_id*.

        Dead-letter records (if any) are intentionally retained.
        """
        ...

    async def read_dead_letters(self, session_id: str) -> list[dict[str, Any]]:
        """Return all dead-letter records for *session_id*, in append order."""
        ...

    async def active_sessions(self) -> list[str]:
        """Return sorted session_ids with undrained (uncommitted) data."""
        ...

    async def recover(self) -> list[str]:
        """Return sorted session_ids that have at least one complete unprocessed line."""
        ...

    async def derive_all_stats(self) -> dict[str, Any]:
        """Derive live queue stats (per-key + aggregate) purely from durable state."""
        ...

    async def spool_stats(self) -> dict[str, int]:
        """Return a cheap, aggregate-only spool footprint (health-endpoint safe)."""
        ...

    async def dead_letter_keys(self) -> list[str]:
        """Return sorted worker keys that have at least one dead-letter record."""
        ...

    async def purge_dead_letters(self, worker_key: str) -> int:
        """Delete all dead-letter records for *worker_key*; return the count removed."""
        ...

    async def recovery_seed_counts(self) -> tuple[int, int]:
        """Return ``(accepted_seed, written_seed)`` derived from durable state at boot."""
        ...

    async def recovery_reconcile_dead(self) -> int:
        """Advance committed offsets past leading already-dead pending lines.

        Returns the total number of lines skipped across all keys.
        """
        ...
