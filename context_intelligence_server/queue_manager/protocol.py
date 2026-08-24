"""QueueManager Protocol — the backend-neutral seam.

A ``QueueManager`` manages a durable, per-session append-only queue for the
event-write pipeline. Consumers depend on this Protocol plus the ``Batch`` /
``Record`` value types, never on a concrete backend class. No ``Path``,
on-disk layout, or ``os.*`` detail appears here or in any value the Protocol
returns — those are private to a concrete backend
(:class:`~context_intelligence_server.queue_manager.filesystem.FileSystemQueueManager`,
and, later, an Azure equivalent).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable


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


@runtime_checkable
class QueueManager(Protocol):
    """Durable, per-session append-only queue.

    The method set mirrors the on-disk backend's public surface. A backend
    reports its own queue root via ``queues_dir``; every other on-disk detail
    stays private to the implementation.
    """

    @property
    def queues_dir(self) -> Any: ...

    async def heal_torn_tails(self) -> dict[str, int]: ...

    async def append(self, session_id: str, raw: bytes) -> None: ...

    async def read_batch(self, session_id: str, max_items: int) -> Batch: ...

    async def commit(self, session_id: str, new_offset: int) -> None: ...

    async def dead_letter(self, session_id: str, raw: bytes, error: str) -> None: ...

    async def delete_drained(self, session_id: str) -> bool: ...

    async def compact_committed_prefix(
        self, session_id: str, min_prefix_bytes: int = 0
    ) -> int: ...

    async def read_dead_letters(self, session_id: str) -> list[dict]: ...

    async def read_first_line(self, key: str) -> bytes | None: ...

    async def classify_session(
        self, key: str, head_is_resumable: Callable[[bytes], bool]
    ) -> Any: ...

    async def reclaim(self, c: Any, is_owned: Callable[[], bool]) -> bool: ...

    async def reclaim_orphans(
        self, before_ts: float, enabled: bool = True
    ) -> dict[str, int]: ...

    async def active_sessions(self) -> list[str]: ...

    async def recover(self) -> list[str]: ...

    async def derive_all_stats(self) -> dict[str, Any]: ...

    async def spool_stats(self) -> dict[str, int]: ...

    async def dead_letter_keys(self) -> list[str]: ...

    async def purge_dead_letters(self, worker_key: str) -> int: ...

    async def expire_dead_letters(
        self, now: float, retention_seconds: float, enabled: bool
    ) -> dict[str, int]: ...

    async def recovery_seed_counts(self) -> tuple[int, int]: ...

    async def recovery_reconcile_dead(self) -> int: ...
