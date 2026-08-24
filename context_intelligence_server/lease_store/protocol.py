"""The writer-lease persistence boundary.

A single named lease record (owner, heartbeat, identity) persisted somewhere
durable. The writer-lease DETECTOR (``writer_lease.py``) owns all policy --
staleness, conflict, the bounded-thread I/O executor -- and reaches the lease
only through this Protocol, so the same detector runs unchanged against any
backend (a filesystem file today, a blob lease or a row tomorrow).

The methods are synchronous by contract: the detector runs each one on its own
private single-thread executor to bound a hung mount to a single leaked thread,
which ``asyncio.to_thread`` (shared pool) cannot guarantee. A backend whose I/O
is natively async wraps itself to satisfy this sync surface.
"""

from __future__ import annotations

import dataclasses
from typing import Protocol


@dataclasses.dataclass
class LeaseRecord:
    """Parsed view of one persisted lease record.

    ``unreadable=True`` marks a synthetic record standing in for a torn or
    hand-mangled lease (decode error / missing key / wrong type / unknown
    ``lease_version``) -- treated at fresh-foreign strength, never at face
    value.
    """

    owner: str
    host: str
    pid: int
    started_at: float
    heartbeat: float
    revision: str | None
    server_version: str
    lease_version: int
    unreadable: bool = False


class LeaseStore(Protocol):
    """Persistence for exactly one writer-lease record.

    All three operations may raise ``OSError`` (a share fault); the detector
    absorbs that as "not armed", never as a conflict.
    """

    def read(self) -> LeaseRecord | None:
        """Return the current lease record, or ``None`` when no lease exists
        (a free directory). A torn/malformed record returns a ``LeaseRecord``
        with ``unreadable=True`` rather than ``None``."""
        ...

    def write(self, record: LeaseRecord) -> None:
        """Persist *record* as the current lease, atomically (a reader never
        observes a half-written record)."""
        ...

    def delete_if_owned(self, owner: str) -> None:
        """Delete the lease only if it is still owned by *owner*.

        Never deletes a foreign lease: if a peer took it over, removing theirs
        would actively hand the directory to a third writer. Best-effort: a
        lease already gone is not an error."""
        ...
