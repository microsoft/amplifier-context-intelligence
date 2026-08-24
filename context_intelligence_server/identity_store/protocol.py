"""IdentityStore Protocol -- the backend-neutral seam.

Each ``IdentityStore`` wraps a durable ``key -> {"id": contributor_id, ...}``
map and keeps an in-process, live-mutated view of it (:attr:`flat_dict`) so
that any object holding a reference (e.g. an auth resolver's keystore) always
sees the latest state without a restart. No ``Path``, on-disk layout, or
``os.*`` detail appears here or in any value the Protocol returns -- that is
private to a concrete backend
(:class:`~context_intelligence_server.identity_store.filesystem.FileSystemIdentityStore`
and, later, an Azure equivalent).

This is AUTH-CRITICAL surface. The following guarantees are part of the
Protocol's contract and every backend MUST uphold them:

**Commit order (ROB F2 -- NON-NEGOTIABLE)**

On every mutation (``put`` / ``delete``):

1. The durable store is written FIRST (whatever "durable" means for the
   backend -- a file, a blob, etc.).
2. **ONLY IF** that write succeeds does in-process state (``flat_dict`` and
   any internal map) get updated, IN-PLACE, so existing references to
   ``flat_dict`` observe the change immediately.
3. If the durable write fails, in-process state is **UNCHANGED** and the
   exception propagates to the caller (who returns 5xx). The durable store
   and in-process memory are never out of sync.

**Fail-CLOSED load()**

On ``load()``:

- Missing / never-persisted store -> empty map (normal first boot). No log,
  no raise.
- Corrupt / unreadable store -> empty map + a LOUD error log. The server
  MUST NOT crash-loop on a bad store. An empty map means "nobody is bound
  yet" -- every auth attempt then fails normally until an admin re-populates
  via the /admin API.

**flat_dict is a live, shared view**

``flat_dict`` is ``{key: contributor_id}``, mutated IN-PLACE on every
``put`` / ``delete`` / ``seed`` / ``load`` so any object holding a reference
to it always sees the latest state.

This module is backend-neutral by construction: it imports nothing from
``os``, ``pathlib``, or any filesystem library.
"""

from __future__ import annotations

from collections.abc import ItemsView
from typing import Protocol, runtime_checkable


@runtime_checkable
class IdentityStore(Protocol):
    """Protocol for a durable, write-through identity map.

    100% backend-neutral: no ``Path``, no on-disk layout, no ``os.*`` --
    ever.
    """

    flat_dict: dict[str, str]
    """Live derived view: ``{key: contributor_id}``. Shared BY REFERENCE with
    consumers (e.g. auth resolvers) so mutations are visible with no restart."""

    def load(self) -> None:
        """Read the durable store and populate the in-process map.

        Fail-closed: missing store -> empty map (silent, normal first boot);
        corrupt store -> empty map + a loud error log. Never raises.
        """
        ...

    def put(self, key: str, value: dict[str, str]) -> None:
        """Upsert ``key`` -> ``value``.

        Commit order (F2): durable write -> in-process update. Raises on
        durable-write failure; in-process state is left unchanged.
        """
        ...

    def delete(self, key: str) -> None:
        """Remove ``key`` from the store.

        Commit order (F2): durable write -> in-process update. Raises on
        durable-write failure; in-process state is left unchanged. No-op if
        ``key`` is not present.
        """
        ...

    def seed(self, data: dict[str, dict[str, str]]) -> None:
        """Bulk-seed from config on first boot.

        Unlike ``put()`` (which enforces F2 write-before-memory strictly),
        in-process state is updated even if the durable write fails -- the
        data came from durable config, so memory-ahead-of-durable-store is
        safe (a restart re-seeds from config again). A warning is logged if
        the durable write fails.
        """
        ...

    def get(self, key: str) -> dict[str, str] | None:
        """Return the value for ``key``, or ``None`` if not present."""
        ...

    def items(self) -> ItemsView[str, dict[str, str]]:
        """Iterate over ``(key, value)`` pairs in the store."""
        ...

    def __len__(self) -> int:
        """Return the number of entries currently in the store."""
        ...

    def exists(self) -> bool:
        """Whether the store has ever been persisted to its backing store."""
        ...
