"""In-memory request deduplication for event ingestion.

Keys are recorded only after a successful durable append; a key in the
cache means the event is on disk.
"""

from __future__ import annotations

import asyncio
import time
from collections import OrderedDict
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager


class EventIdempotencyCache:
    """Bounded in-memory cache of recently seen event idempotency keys."""

    def __init__(
        self, ttl_seconds: float = 7 * 24 * 60 * 60, max_entries: int = 100_000
    ) -> None:
        self._ttl_seconds = ttl_seconds
        self._max_entries = max_entries
        self._seen: OrderedDict[str, float] = OrderedDict()

    def seen(self, key: str, now: float | None = None) -> bool:
        """Return True if *key* was already recorded as durably accepted.

        This is the READ half of the former ``check_and_store``, byte for
        byte: it purges expired entries first and refreshes LRU recency on a
        hit, so eviction behaviour is unchanged. It NEVER records a key --
        recording is ``store``'s job, and happens only after a successful
        durable append.

        NOTE THE POLARITY: this returns True for a DUPLICATE, whereas
        ``check_and_store`` returned True for a NEW key. The name states the
        meaning; the caller's guard is ``if seen(...): return duplicate``.
        """
        current_time = time.time() if now is None else now
        self._purge(current_time)
        if key in self._seen:
            self._seen.move_to_end(key)
            return True
        return False

    def store(self, key: str, now: float | None = None) -> None:
        """Record *key* as durably accepted.

        MUST be called ONLY after the event's durable append has returned
        successfully. Burning a key before durability is a silent-loss
        bug: the client's retry is answered "duplicate" for an event that is
        nowhere on disk, and no recovery path can resurrect it -- the bytes
        never reached the log.
        """
        current_time = time.time() if now is None else now
        self._seen[key] = current_time
        self._seen.move_to_end(key)
        self._trim()

    def clear(self) -> None:
        """Remove all remembered keys."""
        self._seen.clear()

    def _purge(self, now: float) -> None:
        while self._seen:
            _key, seen_at = next(iter(self._seen.items()))
            if now - seen_at < self._ttl_seconds:
                break
            self._seen.popitem(last=False)

    def _trim(self) -> None:
        while len(self._seen) > self._max_entries:
            self._seen.popitem(last=False)


class KeyedAsyncLocks:
    """Per-key asyncio.Lock registry; a key's lock is dropped once idle."""

    def __init__(self) -> None:
        self._locks: dict[str, asyncio.Lock] = {}
        self._waiters: dict[str, int] = {}

    @asynccontextmanager
    async def acquire(self, key: str) -> AsyncIterator[None]:
        lock = self._locks.setdefault(key, asyncio.Lock())
        self._waiters[key] = self._waiters.get(key, 0) + 1
        try:
            async with lock:
                yield
        finally:
            self._waiters[key] -= 1
            if self._waiters[key] <= 0:
                self._waiters.pop(key, None)
                # Only drop the lock if nothing else grabbed a reference to
                # it in the meantime (it is not currently locked/awaited).
                if not lock.locked():
                    self._locks.pop(key, None)
