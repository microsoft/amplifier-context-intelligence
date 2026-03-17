"""In-memory request deduplication for event ingestion."""

from __future__ import annotations

import time
from collections import OrderedDict


class EventIdempotencyCache:
    """Bounded in-memory cache of recently seen event idempotency keys."""

    def __init__(self, ttl_seconds: float = 7 * 24 * 60 * 60, max_entries: int = 100_000) -> None:
        self._ttl_seconds = ttl_seconds
        self._max_entries = max_entries
        self._seen: OrderedDict[str, float] = OrderedDict()

    def check_and_store(self, key: str, now: float | None = None) -> bool:
        """Return True if *key* is new and store it, False if it is a duplicate."""
        current_time = time.time() if now is None else now
        self._purge(current_time)
        if key in self._seen:
            self._seen.move_to_end(key)
            return False
        self._seen[key] = current_time
        self._seen.move_to_end(key)
        self._trim()
        return True

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
