"""Dashboard utilities: event ring buffer and status response builder."""

from __future__ import annotations

import dataclasses
import time
from collections import deque
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from context_intelligence_server.registry import SessionRegistry


# ---------------------------------------------------------------------------
# EventRecord
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class EventRecord:
    """A single processed-event record stored in the ring buffer."""

    timestamp: float
    event: str
    session_id: str
    workspace: str
    result: str  # 'ok' | 'error'
    error: str = ""


# ---------------------------------------------------------------------------
# EventRingBuffer
# ---------------------------------------------------------------------------


class EventRingBuffer:
    """Fixed-size ring buffer of EventRecords, newest-first ordering."""

    def __init__(self, maxlen: int = 50) -> None:
        self._buffer: deque[EventRecord] = deque(maxlen=maxlen)

    def add(self, record: EventRecord) -> None:
        """Prepend *record* so that recent() returns newest items first."""
        self._buffer.appendleft(record)

    def recent(self) -> list[EventRecord]:
        """Return all buffered records as a list (newest first)."""
        return list(self._buffer)


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

ring_buffer: EventRingBuffer = EventRingBuffer()


# ---------------------------------------------------------------------------
# error_count_last_hour
# ---------------------------------------------------------------------------


def error_count_last_hour(ring: EventRingBuffer) -> int:
    """Count error records in *ring* that occurred within the last 3600 seconds."""
    cutoff = time.time() - 3600
    return sum(1 for r in ring.recent() if r.result == "error" and r.timestamp >= cutoff)


# ---------------------------------------------------------------------------
# build_status_response
# ---------------------------------------------------------------------------


def build_status_response(
    registry: SessionRegistry,
    start_time: float,
) -> dict[str, Any]:
    """Build a status response dict from registry state and recent events.

    Args:
        registry: The active SessionRegistry.
        start_time: Server start time as a Unix timestamp (from time.time()).

    Returns:
        A dict with keys: status, uptime_seconds, active_sessions, sessions,
        recent_events, completed_sessions, error_count_last_hour.
    """
    sessions = [
        {
            "session_id": worker.session_id,
            "workspace": worker.workspace,
            "queue_depth": worker.queue.qsize(),
            "last_event": worker.last_event,
            "last_event_time": worker.last_event_time,
            "events_processed": worker.events_processed,
        }
        for worker in registry.workers()
    ]

    return {
        "status": "ok",
        "uptime_seconds": time.time() - start_time,
        "active_sessions": registry.active_count(),
        "sessions": sessions,
        "recent_events": [dataclasses.asdict(rec) for rec in ring_buffer.recent()],
        "completed_sessions": [dataclasses.asdict(s) for s in registry.completed_sessions()],
        "error_count_last_hour": error_count_last_hour(ring_buffer),
    }
