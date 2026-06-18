"""Dashboard utilities: event ring buffer and status response builder."""

from __future__ import annotations

import dataclasses
import time
from collections import deque
from importlib.metadata import version as _pkg_version
from typing import TYPE_CHECKING, Any

from context_intelligence_server.config import get_settings

# Resolved once at import time — never changes within a process lifetime.
SERVER_VERSION: str = _pkg_version("context-intelligence-server")

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
    return sum(
        1 for r in ring.recent() if r.result == "error" and r.timestamp >= cutoff
    )


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
        recent_events, completed_sessions, error_count_last_hour, server_version,
        orphaned_sessions.

        Each entry in ``sessions`` includes the keys: session_id, workspace,
        last_event, last_event_time, events_processed, orphaned,
        last_successful_flush.

        Note: ``orphaned_sessions`` is the count of ALL registered workers whose
        drain task has completed (``task.done()``).  A worker filtered *out* of
        the visible ``sessions`` list by ``dashboard_inactive_timeout`` still
        contributes to this count but will not appear with ``orphaned: True`` in
        any per-session dict.  For a fresh OOM orphan this asymmetry is
        irrelevant (OOM orphans are recent by definition); it can surface for
        long-running orphans whose ``last_event_time`` ages past the timeout.
    """
    settings = get_settings()
    now = time.time()
    timeout = settings.dashboard_inactive_timeout

    # Filter: always show workers that have never received an event (last_event_time == 0.0).
    # Hide workers that have been inactive longer than the configured timeout.
    visible_workers = [
        worker
        for worker in registry.workers()
        if worker.last_event_time == 0.0 or (now - worker.last_event_time) <= timeout
    ]

    # Sort by last_event_time descending (most recent first).
    visible_workers.sort(key=lambda w: w.last_event_time, reverse=True)

    # Compute orphan set ONCE — reused for both the per-session flag and the
    # aggregate count (single source of truth: no inline task.done() calls).
    orphaned_ids = {w.session_id for w in registry.orphaned_sessions()}

    sessions = [
        {
            "session_id": worker.session_id,
            "workspace": worker.workspace,
            "last_event": worker.last_event,
            "last_event_time": worker.last_event_time,
            "events_processed": worker.events_processed,
            "orphaned": worker.session_id in orphaned_ids,
            "last_successful_flush": worker.last_successful_flush,
        }
        for worker in visible_workers
    ]

    return {
        "status": "ok",
        "uptime_seconds": time.time() - start_time,
        "active_sessions": len(visible_workers),
        "sessions": sessions,
        "recent_events": [dataclasses.asdict(rec) for rec in ring_buffer.recent()],
        "completed_sessions": [
            dataclasses.asdict(s) for s in registry.completed_sessions()
        ],
        "error_count_last_hour": error_count_last_hour(ring_buffer),
        "server_version": SERVER_VERSION,
        "orphaned_sessions": len(orphaned_ids),
    }
