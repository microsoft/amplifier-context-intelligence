"""Tests for EventRingBuffer and build_status_response in status.py."""

import asyncio
import time
from unittest.mock import MagicMock, patch


from context_intelligence_server.status import (
    EventRecord,
    EventRingBuffer,
    build_status_response,
    error_count_last_hour,
    ring_buffer,
)
from context_intelligence_server.registry import (
    CompletedSession,
    SessionRegistry,
    SessionWorker,
)
from context_intelligence_server.services import HookStateService


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_record(
    event: str = "tool_call",
    session_id: str = "sess-1",
    workspace: str = "/ws",
    result: str = "ok",
    error: str = "",
    timestamp: float | None = None,
) -> EventRecord:
    return EventRecord(
        timestamp=timestamp if timestamp is not None else time.time(),
        event=event,
        session_id=session_id,
        workspace=workspace,
        result=result,
        error=error,
    )


# ---------------------------------------------------------------------------
# TestEventRingBuffer
# ---------------------------------------------------------------------------


class TestEventRingBuffer:
    def test_add_and_recent(self) -> None:
        """add() stores a record; recent() returns it."""
        buf = EventRingBuffer()
        rec = make_record()
        buf.add(rec)

        recent = buf.recent()
        assert len(recent) == 1
        assert recent[0] is rec

    def test_newest_first(self) -> None:
        """Records are newest-first: last added is first in recent()."""
        buf = EventRingBuffer()
        rec_old = make_record(event="old", timestamp=1000.0)
        rec_new = make_record(event="new", timestamp=2000.0)

        buf.add(rec_old)
        buf.add(rec_new)

        recent = buf.recent()
        assert recent[0] is rec_new
        assert recent[1] is rec_old

    def test_maxlen_respected(self) -> None:
        """Buffer never exceeds maxlen; oldest records are dropped."""
        buf = EventRingBuffer(maxlen=3)
        records = [make_record(event=f"evt-{i}") for i in range(5)]
        for r in records:
            buf.add(r)

        recent = buf.recent()
        assert len(recent) == 3
        # The three most-recently added are retained (newest-first in list)
        assert recent[0] is records[4]
        assert recent[1] is records[3]
        assert recent[2] is records[2]

    def test_error_record(self) -> None:
        """An error record has result='error' and non-empty error field."""
        buf = EventRingBuffer()
        rec = make_record(result="error", error="something went wrong")
        buf.add(rec)

        recent = buf.recent()
        assert recent[0].result == "error"
        assert recent[0].error == "something went wrong"

    def test_empty_buffer(self) -> None:
        """recent() returns an empty list when nothing has been added."""
        buf = EventRingBuffer()
        assert buf.recent() == []


# ---------------------------------------------------------------------------
# TestBuildStatusResponse
# ---------------------------------------------------------------------------


class TestBuildStatusResponse:
    def setup_method(self) -> None:
        """Clear the module-level ring_buffer before each test."""
        ring_buffer._buffer.clear()

    def test_empty_registry(self) -> None:
        """Response has correct keys with an empty registry and no recent events."""
        registry = SessionRegistry()
        start_time = time.time() - 10.0  # 10 seconds ago

        response = build_status_response(registry, start_time)

        assert response["status"] == "ok"
        assert response["uptime_seconds"] >= 10.0
        assert response["active_sessions"] == 0
        assert response["sessions"] == []
        assert response["recent_events"] == []

    def test_with_active_session(self) -> None:
        """Sessions list includes per-session dicts with correct fields."""
        registry = SessionRegistry()

        # Use a recent timestamp so the worker passes the inactive timeout filter
        recent_time = time.time()

        # Inject a worker directly into the internal dict (no async setup needed)
        worker = SessionWorker(
            session_id="sess-abc",
            workspace="/home/user/project",
            services=HookStateService(workspace="/home/user/project"),
            last_event="tool_call",
            last_event_time=recent_time,
            events_processed=42,
        )
        registry._workers["sess-abc"] = worker

        start_time = time.time()
        response = build_status_response(registry, start_time)

        assert response["active_sessions"] == 1
        assert len(response["sessions"]) == 1

        sess = response["sessions"][0]
        assert sess["session_id"] == "sess-abc"
        assert sess["workspace"] == "/home/user/project"
        assert sess["last_event"] == "tool_call"
        assert sess["last_event_time"] == recent_time
        assert sess["events_processed"] == 42

    def test_includes_recent_events(self) -> None:
        """recent_events contains dicts converted from EventRecord via dataclasses.asdict."""
        registry = SessionRegistry()
        start_time = time.time()

        rec1 = make_record(event="session_start", timestamp=1000.0)
        rec2 = make_record(
            event="tool_call", timestamp=2000.0, result="error", error="oops"
        )
        ring_buffer.add(rec1)
        ring_buffer.add(rec2)

        response = build_status_response(registry, start_time)

        assert len(response["recent_events"]) == 2

        # newest-first ordering preserved
        event0 = response["recent_events"][0]
        assert event0["event"] == "tool_call"
        assert event0["result"] == "error"
        assert event0["error"] == "oops"

        event1 = response["recent_events"][1]
        assert event1["event"] == "session_start"

        # all EventRecord fields present
        for evt in response["recent_events"]:
            assert "timestamp" in evt
            assert "event" in evt
            assert "session_id" in evt
            assert "workspace" in evt
            assert "result" in evt
            assert "error" in evt


# ---------------------------------------------------------------------------
# TestErrorCountLastHour
# ---------------------------------------------------------------------------


class TestErrorCountLastHour:
    def test_no_errors_returns_zero(self) -> None:
        """Buffer with only ok records returns 0."""
        buf = EventRingBuffer()
        buf.add(make_record(result="ok"))
        buf.add(make_record(result="ok"))
        assert error_count_last_hour(buf) == 0

    def test_counts_recent_errors(self) -> None:
        """2 recent errors + 1 ok record returns 2."""
        buf = EventRingBuffer()
        buf.add(make_record(result="error"))
        buf.add(make_record(result="error"))
        buf.add(make_record(result="ok"))
        assert error_count_last_hour(buf) == 2

    def test_ignores_old_errors(self) -> None:
        """Old error (2 hours ago) is ignored; only recent error is counted."""
        buf = EventRingBuffer()
        old_ts = time.time() - 7200  # 2 hours ago
        buf.add(make_record(result="error", timestamp=old_ts))
        buf.add(make_record(result="error"))  # recent
        assert error_count_last_hour(buf) == 1

    def test_empty_buffer_returns_zero(self) -> None:
        """Empty buffer returns 0."""
        buf = EventRingBuffer()
        assert error_count_last_hour(buf) == 0


# ---------------------------------------------------------------------------
# TestBuildStatusResponseWithCompleted
# ---------------------------------------------------------------------------


class TestBuildStatusResponseWithCompleted:
    def setup_method(self) -> None:
        """Clear the module-level ring_buffer before each test."""
        ring_buffer._buffer.clear()

    def test_includes_completed_sessions_key(self) -> None:
        """Response includes 'completed_sessions' key as a list."""
        registry = SessionRegistry()
        start_time = time.time()
        response = build_status_response(registry, start_time)
        assert "completed_sessions" in response
        assert isinstance(response["completed_sessions"], list)

    def test_includes_error_count_last_hour_key(self) -> None:
        """Response includes 'error_count_last_hour' key as an int."""
        registry = SessionRegistry()
        start_time = time.time()
        response = build_status_response(registry, start_time)
        assert "error_count_last_hour" in response
        assert isinstance(response["error_count_last_hour"], int)

    def test_completed_sessions_populated(self) -> None:
        """Completed sessions appended to registry._completed appear in response."""
        registry = SessionRegistry()
        now = time.time()
        session = CompletedSession(
            session_id="sess-done",
            workspace="/ws",
            started_at=now - 60,
            ended_at=now,
            events_processed=10,
            error_count=2,
            duration_seconds=60.0,
        )
        registry._completed.append(session)

        start_time = time.time()
        response = build_status_response(registry, start_time)

        completed = response["completed_sessions"]
        assert len(completed) == 1
        assert completed[0]["session_id"] == "sess-done"
        assert completed[0]["events_processed"] == 10
        assert completed[0]["error_count"] == 2


# ---------------------------------------------------------------------------
# TestSessionOrdering
# ---------------------------------------------------------------------------


def _make_worker(session_id: str, last_event_time: float) -> SessionWorker:
    """Helper: create a minimal SessionWorker with the given session_id and last_event_time."""
    return SessionWorker(
        session_id=session_id,
        workspace="/ws",
        services=HookStateService(workspace="/ws"),
        last_event_time=last_event_time,
    )


class TestSessionOrdering:
    def setup_method(self) -> None:
        ring_buffer._buffer.clear()

    def test_sessions_sorted_most_recent_first(self) -> None:
        """Sessions are ordered by last_event_time descending (most recent first).

        Three workers with times 1000, 3000, 2000 — expected order: new (3000),
        mid (2000), old (1000).
        """
        registry = SessionRegistry()
        registry._workers["old"] = _make_worker("old", last_event_time=1000.0)
        registry._workers["new"] = _make_worker("new", last_event_time=3000.0)
        registry._workers["mid"] = _make_worker("mid", last_event_time=2000.0)

        # Use a very large timeout so all workers are visible regardless of age
        mock_settings = MagicMock()
        mock_settings.status_inactive_timeout = 9_999_999_999.0

        with patch(
            "context_intelligence_server.status.get_settings",
            return_value=mock_settings,
        ):
            response = build_status_response(registry, time.time())

        sessions = response["sessions"]
        assert len(sessions) == 3
        assert sessions[0]["session_id"] == "new"  # 3000 — most recent
        assert sessions[1]["session_id"] == "mid"  # 2000
        assert sessions[2]["session_id"] == "old"  # 1000 — oldest


# ---------------------------------------------------------------------------
# TestStatusVisibilityFiltering
# ---------------------------------------------------------------------------


class TestStatusVisibilityFiltering:
    def setup_method(self) -> None:
        ring_buffer._buffer.clear()

    def test_inactive_sessions_hidden(self) -> None:
        """Workers inactive longer than status_inactive_timeout are hidden.

        active worker (5 min = 300 s ago) → visible
        inactive worker (2 hours = 7200 s ago) → hidden (> 1800 s timeout)
        active_sessions count reflects only visible workers.
        """
        registry = SessionRegistry()
        now = time.time()

        registry._workers["active"] = _make_worker("active", last_event_time=now - 300)
        registry._workers["inactive"] = _make_worker(
            "inactive", last_event_time=now - 7200
        )

        mock_settings = MagicMock()
        mock_settings.status_inactive_timeout = 1800.0

        with patch(
            "context_intelligence_server.status.get_settings",
            return_value=mock_settings,
        ):
            response = build_status_response(registry, time.time())

        assert response["active_sessions"] == 1
        assert len(response["sessions"]) == 1
        assert response["sessions"][0]["session_id"] == "active"

    def test_new_worker_no_events_is_visible(self) -> None:
        """Workers with last_event_time == 0.0 are always shown (never received an event)."""
        registry = SessionRegistry()

        registry._workers["fresh"] = _make_worker("fresh", last_event_time=0.0)

        mock_settings = MagicMock()
        mock_settings.status_inactive_timeout = 1800.0

        with patch(
            "context_intelligence_server.status.get_settings",
            return_value=mock_settings,
        ):
            response = build_status_response(registry, time.time())

        assert len(response["sessions"]) == 1
        assert response["sessions"][0]["session_id"] == "fresh"
        assert response["active_sessions"] == 1


# ---------------------------------------------------------------------------
# TestBuildStatusResponseServerVersion
# ---------------------------------------------------------------------------


class TestBuildStatusResponseServerVersion:
    def test_server_version_key_present(self) -> None:
        """build_status_response() includes 'server_version' key."""
        registry = SessionRegistry()
        result = build_status_response(registry, time.time())
        assert "server_version" in result

    def test_server_version_matches_constant(self) -> None:
        """server_version value matches the module-level SERVER_VERSION constant."""
        from context_intelligence_server.status import SERVER_VERSION

        registry = SessionRegistry()
        result = build_status_response(registry, time.time())
        assert result["server_version"] == SERVER_VERSION

    def test_server_version_constant_is_nonempty_string(self) -> None:
        """SERVER_VERSION is a non-empty string."""
        from context_intelligence_server.status import SERVER_VERSION

        assert isinstance(SERVER_VERSION, str)
        assert len(SERVER_VERSION) > 0


# ---------------------------------------------------------------------------
# TestBuildStatusResponseOrphanVisibility
# ---------------------------------------------------------------------------


class TestBuildStatusResponseOrphanVisibility:
    def setup_method(self) -> None:
        """Clear the module-level ring_buffer before each test."""
        ring_buffer._buffer.clear()

    def _worker(self, sid: str) -> SessionWorker:
        """Create a minimal SessionWorker with the given session_id."""
        return SessionWorker(
            session_id=sid,
            workspace="/ws",
            services=HookStateService(workspace="/ws"),
        )

    def test_done_task_worker_is_orphaned(self) -> None:
        """Worker with a done asyncio.Task is flagged orphaned=True; top-level count is 1."""
        registry = SessionRegistry()
        worker = self._worker("sess-orphan")
        mock_task = MagicMock(spec=asyncio.Task)
        mock_task.done.return_value = True
        worker.task = mock_task
        registry._workers["sess-orphan"] = worker

        mock_settings = MagicMock()
        mock_settings.status_inactive_timeout = 9_999_999_999.0

        with patch(
            "context_intelligence_server.status.get_settings",
            return_value=mock_settings,
        ):
            response = build_status_response(registry, time.time())

        assert len(response["sessions"]) == 1
        assert response["sessions"][0]["orphaned"] is True
        assert response["orphaned_sessions"] == 1

    def test_running_task_worker_is_not_orphaned(self) -> None:
        """Worker with a running asyncio.Task is flagged orphaned=False; top-level count is 0."""
        registry = SessionRegistry()
        worker = self._worker("sess-active")
        mock_task = MagicMock(spec=asyncio.Task)
        mock_task.done.return_value = False
        worker.task = mock_task
        registry._workers["sess-active"] = worker

        mock_settings = MagicMock()
        mock_settings.status_inactive_timeout = 9_999_999_999.0

        with patch(
            "context_intelligence_server.status.get_settings",
            return_value=mock_settings,
        ):
            response = build_status_response(registry, time.time())

        assert len(response["sessions"]) == 1
        assert response["sessions"][0]["orphaned"] is False
        assert response["orphaned_sessions"] == 0

    def test_last_successful_flush_present_in_session(self) -> None:
        """Per-session dict includes last_successful_flush from the worker."""
        registry = SessionRegistry()
        worker = self._worker("sess-flush")
        expected_flush = time.time() - 5.0
        worker.last_successful_flush = expected_flush
        registry._workers["sess-flush"] = worker

        mock_settings = MagicMock()
        mock_settings.status_inactive_timeout = 9_999_999_999.0

        with patch(
            "context_intelligence_server.status.get_settings",
            return_value=mock_settings,
        ):
            response = build_status_response(registry, time.time())

        sess = response["sessions"][0]
        assert "last_successful_flush" in sess
        assert sess["last_successful_flush"] == expected_flush
