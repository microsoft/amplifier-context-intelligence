"""Tests for Pydantic request/response models."""

import pytest
from pydantic import ValidationError

from context_intelligence_server.models import (
    EventRequest,
    EventResponse,
    StatusResponse,
)


def test_event_request_valid():
    """Parse a well-formed EventRequest payload."""
    req = EventRequest(
        event="tool:pre",
        workspace="my-feature-branch",
        data={"session_id": "abc123", "tool": "bash"},
    )
    assert req.event == "tool:pre"
    assert req.workspace == "my-feature-branch"
    assert req.data["session_id"] == "abc123"


def test_event_request_missing_event():
    """EventRequest raises ValidationError when event is missing."""
    with pytest.raises(ValidationError):
        EventRequest(workspace="my-feature-branch", data={"session_id": "abc123"})  # type: ignore[call-arg]


def test_event_request_missing_workspace():
    """EventRequest raises ValidationError when workspace is missing."""
    with pytest.raises(ValidationError):
        EventRequest(event="tool:pre", data={"session_id": "abc123"})  # type: ignore[call-arg]


def test_event_request_data_without_session_id():
    """EventRequest accepts data dict that has no session_id key."""
    req = EventRequest(
        event="tool:post",
        workspace="main",
        data={"tool": "read_file", "path": "/tmp/test.py"},
    )
    assert req.data["tool"] == "read_file"
    assert "session_id" not in req.data


def test_event_response_defaults():
    """EventResponse defaults status to 'queued' and accepts a session_id."""
    resp = EventResponse(session_id="sess-001")
    assert resp.status == "queued"
    assert resp.session_id == "sess-001"


def test_event_response_null_session():
    """EventResponse allows session_id to be None."""
    resp = EventResponse()
    assert resp.status == "queued"
    assert resp.session_id is None


def test_status_response():
    """StatusResponse carries status, uptime_seconds, and active_sessions."""
    sr = StatusResponse(status="ok", uptime_seconds=123.45, active_sessions=3)
    assert sr.status == "ok"
    assert sr.uptime_seconds == 123.45
    assert sr.active_sessions == 3
