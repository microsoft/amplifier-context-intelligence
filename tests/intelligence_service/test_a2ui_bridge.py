"""Tests for the A2UI message bridge module."""

from intelligence_service.a2ui_bridge import (
    extract_a2ui_from_response,
    format_action_ack,
    format_error,
    format_response,
    format_session_created,
    parse_incoming,
)


def test_parse_incoming_extracts_type() -> None:
    """parse_incoming extracts msg_type and payload fields correctly."""
    raw = {"type": "message", "text": "hello"}

    result = parse_incoming(raw)

    assert result.msg_type == "message"
    assert result.payload["text"] == "hello"


def test_parse_incoming_defaults_to_unknown() -> None:
    """parse_incoming defaults msg_type to 'unknown' when type is missing."""
    raw = {"text": "hello"}

    result = parse_incoming(raw)

    assert result.msg_type == "unknown"


def test_parse_incoming_preserves_full_payload() -> None:
    """parse_incoming preserves all fields including nested dicts in payload."""
    raw = {
        "type": "action",
        "component_id": "btn-1",
        "data": {"key": "value", "nested": {"x": 1}},
    }

    result = parse_incoming(raw)

    assert result.msg_type == "action"
    assert result.payload["component_id"] == "btn-1"
    assert result.payload["data"] == {"key": "value", "nested": {"x": 1}}


def test_format_session_created() -> None:
    """format_session_created returns dict with camelCase keys matching frontend contract."""
    result = format_session_created(session_id="abc-123", message="Welcome!")

    assert result["type"] == "sessionCreated"
    assert result["sessionId"] == "abc-123"
    assert result["message"] == "Welcome!"


def test_format_session_created_default_message() -> None:
    """format_session_created uses 'Session created.' when message is omitted."""
    result = format_session_created(session_id="abc-123")

    assert result["type"] == "sessionCreated"
    assert result["sessionId"] == "abc-123"
    assert result["message"] == "Session created."


def test_format_response() -> None:
    """format_response returns dict with camelCase keys matching frontend contract."""
    result = format_response(session_id="abc-123", content="Here is the answer.")

    assert result["type"] == "response"
    assert result["sessionId"] == "abc-123"
    assert result["payload"] == "Here is the answer."


def test_format_action_ack() -> None:
    """format_action_ack returns dict with camelCase keys matching frontend contract."""
    result = format_action_ack(session_id="abc-123", component_id="btn-submit")

    assert result["type"] == "actionAck"
    assert result["sessionId"] == "abc-123"
    assert result["actionId"] == "btn-submit"


def test_format_error() -> None:
    """format_error returns dict with type='error', session_id, and message."""
    result = format_error(session_id="abc-123", message="Something went wrong.")

    assert result["type"] == "error"
    assert result["session_id"] == "abc-123"
    assert result["message"] == "Something went wrong."


def test_extract_a2ui_from_string_response_returns_empty_list() -> None:
    """extract_a2ui_from_response returns [] when given a plain string."""
    result = extract_a2ui_from_response("some plain string response")

    assert result == []


def test_extract_a2ui_from_none_returns_empty_list() -> None:
    """extract_a2ui_from_response returns [] when given None."""
    result = extract_a2ui_from_response(None)

    assert result == []


def test_extract_a2ui_return_type_is_list() -> None:
    """extract_a2ui_from_response always returns a list."""
    result = extract_a2ui_from_response("anything")

    assert isinstance(result, list)
