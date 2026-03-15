"""A2UI message bridge for translating between WebSocket messages and the A2UI protocol."""

from dataclasses import dataclass
from typing import Any


@dataclass
class IncomingMessage:
    """Parsed incoming WebSocket message."""

    msg_type: str
    payload: dict[str, Any]


def parse_incoming(raw: dict[str, Any]) -> IncomingMessage:
    """Parse a raw WebSocket message dict into an IncomingMessage.

    The 'type' key is extracted as msg_type; all fields (including 'type')
    are preserved in payload. If 'type' is absent, msg_type defaults to 'unknown'.
    """
    return IncomingMessage(
        msg_type=raw.get("type", "unknown"),
        payload=raw,
    )


def format_session_created(
    session_id: str,
    message: str = "Session created.",
) -> dict[str, Any]:
    """Return a session_created outbound message."""
    return {
        "type": "session_created",
        "session_id": session_id,
        "message": message,
    }


def format_response(session_id: str, content: str) -> dict[str, Any]:
    """Return a response outbound message."""
    return {
        "type": "response",
        "session_id": session_id,
        "content": content,
    }


def format_action_ack(session_id: str, component_id: str) -> dict[str, Any]:
    """Return an action_ack outbound message."""
    return {
        "type": "action_ack",
        "session_id": session_id,
        "component_id": component_id,
    }


def format_error(session_id: str, message: str) -> dict[str, Any]:
    """Return an error outbound message."""
    return {
        "type": "error",
        "session_id": session_id,
        "message": message,
    }
