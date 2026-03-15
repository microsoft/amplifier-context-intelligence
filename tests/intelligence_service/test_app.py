"""Tests for the Intelligence Service FastAPI application."""

import httpx
from fastapi.testclient import TestClient

from intelligence_service.app import app


async def test_health_returns_200_with_status_ok(client: httpx.AsyncClient) -> None:
    """GET /health returns 200 with {'status': 'ok'}."""
    response = await client.get("/health")

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"


async def test_reload_bundle_stub_mode_returns_skipped(
    client: httpx.AsyncClient,
) -> None:
    """POST /admin/reload-bundle returns {'status': 'skipped'} in stub mode."""
    response = await client.post("/admin/reload-bundle")

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "skipped"


async def test_reload_bundle_get_not_allowed(client: httpx.AsyncClient) -> None:
    """GET /admin/reload-bundle returns 405 Method Not Allowed."""
    response = await client.get("/admin/reload-bundle")

    assert response.status_code == 405


# ---------------------------------------------------------------------------
# WebSocket tests — use Starlette TestClient (sync) for WS support
# Each WS test owns its client to keep state isolated
# ---------------------------------------------------------------------------


def test_ws_connect_receives_session_created() -> None:
    """Connecting to /ws immediately receives a session_created message with a session_id."""
    with TestClient(app) as client:
        with client.websocket_connect("/ws") as ws:
            data = ws.receive_json()

    assert data["type"] == "session_created"
    assert "session_id" in data


def test_ws_message_receives_response() -> None:
    """Sending a message yields a response message whose content echoes the text."""
    with TestClient(app) as client:
        with client.websocket_connect("/ws") as ws:
            ws.receive_json()  # consume session_created
            ws.send_json({"type": "message", "text": "hello"})
            data = ws.receive_json()

    assert data["type"] == "response"
    assert "hello" in data["content"]


def test_ws_new_session_returns_different_id() -> None:
    """Sending new_session yields a session_created with a different session_id."""
    with TestClient(app) as client:
        with client.websocket_connect("/ws") as ws:
            first = ws.receive_json()  # initial session_created
            original_id = first["session_id"]
            ws.send_json({"type": "new_session"})
            data = ws.receive_json()

    assert data["type"] == "session_created"
    assert data["session_id"] != original_id


def test_ws_action_receives_ack() -> None:
    """Sending an action message yields an action_ack with the matching component_id."""
    with TestClient(app) as client:
        with client.websocket_connect("/ws") as ws:
            ws.receive_json()  # consume session_created
            ws.send_json({"type": "action", "componentId": "graph-1"})
            data = ws.receive_json()

    assert data["type"] == "action_ack"
    assert data["component_id"] == "graph-1"


def test_ws_unknown_type_receives_error() -> None:
    """Sending an unrecognised message type yields an error containing the type name."""
    with TestClient(app) as client:
        with client.websocket_connect("/ws") as ws:
            ws.receive_json()  # consume session_created
            ws.send_json({"type": "invalid_type"})
            data = ws.receive_json()

    assert data["type"] == "error"
    assert "invalid_type" in data["message"]


def test_ws_new_session_no_drain_leak() -> None:
    """After new_session and disconnect, drain has zero active sessions.

    Regression: the old session_id must be unregistered from drain when a
    new_session reset occurs.  Without the fix the old ID is stranded in
    drain._active, active_count stays 1 after disconnect, and start_drain()
    would always time-out after any session reset.
    """
    with TestClient(app) as client:
        drain = app.state.drain
        with client.websocket_connect("/ws") as ws:
            ws.receive_json()  # initial session_created
            ws.send_json({"type": "new_session"})
            ws.receive_json()  # new session_created
        # After disconnect the finally block must unregister the *new* ID;
        # the *old* ID must have been unregistered at reset time.
        assert drain.active_count == 0


def test_ws_disconnect_unregisters_from_drain() -> None:
    """After a plain connect and disconnect (no new_session), drain has zero active sessions.

    Verifies that the finally block in the WebSocket endpoint always unregisters
    the session from drain, even when no session reset occurs.
    """
    with TestClient(app) as client:
        drain = app.state.drain
        with client.websocket_connect("/ws") as ws:
            ws.receive_json()  # consume session_created
        # After disconnect the finally block must unregister the session ID.
        assert drain.active_count == 0
