"""Tests for the Intelligence Service FastAPI application."""

import httpx
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, patch

from intelligence_service.app import app


async def test_health_returns_disconnected_in_stub_mode(
    client: httpx.AsyncClient,
) -> None:
    """GET /health returns 200 with {'status': 'disconnected'} when runtime failed to start.

    In test environments amplifier_foundation is not installed, so the lifespan
    catches the startup failure and falls back to stub/disconnected mode.
    """
    response = await client.get("/health")

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "disconnected"


async def test_health_returns_ok_when_runtime_connected(
    client: httpx.AsyncClient,
) -> None:
    """GET /health returns {'status': 'ok'} when runtime_connected flag is True."""
    app.state.runtime_connected = True
    try:
        response = await client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "ok"
    finally:
        app.state.runtime_connected = False


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


def test_ws_message_receives_disconnected_error_in_stub_mode() -> None:
    """Sending a message when disconnected yields an error about the service being disconnected.

    In test environments amplifier_foundation is not installed, so the lifespan
    catches the startup failure and the service is in disconnected mode.
    """
    with TestClient(app) as client:
        with client.websocket_connect("/ws") as ws:
            ws.receive_json()  # consume session_created
            ws.send_json({"type": "message", "text": "hello"})
            data = ws.receive_json()

    assert data["type"] == "error"
    assert "disconnected" in data["message"].lower()


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


def test_lifespan_shutdown_awaits_close_all() -> None:
    """Lifespan shutdown must await close_all() on the session manager.

    Regression: close_all() was called without await, silently discarding the
    coroutine object and leaving all active Amplifier sessions unclosed on
    service teardown.  AsyncMock distinguishes call_count from await_count so
    assert_awaited_once() is only satisfied when the coroutine is actually
    driven to completion.
    """
    close_all_mock = AsyncMock()

    with TestClient(app) as client:
        # Inject an async close_all into the already-running session manager.
        # The lifespan shutdown path uses getattr() so setting the attribute
        # here is sufficient to exercise the branch.
        app.state.session_manager.close_all = close_all_mock
        _ = client  # keep the client alive until context exit triggers shutdown

    # Shutdown has now run.  The mock must have been awaited, not merely called.
    close_all_mock.assert_awaited_once()


def test_ws_execute_error_sends_error_and_keeps_connection() -> None:
    """When execute() raises, the client receives an error message and the WS stays open."""
    with TestClient(app) as client:
        # runtime_connected must be True so the handler reaches execute()
        app.state.runtime_connected = True
        try:
            with patch.object(
                app.state.session_manager,
                "execute",
                new_callable=AsyncMock,
                side_effect=RuntimeError("LLM timeout"),
            ):
                with client.websocket_connect("/ws") as ws:
                    ws.receive_json()  # consume session_created
                    ws.send_json({"type": "message", "text": "hello"})
                    data = ws.receive_json()
                    assert data["type"] == "error"
                    assert "LLM timeout" in data["message"]
                    # WS is still open — send another message
                    ws.send_json({"type": "action", "componentId": "test-1"})
                    ack = ws.receive_json()
                    assert ack["type"] == "action_ack"
        finally:
            app.state.runtime_connected = False
