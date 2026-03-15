"""Tests for the Intelligence Service FastAPI application."""

import pytest
import httpx
from starlette.testclient import TestClient

from intelligence_service.app import app


@pytest.mark.asyncio
async def test_health_returns_200_with_status_ok() -> None:
    """GET /health returns 200 with {'status': 'ok'}."""
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.get("/health")

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"


@pytest.mark.asyncio
async def test_reload_bundle_returns_stub_response() -> None:
    """GET /admin/reload-bundle returns 200 with data['status']=='reload_not_implemented'."""
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.get("/admin/reload-bundle")

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "reload_not_implemented"


# ---------------------------------------------------------------------------
# WebSocket tests — use Starlette TestClient (sync) for WS support
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
