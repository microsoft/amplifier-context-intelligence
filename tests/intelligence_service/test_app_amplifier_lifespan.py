"""Tests for Amplifier-aware lifespan and endpoint behavior in app.py.

These tests verify:
  - POST /admin/reload-bundle endpoint has been completely removed (returns 404 or 405)
  - WebSocket 'message' handler dispatches via execute() when available
  - WebSocket 'message' handler echoes text in stub mode (when no execute available)

Note: The lifespan always tries to import AmplifierIntelligenceRuntime; since
amplifier_foundation is not installed in test environments the startup() call
raises NotImplementedError which is caught by ``except Exception`` and the app
gracefully falls back to StubSessionManager.
"""

from unittest.mock import AsyncMock, MagicMock

import httpx
from fastapi.testclient import TestClient

from intelligence_service.app import app


async def test_reload_bundle_endpoint_removed(
    client: httpx.AsyncClient,
) -> None:
    """POST /admin/reload-bundle is no longer registered — returns 404 or 405."""
    response = await client.post("/admin/reload-bundle")

    assert response.status_code in (404, 405)


def test_ws_message_dispatches_via_execute_when_available() -> None:
    """WebSocket 'message' handler calls execute() when runtime is connected."""
    with TestClient(app) as client:
        original_sm = app.state.session_manager

        mock_sm = MagicMock()
        mock_sm.create_session = AsyncMock(return_value="fixed-session-id")
        mock_sm.destroy_session = AsyncMock()
        mock_sm.reset_session = AsyncMock(return_value="new-fixed-session-id")
        mock_sm.execute = AsyncMock(
            return_value={"text": "amplifier says hello", "a2ui": []}
        )

        app.state.session_manager = mock_sm
        app.state.runtime_connected = True
        try:
            with client.websocket_connect("/ws") as ws:
                ws.receive_json()  # consume session_created
                ws.send_json({"type": "message", "text": "ping from test"})
                data = ws.receive_json()

            assert data["type"] == "response"
            assert data["payload"] == "amplifier says hello"
            mock_sm.execute.assert_awaited_once_with(
                "fixed-session-id", "ping from test"
            )
        finally:
            app.state.session_manager = original_sm
            app.state.runtime_connected = False


def test_ws_message_returns_error_when_disconnected() -> None:
    """WebSocket 'message' handler returns a disconnected error in stub/disconnected mode.

    In test environments amplifier_foundation is not installed, so the lifespan
    catches the startup failure. Messages sent while disconnected must receive an
    explicit error rather than a silent echo.
    """
    with TestClient(app) as client:
        with client.websocket_connect("/ws") as ws:
            ws.receive_json()  # consume session_created
            ws.send_json({"type": "message", "text": "echo this"})
            data = ws.receive_json()

    assert data["type"] == "error"
    assert "disconnected" in data["message"].lower()
