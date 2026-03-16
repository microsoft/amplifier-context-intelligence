"""Tests for Amplifier-aware lifespan and new endpoint behaviors in app.py.

These tests verify:
  - reload_bundle is now POST (not GET)
  - Stub mode returns {'status': 'skipped'} on POST /admin/reload-bundle
  - app.state.amplifier_app is None in stub mode
  - POST /admin/reload-bundle returns {'status': 'reloaded'} with a working amplifier_app
  - POST /admin/reload-bundle returns {'status': 'error'} when reload() raises
  - WebSocket 'message' handler dispatches via execute() when available
"""

from unittest.mock import AsyncMock, MagicMock

import httpx
from fastapi.testclient import TestClient

from intelligence_service.app import app


async def test_reload_bundle_post_returns_skipped_in_stub_mode(
    client: httpx.AsyncClient,
) -> None:
    """POST /admin/reload-bundle returns {'status': 'skipped'} when bundle_path is empty."""
    response = await client.post("/admin/reload-bundle")

    assert response.status_code == 200
    assert response.json() == {"status": "skipped"}


async def test_reload_bundle_get_no_longer_exists(
    client: httpx.AsyncClient,
) -> None:
    """GET /admin/reload-bundle no longer exists (returns 405 after POST migration)."""
    response = await client.get("/admin/reload-bundle")

    assert response.status_code == 405


def test_amplifier_app_state_is_none_in_stub_mode() -> None:
    """In stub mode (no bundle_path), app.state.amplifier_app is set to None."""
    with TestClient(app):
        assert app.state.amplifier_app is None


async def test_reload_bundle_post_returns_reloaded_when_amplifier_app_set(
    client: httpx.AsyncClient,
) -> None:
    """POST /admin/reload-bundle returns {'status': 'reloaded'} when amplifier_app is configured."""
    mock_amplifier_app = AsyncMock()
    mock_amplifier_app.reload = AsyncMock()

    original = app.state.amplifier_app
    app.state.amplifier_app = mock_amplifier_app
    try:
        response = await client.post("/admin/reload-bundle")

        assert response.status_code == 200
        assert response.json() == {"status": "reloaded"}
        mock_amplifier_app.reload.assert_awaited_once()
    finally:
        app.state.amplifier_app = original


async def test_reload_bundle_post_returns_error_when_amplifier_app_raises(
    client: httpx.AsyncClient,
) -> None:
    """POST /admin/reload-bundle returns {'status': 'error', 'message': ...} when reload() raises."""
    mock_amplifier_app = AsyncMock()
    mock_amplifier_app.reload = AsyncMock(
        side_effect=RuntimeError("bundle reload failed")
    )

    original = app.state.amplifier_app
    app.state.amplifier_app = mock_amplifier_app
    try:
        response = await client.post("/admin/reload-bundle")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "error"
        assert "bundle reload failed" in data["message"]
    finally:
        app.state.amplifier_app = original


def test_ws_message_dispatches_via_execute_when_available() -> None:
    """WebSocket 'message' handler calls execute() when session_manager has that method."""
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
        try:
            with client.websocket_connect("/ws") as ws:
                ws.receive_json()  # consume session_created
                ws.send_json({"type": "message", "text": "ping from test"})
                data = ws.receive_json()

            assert data["type"] == "response"
            assert data["content"] == "amplifier says hello"
            mock_sm.execute.assert_awaited_once_with(
                "fixed-session-id", "ping from test"
            )
        finally:
            app.state.session_manager = original_sm


def test_ws_message_echoes_text_when_no_execute() -> None:
    """WebSocket 'message' handler echoes text when session_manager has no execute()."""
    with TestClient(app) as client:
        with client.websocket_connect("/ws") as ws:
            ws.receive_json()  # consume session_created
            ws.send_json({"type": "message", "text": "echo this"})
            data = ws.receive_json()

    assert data["type"] == "response"
    assert "echo this" in data["content"]
