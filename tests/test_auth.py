"""Tests for bearer token authentication middleware."""

import json
from unittest.mock import AsyncMock

import httpx

from context_intelligence_server.auth import BearerTokenMiddleware


def _make_scope(path: str = "/events", method: str = "POST") -> dict:
    """Build a minimal ASGI scope dict."""
    return {
        "type": "http",
        "path": path,
        "method": method,
        "headers": [],
    }


def _make_scope_with_auth(
    token: str, path: str = "/events", method: str = "POST"
) -> dict:
    """Build an ASGI scope with an Authorization header."""
    return {
        "type": "http",
        "path": path,
        "method": method,
        "headers": [(b"authorization", f"Bearer {token}".encode())],
    }


class TestMiddlewareNoApiKey:
    """When api_key is None (not configured), all requests pass through."""

    async def test_request_passes_without_api_key_configured(self) -> None:
        """No api_key configured means no auth required — request reaches the app."""
        app = AsyncMock()
        middleware = BearerTokenMiddleware(app, api_key=None)

        scope = _make_scope("/events")
        receive = AsyncMock()
        send = AsyncMock()

        await middleware(scope, receive, send)
        app.assert_called_once_with(scope, receive, send)


class TestMiddlewareWithApiKey:
    """When api_key is set, all requests except /status require valid token."""

    async def test_missing_token_returns_401(self) -> None:
        """Request without Authorization header gets 401."""
        app = AsyncMock()
        middleware = BearerTokenMiddleware(app, api_key="secret-token")

        scope = _make_scope("/events")
        receive = AsyncMock()
        send = AsyncMock()

        await middleware(scope, receive, send)

        app.assert_not_called()
        response_start = send.call_args_list[0][0][0]
        assert response_start["type"] == "http.response.start"
        assert response_start["status"] == 401

    async def test_wrong_token_returns_401(self) -> None:
        """Request with wrong bearer token gets 401."""
        app = AsyncMock()
        middleware = BearerTokenMiddleware(app, api_key="secret-token")

        scope = _make_scope_with_auth("wrong-token", "/events")
        receive = AsyncMock()
        send = AsyncMock()

        await middleware(scope, receive, send)

        app.assert_not_called()
        response_start = send.call_args_list[0][0][0]
        assert response_start["status"] == 401

    async def test_valid_token_passes_through(self) -> None:
        """Request with correct bearer token reaches the app."""
        app = AsyncMock()
        middleware = BearerTokenMiddleware(app, api_key="secret-token")

        scope = _make_scope_with_auth("secret-token", "/events")
        receive = AsyncMock()
        send = AsyncMock()

        await middleware(scope, receive, send)
        app.assert_called_once_with(scope, receive, send)

    async def test_status_exempt_without_token(self) -> None:
        """/status is accessible without any token."""
        app = AsyncMock()
        middleware = BearerTokenMiddleware(app, api_key="secret-token")

        scope = _make_scope("/status", "GET")
        receive = AsyncMock()
        send = AsyncMock()

        await middleware(scope, receive, send)
        app.assert_called_once_with(scope, receive, send)

    async def test_non_http_scope_passes_through(self) -> None:
        """Non-HTTP scopes (e.g. websocket, lifespan) are not intercepted."""
        app = AsyncMock()
        middleware = BearerTokenMiddleware(app, api_key="secret-token")

        scope = {"type": "lifespan"}
        receive = AsyncMock()
        send = AsyncMock()

        await middleware(scope, receive, send)
        app.assert_called_once_with(scope, receive, send)

    async def test_401_response_body_is_json(self) -> None:
        """401 response body is JSON with a detail field."""
        app = AsyncMock()
        middleware = BearerTokenMiddleware(app, api_key="secret-token")

        scope = _make_scope("/events")
        receive = AsyncMock()
        send = AsyncMock()

        await middleware(scope, receive, send)

        response_body = send.call_args_list[1][0][0]
        assert response_body["type"] == "http.response.body"
        body = json.loads(response_body["body"])
        assert "detail" in body

    async def test_logs_stream_exempt_without_token(self) -> None:
        """/logs/stream is accessible without any token (EventSource compatibility)."""
        app = AsyncMock()
        middleware = BearerTokenMiddleware(app, api_key="secret-token")

        scope = _make_scope("/logs/stream", "GET")
        receive = AsyncMock()
        send = AsyncMock()

        await middleware(scope, receive, send)
        app.assert_called_once_with(scope, receive, send)

    async def test_index_page_exempt(self) -> None:
        """GET / without token passes through (dashboard HTML must load before JS auth)."""
        app = AsyncMock()
        middleware = BearerTokenMiddleware(app, api_key="secret-token")

        scope = _make_scope("/", "GET")
        receive = AsyncMock()
        send = AsyncMock()

        await middleware(scope, receive, send)
        app.assert_called_once_with(scope, receive, send)

    async def test_dashboard_page_exempt(self) -> None:
        """GET /dashboard without token passes through."""
        app = AsyncMock()
        middleware = BearerTokenMiddleware(app, api_key="secret-token")

        scope = _make_scope("/dashboard", "GET")
        receive = AsyncMock()
        send = AsyncMock()

        await middleware(scope, receive, send)
        app.assert_called_once_with(scope, receive, send)

    async def test_static_assets_exempt(self) -> None:
        """GET /static/js/api.js without token passes through (static assets must load before auth)."""
        app = AsyncMock()
        middleware = BearerTokenMiddleware(app, api_key="secret-token")

        scope = _make_scope("/static/js/api.js", "GET")
        receive = AsyncMock()
        send = AsyncMock()

        await middleware(scope, receive, send)
        app.assert_called_once_with(scope, receive, send)

    async def test_docs_page_exempt(self) -> None:
        """GET /docs without token passes through."""
        app = AsyncMock()
        middleware = BearerTokenMiddleware(app, api_key="secret-token")

        scope = _make_scope("/docs", "GET")
        receive = AsyncMock()
        send = AsyncMock()

        await middleware(scope, receive, send)
        app.assert_called_once_with(scope, receive, send)


class TestConstantTimeComparison:
    """Token comparison must be constant-time to prevent timing attacks."""

    def test_auth_uses_hmac_compare_digest(self) -> None:
        """auth.py must use hmac.compare_digest for token comparison, not ==."""
        import inspect

        import context_intelligence_server.auth as auth_module

        source = inspect.getsource(auth_module)
        assert "hmac.compare_digest" in source, (
            "Token comparison must use hmac.compare_digest() "
            "to prevent timing attacks, not == or !="
        )


# ---------------------------------------------------------------------------
# Module-level asgi_app export tests
# ---------------------------------------------------------------------------


class TestAsgiAppModuleLevel:
    """asgi_app must be exposed at module level so Gunicorn CMD can reference it."""

    def test_asgi_app_is_importable(self) -> None:
        """asgi_app is importable from context_intelligence_server.main."""
        from context_intelligence_server.main import asgi_app  # noqa: PLC0415

        assert isinstance(asgi_app, BearerTokenMiddleware)

    def test_asgi_app_wraps_fastapi_app(self) -> None:
        """asgi_app.app is the FastAPI app instance."""
        from context_intelligence_server.main import app, asgi_app  # noqa: PLC0415

        assert asgi_app.app is app


# ---------------------------------------------------------------------------
# Integration tests: auth enforcement through asgi_app
# ---------------------------------------------------------------------------


class TestAuthEnforcedViaAsgiApp:
    """Verify auth middleware IS applied when requests flow through asgi_app."""

    async def test_events_without_token_returns_401(
        self, auth_client: httpx.AsyncClient
    ) -> None:
        """POST /events without Authorization header returns 401 when api_key is set."""
        response = await auth_client.post(
            "/events",
            json={
                "event": "tool_use",
                "workspace": "/ws",
                "data": {"session_id": "s1"},
            },
        )
        assert response.status_code == 401

    async def test_events_with_wrong_token_returns_401(
        self, auth_client: httpx.AsyncClient
    ) -> None:
        """POST /events with incorrect bearer token returns 401."""
        response = await auth_client.post(
            "/events",
            json={
                "event": "tool_use",
                "workspace": "/ws",
                "data": {"session_id": "s1"},
            },
            headers={"Authorization": "Bearer wrong-token"},
        )
        assert response.status_code == 401

    async def test_events_with_correct_token_passes(
        self, auth_client: httpx.AsyncClient
    ) -> None:
        """POST /events with correct bearer token reaches the route handler (not 401)."""
        response = await auth_client.post(
            "/events",
            json={
                "event": "tool_use",
                "workspace": "/ws",
                "data": {"session_id": "s1"},
            },
            headers={"Authorization": "Bearer test-secret"},
        )
        assert response.status_code != 401

    async def test_status_without_token_returns_200(
        self, auth_client: httpx.AsyncClient
    ) -> None:
        """GET /status is always exempt — returns 200 without any token."""
        response = await auth_client.get("/status")
        assert response.status_code == 200

    async def test_blobs_without_token_returns_401(
        self, auth_client: httpx.AsyncClient
    ) -> None:
        """GET /blobs/{session_id} without token returns 401 when api_key is set."""
        response = await auth_client.get("/blobs/test-session")
        assert response.status_code == 401

    async def test_cypher_without_token_returns_401(
        self, auth_client: httpx.AsyncClient
    ) -> None:
        """POST /cypher without token returns 401 when api_key is set."""
        response = await auth_client.post(
            "/cypher", json={"query": "MATCH (n) RETURN n"}
        )
        assert response.status_code == 401
