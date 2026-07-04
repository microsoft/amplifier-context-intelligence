"""Tests for bearer token authentication middleware."""

import hashlib
import json
import logging
from unittest.mock import AsyncMock

import httpx

from context_intelligence_server.auth import BearerTokenMiddleware, _resolve_token


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


def _keystore(token: str, contributor_id: str = "tester") -> dict[str, str]:
    """Build a minimal keystore from a raw token string (for test convenience)."""
    return {hashlib.sha256(token.encode()).hexdigest(): contributor_id}


class TestMiddlewareNoApiKey:
    """When keystore is empty (no keys configured), all requests pass through."""

    async def test_request_passes_without_keystore_configured(self) -> None:
        """Empty keystore means no auth required — request reaches the app."""
        app = AsyncMock()
        middleware = BearerTokenMiddleware(app, keystore={})

        scope = _make_scope("/events")
        receive = AsyncMock()
        send = AsyncMock()

        await middleware(scope, receive, send)
        app.assert_called_once_with(scope, receive, send)

    async def test_request_passes_with_none_keystore(self) -> None:
        """None keystore (default) is equivalent to empty — no auth required."""
        app = AsyncMock()
        middleware = BearerTokenMiddleware(app)  # keystore=None → {}

        scope = _make_scope("/events")
        receive = AsyncMock()
        send = AsyncMock()

        await middleware(scope, receive, send)
        app.assert_called_once_with(scope, receive, send)


class TestMiddlewareWithKeystore:
    """When keystore is non-empty, all requests except exempt paths require a valid token."""

    async def test_missing_token_returns_401(self) -> None:
        """Request without Authorization header gets 401."""
        app = AsyncMock()
        middleware = BearerTokenMiddleware(app, keystore=_keystore("secret-token"))

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
        middleware = BearerTokenMiddleware(app, keystore=_keystore("secret-token"))

        scope = _make_scope_with_auth("wrong-token", "/events")
        receive = AsyncMock()
        send = AsyncMock()

        await middleware(scope, receive, send)

        app.assert_not_called()
        response_start = send.call_args_list[0][0][0]
        assert response_start["status"] == 401

    async def test_wrong_token_logs_auth_denied(self, caplog) -> None:
        """A static-key miss (result is None) must log auth_event=auth_denied.

        Regression guard: this 401 path previously returned without any log
        line, so a genuine rejection (e.g. a redacted 'Bearer [REDACTED]')
        left zero trace in server.jsonl and looked impossible to diagnose.
        """
        app = AsyncMock()
        middleware = BearerTokenMiddleware(app, keystore=_keystore("secret-token"))

        scope = _make_scope_with_auth("wrong-token", "/events")
        receive = AsyncMock()
        send = AsyncMock()

        with caplog.at_level(logging.INFO, logger="context_intelligence_server.auth"):
            await middleware(scope, receive, send)

        app.assert_not_called()
        assert send.call_args_list[0][0][0]["status"] == 401
        # A greppable auth_denied marker is emitted, with a token fingerprint
        # (sha256 prefix) and NOT the raw token.
        messages = [r.getMessage() for r in caplog.records]
        denied = [m for m in messages if "auth_event=auth_denied" in m]
        assert denied, f"expected an auth_denied log; got {messages!r}"
        expected_fp = hashlib.sha256(b"wrong-token").hexdigest()[:12]
        assert any(expected_fp in m for m in denied)
        assert not any("wrong-token" in m for m in messages)

    async def test_redacted_sentinel_token_logs_recognisable_digest(
        self, caplog
    ) -> None:
        """The '[REDACTED]' sentinel 401 is logged with its recognisable digest.

        This is the exact failure the fix makes visible: a resumed session
        mounting a redacted credential sends 'Bearer [REDACTED]'.
        """
        app = AsyncMock()
        middleware = BearerTokenMiddleware(app, keystore=_keystore("secret-token"))

        scope = _make_scope_with_auth("[REDACTED]", "/events")
        receive = AsyncMock()
        send = AsyncMock()

        with caplog.at_level(logging.INFO, logger="context_intelligence_server.auth"):
            await middleware(scope, receive, send)

        assert send.call_args_list[0][0][0]["status"] == 401
        fp = hashlib.sha256(b"[REDACTED]").hexdigest()[:12]
        assert any(
            "auth_event=auth_denied" in r.getMessage() and fp in r.getMessage()
            for r in caplog.records
        )

    async def test_valid_token_passes_through(self) -> None:
        """Request with correct bearer token reaches the app."""
        app = AsyncMock()
        middleware = BearerTokenMiddleware(app, keystore=_keystore("secret-token"))

        scope = _make_scope_with_auth("secret-token", "/events")
        receive = AsyncMock()
        send = AsyncMock()

        await middleware(scope, receive, send)
        app.assert_called_once_with(scope, receive, send)

    async def test_valid_token_injects_contributor_id_into_scope(self) -> None:
        """T10: valid token injects contributor_id into scope['state']."""
        app = AsyncMock()
        middleware = BearerTokenMiddleware(
            app, keystore=_keystore("secret-token", contributor_id="alice")
        )

        scope = _make_scope_with_auth("secret-token", "/events")
        receive = AsyncMock()
        send = AsyncMock()

        await middleware(scope, receive, send)

        assert scope.get("state", {}).get("contributor_id") == "alice"

    async def test_status_stays_exempt(self) -> None:
        """/status stays exempt from auth -- it is the Azure Container Apps
        liveness/health probe and must never require a token."""
        app = AsyncMock()
        middleware = BearerTokenMiddleware(app, keystore=_keystore("secret-token"))

        scope = _make_scope("/status", "GET")
        receive = AsyncMock()
        send = AsyncMock()

        await middleware(scope, receive, send)
        app.assert_called_once_with(scope, receive, send)

    async def test_non_http_scope_passes_through(self) -> None:
        """Non-HTTP scopes (e.g. websocket, lifespan) are not intercepted."""
        app = AsyncMock()
        middleware = BearerTokenMiddleware(app, keystore=_keystore("secret-token"))

        scope = {"type": "lifespan"}
        receive = AsyncMock()
        send = AsyncMock()

        await middleware(scope, receive, send)
        app.assert_called_once_with(scope, receive, send)

    async def test_401_response_body_is_json(self) -> None:
        """401 response body is JSON with a detail field."""
        app = AsyncMock()
        middleware = BearerTokenMiddleware(app, keystore=_keystore("secret-token"))

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
        middleware = BearerTokenMiddleware(app, keystore=_keystore("secret-token"))

        scope = _make_scope("/logs/stream", "GET")
        receive = AsyncMock()
        send = AsyncMock()

        await middleware(scope, receive, send)
        app.assert_called_once_with(scope, receive, send)

    async def test_index_page_exempt(self) -> None:
        """GET / without token passes through (dashboard HTML must load before JS auth)."""
        app = AsyncMock()
        middleware = BearerTokenMiddleware(app, keystore=_keystore("secret-token"))

        scope = _make_scope("/", "GET")
        receive = AsyncMock()
        send = AsyncMock()

        await middleware(scope, receive, send)
        app.assert_called_once_with(scope, receive, send)

    async def test_dashboard_page_exempt(self) -> None:
        """GET /dashboard without token passes through."""
        app = AsyncMock()
        middleware = BearerTokenMiddleware(app, keystore=_keystore("secret-token"))

        scope = _make_scope("/dashboard", "GET")
        receive = AsyncMock()
        send = AsyncMock()

        await middleware(scope, receive, send)
        app.assert_called_once_with(scope, receive, send)

    async def test_static_assets_exempt(self) -> None:
        """GET /static/js/api.js without token passes through (static assets must load before auth)."""
        app = AsyncMock()
        middleware = BearerTokenMiddleware(app, keystore=_keystore("secret-token"))

        scope = _make_scope("/static/js/api.js", "GET")
        receive = AsyncMock()
        send = AsyncMock()

        await middleware(scope, receive, send)
        app.assert_called_once_with(scope, receive, send)

    async def test_docs_page_exempt(self) -> None:
        """GET /docs without token passes through."""
        app = AsyncMock()
        middleware = BearerTokenMiddleware(app, keystore=_keystore("secret-token"))

        scope = _make_scope("/docs", "GET")
        receive = AsyncMock()
        send = AsyncMock()

        await middleware(scope, receive, send)
        app.assert_called_once_with(scope, receive, send)


# ---------------------------------------------------------------------------
# T6-T8: _resolve_token unit tests
# ---------------------------------------------------------------------------


class TestResolveToken:
    """T6-T8: _resolve_token resolves correctly, returns None on miss, never 'unknown'."""

    def test_resolve_token_returns_contributor_id_on_match(self) -> None:
        """T6: _resolve_token returns contributor_id when sha256(token) is in keystore."""
        token = "mysecrettoken"
        contributor_id = "alice"
        ks = {hashlib.sha256(token.encode()).hexdigest(): contributor_id}
        assert _resolve_token(token, ks) == contributor_id

    def test_resolve_token_returns_none_on_miss(self) -> None:
        """T7: _resolve_token returns None (not 'unknown') when token is not in keystore."""
        ks = {hashlib.sha256(b"other-token").hexdigest(): "bob"}
        result = _resolve_token("wrong-token", ks)
        assert result is None
        assert result != "unknown"

    def test_resolve_token_returns_none_for_empty_keystore(self) -> None:
        """T8: _resolve_token returns None for any token when keystore is empty."""
        assert _resolve_token("any-token", {}) is None


# ---------------------------------------------------------------------------
# T11: source inspection — _resolve_token in source, no hmac.compare_digest
# ---------------------------------------------------------------------------


class TestAuthSourceInspection:
    """T11: source inspection for auth.py security implementation."""

    def test_resolve_token_used_for_comparison(self) -> None:
        """T11: auth.py uses _resolve_token (sha256 hash lookup), not hmac.compare_digest."""
        import inspect

        import context_intelligence_server.auth as auth_module

        source = inspect.getsource(auth_module)
        assert "_resolve_token" in source, (
            "auth.py must use _resolve_token for token comparison"
        )
        assert "hmac.compare_digest" not in source, (
            "auth.py must NOT use hmac.compare_digest after the keystore migration"
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
        """GET /status stays exempt from auth -- it is the Azure Container Apps
        liveness/health probe and must never require a token."""
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


class TestSkillsEndpointExempt:
    """GET /skills/* is exempt from authentication even when keystore is configured."""

    async def test_skills_path_exempt_from_auth(self) -> None:
        from context_intelligence_server.auth import BearerTokenMiddleware

        received: list[int] = []

        async def mock_app(scope, receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b""})

        async def mock_send(event):
            if event["type"] == "http.response.start":
                received.append(event["status"])

        middleware = BearerTokenMiddleware(mock_app, keystore=_keystore("secret"))
        scope = {
            "type": "http",
            "path": "/skills/context-intelligence-graph-query",
            "headers": [],  # No Authorization header
        }
        await middleware(scope, None, mock_send)
        assert received == [200], f"Expected 200 (exempt), got {received}"

    async def test_skills_subpath_exempt(self) -> None:
        from context_intelligence_server.auth import BearerTokenMiddleware

        received: list[int] = []

        async def mock_app(scope, receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b""})

        async def mock_send(event):
            if event["type"] == "http.response.start":
                received.append(event["status"])

        middleware = BearerTokenMiddleware(mock_app, keystore=_keystore("secret"))
        scope = {
            "type": "http",
            "path": "/skills/any-other-skill",
            "headers": [],
        }
        await middleware(scope, None, mock_send)
        assert received == [200]


class TestVersionEndpointExempt:
    """/version is exempt from authentication even when keystore is configured."""

    async def test_version_path_exempt_from_auth(self) -> None:
        """GET /version without token passes through (version info must be accessible without auth)."""
        received: list[int] = []

        async def mock_app(scope, receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b""})

        async def mock_send(event):
            if event["type"] == "http.response.start":
                received.append(event["status"])

        middleware = BearerTokenMiddleware(mock_app, keystore=_keystore("secret"))
        scope = {
            "type": "http",
            "path": "/version",
            "headers": [],  # No Authorization header
        }
        await middleware(scope, None, mock_send)
        assert received == [200], f"Expected 200 (exempt), got {received}"

    async def test_version_path_with_auth_also_passes(self) -> None:
        """GET /version with a valid token also returns 200."""
        received: list[int] = []

        async def mock_app(scope, receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b""})

        async def mock_send(event):
            if event["type"] == "http.response.start":
                received.append(event["status"])

        middleware = BearerTokenMiddleware(mock_app, keystore=_keystore("secret"))
        scope = {
            "type": "http",
            "path": "/version",
            "headers": [(b"authorization", b"Bearer secret")],
        }
        await middleware(scope, None, mock_send)
        assert received == [200], f"Expected 200, got {received}"
