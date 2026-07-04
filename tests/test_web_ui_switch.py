"""F4: web_ui_enabled switch tests — tester-breaker spec.

Proves:
  1. Settings.web_ui_enabled flag exists and defaults to True.
  2. create_asgi_app(web_ui_enabled=False) wires _EXEMPT_PATHS_API_ONLY into the
     middleware — /logs/stream and web-UI paths are NOT in the exempt set.
  3. A minimal API-only ASGI app (FastAPI with openapi_url=None, no web routes):
       GET /openapi.json  → NOT 200 (schema not served)
       GET /docs          → 404
       GET /dashboard invalid bearer → 401 or 404, NEVER a 200 bypass
       GET /logs/stream   → 401 (not in exempt set; middleware gates it)
       GET /skills/<x>    → NOT 401 (prefix-exempt, bundle path stays)
       GET /status        → 200 (always exempt)
  4. Regression — web_ui_enabled=True (default): existing routes unbroken.

F4 (tester-breaker gate): /openapi.json bypass is the sneakiest path.
"""

from __future__ import annotations

import hashlib
from collections.abc import AsyncGenerator

import httpx
import pytest

# ---------------------------------------------------------------------------
# Test constants — never real credentials
# ---------------------------------------------------------------------------

_TOKEN = "api-only-test-token-f4"
_DIGEST = hashlib.sha256(_TOKEN.encode()).hexdigest()


# ---------------------------------------------------------------------------
# Minimal API-only fixture (mirrors production web_ui_enabled=False)
# ---------------------------------------------------------------------------


@pytest.fixture
async def api_only_client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """httpx client backed by a minimal API-only ASGI app.

    Mirrors the production setup when web_ui_enabled=False:
    - FastAPI with docs_url=None, redoc_url=None, openapi_url=None
    - Only /status and /skills/* registered (no /, /dashboard, /logs/stream)
    - BearerTokenMiddleware with _EXEMPT_PATHS_API_ONLY (no web-UI paths exempt)
    """
    from fastapi import FastAPI  # noqa: PLC0415

    from context_intelligence_server.auth import (  # noqa: PLC0415
        BearerTokenMiddleware,
        StaticKeyResolver,
        _EXEMPT_PATHS_API_ONLY,
    )
    from context_intelligence_server.routers.skills import (  # noqa: PLC0415
        SkillRegistry,
        router as skills_router,
    )

    # Mirrors FastAPI construction with web_ui_enabled=False
    mini_app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

    @mini_app.get("/status")
    async def _status() -> dict[str, str]:
        return {"status": "ok"}

    mini_app.include_router(skills_router)
    # Guard: skills router reads app.state.skill_registry in the handler
    mini_app.state.skill_registry = SkillRegistry()

    resolver = StaticKeyResolver({_DIGEST: "owner"})
    asgi = BearerTokenMiddleware(
        mini_app, resolver=resolver, exempt_paths=_EXEMPT_PATHS_API_ONLY
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=asgi),
        base_url="http://test",
    ) as c:
        yield c


# ---------------------------------------------------------------------------
# 1. Settings flag
# ---------------------------------------------------------------------------


class TestWebUiEnabledFlag:
    """Settings.web_ui_enabled exists, defaults True, reads from env."""

    def test_web_ui_enabled_defaults_true(self) -> None:
        """web_ui_enabled defaults to True — current behavior is preserved."""
        from context_intelligence_server.config import Settings  # noqa: PLC0415

        s = Settings()
        assert s.web_ui_enabled is True

    def test_web_ui_enabled_false_via_settings(self) -> None:
        """web_ui_enabled=False can be set programmatically."""
        from context_intelligence_server.config import Settings  # noqa: PLC0415

        s = Settings(web_ui_enabled=False)
        assert s.web_ui_enabled is False

    def test_web_ui_enabled_false_via_env(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """web_ui_enabled=False is readable from the env-prefixed variable."""
        from context_intelligence_server.config import Settings  # noqa: PLC0415

        monkeypatch.setenv(
            "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_WEB_UI_ENABLED", "false"
        )
        s = Settings()
        assert s.web_ui_enabled is False


# ---------------------------------------------------------------------------
# 2. create_asgi_app() exempt path selection
# ---------------------------------------------------------------------------


class TestCreateAsgiAppExemptPaths:
    """create_asgi_app() sets _exempt_paths based on web_ui_enabled."""

    def test_web_ui_disabled_uses_api_only_exempt_paths(self) -> None:
        """web_ui_enabled=False → middleware gets _EXEMPT_PATHS_API_ONLY."""
        from context_intelligence_server.auth import _EXEMPT_PATHS_API_ONLY  # noqa: PLC0415
        from context_intelligence_server.config import Settings  # noqa: PLC0415
        from context_intelligence_server.main import create_asgi_app  # noqa: PLC0415

        settings = Settings(
            auth_mode="static", allow_unauthenticated=True, web_ui_enabled=False
        )
        wrapped = create_asgi_app(settings=settings)

        assert wrapped._exempt_paths == _EXEMPT_PATHS_API_ONLY, (
            "web_ui_enabled=False must wire _EXEMPT_PATHS_API_ONLY into middleware"
        )

    def test_web_ui_disabled_logs_stream_not_exempt(self) -> None:
        """/logs/stream is NOT exempt when web_ui_enabled=False."""
        from context_intelligence_server.config import Settings  # noqa: PLC0415
        from context_intelligence_server.main import create_asgi_app  # noqa: PLC0415

        settings = Settings(
            auth_mode="static", allow_unauthenticated=True, web_ui_enabled=False
        )
        wrapped = create_asgi_app(settings=settings)

        assert "/logs/stream" not in wrapped._exempt_paths, (
            "/logs/stream must NOT be exempt in api-only mode — "
            "it is an unauthenticated log drain if exempt"
        )

    def test_web_ui_disabled_status_stays_exempt(self) -> None:
        """/status IS exempt in api-only mode -- it is the Azure Container Apps
        liveness/health probe and must NEVER require auth.

        /status and /version are both unauthenticated liveness carve-outs.
        """
        from context_intelligence_server.config import Settings  # noqa: PLC0415
        from context_intelligence_server.main import create_asgi_app  # noqa: PLC0415

        settings = Settings(
            auth_mode="static", allow_unauthenticated=True, web_ui_enabled=False
        )
        wrapped = create_asgi_app(settings=settings)

        assert "/status" in wrapped._exempt_paths, (
            "/status must stay exempt -- it is the unauthenticated health/liveness probe"
        )
        assert "/version" in wrapped._exempt_paths, (
            "/version must remain exempt even in api-only mode (health check)"
        )

    def test_web_ui_enabled_uses_full_exempt_paths(self) -> None:
        """web_ui_enabled=True (default) → middleware gets full _EXEMPT_PATHS."""
        from context_intelligence_server.auth import _EXEMPT_PATHS  # noqa: PLC0415
        from context_intelligence_server.config import Settings  # noqa: PLC0415
        from context_intelligence_server.main import create_asgi_app  # noqa: PLC0415

        settings = Settings(
            auth_mode="static", allow_unauthenticated=True, web_ui_enabled=True
        )
        wrapped = create_asgi_app(settings=settings)

        assert wrapped._exempt_paths == _EXEMPT_PATHS, (
            "web_ui_enabled=True must wire full _EXEMPT_PATHS into middleware"
        )
        # Web-UI paths must be exempt in full mode
        assert "/logs/stream" in wrapped._exempt_paths
        assert "/dashboard" in wrapped._exempt_paths

    def test_module_level_asgi_app_has_full_exempt_paths(self) -> None:
        """Module-level asgi_app (web_ui_enabled=True by default) has full exempt paths.

        Regression: tests that rely on /logs/stream, /, /dashboard being exempt
        must not break.
        """
        from context_intelligence_server.auth import _EXEMPT_PATHS  # noqa: PLC0415
        from context_intelligence_server.main import asgi_app  # noqa: PLC0415

        assert asgi_app._exempt_paths == _EXEMPT_PATHS, (
            "Default module-level asgi_app must use full _EXEMPT_PATHS"
        )


# ---------------------------------------------------------------------------
# 3. F4 HTTP tests — API-only mode via minimal test app
# ---------------------------------------------------------------------------


class TestApiOnlyHttpBehavior:
    """HTTP behavior in API-only mode (web_ui_enabled=False) via httpx.

    Every assertion is on a real HTTP response status.  The minimal test
    app mirrors production web_ui_enabled=False: FastAPI with openapi_url=None,
    docs_url=None, no web-UI routes, _EXEMPT_PATHS_API_ONLY in middleware.
    """

    # ------------------------------------------------------------------
    # Schema and docs — must NOT be served (F4: /openapi.json is sneaky)
    # ------------------------------------------------------------------

    async def test_openapi_json_not_200_in_api_only_mode(
        self, api_only_client: httpx.AsyncClient
    ) -> None:
        """/openapi.json → NOT 200 when openapi_url=None (F4 primary assertion).

        The 'sneaky bypass': if openapi.json were still reachable in api-only
        mode the schema is a road-map for attackers.  With openapi_url=None
        FastAPI does not register the route → 404.
        """
        response = await api_only_client.get("/openapi.json")
        assert response.status_code != 200, (
            f"GET /openapi.json must NOT return 200 in api-only mode, "
            f"got {response.status_code}"
        )

    async def test_docs_not_served_in_api_only_mode(
        self, api_only_client: httpx.AsyncClient
    ) -> None:
        """GET /docs → 404 when docs_url=None (valid token passes auth, FastAPI says 404).

        Uses a valid bearer so the middleware passes and we prove FastAPI itself
        does not serve the route (not just middleware blocking with 401).
        """
        response = await api_only_client.get(
            "/docs",
            headers={"Authorization": f"Bearer {_TOKEN}"},
        )
        assert response.status_code == 404, (
            f"GET /docs with valid token must return 404 in api-only mode "
            f"(docs_url=None — FastAPI does not register the route), "
            f"got {response.status_code}"
        )

    # ------------------------------------------------------------------
    # /dashboard — route absent; invalid bearer must NEVER produce 200
    # ------------------------------------------------------------------

    async def test_dashboard_invalid_bearer_never_200(
        self, api_only_client: httpx.AsyncClient
    ) -> None:
        """GET /dashboard with invalid bearer → 401 or 404, NEVER a 200 bypass.

        F4 critical assertion: a wrong token on an absent web-UI route must not
        produce 200. Proves auth ran (401) or route is absent (404).
        """
        response = await api_only_client.get(
            "/dashboard",
            headers={"Authorization": "Bearer obviously-wrong-token"},
        )
        assert response.status_code != 200, (
            f"GET /dashboard with invalid bearer must not return 200 in api-only "
            f"mode, got {response.status_code} — "
            "auth ran (401) or route absent (404) both acceptable"
        )

    async def test_dashboard_no_token_not_200(
        self, api_only_client: httpx.AsyncClient
    ) -> None:
        """GET /dashboard without any token → not 200 in api-only mode."""
        response = await api_only_client.get("/dashboard")
        assert response.status_code != 200, (
            f"GET /dashboard without token must not return 200 in api-only mode, "
            f"got {response.status_code}"
        )

    # ------------------------------------------------------------------
    # /logs/stream — must require auth (not an unauthenticated drain)
    # ------------------------------------------------------------------

    async def test_logs_stream_requires_auth_in_api_only_mode(
        self, api_only_client: httpx.AsyncClient
    ) -> None:
        """GET /logs/stream without token → 401 in api-only mode.

        /logs/stream is NOT in _EXEMPT_PATHS_API_ONLY.  The middleware must
        gate the request before it reaches FastAPI, returning 401 — never pass
        an unauthenticated stream.
        """
        response = await api_only_client.get("/logs/stream")
        assert response.status_code == 401, (
            f"GET /logs/stream without token must return 401 in api-only mode "
            f"(not in exempt set), got {response.status_code}"
        )

    # ------------------------------------------------------------------
    # Paths that MUST still be reachable
    # ------------------------------------------------------------------

    async def test_status_stays_public_in_api_only_mode(
        self, api_only_client: httpx.AsyncClient
    ) -> None:
        """GET /status without token → 200 in api-only mode.

        /status IS in _EXEMPT_PATHS_API_ONLY -- it is the Azure Container Apps
        liveness/health probe and must never require auth, alongside /version.
        """
        response = await api_only_client.get("/status")
        assert response.status_code == 200, (
            f"GET /status without token must return 200 in api-only mode "
            f"(exempt -- health/liveness probe), got {response.status_code}"
        )

    async def test_skills_prefix_not_auth_blocked_in_api_only_mode(
        self, api_only_client: httpx.AsyncClient
    ) -> None:
        """GET /skills/<x> → NOT 401 — prefix-exempt; the bundle fetches skills here."""
        response = await api_only_client.get("/skills/nonexistent-skill-f4-test")
        assert response.status_code != 401, (
            f"GET /skills/* must not return 401 in api-only mode (prefix-exempt), "
            f"got {response.status_code}"
        )


# ---------------------------------------------------------------------------
# 4. Regression — web_ui_enabled=True (default): existing routes unbroken
# ---------------------------------------------------------------------------


class TestWebUiEnabledRegression:
    """web_ui_enabled=True (default) — existing web UI routes are still registered."""

    async def test_dashboard_returns_200_with_web_ui_enabled(
        self, client: httpx.AsyncClient
    ) -> None:
        """GET /dashboard → 200 with default web_ui_enabled=True.  Regression."""
        response = await client.get("/dashboard")
        assert response.status_code == 200, (
            f"GET /dashboard must return 200 with web_ui_enabled=True (default), "
            f"got {response.status_code}"
        )

    async def test_openapi_json_served_with_web_ui_enabled(
        self, client: httpx.AsyncClient
    ) -> None:
        """GET /openapi.json → 200 with default web_ui_enabled=True.  Regression."""
        response = await client.get("/openapi.json")
        assert response.status_code == 200, (
            f"GET /openapi.json must return 200 with web_ui_enabled=True (default), "
            f"got {response.status_code}"
        )

    async def test_index_returns_200_with_web_ui_enabled(
        self, client: httpx.AsyncClient
    ) -> None:
        """GET / → 200 with default web_ui_enabled=True.  Regression."""
        response = await client.get("/")
        assert response.status_code == 200, (
            f"GET / must return 200 with web_ui_enabled=True (default), "
            f"got {response.status_code}"
        )

    def test_logs_stream_in_full_exempt_paths(self) -> None:
        """/logs/stream is in full exempt set (default mode). Regression."""
        from context_intelligence_server.auth import _EXEMPT_PATHS  # noqa: PLC0415

        assert "/logs/stream" in _EXEMPT_PATHS, (
            "/logs/stream must be in _EXEMPT_PATHS (default full-web mode)"
        )

    async def test_api_routes_still_work_with_web_ui_enabled(
        self, client: httpx.AsyncClient
    ) -> None:
        """/status → 200 with web_ui_enabled=True. Confirms API routes untouched."""
        response = await client.get("/status")
        assert response.status_code == 200
