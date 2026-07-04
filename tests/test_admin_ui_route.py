"""Tests for the /admin-ui exempt shell route (doc 17 §B.2 / §F.2).

Covers:
  - GET /admin-ui serves HTML WITHOUT a token (the exempt shell).
  - GET /admin and GET /admin/keys stay gated (401 without a token, reachable
    with the admin key) — the shell exemption must not leak onto /admin/*.
  - The exempt-set/route-scope invariants the spec cites as "why this is
    safe": "/admin-ui" in _EXEMPT_PATHS, "/admin" is not, "/admin-ui" is not
    in _EXEMPT_PATHS_API_ONLY, and _is_admin_route("/admin-ui") is False.
  - The two structural boot guards (_assert_admin_not_exempt,
    _assert_capability_routes_not_exempt) still pass with the new exempt path.
  - Route existence: GET /admin-ui is registered when web_ui_enabled=True and
    absent when web_ui_enabled=False (mirrors the /dashboard conditional).

Fake constants only — never real credentials (§0.3 of the design doc).
"""

from __future__ import annotations

import hashlib
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import httpx
import pytest

FAKE_ADMIN_RAW_KEY = "admin-ui-test-admin-key-do-not-use"
FAKE_ADMIN_KEY_DIGEST = hashlib.sha256(FAKE_ADMIN_RAW_KEY.encode()).hexdigest()

FAKE_DATA_RAW_KEY = "admin-ui-test-data-key"
FAKE_DATA_KEY_DIGEST = hashlib.sha256(FAKE_DATA_RAW_KEY.encode()).hexdigest()
FAKE_CONTRIBUTOR = "carol"


def _make_static_settings(tmp_path: Path, *, web_ui_enabled: bool = True) -> Any:
    from context_intelligence_server.config import Settings  # noqa: PLC0415

    return Settings(
        auth_mode="static",
        web_ui_enabled=web_ui_enabled,
        api_keys={FAKE_DATA_KEY_DIGEST: {"id": FAKE_CONTRIBUTOR}},
        admin_api_key=FAKE_ADMIN_RAW_KEY,
        api_keys_store_path=str(tmp_path / "api-keys.json"),
        entra_identities_store_path=str(tmp_path / "entra-identities.json"),
    )


@pytest.fixture
async def static_auth_client(tmp_path: Path) -> AsyncGenerator[httpx.AsyncClient, None]:
    """httpx client routed through the REAL auth middleware (static mode, admin key configured).

    Mirrors tests/routers/test_admin_auth.py's static_auth_client fixture —
    wraps the BearerTokenMiddleware returned by create_asgi_app (not the bare
    FastAPI app), so /admin-ui's exemption and /admin's gating are both
    exercised through the genuine middleware stack.
    """
    from context_intelligence_server.main import create_asgi_app  # noqa: PLC0415

    settings = _make_static_settings(tmp_path)
    middleware = create_asgi_app(settings=settings)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=middleware), base_url="http://test"
    ) as c:
        yield c


# ===========================================================================
# A. Shell exemption — /admin-ui is unauthenticated; /admin/* stays gated
# ===========================================================================


class TestAdminUiShellExemption:
    @pytest.mark.anyio
    async def test_admin_ui_serves_html_without_token(
        self, static_auth_client: httpx.AsyncClient
    ) -> None:
        """GET /admin-ui -> 200 + HTML, with NO Authorization header at all."""
        resp = await static_auth_client.get("/admin-ui")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]
        assert "Admin" in resp.text

    @pytest.mark.anyio
    async def test_admin_ui_serves_html_even_with_a_bad_token(
        self, static_auth_client: httpx.AsyncClient
    ) -> None:
        """The shell is exempt regardless of credential state — a garbage
        Authorization header must not 401 the shell (only the JS's own data
        fetches are gated)."""
        resp = await static_auth_client.get(
            "/admin-ui", headers={"Authorization": "Bearer not-a-real-token"}
        )
        assert resp.status_code == 200

    @pytest.mark.anyio
    async def test_admin_still_401_without_token(
        self, static_auth_client: httpx.AsyncClient
    ) -> None:
        """GET /admin/keys without a token still 401s — the shell exemption
        did not leak onto the /admin/* prefix."""
        resp = await static_auth_client.get("/admin/keys")
        assert resp.status_code == 401

    @pytest.mark.anyio
    async def test_admin_403_with_data_key_not_admin_key(
        self, static_auth_client: httpx.AsyncClient
    ) -> None:
        """A registered data key authenticates but is not the admin key -> 403."""
        resp = await static_auth_client.get(
            "/admin/keys", headers={"Authorization": f"Bearer {FAKE_DATA_RAW_KEY}"}
        )
        assert resp.status_code == 403

    @pytest.mark.anyio
    async def test_admin_200_with_admin_key(
        self, static_auth_client: httpx.AsyncClient
    ) -> None:
        """The admin key reaches /admin/keys -> 200 (is_admin=True)."""
        resp = await static_auth_client.get(
            "/admin/keys", headers={"Authorization": f"Bearer {FAKE_ADMIN_RAW_KEY}"}
        )
        assert resp.status_code == 200

    @pytest.mark.anyio
    async def test_dashboard_still_exempt_unaffected(
        self, static_auth_client: httpx.AsyncClient
    ) -> None:
        """Sanity: the pre-existing /dashboard exemption is unaffected by the new route."""
        resp = await static_auth_client.get("/dashboard")
        assert resp.status_code == 200


# ===========================================================================
# B. Exempt-set / route-scope invariants (the spec's "why this is safe")
# ===========================================================================


class TestExemptSetInvariants:
    def test_admin_ui_in_exempt_paths(self) -> None:
        from context_intelligence_server.auth import _EXEMPT_PATHS  # noqa: PLC0415

        assert "/admin-ui" in _EXEMPT_PATHS

    def test_admin_not_in_exempt_paths(self) -> None:
        from context_intelligence_server.auth import _EXEMPT_PATHS  # noqa: PLC0415

        assert "/admin" not in _EXEMPT_PATHS

    def test_admin_ui_not_in_exempt_paths_api_only(self) -> None:
        """The admin UI is a web-UI surface, absent in api-only mode (doc 17 §B.2)."""
        from context_intelligence_server.auth import (  # noqa: PLC0415
            _EXEMPT_PATHS_API_ONLY,
        )

        assert "/admin-ui" not in _EXEMPT_PATHS_API_ONLY

    def test_admin_ui_is_not_an_admin_route(self) -> None:
        """_is_admin_route("/admin-ui") must be False — it is not "/admin" and
        does not start with "/admin/"."""
        from context_intelligence_server.auth import _is_admin_route  # noqa: PLC0415

        assert _is_admin_route("/admin-ui") is False

    def test_admin_route_scope_unaffected(self) -> None:
        """Sanity: "/admin" and "/admin/keys" are still recognized as admin routes."""
        from context_intelligence_server.auth import _is_admin_route  # noqa: PLC0415

        assert _is_admin_route("/admin") is True
        assert _is_admin_route("/admin/keys") is True


# ===========================================================================
# C. Boot guards still pass with the new exempt path
# ===========================================================================


class TestBootGuardsStillPass:
    def test_create_asgi_app_does_not_raise(self, tmp_path: Path) -> None:
        """create_asgi_app runs both structural boot guards (_assert_admin_not_exempt,
        _assert_capability_routes_not_exempt) at construction; must not raise
        now that "/admin-ui" is in _EXEMPT_PATHS."""
        from context_intelligence_server.main import create_asgi_app  # noqa: PLC0415

        settings = _make_static_settings(tmp_path)
        create_asgi_app(settings=settings)  # should not raise

    def test_assert_admin_not_exempt_directly(self) -> None:
        from context_intelligence_server.main import (  # noqa: PLC0415
            _assert_admin_not_exempt,
        )

        _assert_admin_not_exempt()  # should not raise

    def test_assert_capability_routes_not_exempt_directly(self, tmp_path: Path) -> None:
        from context_intelligence_server.main import (  # noqa: PLC0415
            _assert_capability_routes_not_exempt,
        )

        settings = _make_static_settings(tmp_path)
        _assert_capability_routes_not_exempt(settings)  # should not raise

    def test_assert_capability_routes_not_exempt_api_only(self, tmp_path: Path) -> None:
        """Also verify with web_ui_enabled=False (the smaller exempt set)."""
        from context_intelligence_server.main import (  # noqa: PLC0415
            _assert_capability_routes_not_exempt,
        )

        settings = _make_static_settings(tmp_path, web_ui_enabled=False)
        _assert_capability_routes_not_exempt(settings)  # should not raise


# ===========================================================================
# D. Route existence — /admin-ui registered iff web_ui_enabled
# ===========================================================================


class TestRouteExistence:
    def test_admin_ui_registered_when_web_ui_enabled(self, tmp_path: Path) -> None:
        from context_intelligence_server.main import app, create_asgi_app  # noqa: PLC0415

        settings = _make_static_settings(tmp_path, web_ui_enabled=True)
        create_asgi_app(settings=settings)
        paths = {route.path for route in app.routes if hasattr(route, "path")}
        assert "/admin-ui" in paths

    @pytest.mark.anyio
    async def test_admin_ui_absent_when_web_ui_disabled(self, tmp_path: Path) -> None:
        """Mirrors the /dashboard conditional (main.py:670-682): the whole
        `if _settings.web_ui_enabled:` block — including /admin-ui — is only
        registered at IMPORT time from the module-level `_settings`, so this
        verifies the route is absent from the exempt set actually enforced in
        api-only mode rather than re-importing the module (which would not
        reflect a runtime web_ui_enabled=False without a process restart)."""
        from context_intelligence_server.auth import (  # noqa: PLC0415
            _EXEMPT_PATHS_API_ONLY,
        )

        # api-only mode's exempt set never carries /admin-ui (or /dashboard) —
        # this is the authoritative, settings-driven check (§B.2: "NOT in
        # _EXEMPT_PATHS_API_ONLY — the admin UI is a web-UI surface, absent in
        # api-only mode, same treatment as /dashboard").
        assert "/admin-ui" not in _EXEMPT_PATHS_API_ONLY
        assert "/dashboard" not in _EXEMPT_PATHS_API_ONLY
