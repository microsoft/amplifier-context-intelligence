"""W3 (doc 16 §5.3) — /status now requires auth; /version remains the
unauthenticated liveness carve-out.

Exercises BOTH auth-exempt sets (full-web _EXEMPT_PATHS and API-only
_EXEMPT_PATHS_API_ONLY) so a future change can't silently re-exempt /status
in one set while leaving the other correct.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import httpx
import pytest

_DATA_TOKEN = "status-auth-w3-test-token"  # noqa: S105 (test fixture, not a real secret)
_DATA_DIGEST = hashlib.sha256(_DATA_TOKEN.encode()).hexdigest()


def _make_settings(tmp_path: Path, *, web_ui_enabled: bool):
    from context_intelligence_server.config import Settings  # noqa: PLC0415

    return Settings(
        auth_mode="static",
        allow_unauthenticated=False,
        api_keys={_DATA_DIGEST: {"id": "alice"}},
        web_ui_enabled=web_ui_enabled,
        api_keys_store_path=str(tmp_path / "api-keys.json"),
        entra_identities_store_path=str(tmp_path / "entra-identities.json"),
    )


@pytest.mark.anyio
async def test_status_requires_auth_when_web_ui_enabled(tmp_path: Path) -> None:
    """GET /status with no Authorization header → 401 (web_ui_enabled=True,
    exercises _EXEMPT_PATHS)."""
    from context_intelligence_server.main import create_asgi_app  # noqa: PLC0415

    settings = _make_settings(tmp_path, web_ui_enabled=True)
    wrapped = create_asgi_app(settings=settings)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=wrapped), base_url="http://test"
    ) as c:
        resp = await c.get("/status")
    assert resp.status_code == 401


@pytest.mark.anyio
async def test_status_requires_auth_when_api_only(tmp_path: Path) -> None:
    """GET /status with no Authorization header → 401 (web_ui_enabled=False,
    exercises _EXEMPT_PATHS_API_ONLY — the Azure/API-only config)."""
    from context_intelligence_server.main import create_asgi_app  # noqa: PLC0415

    settings = _make_settings(tmp_path, web_ui_enabled=False)
    wrapped = create_asgi_app(settings=settings)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=wrapped), base_url="http://test"
    ) as c:
        resp = await c.get("/status")
    assert resp.status_code == 401


@pytest.mark.anyio
async def test_status_authenticated_returns_200(tmp_path: Path) -> None:
    """TB-5: /status WITH a valid bearer token → 200 (auth-enabled app).

    Guards against an "always-401 even with valid auth" regression — proves the
    middleware admits an authenticated principal to /status (which carries no
    capability dependency), not merely that it rejects the unauthenticated case.
    """
    from context_intelligence_server.main import create_asgi_app  # noqa: PLC0415

    settings = _make_settings(tmp_path, web_ui_enabled=True)
    wrapped = create_asgi_app(settings=settings)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=wrapped), base_url="http://test"
    ) as c:
        resp = await c.get(
            "/status", headers={"Authorization": f"Bearer {_DATA_TOKEN}"}
        )
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_version_still_exempt(tmp_path: Path) -> None:
    """GET /version with no Authorization header → 200 for both exempt sets —
    pins the liveness carve-out so a future change can't silently re-exempt
    /status by widening the set."""
    from context_intelligence_server.main import create_asgi_app  # noqa: PLC0415

    for web_ui_enabled in (True, False):
        wrapped = create_asgi_app(
            settings=_make_settings(tmp_path, web_ui_enabled=web_ui_enabled)
        )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=wrapped), base_url="http://test"
        ) as c:
            resp = await c.get("/version")
        assert resp.status_code == 200, (
            f"/version must stay exempt (web_ui_enabled={web_ui_enabled}), "
            f"got {resp.status_code}"
        )
