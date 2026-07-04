"""/status MUST remain a public, unauthenticated route -- it is the Azure
Container Apps liveness/health probe. Gating it breaks the deployment.

This test is a tripwire: it fails loudly if anyone ever removes /status from
the exempt sets (_EXEMPT_PATHS / _EXEMPT_PATHS_API_ONLY).

Exercises BOTH auth-exempt sets (full-web _EXEMPT_PATHS and API-only
_EXEMPT_PATHS_API_ONLY) so a future change can't silently re-gate /status
in one set while leaving the other correct.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import httpx
import pytest

from context_intelligence_server.auth import _EXEMPT_PATHS, _EXEMPT_PATHS_API_ONLY

_DATA_TOKEN = "status-public-guard-test-token"  # noqa: S105 (test fixture, not a real secret)
_DATA_DIGEST = hashlib.sha256(_DATA_TOKEN.encode()).hexdigest()


def test_status_in_exempt_paths() -> None:
    """/status must be present in the full-web exempt set."""
    assert "/status" in _EXEMPT_PATHS


def test_status_in_exempt_paths_api_only() -> None:
    """/status must be present in the API-only exempt set (the Azure/ACA config)."""
    assert "/status" in _EXEMPT_PATHS_API_ONLY


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
async def test_status_stays_public_when_web_ui_enabled(tmp_path: Path) -> None:
    """GET /status with no Authorization header -> 200 (web_ui_enabled=True,
    exercises _EXEMPT_PATHS)."""
    from context_intelligence_server.main import create_asgi_app  # noqa: PLC0415

    settings = _make_settings(tmp_path, web_ui_enabled=True)
    wrapped = create_asgi_app(settings=settings)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=wrapped), base_url="http://test"
    ) as c:
        resp = await c.get("/status")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_status_stays_public_when_api_only(tmp_path: Path) -> None:
    """GET /status with no Authorization header -> 200 (web_ui_enabled=False,
    exercises _EXEMPT_PATHS_API_ONLY -- the Azure/API-only config used by ACA
    liveness/health probes)."""
    from context_intelligence_server.main import create_asgi_app  # noqa: PLC0415

    settings = _make_settings(tmp_path, web_ui_enabled=False)
    wrapped = create_asgi_app(settings=settings)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=wrapped), base_url="http://test"
    ) as c:
        resp = await c.get("/status")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_status_authenticated_also_returns_200(tmp_path: Path) -> None:
    """/status WITH a valid bearer token -> also 200 (auth-enabled app).

    /status is exempt, so a request never NEEDS a token -- but a request that
    happens to carry a valid one must not be rejected either.
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
    """GET /version with no Authorization header -> 200 for both exempt sets --
    pins the liveness carve-out alongside /status."""
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
