"""Non-neo4j auth test for POST /admin/blobs/reclaim.

Mirrors the real-auth-enforcement pattern in ``test_admin_auth.py`` (static
mode, no ``require_admin`` dependency override): a data key authenticates via
BearerTokenMiddleware but is not the admin key, so ``require_admin`` must
reject it with 403 before the handler ever runs. This test never reaches
Neo4j or the filesystem selection logic -- it only proves the auth gate.

Fake constants only -- never real credentials (see repo AGENTS.md / design
doc §0.3 convention followed by test_admin_auth.py).
"""

from __future__ import annotations

import hashlib
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import httpx
import pytest

FAKE_ADMIN_RAW_KEY = "blob-reclaim-admin-key-do-not-use"
FAKE_ADMIN_KEY_DIGEST = hashlib.sha256(FAKE_ADMIN_RAW_KEY.encode()).hexdigest()

FAKE_DATA_RAW_KEY = "blob-reclaim-data-key-ordinary-user"
FAKE_DATA_KEY_DIGEST = hashlib.sha256(FAKE_DATA_RAW_KEY.encode()).hexdigest()
FAKE_CONTRIBUTOR = "carol"


def _make_static_settings(tmp_path: Path) -> Any:
    from context_intelligence_server.config import Settings

    return Settings(
        auth_mode="static",
        api_keys={FAKE_DATA_KEY_DIGEST: {"id": FAKE_CONTRIBUTOR}},
        admin_api_key=FAKE_ADMIN_RAW_KEY,
        api_keys_store_path=str(tmp_path / "api-keys.json"),
        entra_identities_store_path=str(tmp_path / "entra-identities.json"),
    )


@pytest.fixture
async def static_auth_client(tmp_path: Path) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Client routed through the REAL auth middleware (no require_admin override)."""
    from context_intelligence_server.main import create_asgi_app

    settings = _make_static_settings(tmp_path)
    middleware = create_asgi_app(settings=settings)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=middleware), base_url="http://test"
    ) as c:
        yield c


@pytest.mark.anyio
async def test_data_key_403_on_blob_reclaim(
    static_auth_client: httpx.AsyncClient,
) -> None:
    """A data (non-admin) key authenticates but must get 403 on the reclaim route."""
    resp = await static_auth_client.post(
        "/admin/blobs/reclaim",
        json={"dry_run": True},
        headers={"Authorization": f"Bearer {FAKE_DATA_RAW_KEY}"},
    )
    assert resp.status_code == 403


@pytest.mark.anyio
async def test_no_token_401_on_blob_reclaim(
    static_auth_client: httpx.AsyncClient,
) -> None:
    """No bearer token at all must 401 before reaching require_admin (TB-07)."""
    resp = await static_auth_client.post("/admin/blobs/reclaim", json={"dry_run": True})
    assert resp.status_code == 401


@pytest.mark.anyio
async def test_admin_key_reaches_handler_and_validates_body(
    static_auth_client: httpx.AsyncClient,
) -> None:
    """The admin key passes require_admin; a bad body (max_delete missing) still 422s.

    Proves auth is satisfied (not 401/403) and the handler's own body
    validation runs -- without touching Neo4j or the filesystem scan.
    """
    resp = await static_auth_client.post(
        "/admin/blobs/reclaim",
        json={"dry_run": False},
        headers={"Authorization": f"Bearer {FAKE_ADMIN_RAW_KEY}"},
    )
    assert resp.status_code == 422


@pytest.mark.anyio
async def test_admin_key_min_age_below_floor_422(
    static_auth_client: httpx.AsyncClient,
) -> None:
    """min_age_minutes below the hard safety floor is rejected with 422."""
    resp = await static_auth_client.post(
        "/admin/blobs/reclaim",
        json={"dry_run": True, "min_age_minutes": 0},
        headers={"Authorization": f"Bearer {FAKE_ADMIN_RAW_KEY}"},
    )
    assert resp.status_code == 422
