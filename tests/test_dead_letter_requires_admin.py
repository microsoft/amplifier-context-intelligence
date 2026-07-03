"""W2 (doc 16 §4.4) — dead-letter tier boundary + the TB-1 positive-admin proof.

Human decision (authoritative): dead-letter LIST is fine for any authenticated
principal; PURGE/REPLAY are destructive → admin-only. Because ``require_admin``
in static mode only allows when the middleware set ``is_admin=True``, and that
flag is set ONLY by the ``/admin/*`` admin-key fast-path (auth._is_admin_route),
purge/replay were RELOCATED under ``/admin`` so the static admin key can reach
them (council TB-1). This file PROVES the boundary through the REAL ``asgi_app``
gate — it deliberately does NOT apply the ``require_admin`` override.

Route map after W2:
  - GET  /queues/dead-letter                          → require_read (any principal)
  - POST /admin/queues/dead-letter/{worker}/purge      → require_admin
  - POST /admin/queues/dead-letter/{worker}/replay     → require_admin
"""

from __future__ import annotations

import asyncio
import hashlib
import time
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import httpx
import pytest

# ---------------------------------------------------------------------------
# Test constants — never real credentials
# ---------------------------------------------------------------------------

_DATA_TOKEN = "dead-letter-w2-non-admin-data-key"  # noqa: S105 (test fixture)
_DATA_DIGEST = hashlib.sha256(_DATA_TOKEN.encode()).hexdigest()
_ADMIN_TOKEN = "dead-letter-w2-admin-key-do-not-use"  # noqa: S105 (test fixture)

# Entra fakes (mirror tests/test_m2_service_auth.py)
_FAKE_CLIENT_ID = "aaaabbbb-1111-2222-3333-ccccddddeeee"
_FAKE_TENANT_ID = "ffffeeee-dddd-cccc-bbbb-aaaa99998888"
_FAKE_OID_SERVICE = "aaaabbbb-9999-9999-9999-ccccddddffff"
_FAKE_APPID = "bbbbcccc-2222-3333-4444-eeeeffff0000"
_FAKE_ISSUER = f"https://login.microsoftonline.com/{_FAKE_TENANT_ID}/v2.0"
_ENTRA_ADMIN_ROLE = "IdentityAdmin"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _point_registry_at(tmp_path: Path) -> None:
    """Point the shared registry's durable infra at a tmp_path queues dir so
    the purge/replay/list handlers return cleanly (mirrors
    tests/routers/test_queues.py::_point_registry_at)."""
    from context_intelligence_server.main import registry  # noqa: PLC0415
    from context_intelligence_server.queue_manager import QueueManager  # noqa: PLC0415

    registry._queue_manager = QueueManager(queues_dir=tmp_path / "queues")
    registry._write_semaphore = asyncio.Semaphore(2)
    registry._max_delivery_attempts = 5


@pytest.fixture(scope="module")
def _rsa_keypair() -> tuple[Any, Any]:
    """Generate a 2048-bit RSA keypair once per module (entra token signing)."""
    from cryptography.hazmat.primitives.asymmetric import rsa  # noqa: PLC0415

    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return private_key, private_key.public_key()


class _StubSigningKey:
    def __init__(self, key: Any) -> None:
        self.key = key


class _StubJWKSClient:
    """Minimal stub JWKS client — no network (mirrors the M2 tests)."""

    def __init__(self, key: Any) -> None:
        self._key = _StubSigningKey(key)

    def fetch_data(self) -> None:
        pass

    def get_signing_key_from_jwt(self, token: str) -> _StubSigningKey:
        return self._key

    def get_jwk_set(self) -> Any:
        _k = self._key

        class _FakeJWKSet:
            keys = [_k]

        return _FakeJWKSet()


def _sign_jwt(private_key: Any, claims: dict[str, Any]) -> str:
    import jwt as pyjwt  # noqa: PLC0415

    return pyjwt.encode(claims, private_key, algorithm="RS256")


def _service_admin_claims() -> dict[str, Any]:
    """Service/app token whose roles claim carries the admin App Role (no scp →
    service branch, is_service=True, roles=[IdentityAdmin])."""
    now = int(time.time())
    return {
        "oid": _FAKE_OID_SERVICE,
        "tid": _FAKE_TENANT_ID,
        "aud": _FAKE_CLIENT_ID,
        "iss": _FAKE_ISSUER,
        "exp": now + 3600,
        "iat": now - 10,
        "appid": _FAKE_APPID,
        "roles": [_ENTRA_ADMIN_ROLE],
    }


# ---------------------------------------------------------------------------
# Fixtures — real asgi_app, NO require_admin override
# ---------------------------------------------------------------------------


@pytest.fixture
async def non_admin_client(tmp_path: Path) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Static-mode client authenticated as a non-admin data principal
    (is_admin=False), routed through the real asgi_app."""
    from context_intelligence_server.config import Settings  # noqa: PLC0415
    from context_intelligence_server.main import create_asgi_app  # noqa: PLC0415

    settings = Settings(
        auth_mode="static",
        allow_unauthenticated=False,
        api_keys={_DATA_DIGEST: {"id": "alice"}},
        admin_api_key=_ADMIN_TOKEN,
        api_keys_store_path=str(tmp_path / "api-keys.json"),
        entra_identities_store_path=str(tmp_path / "entra-identities.json"),
    )
    wrapped = create_asgi_app(settings=settings)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=wrapped),
        base_url="http://test",
        headers={"Authorization": f"Bearer {_DATA_TOKEN}"},
    ) as c:
        yield c


@pytest.fixture
async def static_admin_client(
    tmp_path: Path,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Static-mode client presenting the admin_api_key as bearer, routed through
    the real asgi_app (the /admin/* fast-path sets is_admin=True)."""
    from context_intelligence_server.config import Settings  # noqa: PLC0415
    from context_intelligence_server.main import create_asgi_app  # noqa: PLC0415

    settings = Settings(
        auth_mode="static",
        allow_unauthenticated=False,
        api_keys={_DATA_DIGEST: {"id": "alice"}},
        admin_api_key=_ADMIN_TOKEN,
        api_keys_store_path=str(tmp_path / "api-keys.json"),
        entra_identities_store_path=str(tmp_path / "entra-identities.json"),
    )
    wrapped = create_asgi_app(settings=settings)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=wrapped),
        base_url="http://test",
        headers={"Authorization": f"Bearer {_ADMIN_TOKEN}"},
    ) as c:
        yield c


@pytest.fixture
async def entra_admin_client(
    tmp_path: Path, _rsa_keypair: tuple[Any, Any]
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Entra-mode client presenting a token whose roles claim carries the
    configured entra_admin_role, routed through the real asgi_app."""
    from context_intelligence_server.config import Settings  # noqa: PLC0415
    from context_intelligence_server.main import app, create_asgi_app  # noqa: PLC0415
    from context_intelligence_server.routers.skills import SkillRegistry  # noqa: PLC0415

    private_key, public_key = _rsa_keypair
    settings = Settings(
        auth_mode="entra",
        allow_unauthenticated=False,
        azure_client_id=_FAKE_CLIENT_ID,
        azure_tenant_id=_FAKE_TENANT_ID,
        entra_identities={_FAKE_OID_SERVICE: {"id": "svc"}},
        entra_admin_role=_ENTRA_ADMIN_ROLE,
        entra_identities_store_path=str(tmp_path / "entra-identities.json"),
        api_keys_store_path=str(tmp_path / "api-keys.json"),
    )
    if not hasattr(app.state, "skill_registry"):
        app.state.skill_registry = SkillRegistry()
    wrapped = create_asgi_app(
        settings=settings, _jwks_client=_StubJWKSClient(public_key)
    )
    token = _sign_jwt(private_key, _service_admin_claims())
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=wrapped),
        base_url="http://test",
        headers={"Authorization": f"Bearer {token}"},
    ) as c:
        yield c


# ---------------------------------------------------------------------------
# Tier boundary (the human decision)
# ---------------------------------------------------------------------------


class TestDeadLetterTierBoundary:
    """List is open to any authenticated principal; purge/replay are admin-only."""

    @pytest.mark.anyio
    async def test_list_dead_letters_allows_non_admin(
        self, non_admin_client: httpx.AsyncClient, tmp_path: Path
    ) -> None:
        """GET /queues/dead-letter → 200 for a non-admin data key (require_read)."""
        _point_registry_at(tmp_path)
        resp = await non_admin_client.get("/queues/dead-letter")
        assert resp.status_code == 200

    @pytest.mark.anyio
    async def test_purge_dead_letters_denies_non_admin(
        self, non_admin_client: httpx.AsyncClient
    ) -> None:
        """POST /admin/queues/dead-letter/w/purge → 403 for a non-admin key."""
        resp = await non_admin_client.post("/admin/queues/dead-letter/w/purge")
        assert resp.status_code == 403

    @pytest.mark.anyio
    async def test_replay_dead_letters_denies_non_admin(
        self, non_admin_client: httpx.AsyncClient
    ) -> None:
        """POST /admin/queues/dead-letter/w/replay → 403 for a non-admin key."""
        resp = await non_admin_client.post("/admin/queues/dead-letter/w/replay")
        assert resp.status_code == 403

    @pytest.mark.anyio
    async def test_list_dead_letters_denies_unqualified_service_token(
        self, entra_admin_client: httpx.AsyncClient
    ) -> None:
        """GET /queues/dead-letter → 403 for a service principal that authenticates
        but holds NEITHER reader_role NOR service_data_role (require_read DENY
        branch — council TB-N2).

        The newly-reopened list route uses ``require_read``; every other test only
        proves the ALLOW path (200). This proves DENY through the REAL asgi_app,
        no override.

        Principal construction (deliberate): a truly role-EMPTY service token
        (``roles=[]``) is rejected earlier, at the Entra resolver (M2 dual-path:
        "no qualifying App Role" → 403 in the middleware), so it never reaches
        ``require_read``. To exercise ``require_read``'s deny branch through the
        real stack we need a principal that AUTHENTICATES as a service
        (``is_service=True``) yet lacks both data roles. The ``entra_admin_client``
        token is exactly that: ``roles=[IdentityAdmin]`` — a service token that
        the resolver accepts (CAP-SADM-a), carrying the ADMIN role but NEITHER
        ``Reader`` (reader_role) NOR ``Contributor`` (service_data_role). So it
        clears the resolver, reaches ``require_read``, and is denied 403 —
        proving admin authority does NOT confer data-read capability on the list.
        """
        resp = await entra_admin_client.get("/queues/dead-letter")
        assert resp.status_code == 403


# ---------------------------------------------------------------------------
# Route-existence regression (council crusty F-W2a)
# ---------------------------------------------------------------------------


class TestDeadLetterAdminRouteExistence:
    """Guard the silent dropped-router failure mode.

    The purge/replay routes live on ``dead_letter_admin_router``, which
    ``main.py`` must ``include_router()``. If that include is ever dropped, the
    routes vanish and requests 404 — and NOTHING else catches it (the boot guard
    only inspects registered routes; a dropped router registers nothing).

    Deviation from the literal spec (verified empirically, and by reading
    auth.py): the spec suggested an UNAUTHENTICATED request asserting 401/403 !=
    404. But ``BearerTokenMiddleware`` returns 401 for any non-exempt path with
    no bearer token BEFORE routing runs — so a no-auth request 401s whether or
    not the route exists, and CANNOT detect a drop. We therefore send an
    AUTHENTICATED non-admin principal: it clears the middleware so routing
    actually happens, giving 403 when the route EXISTS (require_admin denies) and
    404 when it has been dropped. Asserting ``!= 404`` (route present) and
    ``in (401, 403)`` (gated, not open) fulfills the guard's real purpose.
    """

    @pytest.mark.anyio
    async def test_purge_route_exists_and_is_admin_gated(
        self, non_admin_client: httpx.AsyncClient
    ) -> None:
        """POST /admin/queues/dead-letter/w/purge → gated (not 404).

        Proves the route EXISTS and is auth/admin-gated; guards against a dropped
        include_router(dead_letter_admin_router) silently 404-ing (council
        crusty F-W2a). A 404 here means the route was dropped.
        """
        resp = await non_admin_client.post("/admin/queues/dead-letter/w/purge")
        assert resp.status_code != 404, (
            "route dropped — 404 means the router include was lost"
        )
        assert resp.status_code in (401, 403)

    @pytest.mark.anyio
    async def test_replay_route_exists_and_is_admin_gated(
        self, non_admin_client: httpx.AsyncClient
    ) -> None:
        """POST /admin/queues/dead-letter/w/replay → gated (not 404).

        Proves the route EXISTS and is auth/admin-gated; guards against a dropped
        include_router(dead_letter_admin_router) silently 404-ing (council
        crusty F-W2a). A 404 here means the route was dropped.
        """
        resp = await non_admin_client.post("/admin/queues/dead-letter/w/replay")
        assert resp.status_code != 404, (
            "route dropped — 404 means the router include was lost"
        )
        assert resp.status_code in (401, 403)


# ---------------------------------------------------------------------------
# Positive admin proof (the TB-1 fix — relocation makes admin reachable)
# ---------------------------------------------------------------------------


class TestDeadLetterAdminReachable:
    """PROVE purge is reachable by admin through the REAL gate, in BOTH modes."""

    @pytest.mark.anyio
    async def test_purge_dead_letters_allows_static_admin_key(
        self, static_admin_client: httpx.AsyncClient, tmp_path: Path
    ) -> None:
        """STATIC mode: the admin_api_key as bearer → POST purge → not 403 (200).

        LOAD-BEARING TB-1 REGRESSION PIN. Before the relocation this was
        impossible: require_admin off /admin/* was unreachable by the static
        admin key, so purge/replay were bricked in static mode. The /admin/*
        fast-path sets is_admin=True → require_admin passes → handler runs.
        """
        _point_registry_at(tmp_path)
        resp = await static_admin_client.post("/admin/queues/dead-letter/w/purge")
        assert resp.status_code != 403
        assert resp.status_code == 200

    @pytest.mark.anyio
    async def test_purge_dead_letters_allows_entra_admin_role(
        self, entra_admin_client: httpx.AsyncClient, tmp_path: Path
    ) -> None:
        """ENTRA mode: a token carrying entra_admin_role → POST purge → not 403 (200)."""
        _point_registry_at(tmp_path)
        resp = await entra_admin_client.post("/admin/queues/dead-letter/w/purge")
        assert resp.status_code != 403
        assert resp.status_code == 200
