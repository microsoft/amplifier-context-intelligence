"""T5 tests: real /admin/* authorization enforcement.

These tests do NOT use `app.dependency_overrides[require_admin]`.
They exercise the real `require_admin` dependency with genuine credentials/tokens
to prove the full 401/403/503 matrix.

Auth modes under test:
- Static mode: admin_api_key credential gates /admin/*; data keys get 403.
- Entra mode:  IdentityAdmin App Role gates /admin/*; tokens without it get 403.
- Both modes:  no token → 401 (TB-07: /admin is behind auth, never exempt).
- 503:         admin capability unconfigured → 503 (disabled, not forbidden).
- Startup:     /admin in an exempt set raises at construction (TB-07 structural).
- TB-09:       role in `groups` claim only, not `roles` → 403.

Fake constants only — never real credentials, OIDs, or keys (§0.3 of design doc).
"""

from __future__ import annotations

import hashlib
import time
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import httpx
import pytest

# ---------------------------------------------------------------------------
# Fake constants (mirrors test_admin.py, kept independent)
# ---------------------------------------------------------------------------

FAKE_CLIENT_ID = "aaaabbbb-1111-2222-3333-ccccddddeeee"
FAKE_TENANT_ID = "ffffeeee-dddd-cccc-bbbb-aaaa99998888"
FAKE_OID = "11111111-2222-3333-4444-555566667777"
FAKE_CONTRIBUTOR = "alice"
FAKE_ISSUER = f"https://login.microsoftonline.com/{FAKE_TENANT_ID}/v2.0"

# Static mode keys
FAKE_ADMIN_RAW_KEY = "t5-admin-key-do-not-use-in-production"
FAKE_ADMIN_KEY_DIGEST = hashlib.sha256(FAKE_ADMIN_RAW_KEY.encode()).hexdigest()

FAKE_DATA_RAW_KEY = "t5-data-key-ordinary-user"
FAKE_DATA_KEY_DIGEST = hashlib.sha256(FAKE_DATA_RAW_KEY.encode()).hexdigest()

FAKE_HASH_FOR_PUT = "a" * 64  # 64-hex key hash used in PUT /admin/keys tests


# ---------------------------------------------------------------------------
# RSA keypair fixtures (real crypto — no network) — module scope for speed
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def rsa_keypair() -> tuple[Any, Any]:
    """Generate a 2048-bit RSA keypair for entra JWT signing (module scope = once)."""
    from cryptography.hazmat.primitives.asymmetric import rsa

    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public_key = private_key.public_key()
    return private_key, public_key


# ---------------------------------------------------------------------------
# JWKS stubs (no network)
# ---------------------------------------------------------------------------


class _StubSigningKey:
    def __init__(self, key: Any) -> None:
        self.key = key


class _StubJWKSClient:
    """Stub JWKS client that returns a fixed RSA public key."""

    def __init__(self, public_key: Any) -> None:
        self._key = _StubSigningKey(public_key)

    def fetch_data(self) -> None:
        pass

    def get_signing_key_from_jwt(self, token: str) -> _StubSigningKey:
        return self._key

    def get_jwk_set(self) -> Any:
        _k = self._key

        class _FakeJWKSet:
            keys = [_k]

        return _FakeJWKSet()


class _NoJWKSClient:
    """Stub JWKS client for tests that must never call resolve() (admin tests use override)."""

    def fetch_data(self) -> None:
        pass

    def get_signing_key_from_jwt(self, token: str) -> _StubSigningKey:
        raise NotImplementedError("This test should not call resolve()")

    def get_jwk_set(self) -> Any:
        class _FakeJWKSet:
            keys = [_StubSigningKey("dummy")]

        return _FakeJWKSet()


# ---------------------------------------------------------------------------
# JWT helpers
# ---------------------------------------------------------------------------


def _make_entra_token(
    private_key: Any,
    *,
    oid: str = FAKE_OID,
    roles: list[str] | None = None,
    groups: list[str] | None = None,
    aud: str = FAKE_CLIENT_ID,
    scp: str = "access_as_user",
) -> str:
    """Mint a real RS256 JWT for entra tests.

    Args:
        roles:  Value for the ``roles`` claim (App Role assignments).
        groups: Value for the ``groups`` claim (group memberships — NOT roles).
    """
    import jwt as pyjwt

    now = int(time.time())
    claims: dict[str, Any] = {
        "oid": oid,
        "tid": FAKE_TENANT_ID,
        "scp": scp,
        "aud": aud,
        "iss": FAKE_ISSUER,
        "exp": now + 3600,
        "iat": now - 10,
    }
    if roles is not None:
        claims["roles"] = roles
    if groups is not None:
        claims["groups"] = groups
    return pyjwt.encode(claims, private_key, algorithm="RS256")


# ---------------------------------------------------------------------------
# Settings helpers
# ---------------------------------------------------------------------------


def _make_static_settings(
    tmp_path: Path,
    *,
    with_admin_key: bool = True,
    with_data_key: bool = True,
) -> Any:
    from context_intelligence_server.config import Settings  # noqa: PLC0415

    ks = {FAKE_DATA_KEY_DIGEST: {"id": FAKE_CONTRIBUTOR}} if with_data_key else None
    admin_key = FAKE_ADMIN_RAW_KEY if with_admin_key else None
    return Settings(
        auth_mode="static",
        api_keys=ks,
        admin_api_key=admin_key,
        api_keys_store_path=str(tmp_path / "api-keys.json"),
        entra_identities_store_path=str(tmp_path / "entra-identities.json"),
    )


def _make_static_settings_no_admin(tmp_path: Path) -> Any:
    """Static mode with a data key but NO admin key configured."""
    return _make_static_settings(tmp_path, with_admin_key=False, with_data_key=True)


def _make_entra_settings(
    tmp_path: Path,
    *,
    entra_admin_role: str = "IdentityAdmin",
) -> Any:
    from context_intelligence_server.config import Settings  # noqa: PLC0415

    return Settings(
        auth_mode="entra",
        azure_client_id=FAKE_CLIENT_ID,
        azure_tenant_id=FAKE_TENANT_ID,
        entra_identities={FAKE_OID: {"id": FAKE_CONTRIBUTOR}},
        entra_admin_role=entra_admin_role,
        entra_identities_store_path=str(tmp_path / "entra-identities.json"),
        api_keys_store_path=str(tmp_path / "api-keys.json"),
    )


# ---------------------------------------------------------------------------
# Client fixtures — NO require_admin override (T5: real enforcement)
# ---------------------------------------------------------------------------


@pytest.fixture
async def static_auth_client(tmp_path: Path) -> AsyncGenerator[httpx.AsyncClient, None]:
    """httpx client for static mode routed through the real auth middleware.

    Uses the BearerTokenMiddleware returned by create_asgi_app (not the bare
    FastAPI app) so that:
    - Missing/invalid tokens → 401 from the middleware (not reaching require_admin)
    - Admin key → is_admin=True set on scope state → require_admin passes
    - Data key → is_admin=False → require_admin 403
    """
    from context_intelligence_server.main import create_asgi_app  # noqa: PLC0415

    settings = _make_static_settings(tmp_path)
    middleware = create_asgi_app(settings=settings)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=middleware), base_url="http://test"
    ) as c:
        yield c


@pytest.fixture
async def static_no_admin_client(
    tmp_path: Path,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """httpx client for static mode with admin_api_key NOT configured → 503."""
    from context_intelligence_server.main import create_asgi_app  # noqa: PLC0415

    settings = _make_static_settings_no_admin(tmp_path)
    middleware = create_asgi_app(settings=settings)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=middleware), base_url="http://test"
    ) as c:
        yield c


@pytest.fixture
async def entra_auth_client(
    tmp_path: Path, rsa_keypair: tuple[Any, Any]
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """httpx client for entra mode routed through the real auth middleware."""
    from context_intelligence_server.main import create_asgi_app  # noqa: PLC0415

    _private_key, public_key = rsa_keypair
    settings = _make_entra_settings(tmp_path)
    middleware = create_asgi_app(
        settings=settings, _jwks_client=_StubJWKSClient(public_key)
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=middleware), base_url="http://test"
    ) as c:
        yield c


@pytest.fixture
async def entra_no_role_client(
    tmp_path: Path, rsa_keypair: tuple[Any, Any]
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """httpx client for entra mode with entra_admin_role='' → 503."""
    from context_intelligence_server.main import create_asgi_app  # noqa: PLC0415

    _private_key, public_key = rsa_keypair
    settings = _make_entra_settings(tmp_path, entra_admin_role="")
    middleware = create_asgi_app(
        settings=settings, _jwks_client=_StubJWKSClient(public_key)
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=middleware), base_url="http://test"
    ) as c:
        yield c


# ===========================================================================
# A. Static mode — admin-key recognition (ROB F1)
# ===========================================================================


class TestStaticModeAdminAuth:
    """Static mode: admin key gates /admin/*; data key → 403; no token → 401.

    401/403/503 matrix proved on PUT /admin/keys/{hash} and GET /admin/keys.
    """

    # -- TB-07: /admin is behind auth (no token → 401) ----------------------

    @pytest.mark.anyio
    async def test_no_token_401_on_put_admin_keys(
        self, static_auth_client: httpx.AsyncClient
    ) -> None:
        """TB-07: no token → 401 on PUT /admin/keys (admin endpoint is not exempt)."""
        resp = await static_auth_client.put(
            f"/admin/keys/{FAKE_HASH_FOR_PUT}", json={"id": "someone"}
        )
        assert resp.status_code == 401

    @pytest.mark.anyio
    async def test_no_token_401_on_get_admin_keys(
        self, static_auth_client: httpx.AsyncClient
    ) -> None:
        """TB-07: no token → 401 on GET /admin/keys."""
        resp = await static_auth_client.get("/admin/keys")
        assert resp.status_code == 401

    # -- data key → 403 (not admin) -----------------------------------------

    @pytest.mark.anyio
    async def test_data_key_403_on_put_admin_keys(
        self, static_auth_client: httpx.AsyncClient
    ) -> None:
        """Data key authenticates but is not the admin key → 403 on PUT /admin/keys."""
        resp = await static_auth_client.put(
            f"/admin/keys/{FAKE_HASH_FOR_PUT}",
            json={"id": "someone"},
            headers={"Authorization": f"Bearer {FAKE_DATA_RAW_KEY}"},
        )
        assert resp.status_code == 403

    @pytest.mark.anyio
    async def test_data_key_403_on_get_admin_keys(
        self, static_auth_client: httpx.AsyncClient
    ) -> None:
        """Data key → 403 on GET /admin/keys."""
        resp = await static_auth_client.get(
            "/admin/keys",
            headers={"Authorization": f"Bearer {FAKE_DATA_RAW_KEY}"},
        )
        assert resp.status_code == 403

    # -- admin key → 200 (authenticated + is_admin) -------------------------

    @pytest.mark.anyio
    async def test_admin_key_200_on_put_admin_keys(
        self, static_auth_client: httpx.AsyncClient
    ) -> None:
        """Admin key → 200 on PUT /admin/keys (authenticates + is_admin=True)."""
        resp = await static_auth_client.put(
            f"/admin/keys/{FAKE_HASH_FOR_PUT}",
            json={"id": "new-contributor"},
            headers={"Authorization": f"Bearer {FAKE_ADMIN_RAW_KEY}"},
        )
        assert resp.status_code == 200

    @pytest.mark.anyio
    async def test_admin_key_200_on_get_admin_keys(
        self, static_auth_client: httpx.AsyncClient
    ) -> None:
        """Admin key → 200 on GET /admin/keys."""
        resp = await static_auth_client.get(
            "/admin/keys",
            headers={"Authorization": f"Bearer {FAKE_ADMIN_RAW_KEY}"},
        )
        assert resp.status_code == 200

    # -- admin key unconfigured → 503 ----------------------------------------

    @pytest.mark.anyio
    async def test_admin_key_unconfigured_503_on_put(
        self, static_no_admin_client: httpx.AsyncClient
    ) -> None:
        """Static mode with no admin_api_key → 503 on PUT /admin/keys."""
        resp = await static_no_admin_client.put(
            f"/admin/keys/{FAKE_HASH_FOR_PUT}",
            json={"id": "x"},
            headers={"Authorization": f"Bearer {FAKE_DATA_RAW_KEY}"},
        )
        assert resp.status_code == 503

    @pytest.mark.anyio
    async def test_admin_key_unconfigured_503_on_get(
        self, static_no_admin_client: httpx.AsyncClient
    ) -> None:
        """Static mode with no admin_api_key → 503 on GET /admin/keys."""
        resp = await static_no_admin_client.get(
            "/admin/keys",
            headers={"Authorization": f"Bearer {FAKE_DATA_RAW_KEY}"},
        )
        assert resp.status_code == 503

    # -- admin key is NOT a data-ingestion identity (scoped to /admin/*) -----

    @pytest.mark.anyio
    async def test_admin_key_rejected_on_data_endpoint(
        self, static_auth_client: httpx.AsyncClient
    ) -> None:
        """A bare admin key is REJECTED (401) on POST /events.

        The admin-key fast-path is scoped to /admin/* routes only: the admin key
        is an administration credential, not a data-ingestion identity. On a data
        route it falls through to the keystore resolver; since the admin key is
        not a registered data key here, it is rejected — rather than posting
        events attributed to a synthetic ``created_by="admin"``. (An operator who
        wants to both administer and contribute holds two keys, or registers the
        admin token as a data key too.)
        """
        resp = await static_auth_client.post(
            "/events",
            json={},
            headers={"Authorization": f"Bearer {FAKE_ADMIN_RAW_KEY}"},
        )
        assert resp.status_code == 401


# ===========================================================================
# B. Entra mode — IdentityAdmin App Role enforcement (TB-09)
# ===========================================================================


class TestEntraModeAdminAuth:
    """Entra mode: IdentityAdmin in `roles` gates /admin/*; groups claim → 403.

    401/403/503 matrix proved on PUT /admin/identities/{oid} and GET /admin/identities.
    """

    FAKE_OID_2 = "22222222-3333-4444-5555-666677778888"

    # -- TB-07: /admin is behind auth (no token → 401) ----------------------

    @pytest.mark.anyio
    async def test_no_token_401_on_put_admin_identities(
        self, entra_auth_client: httpx.AsyncClient
    ) -> None:
        """TB-07: no token → 401 on PUT /admin/identities (not exempt)."""
        resp = await entra_auth_client.put(
            f"/admin/identities/{self.FAKE_OID_2}", json={"id": "carol"}
        )
        assert resp.status_code == 401

    @pytest.mark.anyio
    async def test_no_token_401_on_get_admin_identities(
        self, entra_auth_client: httpx.AsyncClient
    ) -> None:
        """TB-07: no token → 401 on GET /admin/identities (not exempt)."""
        resp = await entra_auth_client.get("/admin/identities")
        assert resp.status_code == 401

    # -- token WITHOUT IdentityAdmin → 403 ----------------------------------

    @pytest.mark.anyio
    async def test_token_no_role_403_on_put_identities(
        self, entra_auth_client: httpx.AsyncClient, rsa_keypair: tuple[Any, Any]
    ) -> None:
        """Valid entra token with empty roles → 403 on PUT /admin/identities."""
        private_key, _pub = rsa_keypair
        token = _make_entra_token(private_key, roles=[])
        resp = await entra_auth_client.put(
            f"/admin/identities/{self.FAKE_OID_2}",
            json={"id": "carol"},
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp.status_code == 403

    @pytest.mark.anyio
    async def test_token_no_role_403_on_get_identities(
        self, entra_auth_client: httpx.AsyncClient, rsa_keypair: tuple[Any, Any]
    ) -> None:
        """Valid entra token with no roles claim → 403 on GET /admin/identities."""
        private_key, _pub = rsa_keypair
        token = _make_entra_token(private_key, roles=None)  # no roles claim
        resp = await entra_auth_client.get(
            "/admin/identities",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp.status_code == 403

    @pytest.mark.anyio
    async def test_token_wrong_role_403_on_put_identities(
        self, entra_auth_client: httpx.AsyncClient, rsa_keypair: tuple[Any, Any]
    ) -> None:
        """Valid entra token with a different role (not IdentityAdmin) → 403."""
        private_key, _pub = rsa_keypair
        token = _make_entra_token(private_key, roles=["SomeOtherRole", "AnotherRole"])
        resp = await entra_auth_client.put(
            f"/admin/identities/{self.FAKE_OID_2}",
            json={"id": "carol"},
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp.status_code == 403

    # -- TB-09: role in `groups` but NOT `roles` → 403 ----------------------

    @pytest.mark.anyio
    async def test_tb09_role_in_groups_not_roles_403_on_put(
        self, entra_auth_client: httpx.AsyncClient, rsa_keypair: tuple[Any, Any]
    ) -> None:
        """TB-09: IdentityAdmin in `groups` claim only (not `roles`) → 403.

        The admin check ONLY reads the `roles` claim. A matching value in `groups`
        must NEVER grant admin access — groups claim does not encode App Role assignments.
        """
        private_key, _pub = rsa_keypair
        token = _make_entra_token(
            private_key,
            roles=[],  # NOT in roles
            groups=["IdentityAdmin"],  # only in groups
        )
        resp = await entra_auth_client.put(
            f"/admin/identities/{self.FAKE_OID_2}",
            json={"id": "carol"},
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp.status_code == 403

    @pytest.mark.anyio
    async def test_tb09_role_in_groups_not_roles_403_on_get(
        self, entra_auth_client: httpx.AsyncClient, rsa_keypair: tuple[Any, Any]
    ) -> None:
        """TB-09: IdentityAdmin in `groups` only → 403 on GET /admin/identities."""
        private_key, _pub = rsa_keypair
        token = _make_entra_token(
            private_key,
            roles=None,  # no roles claim
            groups=["IdentityAdmin"],  # only in groups
        )
        resp = await entra_auth_client.get(
            "/admin/identities",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp.status_code == 403

    # -- token WITH IdentityAdmin → 200 -------------------------------------

    @pytest.mark.anyio
    async def test_identity_admin_role_200_on_put_identities(
        self, entra_auth_client: httpx.AsyncClient, rsa_keypair: tuple[Any, Any]
    ) -> None:
        """Valid entra token WITH IdentityAdmin in roles → 200 on PUT /admin/identities."""
        private_key, _pub = rsa_keypair
        token = _make_entra_token(private_key, roles=["IdentityAdmin"])
        resp = await entra_auth_client.put(
            f"/admin/identities/{self.FAKE_OID_2}",
            json={"id": "carol"},
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp.status_code == 200

    @pytest.mark.anyio
    async def test_identity_admin_role_200_on_get_identities(
        self, entra_auth_client: httpx.AsyncClient, rsa_keypair: tuple[Any, Any]
    ) -> None:
        """Valid entra token WITH IdentityAdmin → 200 on GET /admin/identities."""
        private_key, _pub = rsa_keypair
        token = _make_entra_token(private_key, roles=["IdentityAdmin", "AnotherRole"])
        resp = await entra_auth_client.get(
            "/admin/identities",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp.status_code == 200

    # -- entra_admin_role unconfigured → 503 (admin API disabled) ------------

    @pytest.mark.anyio
    async def test_entra_admin_role_empty_503_on_put(
        self,
        entra_no_role_client: httpx.AsyncClient,
        rsa_keypair: tuple[Any, Any],
    ) -> None:
        """Entra mode with entra_admin_role='' → 503 on PUT /admin/identities."""
        private_key, _pub = rsa_keypair
        token = _make_entra_token(private_key, roles=["IdentityAdmin"])
        resp = await entra_no_role_client.put(
            f"/admin/identities/{self.FAKE_OID_2}",
            json={"id": "carol"},
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp.status_code == 503

    @pytest.mark.anyio
    async def test_entra_admin_role_empty_503_on_get(
        self,
        entra_no_role_client: httpx.AsyncClient,
        rsa_keypair: tuple[Any, Any],
    ) -> None:
        """Entra mode with entra_admin_role='' → 503 on GET /admin/identities."""
        private_key, _pub = rsa_keypair
        token = _make_entra_token(private_key, roles=["IdentityAdmin"])
        resp = await entra_no_role_client.get(
            "/admin/identities",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp.status_code == 503

    # -- 403 body names the required role -----------------------------------

    @pytest.mark.anyio
    async def test_403_names_the_required_role(
        self, entra_auth_client: httpx.AsyncClient, rsa_keypair: tuple[Any, Any]
    ) -> None:
        """403 response body must name the required role (operator guidance)."""
        private_key, _pub = rsa_keypair
        token = _make_entra_token(private_key, roles=[])
        resp = await entra_auth_client.get(
            "/admin/identities",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp.status_code == 403
        body = resp.json()
        # The configured role name must appear in the error detail
        assert "IdentityAdmin" in body.get("detail", "")


# ===========================================================================
# C. Startup assertion — /admin must NEVER be exempt (TB-07 structural)
# ===========================================================================


class TestExemptSetStartupAssertion:
    """create_asgi_app raises at startup if /admin is in any exempt set (TB-07)."""

    def test_admin_in_exempt_paths_raises_at_startup(self, tmp_path: Path) -> None:
        """Injecting /admin into _EXEMPT_PATHS before create_asgi_app → RuntimeError."""
        from unittest.mock import patch  # noqa: PLC0415

        from context_intelligence_server.main import create_asgi_app  # noqa: PLC0415
        from context_intelligence_server import auth as _auth  # noqa: PLC0415

        settings = _make_static_settings(tmp_path)
        # Inject /admin into the exempt set
        bad_exempt = _auth._EXEMPT_PATHS | {"/admin"}
        with patch.object(_auth, "_EXEMPT_PATHS", bad_exempt):
            with pytest.raises((RuntimeError, AssertionError)):
                create_asgi_app(settings=settings)

    def test_admin_prefix_in_exempt_prefixes_raises_at_startup(
        self, tmp_path: Path
    ) -> None:
        """Injecting /admin/ into _EXEMPT_PREFIXES → RuntimeError at startup."""
        from unittest.mock import patch  # noqa: PLC0415

        from context_intelligence_server.main import create_asgi_app  # noqa: PLC0415
        from context_intelligence_server import auth as _auth  # noqa: PLC0415

        settings = _make_static_settings(tmp_path)
        bad_prefixes = _auth._EXEMPT_PREFIXES + ("/admin/",)
        with patch.object(_auth, "_EXEMPT_PREFIXES", bad_prefixes):
            with pytest.raises((RuntimeError, AssertionError)):
                create_asgi_app(settings=settings)

    def test_clean_exempt_sets_do_not_raise(self, tmp_path: Path) -> None:
        """Normal create_asgi_app with clean exempt sets must NOT raise."""
        from context_intelligence_server.main import create_asgi_app  # noqa: PLC0415

        settings = _make_static_settings(tmp_path)
        # Should not raise
        create_asgi_app(settings=settings)


# ===========================================================================
# D. T4 tests still pass — require_admin override still works
# ===========================================================================


class TestRequireAdminOverrideStillWorks:
    """Confirm that app.dependency_overrides[require_admin] still bypasses enforcement.

    The T4 test suite relies on this; T5 must not break it.
    """

    @pytest.mark.anyio
    async def test_override_bypasses_require_admin(self, tmp_path: Path) -> None:
        """dependency_overrides[require_admin] = lambda: None → 200 (no auth needed)."""
        from context_intelligence_server.main import app, create_asgi_app  # noqa: PLC0415
        from context_intelligence_server.routers.admin import require_admin  # noqa: PLC0415

        settings = _make_static_settings(tmp_path)
        create_asgi_app(settings=settings)
        app.dependency_overrides[require_admin] = lambda: None
        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as c:
                # Send with the admin key (valid credential) but no require_admin enforcement
                resp = await c.get(
                    "/admin/keys",
                    headers={"Authorization": f"Bearer {FAKE_ADMIN_RAW_KEY}"},
                )
            assert resp.status_code == 200
        finally:
            app.dependency_overrides.pop(require_admin, None)
