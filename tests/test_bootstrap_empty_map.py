"""Tests for the empty-map bootstrap fix (Option A).

Covers the required behaviors from the fix spec (empty-map-bootstrap-fix-spec.md,
"NEW tests to ADD" items 1-7):

  1. entra boots with empty map + logs warning.
  2. static boots with empty keystore + logs warning (admin-key-set and unset).
  3. entra IdentityAdmin-role token NOT in map -> PUT /admin/identities/{oid} 200;
     GET shows the new entry.
  4. entra IdentityAdmin-role token, empty map -> GET /admin/identities 200
     {"identities": []}.
  5. entra NON-admin token (roles=[]), unbound oid -> /admin PUT 403 (require_admin).
  6. entra IdentityAdmin token, oid NOT in map -> POST /events 403 (the admin
     bootstrap exemption is admin-path-scoped ONLY; it never relaxes data routes).
  7. static admin-key token adds the first key to an empty keystore via
     PUT /admin/keys -> 200; a data request with that key then authenticates.

These tests do NOT override ``require_admin`` -- they exercise the real
dependency (mirrors tests/routers/test_admin_auth.py) so the admin-path
bootstrap exemption (auth.py Edit 2.4/2.5/2.6/2.7) is proven end-to-end.
"""

from __future__ import annotations

import hashlib
import logging
import time
from pathlib import Path
from typing import Any

import pytest

# ---------------------------------------------------------------------------
# Fake constants -- never real credentials, OIDs, or keys
# ---------------------------------------------------------------------------

FAKE_CLIENT_ID = "aaaabbbb-1111-2222-3333-ccccddddeeee"
FAKE_TENANT_ID = "ffffeeee-dddd-cccc-bbbb-aaaa99998888"
FAKE_OID_UNBOUND = "33333333-4444-5555-6666-777788889999"
FAKE_CONTRIBUTOR = "new-bootstrap-contributor"
FAKE_ISSUER = f"https://login.microsoftonline.com/{FAKE_TENANT_ID}/v2.0"

FAKE_ADMIN_RAW_KEY = "bootstrap-admin-key-do-not-use-in-production"
FAKE_ADMIN_KEY_DIGEST = hashlib.sha256(FAKE_ADMIN_RAW_KEY.encode()).hexdigest()
FAKE_FIRST_DATA_RAW_KEY = "bootstrap-first-data-key"
FAKE_FIRST_DATA_KEY_DIGEST = hashlib.sha256(
    FAKE_FIRST_DATA_RAW_KEY.encode()
).hexdigest()


# ---------------------------------------------------------------------------
# JWKS stubs (no network) -- mirrors tests/routers/test_admin_auth.py
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


@pytest.fixture(scope="module")
def rsa_keypair() -> tuple[Any, Any]:
    """Generate a 2048-bit RSA keypair for entra JWT signing (module scope)."""
    from cryptography.hazmat.primitives.asymmetric import rsa  # noqa: PLC0415

    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return private_key, private_key.public_key()


def _make_entra_token(
    private_key: Any,
    *,
    oid: str = FAKE_OID_UNBOUND,
    roles: list[str] | None = None,
) -> str:
    """Mint a real RS256 JWT for entra bootstrap tests."""
    import jwt as pyjwt  # noqa: PLC0415

    now = int(time.time())
    claims: dict[str, Any] = {
        "oid": oid,
        "tid": FAKE_TENANT_ID,
        "scp": "access_as_user",
        "aud": FAKE_CLIENT_ID,
        "iss": FAKE_ISSUER,
        "exp": now + 3600,
        "iat": now - 10,
    }
    if roles is not None:
        claims["roles"] = roles
    return pyjwt.encode(claims, private_key, algorithm="RS256")


def _entra_settings_empty_map(tmp_path: Path) -> Any:
    from context_intelligence_server.config import Settings  # noqa: PLC0415

    return Settings(
        auth_mode="entra",
        azure_client_id=FAKE_CLIENT_ID,
        azure_tenant_id=FAKE_TENANT_ID,
        entra_identities={},  # explicit empty map -> supported bootstrap state
        entra_admin_role="IdentityAdmin",
        # Hermetic: the suite-wide conftest sets the env default
        # AMPLIFIER_..._ALLOW_UNAUTHENTICATED=true. These bootstrap tests prove
        # the fail-CLOSED path, so we MUST pin allow_unauthenticated=False here
        # (the production default) or the empty-store server would go wide open.
        # (entra is always auth_enabled=True, so this is belt-and-suspenders.)
        allow_unauthenticated=False,
        entra_identities_store_path=str(tmp_path / "entra-identities.json"),
        api_keys_store_path=str(tmp_path / "api-keys.json"),
    )


def _static_settings_empty_keystore(
    tmp_path: Path, *, with_admin_key: bool = True
) -> Any:
    from context_intelligence_server.config import Settings  # noqa: PLC0415

    return Settings(
        auth_mode="static",
        api_keys={},  # explicit empty map -> supported bootstrap state
        admin_api_key=FAKE_ADMIN_RAW_KEY if with_admin_key else None,
        # Hermetic (load-bearing): the suite-wide conftest sets the env default
        # AMPLIFIER_..._ALLOW_UNAUTHENTICATED=true. With an EMPTY keystore the
        # StaticKeyResolver reports auth_enabled=False, so if allow_unauthenticated
        # leaked in as True the middleware would fail OPEN (wide-open pass-through)
        # and defeat the exact fail-CLOSED bootstrap behavior these tests prove.
        # Pin it to the production default (False) so the server fail-CLOSES.
        allow_unauthenticated=False,
        api_keys_store_path=str(tmp_path / "api-keys.json"),
        entra_identities_store_path=str(tmp_path / "entra-identities.json"),
    )


# ---------------------------------------------------------------------------
# 1 & 2: boot with empty map/keystore + loud warning (fail-closed, not fail-open)
# ---------------------------------------------------------------------------


class TestBootWithEmptyMapLogsWarning:
    """Server boots with an empty identity map/keystore and announces it loudly."""

    def test_entra_boots_with_empty_map_and_logs_warning(
        self,
        tmp_path: Path,
        rsa_keypair: tuple[Any, Any],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """entra: empty entra_identities boots successfully and logs a WARNING."""
        from context_intelligence_server.main import create_asgi_app  # noqa: PLC0415

        _private_key, public_key = rsa_keypair
        settings = _entra_settings_empty_map(tmp_path)

        with caplog.at_level(
            logging.WARNING, logger="context_intelligence_server.main"
        ):
            middleware = create_asgi_app(
                settings=settings, _jwks_client=_StubJWKSClient(public_key)
            )

        assert middleware is not None
        messages = [r.getMessage() for r in caplog.records]
        assert any("identity map is EMPTY" in m for m in messages), (
            f"expected an empty entra identity map warning at startup; got {messages!r}"
        )

    def test_static_boots_with_empty_keystore_and_logs_warning_admin_key_set(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """static: empty api_keys + admin key configured -> boots and warns
        (the bootstrappable variant)."""
        from context_intelligence_server.main import create_asgi_app  # noqa: PLC0415

        settings = _static_settings_empty_keystore(tmp_path, with_admin_key=True)

        with caplog.at_level(
            logging.WARNING, logger="context_intelligence_server.main"
        ):
            middleware = create_asgi_app(settings=settings)

        assert middleware is not None
        messages = [r.getMessage() for r in caplog.records]
        assert any("static keystore is EMPTY" in m for m in messages)
        # Admin key IS configured -> the "CANNOT be bootstrapped" variant must NOT fire.
        assert not any("CANNOT be bootstrapped" in m for m in messages)

    def test_static_boots_with_empty_keystore_and_logs_warning_no_admin_key(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """static: empty api_keys + NO admin key -> boots and warns the
        unbootstrappable variant (the /admin API is itself unreachable)."""
        from context_intelligence_server.main import create_asgi_app  # noqa: PLC0415

        settings = _static_settings_empty_keystore(tmp_path, with_admin_key=False)

        with caplog.at_level(
            logging.WARNING, logger="context_intelligence_server.main"
        ):
            middleware = create_asgi_app(settings=settings)

        assert middleware is not None
        messages = [r.getMessage() for r in caplog.records]
        assert any("CANNOT be bootstrapped" in m for m in messages)


# ---------------------------------------------------------------------------
# 3, 4, 5, 6: entra admin-path bootstrap exemption
# ---------------------------------------------------------------------------


class TestEntraAdminBootstrapExemption:
    """An unbound-but-valid IdentityAdmin token can bootstrap the empty map via
    /admin/*, but the exemption never reaches data routes."""

    @pytest.fixture
    async def entra_empty_map_client(
        self, tmp_path: Path, rsa_keypair: tuple[Any, Any]
    ):
        import httpx  # noqa: PLC0415

        from context_intelligence_server.main import create_asgi_app  # noqa: PLC0415

        _private_key, public_key = rsa_keypair
        settings = _entra_settings_empty_map(tmp_path)
        middleware = create_asgi_app(
            settings=settings, _jwks_client=_StubJWKSClient(public_key)
        )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=middleware),
            base_url="http://test",
        ) as c:
            yield c

    async def test_admin_token_not_in_map_can_bootstrap_identity(
        self,
        entra_empty_map_client,
        rsa_keypair: tuple[Any, Any],
    ) -> None:
        """Item 3: IdentityAdmin token, oid NOT in map -> PUT /admin/identities/{oid}
        200; a subsequent GET shows the new entry."""
        private_key, _public_key = rsa_keypair
        token = _make_entra_token(
            private_key, oid=FAKE_OID_UNBOUND, roles=["IdentityAdmin"]
        )
        headers = {"Authorization": f"Bearer {token}"}

        put_resp = await entra_empty_map_client.put(
            f"/admin/identities/{FAKE_OID_UNBOUND}",
            json={"id": FAKE_CONTRIBUTOR},
            headers=headers,
        )
        assert put_resp.status_code == 200, put_resp.text
        assert put_resp.json()["id"] == FAKE_CONTRIBUTOR

        get_resp = await entra_empty_map_client.get(
            "/admin/identities", headers=headers
        )
        assert get_resp.status_code == 200
        identities = get_resp.json()["identities"]
        assert any(
            entry["oid"] == FAKE_OID_UNBOUND and entry["id"] == FAKE_CONTRIBUTOR
            for entry in identities
        )

    async def test_admin_token_empty_map_lists_no_identities(
        self, entra_empty_map_client, rsa_keypair: tuple[Any, Any]
    ) -> None:
        """Item 4: IdentityAdmin token, empty map -> GET /admin/identities 200
        {"identities": []}."""
        private_key, _public_key = rsa_keypair
        token = _make_entra_token(
            private_key, oid=FAKE_OID_UNBOUND, roles=["IdentityAdmin"]
        )
        headers = {"Authorization": f"Bearer {token}"}

        resp = await entra_empty_map_client.get("/admin/identities", headers=headers)
        assert resp.status_code == 200
        assert resp.json() == {"identities": []}

    async def test_non_admin_token_unbound_oid_403_on_admin_put(
        self, entra_empty_map_client, rsa_keypair: tuple[Any, Any]
    ) -> None:
        """Item 5: NON-admin token (roles=[]), unbound oid -> PUT /admin/* 403
        (require_admin still enforces the role, even though the map-membership
        check was exempted for this path)."""
        private_key, _public_key = rsa_keypair
        token = _make_entra_token(private_key, oid=FAKE_OID_UNBOUND, roles=[])
        headers = {"Authorization": f"Bearer {token}"}

        resp = await entra_empty_map_client.put(
            f"/admin/identities/{FAKE_OID_UNBOUND}",
            json={"id": FAKE_CONTRIBUTOR},
            headers=headers,
        )
        assert resp.status_code == 403

    async def test_admin_token_unbound_oid_403_on_data_route(
        self, entra_empty_map_client, rsa_keypair: tuple[Any, Any]
    ) -> None:
        """Item 6: IdentityAdmin token, oid NOT in map -> POST /events 403.

        The bootstrap exemption (auth.py Edit 2.4) is scoped STRICTLY to
        /admin/* paths -- it must never leak to data routes, even for an
        admin-role-holder whose own oid isn't yet mapped.
        """
        private_key, _public_key = rsa_keypair
        token = _make_entra_token(
            private_key, oid=FAKE_OID_UNBOUND, roles=["IdentityAdmin"]
        )
        headers = {"Authorization": f"Bearer {token}"}

        resp = await entra_empty_map_client.post(
            "/events", json={"event": "test"}, headers=headers
        )
        assert resp.status_code == 403


# ---------------------------------------------------------------------------
# 7: static admin-key bootstraps the first data key
# ---------------------------------------------------------------------------


class TestStaticAdminBootstrapsFirstKey:
    """Item 7: static admin-key token adds the first key to an empty keystore
    via PUT /admin/keys; a data request with that key then authenticates."""

    @pytest.fixture
    async def static_empty_keystore_client(self, tmp_path: Path):
        import httpx  # noqa: PLC0415

        from context_intelligence_server.main import create_asgi_app  # noqa: PLC0415

        settings = _static_settings_empty_keystore(tmp_path, with_admin_key=True)
        middleware = create_asgi_app(settings=settings)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=middleware),
            base_url="http://test",
        ) as c:
            yield c

    async def test_admin_key_adds_first_key_then_data_request_authenticates(
        self, static_empty_keystore_client
    ) -> None:
        admin_headers = {"Authorization": f"Bearer {FAKE_ADMIN_RAW_KEY}"}

        put_resp = await static_empty_keystore_client.put(
            f"/admin/keys/{FAKE_FIRST_DATA_KEY_DIGEST}",
            json={"id": FAKE_CONTRIBUTOR},
            headers=admin_headers,
        )
        assert put_resp.status_code == 200, put_resp.text
        assert put_resp.json() == {
            "hash": FAKE_FIRST_DATA_KEY_DIGEST,
            "id": FAKE_CONTRIBUTOR,
        }

        # The freshly-added data key now authenticates on a data route.
        data_headers = {"Authorization": f"Bearer {FAKE_FIRST_DATA_RAW_KEY}"}
        data_resp = await static_empty_keystore_client.post(
            "/events", json={"event": "test"}, headers=data_headers
        )
        assert data_resp.status_code != 401
        assert data_resp.status_code != 403

    async def test_data_request_401s_before_bootstrap(
        self, static_empty_keystore_client
    ) -> None:
        """Regression guard: before the admin bootstraps any key, the empty
        keystore still fail-CLOSES data requests (proves REQ3's fail-closed
        default, not just the admin fast-path)."""
        data_headers = {"Authorization": f"Bearer {FAKE_FIRST_DATA_RAW_KEY}"}
        resp = await static_empty_keystore_client.post(
            "/events", json={"event": "test"}, headers=data_headers
        )
        assert resp.status_code == 401
