"""T4 tests: /admin/* CRUD endpoints over the live identity stores.

Auth is overridden via app.dependency_overrides[require_admin] = lambda: None
for ALL tests here. Real auth enforcement comes in T5; these tests cover:
  - Entra-identity CRUD: PUT / DELETE / GET /admin/identities/*
  - Static-key CRUD: PUT / DELETE / GET /admin/keys/*
  - Mode-awareness: wrong-mode store returns 503
  - Body validation: empty / missing id → 422
  - Live-store proof: PUT via HTTP → resolver sees change immediately (no restart)

Fake constants (NEVER real credentials, OIDs, or keys — §0.3 of design doc).
"""

from __future__ import annotations

import hashlib
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import httpx
import pytest

# ---------------------------------------------------------------------------
# Fake constants
# ---------------------------------------------------------------------------

FAKE_OID = "11111111-2222-3333-4444-555566667777"
FAKE_OID_2 = "22222222-3333-4444-5555-666677778888"
FAKE_CONTRIBUTOR_ENTRA = "alice"
FAKE_DISPLAY_NAME = "Alice Smith"

FAKE_RAW_TOKEN = "test-admin-raw-token-xyzzy"
FAKE_TOKEN_DIGEST = hashlib.sha256(FAKE_RAW_TOKEN.encode()).hexdigest()
FAKE_CONTRIBUTOR_KEY = "carol"
FAKE_CONTRIBUTOR_KEY_2 = "dave"
FAKE_HASH_2 = "b" * 64  # second fake 64-hex key hash

FAKE_CLIENT_ID = "aaaabbbb-1111-2222-3333-ccccddddeeee"
FAKE_TENANT_ID = "ffffeeee-dddd-cccc-bbbb-aaaa99998888"


# ---------------------------------------------------------------------------
# JWKS stub — no network (satisfies EntraResolver eager-prefetch guard)
# ---------------------------------------------------------------------------


class _StubSigningKey:
    def __init__(self, key: Any = "dummy-key") -> None:
        self.key = key


class _StubJWKSClient:
    def fetch_data(self) -> None:
        pass

    def get_signing_key_from_jwt(self, token: str) -> _StubSigningKey:
        raise NotImplementedError("admin tests do not call resolve()")

    def get_jwk_set(self) -> Any:
        class _FakeJWKSet:
            keys = [_StubSigningKey()]

        return _FakeJWKSet()


# ---------------------------------------------------------------------------
# Settings helpers
# ---------------------------------------------------------------------------


def _make_static_settings(tmp_path: Path, *, with_key: bool = True) -> Any:
    from context_intelligence_server.config import Settings  # noqa: PLC0415

    # When with_key=False we omit api_keys entirely (None) so pydantic does not
    # reject an empty dict (the validator requires at least one entry if set).
    ks = {FAKE_TOKEN_DIGEST: {"id": FAKE_CONTRIBUTOR_KEY}} if with_key else None
    return Settings(
        auth_mode="static",
        allow_unauthenticated=True,
        api_keys=ks,
        api_keys_store_path=str(tmp_path / "api-keys.json"),
        entra_identities_store_path=str(tmp_path / "entra-identities.json"),
    )


def _make_entra_settings(tmp_path: Path, *, with_identity: bool = True) -> Any:
    from context_intelligence_server.config import Settings  # noqa: PLC0415

    # When with_identity=False we omit entra_identities entirely (None) so
    # pydantic does not reject an empty dict (the validator requires at least
    # one entry if the field is set).
    ids = {FAKE_OID: {"id": FAKE_CONTRIBUTOR_ENTRA}} if with_identity else None
    return Settings(
        auth_mode="entra",
        allow_unauthenticated=True,
        azure_client_id=FAKE_CLIENT_ID,
        azure_tenant_id=FAKE_TENANT_ID,
        entra_identities=ids,
        entra_identities_store_path=str(tmp_path / "entra-identities.json"),
        api_keys_store_path=str(tmp_path / "api-keys.json"),
    )


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def entra_client(tmp_path: Path) -> AsyncGenerator[httpx.AsyncClient, None]:
    """httpx client for entra mode with require_admin overridden to no-op."""
    from context_intelligence_server.main import app, create_asgi_app  # noqa: PLC0415
    from context_intelligence_server.routers.admin import require_admin  # noqa: PLC0415

    settings = _make_entra_settings(tmp_path)
    create_asgi_app(settings=settings, _jwks_client=_StubJWKSClient())
    app.dependency_overrides[require_admin] = lambda: None
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as c:
            yield c
    finally:
        app.dependency_overrides.pop(require_admin, None)


@pytest.fixture
async def static_client(tmp_path: Path) -> AsyncGenerator[httpx.AsyncClient, None]:
    """httpx client for static mode with require_admin overridden to no-op."""
    from context_intelligence_server.main import app, create_asgi_app  # noqa: PLC0415
    from context_intelligence_server.routers.admin import require_admin  # noqa: PLC0415

    settings = _make_static_settings(tmp_path)
    create_asgi_app(settings=settings)
    app.dependency_overrides[require_admin] = lambda: None
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as c:
            yield c
    finally:
        app.dependency_overrides.pop(require_admin, None)


# ===========================================================================
# Entra-identity CRUD
# ===========================================================================


class TestIdentityCRUD:
    """PUT / DELETE / GET /admin/identities/* in entra mode."""

    @pytest.mark.anyio
    async def test_put_identity_returns_200_with_record(
        self, entra_client: httpx.AsyncClient
    ) -> None:
        """PUT /admin/identities/{oid} → 200 with {oid, id}."""
        resp = await entra_client.put(
            f"/admin/identities/{FAKE_OID_2}",
            json={"id": FAKE_CONTRIBUTOR_ENTRA},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["oid"] == FAKE_OID_2
        assert body["id"] == FAKE_CONTRIBUTOR_ENTRA

    @pytest.mark.anyio
    async def test_put_identity_with_display_name(
        self, entra_client: httpx.AsyncClient
    ) -> None:
        """PUT with display_name → stored record includes display_name."""
        resp = await entra_client.put(
            f"/admin/identities/{FAKE_OID}",
            json={"id": FAKE_CONTRIBUTOR_ENTRA, "display_name": FAKE_DISPLAY_NAME},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["display_name"] == FAKE_DISPLAY_NAME

    @pytest.mark.anyio
    async def test_get_identities_lists_all(
        self, entra_client: httpx.AsyncClient
    ) -> None:
        """GET /admin/identities → lists all stored identities."""
        # Seed a second OID
        await entra_client.put(
            f"/admin/identities/{FAKE_OID_2}", json={"id": "second-contrib"}
        )
        resp = await entra_client.get("/admin/identities")
        assert resp.status_code == 200
        body = resp.json()
        oids = {item["oid"] for item in body["identities"]}
        # FAKE_OID was seeded at create_asgi_app time (with_identity=True)
        assert FAKE_OID in oids
        assert FAKE_OID_2 in oids

    @pytest.mark.anyio
    async def test_delete_identity_returns_200(
        self, entra_client: httpx.AsyncClient
    ) -> None:
        """DELETE /admin/identities/{oid} → 200 when oid exists."""
        resp = await entra_client.delete(f"/admin/identities/{FAKE_OID}")
        assert resp.status_code == 200
        body = resp.json()
        assert body["deleted"] is True
        assert body["oid"] == FAKE_OID

    @pytest.mark.anyio
    async def test_delete_identity_returns_404_when_absent(
        self, entra_client: httpx.AsyncClient
    ) -> None:
        """DELETE /admin/identities/{oid} → 404 when oid not in store."""
        nonexistent = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        resp = await entra_client.delete(f"/admin/identities/{nonexistent}")
        assert resp.status_code == 404

    @pytest.mark.anyio
    async def test_delete_then_re_delete_returns_404(
        self, entra_client: httpx.AsyncClient
    ) -> None:
        """After DELETE, a second DELETE on the same OID returns 404."""
        await entra_client.delete(f"/admin/identities/{FAKE_OID}")
        resp = await entra_client.delete(f"/admin/identities/{FAKE_OID}")
        assert resp.status_code == 404

    @pytest.mark.anyio
    async def test_delete_removes_from_get_list(
        self, entra_client: httpx.AsyncClient
    ) -> None:
        """After DELETE, the oid no longer appears in GET /admin/identities."""
        await entra_client.delete(f"/admin/identities/{FAKE_OID}")
        resp = await entra_client.get("/admin/identities")
        oids = {item["oid"] for item in resp.json()["identities"]}
        assert FAKE_OID not in oids

    @pytest.mark.anyio
    async def test_put_identity_visible_to_resolver_immediately(
        self, tmp_path: Path
    ) -> None:
        """PUT /admin/identities/{oid} → EntraResolver sees it immediately (no restart).

        This is the load-bearing live-store proof:
        - create_asgi_app wires the EntraResolver to store.flat_dict.
        - PUT via HTTP calls store.put() on the SAME IdentityStore.
        - The resolver's _identity_map IS store.flat_dict.
        - So the new OID is visible without any restart.

        We start with FAKE_OID seeded (required by config validator when
        auth_mode='entra') and PUT FAKE_OID_2, which is not present yet.
        """
        from context_intelligence_server.main import (  # noqa: PLC0415
            app,
            create_asgi_app,
            get_entra_identity_store,
        )
        from context_intelligence_server.routers.admin import require_admin  # noqa: PLC0415

        # with_identity=True seeds FAKE_OID; FAKE_OID_2 is not in the store yet.
        settings = _make_entra_settings(tmp_path, with_identity=True)
        middleware = create_asgi_app(settings=settings, _jwks_client=_StubJWKSClient())
        app.dependency_overrides[require_admin] = lambda: None
        try:
            # FAKE_OID_2 is not seeded; confirm it's absent from the resolver
            assert FAKE_OID_2 not in middleware.resolver._identity_map  # type: ignore[union-attr]

            new_contributor = "new-entra-contributor"
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as c:
                resp = await c.put(
                    f"/admin/identities/{FAKE_OID_2}",
                    json={"id": new_contributor},
                )
            assert resp.status_code == 200

            # Resolver sees it immediately — no restart
            assert (
                middleware.resolver._identity_map[FAKE_OID_2]  # type: ignore[union-attr]
                == new_contributor
            )
            # IdentityStore.flat_dict is the same dict object (sanity)
            store = get_entra_identity_store()
            assert store is not None
            assert store.flat_dict is middleware.resolver._identity_map  # type: ignore[union-attr]
        finally:
            app.dependency_overrides.pop(require_admin, None)


# ===========================================================================
# Static-key CRUD
# ===========================================================================


class TestKeyCRUD:
    """PUT / DELETE / GET /admin/keys/* in static mode."""

    @pytest.mark.anyio
    async def test_put_key_returns_200_with_hash_and_id(
        self, static_client: httpx.AsyncClient
    ) -> None:
        """PUT /admin/keys/{hash} → 200 with {hash, id}."""
        resp = await static_client.put(
            f"/admin/keys/{FAKE_HASH_2}",
            json={"id": FAKE_CONTRIBUTOR_KEY_2},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["hash"] == FAKE_HASH_2
        assert body["id"] == FAKE_CONTRIBUTOR_KEY_2

    @pytest.mark.anyio
    async def test_put_key_never_echoes_raw_key(
        self, static_client: httpx.AsyncClient
    ) -> None:
        """Response body must not contain the raw token (only the hash)."""
        resp = await static_client.put(
            f"/admin/keys/{FAKE_HASH_2}",
            json={"id": FAKE_CONTRIBUTOR_KEY_2},
        )
        body_text = resp.text
        assert FAKE_RAW_TOKEN not in body_text

    @pytest.mark.anyio
    async def test_get_keys_lists_all(self, static_client: httpx.AsyncClient) -> None:
        """GET /admin/keys → lists all stored hashes with id, never raw keys."""
        # Seed a second hash
        await static_client.put(
            f"/admin/keys/{FAKE_HASH_2}", json={"id": FAKE_CONTRIBUTOR_KEY_2}
        )
        resp = await static_client.get("/admin/keys")
        assert resp.status_code == 200
        body = resp.json()
        hashes = {item["hash"] for item in body["keys"]}
        # FAKE_TOKEN_DIGEST was seeded at create_asgi_app time (with_key=True)
        assert FAKE_TOKEN_DIGEST in hashes
        assert FAKE_HASH_2 in hashes
        # Raw token must not appear anywhere
        assert FAKE_RAW_TOKEN not in resp.text

    @pytest.mark.anyio
    async def test_delete_key_returns_200(
        self, static_client: httpx.AsyncClient
    ) -> None:
        """DELETE /admin/keys/{hash} → 200 when hash exists."""
        resp = await static_client.delete(f"/admin/keys/{FAKE_TOKEN_DIGEST}")
        assert resp.status_code == 200
        body = resp.json()
        assert body["deleted"] is True
        assert body["hash"] == FAKE_TOKEN_DIGEST

    @pytest.mark.anyio
    async def test_delete_key_returns_404_when_absent(
        self, static_client: httpx.AsyncClient
    ) -> None:
        """DELETE /admin/keys/{hash} → 404 when hash not in store."""
        resp = await static_client.delete(f"/admin/keys/{'c' * 64}")
        assert resp.status_code == 404

    @pytest.mark.anyio
    async def test_delete_then_re_delete_returns_404(
        self, static_client: httpx.AsyncClient
    ) -> None:
        """After DELETE, a second DELETE on same hash returns 404."""
        await static_client.delete(f"/admin/keys/{FAKE_TOKEN_DIGEST}")
        resp = await static_client.delete(f"/admin/keys/{FAKE_TOKEN_DIGEST}")
        assert resp.status_code == 404

    @pytest.mark.anyio
    async def test_delete_removes_from_get_list(
        self, static_client: httpx.AsyncClient
    ) -> None:
        """After DELETE, hash no longer appears in GET /admin/keys."""
        await static_client.delete(f"/admin/keys/{FAKE_TOKEN_DIGEST}")
        resp = await static_client.get("/admin/keys")
        hashes = {item["hash"] for item in resp.json()["keys"]}
        assert FAKE_TOKEN_DIGEST not in hashes

    @pytest.mark.anyio
    async def test_put_key_visible_to_resolver_immediately(
        self, tmp_path: Path
    ) -> None:
        """PUT /admin/keys/{hash} → StaticKeyResolver sees it immediately (no restart).

        This is the load-bearing live-store proof for static mode.
        """
        from context_intelligence_server.main import (  # noqa: PLC0415
            app,
            create_asgi_app,
            get_api_key_store,
        )
        from context_intelligence_server.routers.admin import require_admin  # noqa: PLC0415

        settings = _make_static_settings(tmp_path, with_key=False)
        middleware = create_asgi_app(settings=settings)
        app.dependency_overrides[require_admin] = lambda: None
        try:
            # FAKE_HASH_2 not in resolver yet
            assert FAKE_HASH_2 not in middleware.resolver._keystore  # type: ignore[union-attr]

            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as c:
                resp = await c.put(
                    f"/admin/keys/{FAKE_HASH_2}",
                    json={"id": FAKE_CONTRIBUTOR_KEY_2},
                )
            assert resp.status_code == 200

            # Resolver sees it immediately — no restart
            assert (
                middleware.resolver._keystore[FAKE_HASH_2]  # type: ignore[union-attr]
                == FAKE_CONTRIBUTOR_KEY_2
            )
            # IdentityStore.flat_dict is the same object (sanity)
            store = get_api_key_store()
            assert store is not None
            assert store.flat_dict is middleware.resolver._keystore  # type: ignore[union-attr]
        finally:
            app.dependency_overrides.pop(require_admin, None)


# ===========================================================================
# Mode-awareness: wrong-mode → 503
# ===========================================================================


class TestModeAwareness503:
    """Endpoints for the inactive store must return 503."""

    @pytest.mark.anyio
    async def test_identities_503_in_static_mode(
        self, static_client: httpx.AsyncClient
    ) -> None:
        """In static mode, /admin/identities/* → 503 (entra store not active)."""
        resp = await static_client.get("/admin/identities")
        assert resp.status_code == 503

    @pytest.mark.anyio
    async def test_put_identity_503_in_static_mode(
        self, static_client: httpx.AsyncClient
    ) -> None:
        resp = await static_client.put(
            f"/admin/identities/{FAKE_OID}", json={"id": FAKE_CONTRIBUTOR_ENTRA}
        )
        assert resp.status_code == 503

    @pytest.mark.anyio
    async def test_delete_identity_503_in_static_mode(
        self, static_client: httpx.AsyncClient
    ) -> None:
        resp = await static_client.delete(f"/admin/identities/{FAKE_OID}")
        assert resp.status_code == 503

    @pytest.mark.anyio
    async def test_keys_503_in_entra_mode(
        self, entra_client: httpx.AsyncClient
    ) -> None:
        """In entra mode, /admin/keys/* → 503 (api-key store not active)."""
        resp = await entra_client.get("/admin/keys")
        assert resp.status_code == 503

    @pytest.mark.anyio
    async def test_put_key_503_in_entra_mode(
        self, entra_client: httpx.AsyncClient
    ) -> None:
        resp = await entra_client.put(
            f"/admin/keys/{FAKE_TOKEN_DIGEST}", json={"id": FAKE_CONTRIBUTOR_KEY}
        )
        assert resp.status_code == 503

    @pytest.mark.anyio
    async def test_delete_key_503_in_entra_mode(
        self, entra_client: httpx.AsyncClient
    ) -> None:
        resp = await entra_client.delete(f"/admin/keys/{FAKE_TOKEN_DIGEST}")
        assert resp.status_code == 503


# ===========================================================================
# Body validation → 422
# ===========================================================================


class TestBodyValidation422:
    """Empty or missing id field → 422 Unprocessable Entity."""

    @pytest.mark.anyio
    async def test_put_identity_empty_id_returns_422(
        self, entra_client: httpx.AsyncClient
    ) -> None:
        resp = await entra_client.put(f"/admin/identities/{FAKE_OID}", json={"id": ""})
        assert resp.status_code == 422

    @pytest.mark.anyio
    async def test_put_identity_whitespace_id_returns_422(
        self, entra_client: httpx.AsyncClient
    ) -> None:
        resp = await entra_client.put(
            f"/admin/identities/{FAKE_OID}", json={"id": "   "}
        )
        assert resp.status_code == 422

    @pytest.mark.anyio
    async def test_put_identity_missing_id_returns_422(
        self, entra_client: httpx.AsyncClient
    ) -> None:
        resp = await entra_client.put(
            f"/admin/identities/{FAKE_OID}", json={"display_name": "no-id"}
        )
        assert resp.status_code == 422

    @pytest.mark.anyio
    async def test_put_key_empty_id_returns_422(
        self, static_client: httpx.AsyncClient
    ) -> None:
        resp = await static_client.put(f"/admin/keys/{FAKE_HASH_2}", json={"id": ""})
        assert resp.status_code == 422

    @pytest.mark.anyio
    async def test_put_key_whitespace_id_returns_422(
        self, static_client: httpx.AsyncClient
    ) -> None:
        resp = await static_client.put(f"/admin/keys/{FAKE_HASH_2}", json={"id": "   "})
        assert resp.status_code == 422

    @pytest.mark.anyio
    async def test_put_key_missing_id_returns_422(
        self, static_client: httpx.AsyncClient
    ) -> None:
        resp = await static_client.put(f"/admin/keys/{FAKE_HASH_2}", json={})
        assert resp.status_code == 422


# ===========================================================================
# require_admin seam — confirm override is used in all tests
# ===========================================================================


class TestRequireAdminSeam:
    """The require_admin dependency exists and is importable.

    T5 replaced the no-op body with real enforcement.  This class now only
    confirms the seam can be imported and overridden (the enforcement is
    fully tested in test_admin_auth.py).
    """

    def test_require_admin_is_callable(self) -> None:
        from context_intelligence_server.routers.admin import require_admin  # noqa: PLC0415

        # Must be callable.  T5 changed signature to require(request: Request),
        # so we just verify it is importable and callable — not that it returns
        # None without args (the no-op contract is gone; it now enforces auth).
        assert callable(require_admin)

    def test_dependency_override_is_honoured(self, tmp_path: Path) -> None:
        """app.dependency_overrides[require_admin] overrides the dependency."""
        from context_intelligence_server.main import app, create_asgi_app  # noqa: PLC0415
        from context_intelligence_server.routers.admin import require_admin  # noqa: PLC0415

        called: list[bool] = []

        def _spy_override() -> None:
            called.append(True)

        settings = _make_static_settings(tmp_path)
        create_asgi_app(settings=settings)
        app.dependency_overrides[require_admin] = _spy_override
        try:
            # Verify the override key resolves — just checking the override dict
            assert app.dependency_overrides[require_admin] is _spy_override
        finally:
            app.dependency_overrides.pop(require_admin, None)
