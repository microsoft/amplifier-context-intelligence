"""T7-E2E: End-to-end proof tests for runtime identity-map management.

These tests prove the *no-redeploy* guarantee: identities added or removed via
/admin/* are effective immediately on subsequent data requests — without
restarting or recreating the ASGI application.

Design reference: IDENTITY-MAP-MANAGEMENT-DESIGN.md §10 (the acceptance gate).

Observation method for ``created_by``
--------------------------------------
The ``/events`` endpoint stamps ``created_by`` and calls
``registry.queue_manager.append(worker_key, body_bytes)``.  We intercept that
call with ``monkeypatch`` — the same technique as T8 / ``test_entra_integration.py``
— to capture the raw bytes and assert ``created_by`` without touching Neo4j or
the real filesystem queue.

Auth in these tests
--------------------
* **Static mode**: real admin key (``admin_api_key``) recognised by the middleware
  as ``is_admin=True``; a seed data key is the pre-existing valid credential;
  the new test key starts unregistered.
* **Entra mode**: real RS256 JWTs minted with an in-test RSA keypair + stub JWKS
  client.  Admin token carries ``roles=[\"IdentityAdmin\"]`` AND has an OID in the
  identity map (required by the middleware).  New user token has an OID that is
  NOT in the identity map initially.

No ``app.dependency_overrides[require_admin]`` in any proof test — real
enforcement is exercised end-to-end throughout.

No-redeploy proof: structural guarantee
-----------------------------------------
Every proof test creates ONE ``create_asgi_app()`` call and ONE
``httpx.AsyncClient`` that routes through that single middleware instance.
All requests — PRE (rejected), ACT (admin mutation), POST (accepted or
rejected again) — share the same ASGI process.  The absence of any
``create_asgi_app()`` call between PRE and POST is the proof.
"""

from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest

# ---------------------------------------------------------------------------
# Fake constants  (NEVER real credentials, OIDs, or keys — §0.3 of design doc)
# ---------------------------------------------------------------------------

FAKE_CLIENT_ID = "aaaabbbb-1111-2222-3333-ccccddddeeee"
FAKE_TENANT_ID = "ffffeeee-dddd-cccc-bbbb-aaaa99998888"
FAKE_ISSUER = f"https://login.microsoftonline.com/{FAKE_TENANT_ID}/v2.0"

# Entra OIDs
FAKE_OID_SEED = (
    "11111111-2222-3333-4444-555566667777"  # seeded in identity map at startup
)
FAKE_OID_NEW = "99999999-8888-7777-6666-555544443333"  # NOT in identity map initially

# Static-mode keys
FAKE_INITIAL_DATA_RAW_KEY = "e2e-initial-data-key-xyzzy"
FAKE_INITIAL_DATA_KEY_DIGEST = hashlib.sha256(
    FAKE_INITIAL_DATA_RAW_KEY.encode()
).hexdigest()

FAKE_NEW_DATA_RAW_KEY = "e2e-new-data-key-qwerty"
FAKE_NEW_DATA_KEY_DIGEST = hashlib.sha256(FAKE_NEW_DATA_RAW_KEY.encode()).hexdigest()

FAKE_ADMIN_RAW_KEY = "e2e-admin-key-do-not-use-in-production"

# A second key kept in the store throughout the offboard test.
# It ensures auth_enabled stays True after FAKE_INITIAL_DATA_RAW_KEY is deleted
# (a StaticKeyResolver with an empty keystore is auth_enabled=False, which would
# let all requests pass through in the test environment — not the scenario we want
# to prove).
FAKE_SURVIVOR_RAW_KEY = "e2e-survivor-key-permanent"
FAKE_SURVIVOR_KEY_DIGEST = hashlib.sha256(FAKE_SURVIVOR_RAW_KEY.encode()).hexdigest()

# Contributors (the values that should appear in created_by)
FAKE_CONTRIBUTOR_SEED = "seed-contributor"
FAKE_CONTRIBUTOR_NEW_STATIC = "alice-e2e"
FAKE_CONTRIBUTOR_NEW_ENTRA = "bob-e2e"

# Admin role for entra mode
IDENTITY_ADMIN_ROLE = "IdentityAdmin"

# Minimal valid event body — used for all data-endpoint calls in this file.
# session_id is included so the idempotency key is unique; different suffixes
# are appended per test to prevent cross-step cache collisions within a test.
_EVENT_BASE = {
    "event": "tool_use",
    "workspace": "/ws-e2e",
    "data": {
        "timestamp": "2026-06-29T00:00:00.000000+00:00",
    },
}


def _event(session_id: str) -> dict[str, Any]:
    """Return a minimal event body with a unique session_id."""
    body = {**_EVENT_BASE, "data": {**_EVENT_BASE["data"], "session_id": session_id}}
    return body


# ---------------------------------------------------------------------------
# RSA keypair fixture — module scope (generated once per module for speed)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def rsa_keypair_e2e() -> tuple[Any, Any]:
    """Generate a 2048-bit RSA keypair for entra JWT signing (expensive; done once)."""
    from cryptography.hazmat.primitives.asymmetric import rsa  # noqa: PLC0415

    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public_key = private_key.public_key()
    return private_key, public_key


# ---------------------------------------------------------------------------
# JWKS stub — no network
# ---------------------------------------------------------------------------


class _StubSigningKey:
    def __init__(self, key: Any) -> None:
        self.key = key


class _StubJWKSClient:
    """JWKS stub: fetch_data is a no-op; always returns the in-test RSA public key."""

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


# ---------------------------------------------------------------------------
# JWT minting helper
# ---------------------------------------------------------------------------


def _mint_entra_token(
    private_key: Any,
    *,
    oid: str,
    roles: list[str] | None = None,
    scp: str = "access_as_user",
) -> str:
    """Sign a minimal RS256 JWT for use in entra-mode e2e tests."""
    import jwt as pyjwt  # noqa: PLC0415

    now = int(time.time())
    claims: dict[str, Any] = {
        "oid": oid,
        "tid": FAKE_TENANT_ID,
        "scp": scp,
        "aud": FAKE_CLIENT_ID,
        "iss": FAKE_ISSUER,
        "exp": now + 3600,
        "iat": now - 10,
    }
    if roles is not None:
        claims["roles"] = roles
    return pyjwt.encode(claims, private_key, algorithm="RS256")


# ---------------------------------------------------------------------------
# Settings helpers
# ---------------------------------------------------------------------------


def _static_settings(tmp_path: Path) -> Any:
    """Static mode: seed data key + survivor key + admin key.

    Two data keys are seeded so that deleting FAKE_INITIAL_DATA_RAW_KEY during
    the offboard proof leaves at least one key in the store.  A
    ``StaticKeyResolver`` with an EMPTY keystore has ``auth_enabled=False``,
    which causes the middleware to let all requests through — not the scenario
    we want to prove.  The survivor key keeps ``auth_enabled=True`` after the
    deletion so that the POST request correctly gets 401 (rejected) rather than
    202 (passed through unauthenticated).

    FAKE_NEW_DATA_RAW_KEY is intentionally NOT seeded; the onboard proof adds it
    at runtime via /admin/keys.
    """
    from context_intelligence_server.config import Settings  # noqa: PLC0415

    return Settings(
        auth_mode="static",
        api_keys={
            FAKE_INITIAL_DATA_KEY_DIGEST: {"id": FAKE_CONTRIBUTOR_SEED},
            FAKE_SURVIVOR_KEY_DIGEST: {"id": "survivor-contrib"},
        },
        admin_api_key=FAKE_ADMIN_RAW_KEY,
        api_keys_store_path=str(tmp_path / "api-keys.json"),
        entra_identities_store_path=str(tmp_path / "entra-identities.json"),
    )


def _entra_settings(tmp_path: Path) -> Any:
    """Entra mode: FAKE_OID_SEED seeded at startup; FAKE_OID_NEW not present."""
    from context_intelligence_server.config import Settings  # noqa: PLC0415

    return Settings(
        auth_mode="entra",
        azure_client_id=FAKE_CLIENT_ID,
        azure_tenant_id=FAKE_TENANT_ID,
        entra_identities={FAKE_OID_SEED: {"id": FAKE_CONTRIBUTOR_SEED}},
        entra_admin_role=IDENTITY_ADMIN_ROLE,
        entra_identities_store_path=str(tmp_path / "entra-identities.json"),
        api_keys_store_path=str(tmp_path / "api-keys.json"),
    )


# ---------------------------------------------------------------------------
# Queue-capture helper
# ---------------------------------------------------------------------------


def _install_queue_capture(
    monkeypatch: pytest.MonkeyPatch,
    captured: list[bytes],
) -> None:
    """Patch registry.queue_manager.append to capture written bytes.

    Also stubs get_or_create so the test doesn't need a live Neo4j drain worker.

    MUST be called AFTER ``create_asgi_app()`` so that accessing
    ``registry.queue_manager`` triggers its lazy initialisation against the
    tmp_path queues directory (set by the autouse ``safe_settings`` fixture).
    """
    import context_intelligence_server.main as main_module  # noqa: PLC0415

    async def _fake_append(worker_key: str, raw: bytes) -> None:
        captured.append(raw)

    monkeypatch.setattr(
        main_module.registry, "get_or_create", lambda *a, **kw: MagicMock()
    )
    # Accessing .queue_manager here triggers lazy init; then we patch append on
    # the live object.
    monkeypatch.setattr(main_module.registry.queue_manager, "append", _fake_append)


# ===========================================================================
# Proof 1 & 2: Static mode — onboard and offboard, no redeploy
# ===========================================================================


class TestStaticModeOnboardOffboard:
    """
    Proof 1: add a new static key at runtime → accepted immediately.
    Proof 2: delete a static key at runtime → rejected immediately.
    Both proofs run against a SINGLE ``create_asgi_app()`` instance.
    Real admin-key auth is used throughout — no ``require_admin`` override.
    """

    async def test_proof1_onboard_new_key_no_redeploy(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Proof 1 (static): register a new key at runtime → immediately accepted.

        Single app instance, no restart between PRE and POST:

          PRE : data request with FAKE_NEW_DATA_RAW_KEY → 401 (not registered).
          ACT : admin PUT /admin/keys/{digest} → 200.
          POST: SAME instance, FAKE_NEW_DATA_RAW_KEY → 202.
          Assert: created_by == FAKE_CONTRIBUTOR_NEW_STATIC in queue bytes.
        """
        from context_intelligence_server.main import create_asgi_app  # noqa: PLC0415

        captured: list[bytes] = []

        # Build the ONE app instance used for all steps.
        settings = _static_settings(tmp_path)
        middleware = create_asgi_app(settings=settings)

        # Install capture AFTER create_asgi_app (so queue_manager lazy-inits).
        _install_queue_capture(monkeypatch, captured)

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=middleware), base_url="http://test"
        ) as c:
            # ── PRE: new key is unknown → 401 ──────────────────────────────
            pre = await c.post(
                "/events",
                json=_event("static-onboard-pre"),
                headers={"Authorization": f"Bearer {FAKE_NEW_DATA_RAW_KEY}"},
            )
            assert pre.status_code == 401, (
                f"[PRE] Expected 401 before registration, "
                f"got {pre.status_code}: {pre.text}"
            )
            assert len(captured) == 0, "No queue append before registration"

            # ── ACT: admin registers the new key ────────────────────────────
            put_resp = await c.put(
                f"/admin/keys/{FAKE_NEW_DATA_KEY_DIGEST}",
                json={"id": FAKE_CONTRIBUTOR_NEW_STATIC},
                headers={"Authorization": f"Bearer {FAKE_ADMIN_RAW_KEY}"},
            )
            assert put_resp.status_code == 200, (
                f"[ACT] Admin PUT failed: {put_resp.status_code}: {put_resp.text}"
            )

            # ── POST: SAME instance — key now accepted (no restart) ─────────
            post = await c.post(
                "/events",
                json=_event("static-onboard-post"),
                headers={"Authorization": f"Bearer {FAKE_NEW_DATA_RAW_KEY}"},
            )
            assert post.status_code == 202, (
                f"[POST] Expected 202 after registration (no redeploy), "
                f"got {post.status_code}: {post.text}"
            )

        # ── Assert created_by from captured queue bytes ──────────────────────
        assert len(captured) == 1, (
            f"Expected exactly one queue-append call, got {len(captured)}"
        )
        body_obj = json.loads(captured[0])
        assert body_obj["created_by"] == FAKE_CONTRIBUTOR_NEW_STATIC, (
            f"created_by should be {FAKE_CONTRIBUTOR_NEW_STATIC!r}, "
            f"got {body_obj.get('created_by')!r}"
        )

    async def test_proof2_offboard_key_is_immediate(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Proof 2 (static): delete a key at runtime → rejected immediately.

        Single app instance, no restart between PRE and POST:

          PRE : FAKE_INITIAL_DATA_RAW_KEY → 202 (registered at startup).
          ACT : admin DELETE /admin/keys/{digest} → 200.
          POST: SAME instance, FAKE_INITIAL_DATA_RAW_KEY → 401 immediately.
        """
        from context_intelligence_server.main import create_asgi_app  # noqa: PLC0415

        captured: list[bytes] = []
        settings = _static_settings(tmp_path)
        middleware = create_asgi_app(settings=settings)

        _install_queue_capture(monkeypatch, captured)

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=middleware), base_url="http://test"
        ) as c:
            # ── PRE: seed data key is registered → 202 ──────────────────────
            pre = await c.post(
                "/events",
                json=_event("static-offboard-pre"),
                headers={"Authorization": f"Bearer {FAKE_INITIAL_DATA_RAW_KEY}"},
            )
            assert pre.status_code == 202, (
                f"[PRE] Expected 202 before deletion, got {pre.status_code}: {pre.text}"
            )

            # ── ACT: admin deletes the key ───────────────────────────────────
            del_resp = await c.delete(
                f"/admin/keys/{FAKE_INITIAL_DATA_KEY_DIGEST}",
                headers={"Authorization": f"Bearer {FAKE_ADMIN_RAW_KEY}"},
            )
            assert del_resp.status_code == 200, (
                f"[ACT] Admin DELETE failed: {del_resp.status_code}: {del_resp.text}"
            )

            # ── POST: SAME instance — key rejected immediately (no restart) ──
            post = await c.post(
                "/events",
                json=_event("static-offboard-post"),
                headers={"Authorization": f"Bearer {FAKE_INITIAL_DATA_RAW_KEY}"},
            )
            assert post.status_code == 401, (
                f"[POST] Expected 401 after deletion (no redeploy), "
                f"got {post.status_code}: {post.text}"
            )


# ===========================================================================
# Proof 3 & 4: Entra mode — onboard and offboard, no redeploy
# ===========================================================================


class TestEntraModeOnboardOffboard:
    """
    Proof 3: add a new OID at runtime → accepted immediately.
    Proof 4: delete an OID at runtime → rejected immediately.
    Both proofs run against a SINGLE ``create_asgi_app()`` instance.
    Real RS256 JWTs + stub JWKS; no ``require_admin`` override.

    ``created_by`` is observed via queue-capture (no Neo4j needed).
    """

    async def test_proof3_onboard_new_oid_no_redeploy(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        rsa_keypair_e2e: tuple[Any, Any],
    ) -> None:
        """Proof 3 (entra): register a new OID at runtime → immediately accepted.

        Single app instance, no restart between PRE and POST:

          PRE : FAKE_OID_NEW token → 403 identity_unbound (OID not in map).
          ACT : admin PUT /admin/identities/{oid} → 200.
          POST: SAME instance, FAKE_OID_NEW token → 202.
          Assert: created_by == FAKE_CONTRIBUTOR_NEW_ENTRA from queue bytes.

        Admin token uses FAKE_OID_SEED (seeded at startup) + IdentityAdmin role.
        """
        from context_intelligence_server.main import create_asgi_app  # noqa: PLC0415

        private_key, public_key = rsa_keypair_e2e
        captured: list[bytes] = []

        settings = _entra_settings(tmp_path)
        middleware = create_asgi_app(
            settings=settings, _jwks_client=_StubJWKSClient(public_key)
        )
        _install_queue_capture(monkeypatch, captured)

        # Admin: seeded OID (in map) + IdentityAdmin role → passes middleware + require_admin.
        # New user: FAKE_OID_NEW (NOT in map yet) + no roles.
        admin_token = _mint_entra_token(
            private_key, oid=FAKE_OID_SEED, roles=[IDENTITY_ADMIN_ROLE]
        )
        new_user_token = _mint_entra_token(private_key, oid=FAKE_OID_NEW, roles=None)

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=middleware), base_url="http://test"
        ) as c:
            # ── PRE: FAKE_OID_NEW not in map → 403 (identity_unbound) ────────
            pre = await c.post(
                "/events",
                json=_event("entra-onboard-pre"),
                headers={"Authorization": f"Bearer {new_user_token}"},
            )
            assert pre.status_code == 403, (
                f"[PRE] Expected 403 (identity_unbound) before registration, "
                f"got {pre.status_code}: {pre.text}"
            )
            assert len(captured) == 0, "No queue append before registration"

            # ── ACT: admin registers FAKE_OID_NEW ───────────────────────────
            put_resp = await c.put(
                f"/admin/identities/{FAKE_OID_NEW}",
                json={"id": FAKE_CONTRIBUTOR_NEW_ENTRA},
                headers={"Authorization": f"Bearer {admin_token}"},
            )
            assert put_resp.status_code == 200, (
                f"[ACT] Admin PUT failed: {put_resp.status_code}: {put_resp.text}"
            )

            # ── POST: SAME instance — OID now accepted (no restart) ──────────
            post = await c.post(
                "/events",
                json=_event("entra-onboard-post"),
                headers={"Authorization": f"Bearer {new_user_token}"},
            )
            assert post.status_code == 202, (
                f"[POST] Expected 202 after registration (no redeploy), "
                f"got {post.status_code}: {post.text}"
            )

        # ── Assert created_by from captured queue bytes ──────────────────────
        assert len(captured) == 1, (
            f"Expected exactly one queue-append call, got {len(captured)}"
        )
        body_obj = json.loads(captured[0])
        assert body_obj["created_by"] == FAKE_CONTRIBUTOR_NEW_ENTRA, (
            f"created_by should be {FAKE_CONTRIBUTOR_NEW_ENTRA!r}, "
            f"got {body_obj.get('created_by')!r}"
        )

    async def test_proof4_offboard_oid_is_immediate(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        rsa_keypair_e2e: tuple[Any, Any],
    ) -> None:
        """Proof 4 (entra): delete an OID at runtime → rejected immediately.

        Single app instance, no restart between PRE and POST:

          PRE : FAKE_OID_SEED token → 202 (seeded at startup).
          ACT : admin DELETE /admin/identities/{oid} → 200.
          POST: SAME instance, FAKE_OID_SEED token → 403 immediately.

        Admin token uses FAKE_OID_SEED + IdentityAdmin; after DELETE the admin
        token also becomes invalid (the OID is no longer in the map).  The POST
        using the same OID proves the deletion is immediate — no TTL, no cache.
        """
        from context_intelligence_server.main import create_asgi_app  # noqa: PLC0415

        private_key, public_key = rsa_keypair_e2e
        captured: list[bytes] = []

        settings = _entra_settings(tmp_path)
        middleware = create_asgi_app(
            settings=settings, _jwks_client=_StubJWKSClient(public_key)
        )
        _install_queue_capture(monkeypatch, captured)

        # Admin token carries IdentityAdmin role (for the DELETE).
        # Data token for the same OID (no role — proving data auth, not admin).
        admin_token = _mint_entra_token(
            private_key, oid=FAKE_OID_SEED, roles=[IDENTITY_ADMIN_ROLE]
        )
        seed_user_token = _mint_entra_token(private_key, oid=FAKE_OID_SEED, roles=None)

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=middleware), base_url="http://test"
        ) as c:
            # ── PRE: seed OID is mapped → 202 ───────────────────────────────
            pre = await c.post(
                "/events",
                json=_event("entra-offboard-pre"),
                headers={"Authorization": f"Bearer {seed_user_token}"},
            )
            assert pre.status_code == 202, (
                f"[PRE] Expected 202 before deletion, got {pre.status_code}: {pre.text}"
            )

            # ── ACT: admin deletes FAKE_OID_SEED ────────────────────────────
            del_resp = await c.delete(
                f"/admin/identities/{FAKE_OID_SEED}",
                headers={"Authorization": f"Bearer {admin_token}"},
            )
            assert del_resp.status_code == 200, (
                f"[ACT] Admin DELETE failed: {del_resp.status_code}: {del_resp.text}"
            )

            # ── POST: SAME instance — OID no longer accepted (no restart) ────
            post = await c.post(
                "/events",
                json=_event("entra-offboard-post"),
                headers={"Authorization": f"Bearer {seed_user_token}"},
            )
            assert post.status_code == 403, (
                f"[POST] Expected 403 after deletion (no redeploy), "
                f"got {post.status_code}: {post.text}"
            )


# ===========================================================================
# Negative matrix — both modes, ≥1 representative endpoint per scenario
# ===========================================================================


class TestNegativeMatrix:
    """Integration-level negative matrix for /admin/* endpoints.

    Scenarios: no token → 401; non-admin credential → 403;
    wrong-mode endpoint → 503.  Each proves real middleware enforcement
    (no ``require_admin`` override).
    """

    # ── Static mode ──────────────────────────────────────────────────────────

    async def test_static_no_token_401_on_admin_keys(self, tmp_path: Path) -> None:
        """No Authorization header → 401 on GET /admin/keys (static mode)."""
        from context_intelligence_server.main import create_asgi_app  # noqa: PLC0415

        middleware = create_asgi_app(settings=_static_settings(tmp_path))
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=middleware), base_url="http://test"
        ) as c:
            resp = await c.get("/admin/keys")
        assert resp.status_code == 401, (
            f"Expected 401 for missing token on /admin/keys, got {resp.status_code}"
        )

    async def test_static_data_key_403_on_admin_keys(self, tmp_path: Path) -> None:
        """Data key authenticates but is NOT the admin key → 403 on GET /admin/keys."""
        from context_intelligence_server.main import create_asgi_app  # noqa: PLC0415

        middleware = create_asgi_app(settings=_static_settings(tmp_path))
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=middleware), base_url="http://test"
        ) as c:
            resp = await c.get(
                "/admin/keys",
                headers={"Authorization": f"Bearer {FAKE_INITIAL_DATA_RAW_KEY}"},
            )
        assert resp.status_code == 403, (
            f"Expected 403 for data-key on /admin/keys, got {resp.status_code}"
        )

    async def test_static_wrong_mode_503_on_admin_identities(
        self, tmp_path: Path
    ) -> None:
        """Static mode: /admin/identities/* → 503 (entra store not active)."""
        from context_intelligence_server.main import create_asgi_app  # noqa: PLC0415

        middleware = create_asgi_app(settings=_static_settings(tmp_path))
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=middleware), base_url="http://test"
        ) as c:
            resp = await c.get(
                "/admin/identities",
                headers={"Authorization": f"Bearer {FAKE_ADMIN_RAW_KEY}"},
            )
        assert resp.status_code == 503, (
            f"Expected 503 for /admin/identities in static mode, got {resp.status_code}"
        )

    # ── Entra mode ────────────────────────────────────────────────────────────

    async def test_entra_no_token_401_on_admin_identities(
        self, tmp_path: Path, rsa_keypair_e2e: tuple[Any, Any]
    ) -> None:
        """No Authorization header → 401 on GET /admin/identities (entra mode)."""
        from context_intelligence_server.main import create_asgi_app  # noqa: PLC0415

        _, public_key = rsa_keypair_e2e
        middleware = create_asgi_app(
            settings=_entra_settings(tmp_path),
            _jwks_client=_StubJWKSClient(public_key),
        )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=middleware), base_url="http://test"
        ) as c:
            resp = await c.get("/admin/identities")
        assert resp.status_code == 401, (
            f"Expected 401 for missing token on /admin/identities, got {resp.status_code}"
        )

    async def test_entra_non_admin_token_403_on_admin_identities(
        self, tmp_path: Path, rsa_keypair_e2e: tuple[Any, Any]
    ) -> None:
        """Valid entra token WITHOUT IdentityAdmin role → 403 on GET /admin/identities."""
        from context_intelligence_server.main import create_asgi_app  # noqa: PLC0415

        private_key, public_key = rsa_keypair_e2e
        middleware = create_asgi_app(
            settings=_entra_settings(tmp_path),
            _jwks_client=_StubJWKSClient(public_key),
        )
        # Seeded OID is in the map (passes middleware), but no roles (fails require_admin).
        non_admin_token = _mint_entra_token(private_key, oid=FAKE_OID_SEED, roles=[])
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=middleware), base_url="http://test"
        ) as c:
            resp = await c.get(
                "/admin/identities",
                headers={"Authorization": f"Bearer {non_admin_token}"},
            )
        assert resp.status_code == 403, (
            f"Expected 403 for non-admin token on /admin/identities, got {resp.status_code}"
        )

    async def test_entra_wrong_mode_503_on_admin_keys(
        self, tmp_path: Path, rsa_keypair_e2e: tuple[Any, Any]
    ) -> None:
        """Entra mode: /admin/keys/* → 503 (api-key store not active)."""
        from context_intelligence_server.main import create_asgi_app  # noqa: PLC0415

        private_key, public_key = rsa_keypair_e2e
        middleware = create_asgi_app(
            settings=_entra_settings(tmp_path),
            _jwks_client=_StubJWKSClient(public_key),
        )
        admin_token = _mint_entra_token(
            private_key, oid=FAKE_OID_SEED, roles=[IDENTITY_ADMIN_ROLE]
        )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=middleware), base_url="http://test"
        ) as c:
            resp = await c.get(
                "/admin/keys",
                headers={"Authorization": f"Bearer {admin_token}"},
            )
        assert resp.status_code == 503, (
            f"Expected 503 for /admin/keys in entra mode, got {resp.status_code}"
        )
