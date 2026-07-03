"""T8: Entra-mode integration tests over real HTTP (httpx → asgi_app).

Proves the full auth chain end-to-end without network or real Entra.
Uses an in-test RSA keypair + stub JWKS client (same pattern as
test_entra_resolver.py).

Coverage:
  AC2/AC9 — valid entra JWT → 202, created_by stamped as mapped contributor
  AC4      — unmapped oid  → 403 HTTP response
  AC3      — expired / garbage / missing bearer → 401 HTTP response
  Exempt   — /status and /skills/* pass through without a token
  Regression — existing static auth_client path unbroken
"""

from __future__ import annotations

import json
import time
from collections.abc import AsyncGenerator
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest

# ---------------------------------------------------------------------------
# Fake constants — NEVER real app-reg ids / oids / tenant ids (§0.3)
# ---------------------------------------------------------------------------
FAKE_CLIENT_ID = "aaaabbbb-1111-2222-3333-ccccddddeeee"
FAKE_TENANT_ID = "ffffeeee-dddd-cccc-bbbb-aaaa99998888"
FAKE_OID_MAPPED = "11111111-2222-3333-4444-555566667777"  # in identity map
FAKE_OID_UNMAPPED = "22222222-3333-4444-5555-666677778888"  # NOT in identity map
FAKE_CONTRIBUTOR = "colombod"
FAKE_ISSUER = f"https://login.microsoftonline.com/{FAKE_TENANT_ID}/v2.0"


# ---------------------------------------------------------------------------
# Stub JWKS client helpers (mirrors test_entra_resolver.py — no network)
# ---------------------------------------------------------------------------


class _StubSigningKey:
    """Mimics ``PyJWKClient.get_signing_key_from_jwt(token).key``."""

    def __init__(self, key: Any) -> None:
        self.key = key


class _StubJWKSClient:
    """JWKS client stub: fetch_data is a no-op; always returns a fixed key."""

    def __init__(self, key: Any) -> None:
        self._key = _StubSigningKey(key)

    def fetch_data(self) -> None:
        pass  # eager-prefetch no-op

    def get_signing_key_from_jwt(self, token: str) -> _StubSigningKey:
        return self._key

    def get_jwk_set(self) -> Any:
        _k = self._key

        class _FakeJWKSet:
            keys = [_k]

        return _FakeJWKSet()


# ---------------------------------------------------------------------------
# JWT helpers
# ---------------------------------------------------------------------------


def _sign_jwt(private_key: Any, claims: dict[str, Any]) -> str:
    """Sign a JWT with the given RSA private key (RS256)."""
    import jwt as pyjwt  # noqa: PLC0415

    return pyjwt.encode(claims, private_key, algorithm="RS256")


def _valid_entra_claims(
    oid: str = FAKE_OID_MAPPED,
    aud: str | list[str] | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return a minimal valid claims dict for integration tests."""
    now = int(time.time())
    claims: dict[str, Any] = {
        "oid": oid,
        "tid": FAKE_TENANT_ID,
        "scp": "access_as_user",
        "aud": aud if aud is not None else FAKE_CLIENT_ID,
        "iss": FAKE_ISSUER,
        "exp": now + 3600,
        "iat": now - 10,
    }
    if extra:
        claims.update(extra)
    return claims


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def rsa_keypair_module() -> tuple[Any, Any]:
    """Generate a 2048-bit RSA keypair once per module (expensive)."""
    from cryptography.hazmat.primitives.asymmetric import rsa  # noqa: PLC0415

    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public_key = private_key.public_key()
    return private_key, public_key


@pytest.fixture
async def entra_auth_client(
    rsa_keypair_module: tuple[Any, Any],
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """httpx client backed by a fresh create_asgi_app(auth_mode=entra) instance.

    Uses an in-test RSA keypair + stub JWKS client — no network, no real Entra.
    The identity map binds FAKE_OID_MAPPED → FAKE_CONTRIBUTOR ("colombod").

    The fixture creates a *new* BearerTokenMiddleware wrapping the existing
    FastAPI ``app`` singleton — it does NOT patch the module-level ``asgi_app``,
    so the existing static ``auth_client`` fixture is unaffected.
    """
    from context_intelligence_server.config import Settings  # noqa: PLC0415
    from context_intelligence_server.main import app, create_asgi_app  # noqa: PLC0415
    from context_intelligence_server.routers.skills import SkillRegistry  # noqa: PLC0415

    _, public_key = rsa_keypair_module

    entra_settings = Settings(
        auth_mode="entra",
        azure_client_id=FAKE_CLIENT_ID,
        azure_tenant_id=FAKE_TENANT_ID,
        entra_identities={
            FAKE_OID_MAPPED: {"id": FAKE_CONTRIBUTOR},
        },
    )

    # The lifespan sets app.state.skill_registry in production; in unit tests
    # the lifespan does not run.  Guard so /skills/* returns 404 (not 500).
    if not hasattr(app.state, "skill_registry"):
        app.state.skill_registry = SkillRegistry()

    entra_asgi = create_asgi_app(
        settings=entra_settings,
        _jwks_client=_StubJWKSClient(public_key),
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=entra_asgi),
        base_url="http://test",
    ) as c:
        yield c


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestEntraAuthIntegrationOverHTTP:
    """Full auth chain over real HTTP (httpx → fresh entra ASGI app).

    Every assertion is on a real HTTP response status code — not on a mocked
    ASGI ``send`` callable.  No neo4j infrastructure required.
    """

    # ------------------------------------------------------------------
    # AC2/AC9: created_by chain — the load-bearing end-to-end proof
    # ------------------------------------------------------------------

    async def test_valid_token_stamps_created_by_as_mapped_contributor(
        self,
        entra_auth_client: httpx.AsyncClient,
        rsa_keypair_module: tuple[Any, Any],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """AC2/AC9: valid entra JWT with mapped oid → 202, created_by='colombod'.

        Proves the full chain end-to-end:
          Bearer token → BearerTokenMiddleware → EntraResolver → contributor_id
          → post_events → durable-queue append (created_by stamped).

        created_by is asserted at queue-append time (same technique as T12 in
        test_main.py) — no Neo4j infrastructure required.
        """
        import context_intelligence_server.main as main_module  # noqa: PLC0415

        private_key, _ = rsa_keypair_module
        token = _sign_jwt(private_key, _valid_entra_claims())

        # Intercept durable-queue append to capture the stamped bytes.
        captured: list[bytes] = []

        async def _fake_append(worker_key: str, raw: bytes) -> None:
            captured.append(raw)

        monkeypatch.setattr(
            main_module.registry, "get_or_create", lambda *a, **kw: MagicMock()
        )
        monkeypatch.setattr(main_module.registry.queue_manager, "append", _fake_append)

        response = await entra_auth_client.post(
            "/events",
            json={
                "event": "tool_use",
                "workspace": "/ws",
                "data": {
                    "session_id": "s-entra-1",
                    "timestamp": "2026-06-16T20:17:11.604690+00:00",
                },
            },
            headers={"Authorization": f"Bearer {token}"},
        )

        # HTTP-level: auth accepted, route handled the request
        assert response.status_code == 202, (
            f"Expected 202 for valid entra token, got {response.status_code}: "
            f"{response.text}"
        )
        # created_by chain: contributor_id flows from resolver → queue append
        assert len(captured) == 1, (
            f"Expected exactly one queue-append call, got {len(captured)}"
        )
        body_obj = json.loads(captured[0])
        assert body_obj["created_by"] == FAKE_CONTRIBUTOR, (
            f"created_by should be {FAKE_CONTRIBUTOR!r}, "
            f"got {body_obj.get('created_by')!r}"
        )

    # ------------------------------------------------------------------
    # AC4: unmapped oid → 403 over real HTTP
    # ------------------------------------------------------------------

    async def test_unmapped_oid_returns_403_over_http(
        self,
        entra_auth_client: httpx.AsyncClient,
        rsa_keypair_module: tuple[Any, Any],
    ) -> None:
        """AC4: valid JWT whose oid is NOT in entra_identities → HTTP 403.

        bearer_identity_unbound path: token passes crypto validation but the oid
        has no entry in the identity map → 403 (not 401, not 200, not 500).
        """
        private_key, _ = rsa_keypair_module
        # Sign a perfectly valid JWT — only the oid is not in the identity map.
        token = _sign_jwt(private_key, _valid_entra_claims(oid=FAKE_OID_UNMAPPED))

        response = await entra_auth_client.post(
            "/events",
            json={
                "event": "tool_use",
                "workspace": "/ws",
                "data": {"session_id": "s-entra-2"},
            },
            headers={"Authorization": f"Bearer {token}"},
        )
        assert response.status_code == 403, (
            f"Expected 403 for unmapped oid, got {response.status_code}: "
            f"{response.text}"
        )

    # ------------------------------------------------------------------
    # AC3: invalid tokens → 401 over real HTTP (three distinct paths)
    # ------------------------------------------------------------------

    async def test_expired_token_returns_401_over_http(
        self,
        entra_auth_client: httpx.AsyncClient,
        rsa_keypair_module: tuple[Any, Any],
    ) -> None:
        """AC3 (real-crypto): expired JWT (exp in the past) → HTTP 401."""
        private_key, _ = rsa_keypair_module
        now = int(time.time())
        expired_claims = _valid_entra_claims(
            extra={"exp": now - 3600, "iat": now - 7200}
        )
        token = _sign_jwt(private_key, expired_claims)

        response = await entra_auth_client.post(
            "/events",
            json={
                "event": "tool_use",
                "workspace": "/ws",
                "data": {"session_id": "s-entra-3"},
            },
            headers={"Authorization": f"Bearer {token}"},
        )
        assert response.status_code == 401, (
            f"Expected 401 for expired token, got {response.status_code}: "
            f"{response.text}"
        )

    async def test_garbage_bearer_returns_401_over_http(
        self,
        entra_auth_client: httpx.AsyncClient,
    ) -> None:
        """Garbage bearer (not a JWT) → HTTP 401."""
        response = await entra_auth_client.post(
            "/events",
            json={
                "event": "tool_use",
                "workspace": "/ws",
                "data": {"session_id": "s-entra-4"},
            },
            headers={"Authorization": "Bearer not.a.real.jwt.string"},
        )
        assert response.status_code == 401, (
            f"Expected 401 for garbage bearer, got {response.status_code}: "
            f"{response.text}"
        )

    async def test_no_auth_header_returns_401_over_http(
        self,
        entra_auth_client: httpx.AsyncClient,
    ) -> None:
        """No Authorization header at all → HTTP 401."""
        response = await entra_auth_client.post(
            "/events",
            json={
                "event": "tool_use",
                "workspace": "/ws",
                "data": {"session_id": "s-entra-5"},
            },
        )
        assert response.status_code == 401, (
            f"Expected 401 for missing auth header, got {response.status_code}: "
            f"{response.text}"
        )

    # ------------------------------------------------------------------
    # Exempt paths: /status and /skills/* open under entra mode
    # ------------------------------------------------------------------

    async def test_status_endpoint_requires_auth_under_entra_mode(
        self,
        entra_auth_client: httpx.AsyncClient,
    ) -> None:
        """GET /status → 401 without any token (Step 3, doc 16 W3) — entra mode active.

        /status is no longer an exempt path; /version is the liveness carve-out.
        """
        response = await entra_auth_client.get("/status")
        assert response.status_code == 401, (
            f"Expected 401 for /status (no longer exempt, Step 3 W3), got "
            f"{response.status_code}: {response.text}"
        )

    async def test_skills_path_exempt_under_entra_mode(
        self,
        entra_auth_client: httpx.AsyncClient,
    ) -> None:
        """GET /skills/<anything> → NOT 401 even with auth_mode=entra.

        The middleware exempts /skills/* entirely; the response comes from the
        route handler (404 for an unknown skill) rather than from auth
        middleware (401).
        """
        response = await entra_auth_client.get("/skills/nonexistent-skill-for-t8")
        assert response.status_code != 401, (
            f"Expected non-401 for /skills/* (always exempt), "
            f"got {response.status_code}"
        )

    # ------------------------------------------------------------------
    # Regression: static auth_client path unbroken
    # ------------------------------------------------------------------

    async def test_static_auth_client_regression_no_token_returns_401(
        self,
        auth_client: httpx.AsyncClient,
    ) -> None:
        """Regression: existing static auth path still returns 401 without token."""
        response = await auth_client.post(
            "/events",
            json={
                "event": "tool_use",
                "workspace": "/ws",
                "data": {"session_id": "s-static-reg"},
            },
        )
        assert response.status_code == 401, (
            f"Regression: static auth_client must 401 without token, "
            f"got {response.status_code}"
        )

    async def test_static_auth_client_regression_valid_token_passes(
        self,
        auth_client: httpx.AsyncClient,
    ) -> None:
        """Regression: existing static auth path still accepts the test-secret token."""
        response = await auth_client.post(
            "/events",
            json={
                "event": "tool_use",
                "workspace": "/ws",
                "data": {"session_id": "s-static-reg-ok"},
            },
            headers={"Authorization": "Bearer test-secret"},
        )
        assert response.status_code != 401, (
            f"Regression: static auth_client must accept valid test-secret, "
            f"got {response.status_code}"
        )
