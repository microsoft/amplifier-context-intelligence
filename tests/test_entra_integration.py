"""T8: Entra-mode integration tests over real HTTP (httpx → asgi_app).

Exercises application policy through the authenticated ASGI wrapper using an
explicit opaque-token validator. This tier does not prove token authenticity;
genuine SDK rejection tests belong to test_mise_validation.py.

Coverage:
  AC2/AC9 — valid entra JWT → 202, created_by stamped as mapped contributor
  AC4      — unmapped oid  → 403 HTTP response
  AC3      — validator denial / unknown / missing bearer → 401 HTTP response
  Exempt   — /status passes through without a token
  Regression — existing static auth_client path unbroken
"""

from __future__ import annotations

import json
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest

from tests.conftest import ApplicationTokenValidator

# ---------------------------------------------------------------------------
# Fake constants — NEVER real app-reg ids / oids / tenant ids (§0.3)
# ---------------------------------------------------------------------------
FAKE_CLIENT_ID = "aaaabbbb-1111-2222-3333-ccccddddeeee"
FAKE_TENANT_ID = "ffffeeee-dddd-cccc-bbbb-aaaa99998888"
FAKE_OID_MAPPED = "11111111-2222-3333-4444-555566667777"  # in identity map
FAKE_OID_UNMAPPED = "22222222-3333-4444-5555-666677778888"  # NOT in identity map
FAKE_CONTRIBUTOR = "colombod"


# ---------------------------------------------------------------------------
# Validated application claims
# ---------------------------------------------------------------------------


def _valid_entra_claims(
    oid: str = FAKE_OID_MAPPED,
) -> dict[str, Any]:
    """Return claims already accepted by the application validator."""
    return {
        "oid": oid,
        "tid": FAKE_TENANT_ID,
        "scp": "access_as_user",
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def entra_auth_client(
    application_validator: ApplicationTokenValidator,
    tmp_path: Path,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """httpx client backed by a fresh create_asgi_app(auth_mode=entra) instance.

    Uses explicit validator injection — no SDK, network, or real Entra.
    The identity map binds FAKE_OID_MAPPED → FAKE_CONTRIBUTOR ("colombod").

    The fixture creates a *new* BearerTokenMiddleware wrapping the existing
    FastAPI ``app`` singleton — it does NOT patch the module-level ``asgi_app``,
    so the existing static ``auth_client`` fixture is unaffected.
    """
    from context_intelligence_server.config import Settings  # noqa: PLC0415
    from context_intelligence_server.main import create_asgi_app  # noqa: PLC0415

    entra_settings = Settings(
        auth_mode="entra",
        azure_client_id=FAKE_CLIENT_ID,
        azure_tenant_id=FAKE_TENANT_ID,
        entra_identities={
            FAKE_OID_MAPPED: {"id": FAKE_CONTRIBUTOR},
        },
        entra_identities_store_path=str(tmp_path / "entra-identities.json"),
        api_keys_store_path=str(tmp_path / "api-keys.json"),
    )

    entra_asgi = create_asgi_app(
        settings=entra_settings,
        _token_validator=application_validator,
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
        application_validator: ApplicationTokenValidator,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """AC2/AC9: accepted claims with mapped oid → 202, created_by='colombod'.

        Proves the full chain end-to-end:
          Bearer token → BearerTokenMiddleware → EntraResolver → contributor_id
          → post_events → durable-queue append (created_by stamped).

        created_by is asserted at queue-append time (same technique as T12 in
        test_main.py) — no Neo4j infrastructure required.
        """
        import context_intelligence_server.main as main_module  # noqa: PLC0415

        token = application_validator.issue(_valid_entra_claims())

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
        assert application_validator.calls == [(token, "http://test/events", "POST")]

    # ------------------------------------------------------------------
    # AC4: unmapped oid → 403 over real HTTP
    # ------------------------------------------------------------------

    async def test_unmapped_oid_returns_403_over_http(
        self,
        entra_auth_client: httpx.AsyncClient,
        application_validator: ApplicationTokenValidator,
    ) -> None:
        """AC4: accepted claims whose oid is NOT in entra_identities → HTTP 403.

        bearer_identity_unbound path: validator accepts the handle but the oid
        has no entry in the identity map → 403 (not 401, not 200, not 500).
        """
        token = application_validator.issue(_valid_entra_claims(oid=FAKE_OID_UNMAPPED))

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

    async def test_validator_denied_token_returns_401_over_http(
        self,
        entra_auth_client: httpx.AsyncClient,
        application_validator: ApplicationTokenValidator,
    ) -> None:
        """Validator denial → HTTP 401; no expiry checks are faked here."""
        token = application_validator.issue(_valid_entra_claims())
        application_validator.reject(token)

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
            f"Expected 401 for validator denial, got {response.status_code}: "
            f"{response.text}"
        )

    async def test_garbage_bearer_returns_401_over_http(
        self,
        entra_auth_client: httpx.AsyncClient,
    ) -> None:
        """Unknown opaque bearer → HTTP 401."""
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
    # Exempt paths: /status opens under entra mode
    # ------------------------------------------------------------------

    async def test_status_endpoint_exempt_under_entra_mode(
        self,
        entra_auth_client: httpx.AsyncClient,
    ) -> None:
        """GET /status → 200 without any token — exempt path, entra mode active."""
        response = await entra_auth_client.get("/status")
        assert response.status_code == 200, (
            f"Expected 200 for /status (always exempt), got "
            f"{response.status_code}: {response.text}"
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
