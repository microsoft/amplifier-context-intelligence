"""Test A (doc 14, EasyAuth browser-identity spec, §5 as amended by §10 C7).

Isolation-only: httpx.ASGITransport + create_asgi_app(settings=..., _jwks_client=
_StubJWKSClient(...)) — no live Neo4j, no network, no real Azure/EasyAuth. This
is the primary harness (doc 14 §5): it proves the startup composition gate
(§2.2), middleware precedence (§3.1), and EntraResolver.resolve_principal_id
(§3.3) end-to-end over real HTTP via ASGITransport.

Fake constants only — never real credentials, OIDs, or keys.

Rows covered (doc 14 §4 + §10 C2's redefinition of row 7 + C7 additions):
    A1  missing header                          -> 401
    A2  empty header                             -> 401
    A3  malformed / non-GUID                     -> 401
    A4  all-zeros GUID                           -> 401
    A5  valid GUID, unmapped                     -> 403 identity_unbound + oid
    A6  valid GUID, mapped                       -> 200; contributor_id/is_admin/roles asserted
    A7  X-MS-CLIENT-PRINCIPAL blob, NO scalar -ID -> 401 (C2: scalar-only, blob ignored)
    A8  Bearer + mapped EasyAuth header both present -> resolves via Bearer; WARNING logged
    A9  INVALID Bearer + mapped EasyAuth header  -> 401 from Bearer branch, no fall-through
    A10 trust_easyauth_principal=False + header  -> 401 (header ignored)
    A11 trust_easyauth_principal=True + auth_mode="static" -> raises at construction
    A12 (C1) post-construction mutation of trust_easyauth_principal -> raises
    dup duplicate X-MS-CLIENT-PRINCIPAL-ID headers -> first-match wins (defined behaviour)
"""

from __future__ import annotations

import base64
import json
import logging
import time
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import httpx
import pytest
from pydantic import ValidationError

from context_intelligence_server.config import Settings
from context_intelligence_server.main import create_asgi_app

# pytest-asyncio is configured with asyncio_mode = "auto" (pyproject.toml), so
# plain `async def test_...` methods below run without an explicit marker.

# ---------------------------------------------------------------------------
# Fake constants (never real credentials/OIDs)
# ---------------------------------------------------------------------------

FAKE_CLIENT_ID = "aaaabbbb-1111-2222-3333-ccccddddeeee"
FAKE_TENANT_ID = "ffffeeee-dddd-cccc-bbbb-aaaa99998888"
FAKE_OID_MAPPED = "11111111-1111-1111-1111-111111111111"
FAKE_OID_UNMAPPED = "22222222-2222-2222-2222-222222222222"
FAKE_CONTRIBUTOR = "alice"
FAKE_ISSUER = f"https://login.microsoftonline.com/{FAKE_TENANT_ID}/v2.0"

_EASYAUTH_ID_HEADER = "X-MS-CLIENT-PRINCIPAL-ID"

# Non-exempt data route (doc 14 §5: "a non-exempt data route (e.g. GET /sessions)").
# /events is POST-only, non-exempt, and does NOT touch Neo4j (persist-then-202 to
# the durable append-log) -- safe for isolation (no DB, no network).
_TARGET_PATH = "/events"
_TARGET_BODY: dict[str, Any] = {
    "event": "test_event",
    "workspace": "test-workspace",
    "data": {"timestamp": "2025-01-01T00:00:00Z"},
}


# ---------------------------------------------------------------------------
# Stub JWKS client (no network) -- mirrors tests/routers/test_admin_auth.py
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
    """Generate a 2048-bit RSA keypair for entra JWT signing (module scope = once)."""
    from cryptography.hazmat.primitives.asymmetric import rsa

    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public_key = private_key.public_key()
    return private_key, public_key


def _make_bearer_token(
    private_key: Any,
    *,
    oid: str = FAKE_OID_MAPPED,
    valid: bool = True,
) -> str:
    """Mint a real RS256 JWT (valid=True) or a garbage token (valid=False)."""
    if not valid:
        return "not-a-valid-jwt-at-all"
    import jwt as pyjwt

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
    return pyjwt.encode(claims, private_key, algorithm="RS256")


# ---------------------------------------------------------------------------
# Settings helpers
# ---------------------------------------------------------------------------


def _make_easyauth_settings(
    tmp_path: Path,
    *,
    trust_easyauth_principal: bool = True,
) -> Settings:
    return Settings(
        auth_mode="entra",
        web_ui_enabled=True,
        trust_easyauth_principal=trust_easyauth_principal,
        azure_client_id=FAKE_CLIENT_ID,
        azure_tenant_id=FAKE_TENANT_ID,
        entra_identities={FAKE_OID_MAPPED: {"id": FAKE_CONTRIBUTOR}},
        entra_identities_store_path=str(tmp_path / "entra-identities.json"),
        api_keys_store_path=str(tmp_path / "api-keys.json"),
    )


async def _make_client(
    settings: Settings, rsa_keypair: tuple[Any, Any]
) -> AsyncGenerator[httpx.AsyncClient, None]:
    _private_key, public_key = rsa_keypair
    middleware = create_asgi_app(
        settings=settings, _jwks_client=_StubJWKSClient(public_key)
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=middleware), base_url="http://t"
    ) as client:
        yield client


@pytest.fixture
async def easyauth_client(
    tmp_path: Path, rsa_keypair: tuple[Any, Any]
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Client with trust_easyauth_principal=True (the on state under test)."""
    settings = _make_easyauth_settings(tmp_path)
    async for c in _make_client(settings, rsa_keypair):
        yield c


@pytest.fixture
async def easyauth_off_client(
    tmp_path: Path, rsa_keypair: tuple[Any, Any]
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Client with trust_easyauth_principal=False (A10: header must be ignored)."""
    settings = _make_easyauth_settings(tmp_path, trust_easyauth_principal=False)
    async for c in _make_client(settings, rsa_keypair):
        yield c


# ---------------------------------------------------------------------------
# A1-A7: the 7 rows (per doc 14 §4, row 7 redefined by C2)
# ---------------------------------------------------------------------------


class TestEasyAuthRows:
    async def test_a1_missing_header_401(
        self, easyauth_client: httpx.AsyncClient
    ) -> None:
        resp = await easyauth_client.post(_TARGET_PATH, json=_TARGET_BODY)
        assert resp.status_code == 401

    async def test_a2_empty_header_401(
        self, easyauth_client: httpx.AsyncClient
    ) -> None:
        resp = await easyauth_client.post(
            _TARGET_PATH, json=_TARGET_BODY, headers={_EASYAUTH_ID_HEADER: ""}
        )
        assert resp.status_code == 401

    async def test_a3_malformed_non_guid_401(
        self, easyauth_client: httpx.AsyncClient
    ) -> None:
        resp = await easyauth_client.post(
            _TARGET_PATH,
            json=_TARGET_BODY,
            headers={_EASYAUTH_ID_HEADER: "not-a-guid"},
        )
        assert resp.status_code == 401

    async def test_a4_all_zeros_guid_401(
        self, easyauth_client: httpx.AsyncClient
    ) -> None:
        resp = await easyauth_client.post(
            _TARGET_PATH,
            json=_TARGET_BODY,
            headers={_EASYAUTH_ID_HEADER: "00000000-0000-0000-0000-000000000000"},
        )
        assert resp.status_code == 401

    async def test_a5_unmapped_valid_guid_403_identity_unbound(
        self, easyauth_client: httpx.AsyncClient
    ) -> None:
        resp = await easyauth_client.post(
            _TARGET_PATH,
            json=_TARGET_BODY,
            headers={_EASYAUTH_ID_HEADER: FAKE_OID_UNMAPPED},
        )
        assert resp.status_code == 403
        body = resp.json()
        assert "identity_unbound" in body["detail"]
        assert FAKE_OID_UNMAPPED in body["detail"]

    async def test_a6_mapped_guid_accepted(
        self, easyauth_client: httpx.AsyncClient
    ) -> None:
        """The wiring-level accept (200/202) for a mapped oid.

        C7's requirement that is_admin=False / roles=[] are ALSO asserted on
        the accepted request is proven at the middleware-unit level in
        TestMiddlewareStateDirect below, since /events does not echo
        scope["state"] back in its response body.
        """
        resp = await easyauth_client.post(
            _TARGET_PATH,
            json=_TARGET_BODY,
            headers={_EASYAUTH_ID_HEADER: FAKE_OID_MAPPED},
        )
        assert resp.status_code == 202
        assert resp.json()["status"] == "queued"

    async def test_a7_forged_blob_no_scalar_header_401(
        self, easyauth_client: httpx.AsyncClient
    ) -> None:
        """C2 (BINDING): scalar-only. A blob with NO scalar -ID header -> 401."""
        principal = {"claims": []}
        blob = base64.b64encode(json.dumps(principal).encode()).decode()
        resp = await easyauth_client.post(
            _TARGET_PATH,
            json=_TARGET_BODY,
            headers={"X-MS-CLIENT-PRINCIPAL": blob},
        )
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# A8-A11: precedence + downgrade regression + startup gate
# ---------------------------------------------------------------------------


class TestEasyAuthPrecedence:
    async def test_a8_bearer_wins_over_easyauth_and_logs_warning(
        self,
        easyauth_client: httpx.AsyncClient,
        rsa_keypair: tuple[Any, Any],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        private_key, _public_key = rsa_keypair
        token = _make_bearer_token(private_key, oid=FAKE_OID_MAPPED, valid=True)
        with caplog.at_level(
            logging.WARNING, logger="context_intelligence_server.auth"
        ):
            resp = await easyauth_client.post(
                _TARGET_PATH,
                json=_TARGET_BODY,
                headers={
                    "Authorization": f"Bearer {token}",
                    _EASYAUTH_ID_HEADER: FAKE_OID_MAPPED,
                },
            )
        assert resp.status_code == 202
        assert "auth_event=easyauth_header_with_bearer" in caplog.text

    async def test_a9_invalid_bearer_no_fallthrough_to_easyauth(
        self, easyauth_client: httpx.AsyncClient, rsa_keypair: tuple[Any, Any]
    ) -> None:
        invalid_token = _make_bearer_token(rsa_keypair[0], valid=False)
        resp = await easyauth_client.post(
            _TARGET_PATH,
            json=_TARGET_BODY,
            headers={
                "Authorization": f"Bearer {invalid_token}",
                _EASYAUTH_ID_HEADER: FAKE_OID_MAPPED,
            },
        )
        assert resp.status_code == 401

    async def test_a10_trust_off_ignores_header(
        self, easyauth_off_client: httpx.AsyncClient
    ) -> None:
        resp = await easyauth_off_client.post(
            _TARGET_PATH,
            json=_TARGET_BODY,
            headers={_EASYAUTH_ID_HEADER: FAKE_OID_MAPPED},
        )
        assert resp.status_code == 401

    def test_a11_startup_gate_trust_without_entra_mode(self, tmp_path: Path) -> None:
        with pytest.raises((ValueError, RuntimeError)):
            settings = Settings(
                auth_mode="static",
                trust_easyauth_principal=True,
                api_key="fake-static-key-for-a11",
                entra_identities_store_path=str(tmp_path / "entra-identities.json"),
                api_keys_store_path=str(tmp_path / "api-keys.json"),
            )
            create_asgi_app(settings=settings)

    def test_a11b_startup_gate_trust_without_web_ui(self, tmp_path: Path) -> None:
        """§2.2 second arm: trust_easyauth_principal=True requires web_ui_enabled=True."""
        with pytest.raises((ValueError, RuntimeError)):
            Settings(
                auth_mode="entra",
                web_ui_enabled=False,
                trust_easyauth_principal=True,
                azure_client_id=FAKE_CLIENT_ID,
                azure_tenant_id=FAKE_TENANT_ID,
                entra_identities={FAKE_OID_MAPPED: {"id": FAKE_CONTRIBUTOR}},
                entra_identities_store_path=str(tmp_path / "entra-identities.json"),
                api_keys_store_path=str(tmp_path / "api-keys.json"),
            )


# ---------------------------------------------------------------------------
# A12 (C1): Settings is frozen -- post-construction mutation rejected
# ---------------------------------------------------------------------------


class TestSettingsFrozen:
    def test_a12_post_construction_mutation_rejected(self, tmp_path: Path) -> None:
        settings = _make_easyauth_settings(tmp_path)
        with pytest.raises(ValidationError):
            settings.trust_easyauth_principal = False  # type: ignore[misc]


# ---------------------------------------------------------------------------
# TB-6: invalid bearer + valid EasyAuth header -> 401 from BEARER branch, no
# fall-through; C6 co-presence WARNING may fire but must NOT change the outcome
# ---------------------------------------------------------------------------


class TestNoFallthrough:
    async def test_tb6_invalid_bearer_wins_401_warning_does_not_change_outcome(
        self,
        easyauth_client: httpx.AsyncClient,
        rsa_keypair: tuple[Any, Any],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """An invalid/expired bearer token is present, so token is not None and
        the EasyAuth branch is structurally unreachable — the invalid bearer must
        produce 401 from the bearer branch (no silent downgrade to the mapped
        EasyAuth header). The C6 co-presence WARNING may be emitted, but the
        outcome stays 401 regardless.
        """
        invalid_token = _make_bearer_token(rsa_keypair[0], valid=False)
        with caplog.at_level(
            logging.WARNING, logger="context_intelligence_server.auth"
        ):
            resp = await easyauth_client.post(
                _TARGET_PATH,
                json=_TARGET_BODY,
                headers={
                    "Authorization": f"Bearer {invalid_token}",
                    _EASYAUTH_ID_HEADER: FAKE_OID_MAPPED,  # valid + mapped
                },
            )
        assert resp.status_code == 401
        # The co-presence anomaly is logged (bearer + EasyAuth header both
        # present), but it does not — and must not — rescue the request into the
        # EasyAuth path.
        assert "auth_event=easyauth_header_with_bearer" in caplog.text


# ---------------------------------------------------------------------------
# TB-8: GUID_RE is ASCII-only — a non-ASCII "digit" oid is rejected (401)
# ---------------------------------------------------------------------------


class TestGuidAsciiOnly:
    """TB-8 at the resolver/regex level.

    Driving this over HTTP is impossible: httpx refuses to encode a non-ASCII
    header value (and the ASGI latin-1 layer can't carry U+0663 either), so the
    request never reaches the middleware. The meaningful assertion — that
    GUID_RE's ``[0-9a-f]`` class is ASCII-only and no Unicode "digit" slips
    through to a map lookup — is made directly against GUID_RE and
    resolve_principal_id.
    """

    def test_tb8_guid_re_rejects_non_ascii_digit(self) -> None:
        from context_intelligence_server.config import GUID_RE

        # U+0663 (Arabic-Indic THREE, '٣') in place of ASCII '3'.
        non_ascii_oid = "1111111\u0663-1111-1111-1111-111111111111"
        assert GUID_RE.fullmatch(non_ascii_oid) is None

    def test_tb8_resolve_principal_id_rejects_non_ascii_digit_401(
        self, rsa_keypair: tuple[Any, Any]
    ) -> None:
        from context_intelligence_server.auth import AuthError, EntraResolver

        _private_key, public_key = rsa_keypair
        resolver = EntraResolver(
            FAKE_CLIENT_ID,
            FAKE_TENANT_ID,
            {FAKE_OID_MAPPED: FAKE_CONTRIBUTOR},
            jwks_client=_StubJWKSClient(public_key),
        )
        non_ascii_oid = "1111111\u0663-1111-1111-1111-111111111111"
        with pytest.raises(AuthError) as exc_info:
            resolver.resolve_principal_id(non_ascii_oid)
        assert exc_info.value.status_code == 401


# ---------------------------------------------------------------------------
# TB-3: oid/key case normalization — config key case and incoming header case
# are both normalized to lowercase, so they resolve regardless of casing
# ---------------------------------------------------------------------------


_MIXED_CASE_OID_CANONICAL = "aaaabbbb-cccc-dddd-eeee-ffff00001111"


def _make_mixed_case_key_settings(tmp_path: Path) -> Settings:
    """Settings whose entra_identities KEY is UPPER/mixed-case in config."""
    return Settings(
        auth_mode="entra",
        web_ui_enabled=True,
        trust_easyauth_principal=True,
        azure_client_id=FAKE_CLIENT_ID,
        azure_tenant_id=FAKE_TENANT_ID,
        entra_identities={_MIXED_CASE_OID_CANONICAL.upper(): {"id": "mixedcase-user"}},
        entra_identities_store_path=str(tmp_path / "entra-identities.json"),
        api_keys_store_path=str(tmp_path / "api-keys.json"),
    )


class TestCaseNormalization:
    async def test_tb3_upper_config_key_lower_header_resolves(
        self, tmp_path: Path, rsa_keypair: tuple[Any, Any]
    ) -> None:
        """Config key in UPPER case + incoming EasyAuth oid in lower case -> 200.

        (The validator lowercases stored keys; resolve_principal_id lowercases
        the incoming oid — both sides meet at lowercase.)
        """
        settings = _make_mixed_case_key_settings(tmp_path)
        async for client in _make_client(settings, rsa_keypair):
            resp = await client.post(
                _TARGET_PATH,
                json=_TARGET_BODY,
                headers={_EASYAUTH_ID_HEADER: _MIXED_CASE_OID_CANONICAL.lower()},
            )
            assert resp.status_code == 202

    async def test_tb3_lower_config_key_upper_header_resolves(
        self, tmp_path: Path, rsa_keypair: tuple[Any, Any]
    ) -> None:
        """Reverse: config key lower-case (canonical) + incoming oid UPPER-case -> 200."""
        settings = _make_easyauth_settings(tmp_path)  # key = FAKE_OID_MAPPED (lower)
        async for client in _make_client(settings, rsa_keypair):
            resp = await client.post(
                _TARGET_PATH,
                json=_TARGET_BODY,
                headers={_EASYAUTH_ID_HEADER: FAKE_OID_MAPPED.upper()},
            )
            assert resp.status_code == 202


# ---------------------------------------------------------------------------
# TB-2: duplicate scalar header -> 401 anomaly (supersedes the earlier C7
# "first-match wins" decision; adversarial review hardening)
# ---------------------------------------------------------------------------


class TestDuplicateHeader:
    async def test_duplicate_scalar_header_rejected_401(
        self, easyauth_client: httpx.AsyncClient
    ) -> None:
        """TB-2 (supersedes C7 first-match): TWO X-MS-CLIENT-PRINCIPAL-ID headers

        -> 401, NOT silently resolving to either value. A legit EasyAuth edge
        injects exactly one header and strips inbound copies, so 2+ is the
        fingerprint of an attacker smuggling a forged value (here: a valid
        MAPPED oid first, an UNMAPPED oid second). The request must be rejected
        outright rather than letting header ordering choose the winner.
        """
        # httpx's high-level `headers=` dict cannot express a repeated header, so
        # build an explicit multi-value Headers object (preserves order + dupes).
        headers = httpx.Headers(
            [
                (_EASYAUTH_ID_HEADER, FAKE_OID_MAPPED),
                (_EASYAUTH_ID_HEADER, FAKE_OID_UNMAPPED),
            ]
        )
        resp = await easyauth_client.post(
            _TARGET_PATH, json=_TARGET_BODY, headers=headers
        )
        # Neither oid wins — the duplicate is an anomaly -> 401 (not 202, not 403).
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Direct middleware unit test: verifies scope["state"] contents for A6 (C7)
# ---------------------------------------------------------------------------


class TestMiddlewareStateDirect:
    """Drives BearerTokenMiddleware.__call__ directly with a raw ASGI scope
    (the acceptable unit alternative per doc 14 §5) to assert the EXACT
    scope["state"] contents on a mapped EasyAuth accept -- something the
    HTTP-level ASGITransport tests above cannot easily introspect.
    """

    async def test_mapped_oid_sets_expected_state(self) -> None:
        from unittest.mock import AsyncMock

        from context_intelligence_server.auth import BearerTokenMiddleware

        captured_scope: dict[str, Any] = {}

        async def _app(scope: dict, receive: Any, send: Any) -> None:
            captured_scope.update(scope)

        def _resolve(oid: str) -> tuple[str, list[str], bool]:
            assert oid == FAKE_OID_MAPPED
            return (FAKE_CONTRIBUTOR, [], False)

        # keystore={} alone makes auth_enabled=False (fail-open), which would
        # skip the EasyAuth branch. Inject a minimal always-on resolver whose
        # resolve() is never reached (no bearer token in the scope) so the
        # EasyAuth branch is exercised.
        class _AlwaysOnResolver:
            auth_enabled = True

            def resolve(self, token: str) -> tuple[str, list[str], bool] | None:
                return None

        middleware = BearerTokenMiddleware(
            _app, resolver=_AlwaysOnResolver(), easyauth_resolve=_resolve
        )

        scope = {
            "type": "http",
            "path": "/events",
            "method": "POST",
            "headers": [
                (_EASYAUTH_ID_HEADER.lower().encode(), FAKE_OID_MAPPED.encode())
            ],
        }
        receive = AsyncMock()
        send = AsyncMock()

        await middleware(scope, receive, send)

        state = captured_scope.get("state", {})
        assert state.get("contributor_id") == FAKE_CONTRIBUTOR
        assert state.get("is_admin") is False
        assert state.get("roles") == []
        assert state.get("is_service") is False
