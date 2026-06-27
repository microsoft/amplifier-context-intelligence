"""Tests for EntraResolver — T4: Entra JWT validation.

Two tiers:
  Mock-seam tier  — patches jwks_client + jwt.decode to drive claim dicts;
                    proves the 401/403 dispatch matrix.
  Real-crypto tier — generates an in-test RSA keypair, signs real JWTs,
                     injects a stub JWKS client; proves PyJWT actually
                     rejects expired / wrong-aud / tampered / alg=none /
                     HS256 tokens (AC5).  No network.
"""

from __future__ import annotations

import time
from typing import Any
from unittest.mock import patch

import pytest

# ---------------------------------------------------------------------------
# Fake constants — NEVER real app-reg ids / oids / tenant ids (§0.3)
# ---------------------------------------------------------------------------
FAKE_CLIENT_ID = "aaaabbbb-1111-2222-3333-ccccddddeeee"
FAKE_TENANT_ID = "ffffeeee-dddd-cccc-bbbb-aaaa99998888"
FAKE_OID_1 = "11111111-2222-3333-4444-555566667777"
FAKE_OID_2 = "22222222-3333-4444-5555-666677778888"
FAKE_CONTRIBUTOR = "colombod"
FAKE_ISSUER = f"https://login.microsoftonline.com/{FAKE_TENANT_ID}/v2.0"
FAKE_IDENTITY_MAP = {FAKE_OID_1.lower(): FAKE_CONTRIBUTOR}


# ---------------------------------------------------------------------------
# Helpers — stub JWKS client (used in both tiers)
# ---------------------------------------------------------------------------


class _StubSigningKey:
    """Mimics `PyJWKClient.get_signing_key_from_jwt(token).key`."""

    def __init__(self, key: Any) -> None:
        self.key = key


class _StubJWKSClient:
    """JWKS client stub: fetch_data is a no-op; always returns a fixed key."""

    def __init__(self, key: Any) -> None:
        self._key = _StubSigningKey(key)

    def fetch_data(self) -> None:  # pragma: no cover — eager-prefetch stub
        pass

    def get_signing_key_from_jwt(self, token: str) -> _StubSigningKey:
        return self._key

    def get_jwk_set(self) -> Any:
        """Return a fake JWK set with one key (non-empty) — exercises the production check."""
        _k = self._key

        class _FakeJWKSet:
            keys = [_k]

        return _FakeJWKSet()


class _FailingJWKSClient:
    """Stub that raises on fetch_data — simulates unreachable JWKS endpoint."""

    def fetch_data(self) -> None:
        raise ConnectionError("JWKS endpoint unreachable")

    def get_signing_key_from_jwt(
        self, token: str
    ) -> _StubSigningKey:  # pragma: no cover
        raise ConnectionError("JWKS endpoint unreachable")


class _EmptyJWKSClient:
    """Stub: fetch_data() succeeds but JWKS has zero signing keys.

    Simulates an endpoint that is reachable but returns {"keys": []}.
    The EntraResolver must detect this at construction (fail-closed).
    """

    def fetch_data(self) -> None:
        pass  # Succeeds — simulates a reachable but empty JWKS endpoint

    def get_signing_key_from_jwt(self, token: str) -> Any:  # pragma: no cover
        raise RuntimeError("No signing keys available")

    def get_jwk_set(self) -> Any:
        class _FakeJWKSet:
            keys: list[Any] = []

        return _FakeJWKSet()


# ---------------------------------------------------------------------------
# Fixture: resolver with a mock JWKS client (mock-seam tier)
# ---------------------------------------------------------------------------


def _make_resolver(
    identity_map: dict[str, str] | None = None,
    jwks_key: Any = "dummy-key",
) -> "Any":
    """Build an EntraResolver with a stub JWKS client (no network)."""
    from context_intelligence_server.auth import EntraResolver

    return EntraResolver(
        client_id=FAKE_CLIENT_ID,
        tenant_id=FAKE_TENANT_ID,
        identity_map=identity_map if identity_map is not None else FAKE_IDENTITY_MAP,
        jwks_client=_StubJWKSClient(jwks_key),
    )


# ---------------------------------------------------------------------------
# Helper to build a minimal valid claims dict
# ---------------------------------------------------------------------------


def _valid_claims(
    oid: str = FAKE_OID_1,
    tid: str = FAKE_TENANT_ID,
    scp: str = "access_as_user",
    aud: str | list[str] | None = None,
    iss: str | None = None,
) -> dict[str, Any]:
    now = int(time.time())
    return {
        "oid": oid,
        "tid": tid,
        "scp": scp,
        "aud": aud if aud is not None else FAKE_CLIENT_ID,
        "iss": iss if iss is not None else FAKE_ISSUER,
        "exp": now + 3600,
        "iat": now - 10,
    }


# ===========================================================================
# MOCK-SEAM TIER
# Maps to: AC2, AC3, AC4, AC12 (oid missing → 401 not 500)
# ===========================================================================


class TestEntraResolverMockSeam:
    """Mock-seam tests: patch jwt.decode to drive claims; prove 401/403 matrix.

    Gate coverage:
      AC2  — valid token + mapped oid → contributor
      AC3  — wrong tid / missing scp / expired → 401
      AC4  — valid token, unmapped oid → 403
      AC12 — missing oid → 401 (not 500, not 403)
    """

    def _resolve_with_claims(
        self,
        claims: dict[str, Any],
        identity_map: dict[str, str] | None = None,
    ) -> str:
        """Patch jwt.decode → return claims; call resolver.resolve('dummy')."""
        resolver = _make_resolver(identity_map=identity_map)
        with patch("context_intelligence_server.auth.jwt") as mock_jwt:
            mock_jwt.decode.return_value = claims
            mock_jwt.PyJWTError = __import__("jwt", fromlist=["PyJWTError"]).PyJWTError
            return resolver.resolve("dummy-token")

    def _resolve_raises_jwt_error(self, exc: Exception) -> "Any":
        """Patch jwt.decode to raise; return the AuthError raised by resolve()."""
        import pytest

        from context_intelligence_server.auth import AuthError

        resolver = _make_resolver()
        with patch("context_intelligence_server.auth.jwt") as mock_jwt:
            import jwt as real_jwt

            mock_jwt.PyJWTError = real_jwt.PyJWTError
            mock_jwt.decode.side_effect = exc
            with pytest.raises(AuthError) as exc_info:
                resolver.resolve("dummy-token")
        return exc_info.value

    # -- AC2: happy path ----------------------------------------------------

    def test_valid_claims_returns_contributor(self) -> None:
        """AC2: valid claims with mapped oid returns the contributor id."""
        result = self._resolve_with_claims(_valid_claims())
        assert result == FAKE_CONTRIBUTOR

    def test_oid_lookup_is_case_insensitive(self) -> None:
        """AC12: oid claim in UPPERCASE maps to lowercase key in identity_map."""
        result = self._resolve_with_claims(
            _valid_claims(oid=FAKE_OID_1.upper()),
            identity_map={FAKE_OID_1.lower(): FAKE_CONTRIBUTOR},
        )
        assert result == FAKE_CONTRIBUTOR

    # -- AC4: unmapped oid → 403 -------------------------------------------

    def test_unmapped_oid_raises_auth_error_403(self) -> None:
        """AC4: valid token whose oid is not in the identity_map → AuthError(403)."""
        from context_intelligence_server.auth import AuthError

        resolver = _make_resolver()
        with patch("context_intelligence_server.auth.jwt") as mock_jwt:
            import jwt as real_jwt

            mock_jwt.PyJWTError = real_jwt.PyJWTError
            mock_jwt.decode.return_value = _valid_claims(oid=FAKE_OID_2)
            with pytest.raises(AuthError) as exc_info:
                resolver.resolve("dummy-token")
        assert exc_info.value.status_code == 403

    def test_403_reason_names_the_oid(self) -> None:
        """AC3/user-advocate: 403 error reason must name the unbound oid."""
        from context_intelligence_server.auth import AuthError

        resolver = _make_resolver()
        with patch("context_intelligence_server.auth.jwt") as mock_jwt:
            import jwt as real_jwt

            mock_jwt.PyJWTError = real_jwt.PyJWTError
            mock_jwt.decode.return_value = _valid_claims(oid=FAKE_OID_2)
            with pytest.raises(AuthError) as exc_info:
                resolver.resolve("dummy-token")
        # The reason must name the oid so the operator knows what to add.
        assert FAKE_OID_2.lower() in exc_info.value.reason.lower()

    # -- AC12: missing oid → 401 (NOT 403, NOT 500) ------------------------

    def test_missing_oid_raises_auth_error_401(self) -> None:
        """AC12: oid claim absent from token → AuthError(401), never 403 or 500."""
        from context_intelligence_server.auth import AuthError

        claims = _valid_claims()
        del claims["oid"]
        resolver = _make_resolver()
        with patch("context_intelligence_server.auth.jwt") as mock_jwt:
            import jwt as real_jwt

            mock_jwt.PyJWTError = real_jwt.PyJWTError
            mock_jwt.decode.return_value = claims
            with pytest.raises(AuthError) as exc_info:
                resolver.resolve("dummy-token")
        assert exc_info.value.status_code == 401

    def test_empty_oid_raises_auth_error_401(self) -> None:
        """AC12: empty oid string is treated as missing → AuthError(401)."""
        from context_intelligence_server.auth import AuthError

        resolver = _make_resolver()
        with patch("context_intelligence_server.auth.jwt") as mock_jwt:
            import jwt as real_jwt

            mock_jwt.PyJWTError = real_jwt.PyJWTError
            mock_jwt.decode.return_value = _valid_claims(oid="")
            with pytest.raises(AuthError) as exc_info:
                resolver.resolve("dummy-token")
        assert exc_info.value.status_code == 401

    # -- AC3: wrong tid → 401 -----------------------------------------------

    def test_wrong_tid_raises_auth_error_401(self) -> None:
        """AC3: tid claim != tenant_id → AuthError(401)."""
        from context_intelligence_server.auth import AuthError

        resolver = _make_resolver()
        with patch("context_intelligence_server.auth.jwt") as mock_jwt:
            import jwt as real_jwt

            mock_jwt.PyJWTError = real_jwt.PyJWTError
            mock_jwt.decode.return_value = _valid_claims(
                tid="00000000-0000-0000-0000-999999999999"
            )
            with pytest.raises(AuthError) as exc_info:
                resolver.resolve("dummy-token")
        assert exc_info.value.status_code == 401

    # -- AC3: missing scp → 401 --------------------------------------------

    def test_missing_scp_raises_auth_error_401(self) -> None:
        """AC3: scp claim absent → AuthError(401)."""
        from context_intelligence_server.auth import AuthError

        claims = _valid_claims()
        del claims["scp"]
        resolver = _make_resolver()
        with patch("context_intelligence_server.auth.jwt") as mock_jwt:
            import jwt as real_jwt

            mock_jwt.PyJWTError = real_jwt.PyJWTError
            mock_jwt.decode.return_value = claims
            with pytest.raises(AuthError) as exc_info:
                resolver.resolve("dummy-token")
        assert exc_info.value.status_code == 401

    def test_wrong_scp_raises_auth_error_401(self) -> None:
        """AC3: scp without 'access_as_user' → AuthError(401)."""
        from context_intelligence_server.auth import AuthError

        resolver = _make_resolver()
        with patch("context_intelligence_server.auth.jwt") as mock_jwt:
            import jwt as real_jwt

            mock_jwt.PyJWTError = real_jwt.PyJWTError
            mock_jwt.decode.return_value = _valid_claims(scp="openid profile")
            with pytest.raises(AuthError) as exc_info:
                resolver.resolve("dummy-token")
        assert exc_info.value.status_code == 401

    def test_scp_with_access_as_user_among_others_succeeds(self) -> None:
        """AC2: scp containing 'access_as_user' alongside other scopes is valid."""
        result = self._resolve_with_claims(
            _valid_claims(scp="openid access_as_user profile")
        )
        assert result == FAKE_CONTRIBUTOR

    # -- AC3: jwt.ExpiredSignatureError side_effect → 401 ------------------

    def test_expired_signature_error_raises_auth_error_401(self) -> None:
        """AC3: jwt.ExpiredSignatureError from decode → AuthError(401)."""
        import jwt as real_jwt

        auth_err = self._resolve_raises_jwt_error(
            real_jwt.ExpiredSignatureError("expired")
        )
        assert auth_err.status_code == 401

    def test_invalid_signature_error_raises_auth_error_401(self) -> None:
        """AC3: jwt.InvalidSignatureError from decode → AuthError(401)."""
        import jwt as real_jwt

        auth_err = self._resolve_raises_jwt_error(
            real_jwt.InvalidSignatureError("bad sig")
        )
        assert auth_err.status_code == 401

    def test_invalid_audience_error_raises_auth_error_401(self) -> None:
        """AC3: jwt.InvalidAudienceError from decode → AuthError(401)."""
        import jwt as real_jwt

        auth_err = self._resolve_raises_jwt_error(
            real_jwt.InvalidAudienceError("bad aud")
        )
        assert auth_err.status_code == 401


# ===========================================================================
# REAL-CRYPTO TIER
# Uses in-test RSA keypair + real jwt.decode — proves library actually rejects.
# Maps to: AC2 (valid), AC3 (expired/wrong-aud/tampered/nbf), AC5 (alg=none/HS256)
# + dual-aud (§9 Q-AUD)
# ===========================================================================


@pytest.fixture(scope="module")
def rsa_keypair():
    """Generate a 2048-bit RSA keypair for real-crypto tests (module scope = once)."""
    from cryptography.hazmat.primitives.asymmetric import rsa

    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public_key = private_key.public_key()
    return private_key, public_key


@pytest.fixture(scope="module")
def rsa_keypair_2():
    """Second RSA keypair for tampered-signature tests."""
    from cryptography.hazmat.primitives.asymmetric import rsa

    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return private_key


def _sign_jwt(private_key: Any, claims: dict[str, Any]) -> str:
    """Sign a JWT with the given RSA private key (RS256)."""
    import jwt as pyjwt

    return pyjwt.encode(claims, private_key, algorithm="RS256")


def _make_real_crypto_resolver(
    public_key: Any,
    identity_map: dict[str, str] | None = None,
) -> "Any":
    """Build EntraResolver with a stub JWKS client backed by an in-test RSA key."""
    from context_intelligence_server.auth import EntraResolver

    return EntraResolver(
        client_id=FAKE_CLIENT_ID,
        tenant_id=FAKE_TENANT_ID,
        identity_map=identity_map if identity_map is not None else FAKE_IDENTITY_MAP,
        jwks_client=_StubJWKSClient(public_key),
    )


def _valid_real_claims(
    oid: str = FAKE_OID_1,
    aud: str | list[str] | None = None,
    extra: dict | None = None,
) -> dict[str, Any]:
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


class TestEntraResolverRealCrypto:
    """Real-crypto tier: no mocking of jwt.decode.

    Generates an RSA keypair in-test, signs real JWTs, injects a stub JWKS
    client that returns the public key.  Proves PyJWT's OWN enforcement fires.
    """

    # -- AC2: valid token → contributor ------------------------------------

    def test_valid_token_returns_contributor(
        self, rsa_keypair: tuple[Any, Any]
    ) -> None:
        """AC2 (real-crypto): valid RS256 JWT + mapped oid → contributor id."""
        private_key, public_key = rsa_keypair
        resolver = _make_real_crypto_resolver(public_key)
        token = _sign_jwt(private_key, _valid_real_claims())
        result = resolver.resolve(token)
        assert result == FAKE_CONTRIBUTOR

    # -- Dual-aud: bare GUID AND api:// both succeed (§9 Q-AUD) -----------

    def test_dual_aud_bare_guid_succeeds(self, rsa_keypair: tuple[Any, Any]) -> None:
        """Q-AUD: token with aud=<bare GUID> is accepted."""
        private_key, public_key = rsa_keypair
        resolver = _make_real_crypto_resolver(public_key)
        token = _sign_jwt(private_key, _valid_real_claims(aud=FAKE_CLIENT_ID))
        assert resolver.resolve(token) == FAKE_CONTRIBUTOR

    def test_dual_aud_api_prefix_succeeds(self, rsa_keypair: tuple[Any, Any]) -> None:
        """Q-AUD: token with aud=api://<client_id> is accepted."""
        private_key, public_key = rsa_keypair
        resolver = _make_real_crypto_resolver(public_key)
        token = _sign_jwt(
            private_key, _valid_real_claims(aud=f"api://{FAKE_CLIENT_ID}")
        )
        assert resolver.resolve(token) == FAKE_CONTRIBUTOR

    # -- AC3: expired token → 401 -----------------------------------------

    def test_expired_token_raises_auth_error_401(
        self, rsa_keypair: tuple[Any, Any]
    ) -> None:
        """AC3 (real-crypto): expired JWT (exp in the past) → AuthError(401)."""
        from context_intelligence_server.auth import AuthError

        private_key, public_key = rsa_keypair
        resolver = _make_real_crypto_resolver(public_key)
        now = int(time.time())
        claims = _valid_real_claims(extra={"exp": now - 3600, "iat": now - 7200})
        token = _sign_jwt(private_key, claims)
        with pytest.raises(AuthError) as exc_info:
            resolver.resolve(token)
        assert exc_info.value.status_code == 401

    # -- AC3: nbf in the future → 401 ------------------------------------

    def test_nbf_in_future_raises_auth_error_401(
        self, rsa_keypair: tuple[Any, Any]
    ) -> None:
        """AC3 (real-crypto): nbf > now (token not yet valid) → AuthError(401)."""
        from context_intelligence_server.auth import AuthError

        private_key, public_key = rsa_keypair
        resolver = _make_real_crypto_resolver(public_key)
        now = int(time.time())
        claims = _valid_real_claims(extra={"nbf": now + 600})
        token = _sign_jwt(private_key, claims)
        with pytest.raises(AuthError) as exc_info:
            resolver.resolve(token)
        assert exc_info.value.status_code == 401

    # -- AC3: wrong audience → 401 ----------------------------------------

    def test_wrong_aud_raises_auth_error_401(
        self, rsa_keypair: tuple[Any, Any]
    ) -> None:
        """AC3 (real-crypto): token with unrecognized audience → AuthError(401)."""
        from context_intelligence_server.auth import AuthError

        private_key, public_key = rsa_keypair
        resolver = _make_real_crypto_resolver(public_key)
        token = _sign_jwt(private_key, _valid_real_claims(aud="some-other-app"))
        with pytest.raises(AuthError) as exc_info:
            resolver.resolve(token)
        assert exc_info.value.status_code == 401

    # -- AC3: tampered signature → 401 ------------------------------------

    def test_tampered_signature_raises_auth_error_401(
        self, rsa_keypair: tuple[Any, Any], rsa_keypair_2: Any
    ) -> None:
        """AC3 (real-crypto): token signed with a DIFFERENT key → AuthError(401).

        The stub JWKS client returns key1 (public key), but the token is signed
        with key2 → signature verification fails.
        """
        from context_intelligence_server.auth import AuthError

        _private_key_1, public_key_1 = rsa_keypair
        private_key_2 = rsa_keypair_2
        resolver = _make_real_crypto_resolver(public_key_1)
        token = _sign_jwt(private_key_2, _valid_real_claims())
        with pytest.raises(AuthError) as exc_info:
            resolver.resolve(token)
        assert exc_info.value.status_code == 401

    # -- AC5: alg=none → 401 (H1 RS256 pin) ------------------------------

    def test_alg_none_token_raises_auth_error_401(
        self, rsa_keypair: tuple[Any, Any]
    ) -> None:
        """AC5/H1: alg=none token → AuthError(401). Proves RS256 pin works."""
        import base64
        import json

        from context_intelligence_server.auth import AuthError

        _private_key, public_key = rsa_keypair
        resolver = _make_real_crypto_resolver(public_key)

        # Build an alg=none token manually (PyJWT refuses to encode alg=none
        # in strict mode, so we craft the three-part structure directly).
        header = (
            base64.urlsafe_b64encode(json.dumps({"alg": "none", "typ": "JWT"}).encode())
            .rstrip(b"=")
            .decode()
        )
        payload = (
            base64.urlsafe_b64encode(json.dumps(_valid_real_claims()).encode())
            .rstrip(b"=")
            .decode()
        )
        alg_none_token = f"{header}.{payload}."

        with pytest.raises(AuthError) as exc_info:
            resolver.resolve(alg_none_token)
        assert exc_info.value.status_code == 401

    # -- AC5: HS256 confusion → 401 (H1 RS256 pin) -----------------------

    def test_hs256_token_raises_auth_error_401(
        self, rsa_keypair: tuple[Any, Any]
    ) -> None:
        """AC5/H1: HS256-signed token → AuthError(401). RS256 pin rejects it."""
        import jwt as pyjwt

        from context_intelligence_server.auth import AuthError

        _private_key, public_key = rsa_keypair
        resolver = _make_real_crypto_resolver(public_key)
        token = pyjwt.encode(_valid_real_claims(), "hs256-secret", algorithm="HS256")
        with pytest.raises(AuthError) as exc_info:
            resolver.resolve(token)
        assert exc_info.value.status_code == 401


# ===========================================================================
# EAGER JWKS PREFETCH — fail-closed at construction (§8b crusty gate / AC6)
# ===========================================================================


class TestEntraResolverEagerPrefetch:
    """EntraResolver raises at construction if JWKS prefetch fails (H3 / §8b)."""

    def test_failing_jwks_prefetch_raises_at_construction(self) -> None:
        """AC6/§8b: JWKS prefetch failure → raises at construction (fail-closed)."""
        from context_intelligence_server.auth import EntraResolver

        with pytest.raises(Exception, match="JWKS"):
            EntraResolver(
                client_id=FAKE_CLIENT_ID,
                tenant_id=FAKE_TENANT_ID,
                identity_map=FAKE_IDENTITY_MAP,
                jwks_client=_FailingJWKSClient(),
            )


# ===========================================================================
# MIDDLEWARE INTEGRATION — AuthError → correct HTTP status code
# Maps to: §3 (BearerTokenMiddleware catches AuthError → its status_code)
# Existing test_auth.py stays GREEN (StaticKeyResolver returning None → 401)
# ===========================================================================


class TestMiddlewareAuthError:
    """BearerTokenMiddleware dispatches AuthError.status_code to the response.

    §3: catch AuthError → respond with its status_code.
        resolver returning None still → 401 (StaticKeyResolver path unchanged).
    """

    async def _call_middleware(
        self, resolver: Any, token: str | None = "bearer-token"
    ) -> int:
        """Run the middleware and return the HTTP status code sent."""
        from unittest.mock import AsyncMock

        from context_intelligence_server.auth import BearerTokenMiddleware

        app = AsyncMock()
        received_status: list[int] = []

        async def capture_send(event: dict) -> None:
            if event.get("type") == "http.response.start":
                received_status.append(event["status"])

        headers = [(b"authorization", f"Bearer {token}".encode())] if token else []
        scope = {
            "type": "http",
            "path": "/events",
            "method": "POST",
            "headers": headers,
        }
        receive = AsyncMock()
        middleware = BearerTokenMiddleware(app, resolver=resolver)
        await middleware(scope, receive, capture_send)
        return received_status[0] if received_status else 200

    async def test_auth_error_401_returns_http_401(self) -> None:
        """Middleware: AuthError(401) from resolver → HTTP 401."""

        resolver = _make_resolver()
        with patch("context_intelligence_server.auth.jwt") as mock_jwt:
            import jwt as real_jwt

            mock_jwt.PyJWTError = real_jwt.PyJWTError
            mock_jwt.decode.side_effect = real_jwt.ExpiredSignatureError("expired")
            status = await self._call_middleware(resolver)
        assert status == 401

    async def test_auth_error_403_returns_http_403(self) -> None:
        """Middleware: AuthError(403) from resolver → HTTP 403."""

        resolver = _make_resolver()
        with patch("context_intelligence_server.auth.jwt") as mock_jwt:
            import jwt as real_jwt

            mock_jwt.PyJWTError = real_jwt.PyJWTError
            # Return claims with an oid not in the identity_map → 403
            mock_jwt.decode.return_value = _valid_claims(oid=FAKE_OID_2)
            status = await self._call_middleware(resolver)
        assert status == 403

    async def test_resolver_returning_none_still_401(self) -> None:
        """§3 backward-compat: resolver returning None → HTTP 401 (StaticKeyResolver path)."""
        from context_intelligence_server.auth import StaticKeyResolver

        resolver = StaticKeyResolver({"some-digest": "alice"})
        # No Authorization header → token is None → StaticKeyResolver.resolve never called
        # → middleware sends 401 before resolve()
        status = await self._call_middleware(resolver, token=None)
        assert status == 401

    async def test_static_resolver_wrong_token_still_401(self) -> None:
        """§3 backward-compat: StaticKeyResolver returning None → HTTP 401."""
        from context_intelligence_server.auth import StaticKeyResolver

        resolver = StaticKeyResolver({"some-digest": "alice"})
        # "wrong-token" won't match "some-digest"
        status = await self._call_middleware(resolver, token="wrong-token")
        assert status == 401

    async def test_unexpected_resolver_exception_returns_401(self) -> None:
        """Middleware catch-all: non-AuthError exception from resolver → HTTP 401 (fail-closed).

        Before fix: RuntimeError propagates out of the middleware (no catch-all) → unhandled 500.
        After fix:  middleware catches it, logs at ERROR, responds 401.
        """

        class _BrokenResolver:
            def resolve(self, token: str) -> str | None:
                raise RuntimeError("unexpected internal error — simulated bug")

        status = await self._call_middleware(_BrokenResolver())
        assert status == 401


# ===========================================================================
# BUG REPROS — FAIL-1, FAIL-2, FAIL-3 (tester-breaker crashes)
# Each test MUST be RED (crashes/wrong status) before the auth.py fix
# and GREEN (AuthError 401) after.
# ===========================================================================


class TestEntraResolverBugRepros:
    """Direct repros of the three tester-breaker crash bugs in EntraResolver.resolve().

    All three use the mock-seam: jwt.decode is patched to return the malformed
    claims dict without network or crypto overhead.
    """

    def _resolve_with_claims(
        self,
        claims: dict[str, Any],
        identity_map: dict[str, str] | None = None,
    ) -> str:
        """Patch jwt.decode → claims; call resolver.resolve('dummy')."""
        resolver = _make_resolver(identity_map=identity_map)
        with patch("context_intelligence_server.auth.jwt") as mock_jwt:
            import jwt as real_jwt

            mock_jwt.PyJWTError = real_jwt.PyJWTError
            mock_jwt.decode.return_value = claims
            return resolver.resolve("dummy-token")

    def test_fail1_oid_int_raises_auth_error_401_not_attribute_error(self) -> None:
        """FAIL-1 repro: oid=42 (int) → AuthError(401), NOT AttributeError → 500.

        Before fix: truthy int passes ``if not oid`` guard, then ``oid.lower()``
        raises AttributeError — escapes as unhandled 500.
        After fix:  isinstance guard catches it → AuthError(401).
        """
        from context_intelligence_server.auth import AuthError

        claims = _valid_claims()
        claims["oid"] = 42  # truthy int — exercises the isinstance guard
        with pytest.raises(AuthError) as exc_info:
            self._resolve_with_claims(claims)
        assert exc_info.value.status_code == 401

    def test_fail1_oid_list_raises_auth_error_401_not_attribute_error(self) -> None:
        """FAIL-1 variant: oid=[...] (list) → AuthError(401), NOT AttributeError."""
        from context_intelligence_server.auth import AuthError

        claims = _valid_claims()
        claims["oid"] = ["11111111-2222-3333-4444-555566667777"]  # truthy list
        with pytest.raises(AuthError) as exc_info:
            self._resolve_with_claims(claims)
        assert exc_info.value.status_code == 401

    def test_fail2_scp_list_raises_auth_error_401_not_attribute_error(self) -> None:
        """FAIL-2 repro: scp=["access_as_user"] (list) → AuthError(401), NOT AttributeError.

        Before fix: truthy list passes ``or ""`` fallback, then ``scp.split()``
        raises AttributeError — escapes as unhandled 500.
        After fix:  isinstance guard coerces to "" → missing scope → AuthError(401).
        """
        from context_intelligence_server.auth import AuthError

        claims = _valid_claims()
        claims["scp"] = [
            "access_as_user"
        ]  # truthy list — exercises the isinstance guard
        with pytest.raises(AuthError) as exc_info:
            self._resolve_with_claims(claims)
        assert exc_info.value.status_code == 401

    def test_fail3_whitespace_oid_raises_auth_error_401_not_403(self) -> None:
        """FAIL-3 repro: oid='   ' (whitespace-only) → AuthError(401), NOT 403.

        Before fix: ``if not oid`` is False for a non-empty string, so ``oid.lower()``
        runs, yielding '   ' → identity_map miss → 403 (wrong: invalid claim,
        not unbound identity).
        After fix:  ``not oid.strip()`` catches whitespace → AuthError(401).
        """
        from context_intelligence_server.auth import AuthError

        with pytest.raises(AuthError) as exc_info:
            self._resolve_with_claims(_valid_claims(oid="   "))
        assert exc_info.value.status_code == 401

    def test_fail3_tab_oid_raises_auth_error_401(self) -> None:
        """FAIL-3 variant: oid='\\t' (tab-only whitespace) → AuthError(401)."""
        from context_intelligence_server.auth import AuthError

        with pytest.raises(AuthError) as exc_info:
            self._resolve_with_claims(_valid_claims(oid="\t"))
        assert exc_info.value.status_code == 401


# ===========================================================================
# COVERAGE GAP TESTS — scp substring trap, app-only tokens
# ===========================================================================


class TestEntraResolverScpCoverage:
    """scp field edge cases: substring trap and app-only tokens."""

    def _resolve_with_claims(self, claims: dict[str, Any]) -> None:
        """Patch jwt.decode → claims; call resolve(); propagate any exception."""
        resolver = _make_resolver()
        with patch("context_intelligence_server.auth.jwt") as mock_jwt:
            import jwt as real_jwt

            mock_jwt.PyJWTError = real_jwt.PyJWTError
            mock_jwt.decode.return_value = claims
            resolver.resolve("dummy-token")

    def test_scp_access_as_user_admin_suffix_is_rejected(self) -> None:
        """scp='access_as_user_admin' → 401 (split-membership check, not substring)."""
        from context_intelligence_server.auth import AuthError

        with pytest.raises(AuthError) as exc_info:
            self._resolve_with_claims(_valid_claims(scp="access_as_user_admin"))
        assert exc_info.value.status_code == 401

    def test_scp_xaccess_as_user_prefix_is_rejected(self) -> None:
        """scp='xaccess_as_user' → 401 (prefix trap rejected by split-membership check)."""
        from context_intelligence_server.auth import AuthError

        with pytest.raises(AuthError) as exc_info:
            self._resolve_with_claims(_valid_claims(scp="xaccess_as_user"))
        assert exc_info.value.status_code == 401

    def test_app_only_token_with_roles_no_scp_is_rejected(self) -> None:
        """App-only token: 'roles' present but no 'scp' → 401 (delegated flow required in V1)."""
        from context_intelligence_server.auth import AuthError

        claims = _valid_claims()
        del claims["scp"]
        claims["roles"] = ["Directory.Read.All"]  # app-only token pattern
        resolver = _make_resolver()
        with patch("context_intelligence_server.auth.jwt") as mock_jwt:
            import jwt as real_jwt

            mock_jwt.PyJWTError = real_jwt.PyJWTError
            mock_jwt.decode.return_value = claims
            with pytest.raises(AuthError) as exc_info:
                resolver.resolve("dummy-token")
        assert exc_info.value.status_code == 401


# ===========================================================================
# COVERAGE GAP TESTS — alg header variants beyond 'none' (AC5/H1)
# ===========================================================================


class TestEntraResolverAlgVariants:
    """alg='None'/'NONE'/ES256/PS256 → all 401 (AC5/H1 RS256 pin)."""

    @staticmethod
    def _make_alg_token(alg: str, claims: dict[str, Any]) -> str:
        """Craft a JWT-shaped token with given alg header and a junk signature."""
        import base64
        import json

        header = (
            base64.urlsafe_b64encode(json.dumps({"alg": alg, "typ": "JWT"}).encode())
            .rstrip(b"=")
            .decode()
        )
        payload = (
            base64.urlsafe_b64encode(json.dumps(claims).encode()).rstrip(b"=").decode()
        )
        return f"{header}.{payload}.AAAA"

    def test_alg_None_titlecase_raises_auth_error_401(
        self, rsa_keypair: tuple[Any, Any]
    ) -> None:
        """alg='None' (title-case) → AuthError(401). RS256 pin rejects all non-RS256 algs."""
        from context_intelligence_server.auth import AuthError

        _priv, pub = rsa_keypair
        resolver = _make_real_crypto_resolver(pub)
        token = self._make_alg_token("None", _valid_real_claims())
        with pytest.raises(AuthError) as exc_info:
            resolver.resolve(token)
        assert exc_info.value.status_code == 401

    def test_alg_NONE_uppercase_raises_auth_error_401(
        self, rsa_keypair: tuple[Any, Any]
    ) -> None:
        """alg='NONE' (upper-case) → AuthError(401). RS256 pin rejects all non-RS256 algs."""
        from context_intelligence_server.auth import AuthError

        _priv, pub = rsa_keypair
        resolver = _make_real_crypto_resolver(pub)
        token = self._make_alg_token("NONE", _valid_real_claims())
        with pytest.raises(AuthError) as exc_info:
            resolver.resolve(token)
        assert exc_info.value.status_code == 401

    def test_alg_ES256_raises_auth_error_401(
        self, rsa_keypair: tuple[Any, Any]
    ) -> None:
        """alg='ES256' → AuthError(401). RS256 pin rejects EC algorithms."""
        from context_intelligence_server.auth import AuthError

        _priv, pub = rsa_keypair
        resolver = _make_real_crypto_resolver(pub)
        token = self._make_alg_token("ES256", _valid_real_claims())
        with pytest.raises(AuthError) as exc_info:
            resolver.resolve(token)
        assert exc_info.value.status_code == 401

    def test_alg_PS256_raises_auth_error_401(
        self, rsa_keypair: tuple[Any, Any]
    ) -> None:
        """alg='PS256' → AuthError(401). RS256 pin rejects RSA-PSS variants."""
        from context_intelligence_server.auth import AuthError

        _priv, pub = rsa_keypair
        resolver = _make_real_crypto_resolver(pub)
        token = self._make_alg_token("PS256", _valid_real_claims())
        with pytest.raises(AuthError) as exc_info:
            resolver.resolve(token)
        assert exc_info.value.status_code == 401


# ===========================================================================
# COVERAGE GAP TESTS — malformed / garbage token strings
# ===========================================================================


class TestEntraResolverBadTokens:
    """Malformed / garbage token strings → AuthError(401), never AttributeError/500."""

    def test_empty_bearer_raises_auth_error_401(
        self, rsa_keypair: tuple[Any, Any]
    ) -> None:
        """resolve('') → AuthError(401), not 500."""
        from context_intelligence_server.auth import AuthError

        _priv, pub = rsa_keypair
        resolver = _make_real_crypto_resolver(pub)
        with pytest.raises(AuthError) as exc_info:
            resolver.resolve("")
        assert exc_info.value.status_code == 401

    def test_garbage_token_raises_auth_error_401(
        self, rsa_keypair: tuple[Any, Any]
    ) -> None:
        """resolve('not-a-jwt-at-all') → AuthError(401), not 500."""
        from context_intelligence_server.auth import AuthError

        _priv, pub = rsa_keypair
        resolver = _make_real_crypto_resolver(pub)
        with pytest.raises(AuthError) as exc_info:
            resolver.resolve("not-a-jwt-at-all")
        assert exc_info.value.status_code == 401

    def test_two_segment_token_raises_auth_error_401(
        self, rsa_keypair: tuple[Any, Any]
    ) -> None:
        """resolve('header.payload') (missing sig segment) → AuthError(401), not 500."""
        import base64
        import json

        from context_intelligence_server.auth import AuthError

        header = (
            base64.urlsafe_b64encode(
                json.dumps({"alg": "RS256", "typ": "JWT"}).encode()
            )
            .rstrip(b"=")
            .decode()
        )
        payload = (
            base64.urlsafe_b64encode(json.dumps(_valid_real_claims()).encode())
            .rstrip(b"=")
            .decode()
        )
        two_seg = f"{header}.{payload}"  # missing third segment
        _priv, pub = rsa_keypair
        resolver = _make_real_crypto_resolver(pub)
        with pytest.raises(AuthError) as exc_info:
            resolver.resolve(two_seg)
        assert exc_info.value.status_code == 401


# ===========================================================================
# COVERAGE GAP TESTS — aud as array (§9 Q-AUD)
# ===========================================================================


class TestEntraResolverAudArray:
    """aud claim as an array (multi-audience token) — success and rejection paths."""

    def test_aud_array_containing_client_id_succeeds(
        self, rsa_keypair: tuple[Any, Any]
    ) -> None:
        """aud=['other-app', client_id] → succeeds (client_id is in the audience list)."""
        private_key, public_key = rsa_keypair
        resolver = _make_real_crypto_resolver(public_key)
        claims = _valid_real_claims(aud=["other-app", FAKE_CLIENT_ID])
        token = _sign_jwt(private_key, claims)
        assert resolver.resolve(token) == FAKE_CONTRIBUTOR

    def test_aud_array_without_matching_audience_raises_401(
        self, rsa_keypair: tuple[Any, Any]
    ) -> None:
        """aud=['other-app', 'another-app'] → AuthError(401) (no element matches expected_aud)."""
        from context_intelligence_server.auth import AuthError

        private_key, public_key = rsa_keypair
        resolver = _make_real_crypto_resolver(public_key)
        claims = _valid_real_claims(aud=["other-app", "another-app"])
        token = _sign_jwt(private_key, claims)
        with pytest.raises(AuthError) as exc_info:
            resolver.resolve(token)
        assert exc_info.value.status_code == 401


# ===========================================================================
# EMPTY JWKS AT CONSTRUCTION (harden fail-closed prefetch)
# ===========================================================================


class TestEntraResolverEmptyJWKS:
    """EntraResolver raises at construction when JWKS fetch succeeds but has zero keys."""

    def test_empty_jwks_raises_runtime_error_at_construction(self) -> None:
        """Empty JWKS ({'keys': []}) → RuntimeError at construction (fail-closed).

        Before fix: construction succeeds; every resolve() call then 401s lazily.
        After fix:  construction raises immediately so the server refuses to start.
        """
        from context_intelligence_server.auth import EntraResolver

        with pytest.raises(RuntimeError, match="zero signing keys"):
            EntraResolver(
                client_id=FAKE_CLIENT_ID,
                tenant_id=FAKE_TENANT_ID,
                identity_map=FAKE_IDENTITY_MAP,
                jwks_client=_EmptyJWKSClient(),
            )
