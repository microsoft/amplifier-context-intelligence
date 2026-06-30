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
        """Patch jwt.decode → return claims; call resolver.resolve('dummy').

        T5 protocol change: resolve() now returns (contributor_id, roles).
        This helper returns contributor_id only (the original contract).
        """
        resolver = _make_resolver(identity_map=identity_map)
        with patch("context_intelligence_server.auth.jwt") as mock_jwt:
            mock_jwt.decode.return_value = claims
            mock_jwt.PyJWTError = __import__("jwt", fromlist=["PyJWTError"]).PyJWTError
            result = resolver.resolve("dummy-token")
        contributor_id, _roles, _is_service = result  # M2: 3-tuple
        return contributor_id

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

    def test_missing_scp_raises_auth_error_403(self) -> None:
        """M2 behavior: scp claim absent → service branch → no qualifying role → AuthError(403).

        V1 behavior was AuthError(401) (missing scope, user branch).
        Phase 2 routes no-scp tokens to the service branch; with default resolver
        (all role names empty) there is no qualifying role → 403 (not 401).
        The crash/500 is still prevented; the status code changed.
        """
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
        assert exc_info.value.status_code == 403

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
        # M2 protocol change: resolve() returns (contributor_id, roles, is_service) 3-tuple.
        assert result is not None
        contributor_id, _roles, _is_service = result  # M2: 3-tuple
        assert contributor_id == FAKE_CONTRIBUTOR

    # -- Dual-aud: bare GUID AND api:// both succeed (§9 Q-AUD) -----------

    def test_dual_aud_bare_guid_succeeds(self, rsa_keypair: tuple[Any, Any]) -> None:
        """Q-AUD: token with aud=<bare GUID> is accepted."""
        private_key, public_key = rsa_keypair
        resolver = _make_real_crypto_resolver(public_key)
        token = _sign_jwt(private_key, _valid_real_claims(aud=FAKE_CLIENT_ID))
        result = resolver.resolve(token)
        assert result is not None and result[0] == FAKE_CONTRIBUTOR

    def test_dual_aud_api_prefix_succeeds(self, rsa_keypair: tuple[Any, Any]) -> None:
        """Q-AUD: token with aud=api://<client_id> is accepted."""
        private_key, public_key = rsa_keypair
        resolver = _make_real_crypto_resolver(public_key)
        token = _sign_jwt(
            private_key, _valid_real_claims(aud=f"api://{FAKE_CLIENT_ID}")
        )
        result = resolver.resolve(token)
        assert result is not None and result[0] == FAKE_CONTRIBUTOR

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
            # auth_enabled=True so the middleware proceeds to call resolve()
            @property
            def auth_enabled(self) -> bool:
                return True

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

    def test_fail2_scp_list_raises_auth_error_not_attribute_error(self) -> None:
        """FAIL-2 repro: scp=["access_as_user"] (list) → AuthError (no crash), NOT AttributeError.

        Before fix: truthy list passes ``or ""`` fallback, then ``scp.split()``
        raises AttributeError — escapes as unhandled 500.
        After V1 fix: isinstance guard coerces to "" → missing scope → AuthError(401).
        After M2: list scp → "" → no scp → service branch → no qualifying role
        (default resolver has empty role names) → AuthError(403).
        The key invariant is preserved: no crash, graceful auth rejection.
        """
        from context_intelligence_server.auth import AuthError

        claims = _valid_claims()
        claims["scp"] = [
            "access_as_user"
        ]  # truthy list — exercises the isinstance guard
        with pytest.raises(AuthError) as exc_info:
            self._resolve_with_claims(claims)
        # M2 behavior: service branch → no qualifying role → 403 (changed from 401 in V1).
        assert exc_info.value.status_code == 403

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

    def test_app_only_token_with_unrecognized_role_is_rejected(self) -> None:
        """App-only token: 'roles' present but no 'scp' → M2 service branch → 403.

        V1 behavior: 401 (delegated flow required, rejected in user branch).
        M2 behavior: no scp → service branch → role "Directory.Read.All" is not a
        configured App Role (default resolver has empty role names) → AuthError(403).
        This is the RG-unknown case: service token with an unrecognized role.
        """
        from context_intelligence_server.auth import AuthError

        claims = _valid_claims()
        del claims["scp"]
        claims["roles"] = [
            "Directory.Read.All"
        ]  # app-only token, not a configured role
        resolver = _make_resolver()
        with patch("context_intelligence_server.auth.jwt") as mock_jwt:
            import jwt as real_jwt

            mock_jwt.PyJWTError = real_jwt.PyJWTError
            mock_jwt.decode.return_value = claims
            with pytest.raises(AuthError) as exc_info:
                resolver.resolve("dummy-token")
        # M2 behavior: service branch → no qualifying configured role → 403.
        assert exc_info.value.status_code == 403


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
        result = resolver.resolve(token)
        assert result is not None and result[0] == FAKE_CONTRIBUTOR

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


# ===========================================================================
# M2 SERVICE BRANCH — §7 test matrix (resolver level)
# ===========================================================================

# Additional fake constants for service tests
FAKE_SERVICE_OID = "aaaabbbb-1111-1111-1111-ccccddddffff"
FAKE_APPID = "bbbbcccc-2222-3333-4444-eeeeffff0000"
FAKE_AZP = "ccccdddd-3333-4444-5555-ffff00001111"
FAKE_SERVICE_CONTRIBUTOR = "svc-contributor"


def _service_claims(
    oid: str = FAKE_SERVICE_OID,
    tid: str = FAKE_TENANT_ID,
    roles: list[str] | None = None,
    appid: str | None = None,
    azp: str | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return a minimal valid service-token claims dict (no scp)."""
    now = int(time.time())
    claims: dict[str, Any] = {
        "oid": oid,
        "tid": tid,
        "aud": FAKE_CLIENT_ID,
        "iss": FAKE_ISSUER,
        "exp": now + 3600,
        "iat": now - 10,
        # Deliberately NO "scp" — service / app-only token
    }
    if roles is not None:
        claims["roles"] = roles
    if appid is not None:
        claims["appid"] = appid
    if azp is not None:
        claims["azp"] = azp
    if extra:
        claims.update(extra)
    return claims


def _make_service_resolver(
    service_map: dict[str, str] | None = None,
    service_data_role: str = "Contributor",
    reader_role: str = "Reader",
    entra_admin_role: str = "IdentityAdmin",
) -> "Any":
    """Build an EntraResolver configured with M2 service roles."""
    from context_intelligence_server.auth import EntraResolver

    return EntraResolver(
        client_id=FAKE_CLIENT_ID,
        tenant_id=FAKE_TENANT_ID,
        identity_map=FAKE_IDENTITY_MAP,
        service_identity_map=service_map or {},
        service_data_role=service_data_role,
        reader_role=reader_role,
        entra_admin_role=entra_admin_role,
        jwks_client=_StubJWKSClient("dummy-key"),
    )


class TestM2ServiceBranch:
    """M2 §7 resolver-level tests — service / app-token path.

    Uses the mock-seam pattern: jwt.decode is patched to return crafted claims
    dicts.  Tests cover the full §7 matrix (B1/B2/B6/B8/B7/RG/SV/HR/ST rows).
    """

    def _resolve_service(
        self,
        claims: dict[str, Any],
        resolver: "Any" = None,
    ) -> "tuple[str, list[str], bool]":
        """Patch jwt.decode → claims; call resolve(); return 3-tuple."""
        if resolver is None:
            resolver = _make_service_resolver()
        with patch("context_intelligence_server.auth.jwt") as mock_jwt:
            import jwt as real_jwt

            mock_jwt.PyJWTError = real_jwt.PyJWTError
            mock_jwt.decode.return_value = claims
            result = resolver.resolve("dummy-token")
        assert result is not None
        return result  # type: ignore[return-value]

    def _resolve_raises(
        self,
        claims: dict[str, Any],
        resolver: "Any" = None,
    ) -> "Any":
        """Patch jwt.decode → claims; call resolve(); return raised AuthError."""
        from context_intelligence_server.auth import AuthError

        if resolver is None:
            resolver = _make_service_resolver()
        with patch("context_intelligence_server.auth.jwt") as mock_jwt:
            import jwt as real_jwt

            mock_jwt.PyJWTError = real_jwt.PyJWTError
            mock_jwt.decode.return_value = claims
            with pytest.raises(AuthError) as exc_info:
                resolver.resolve("dummy-token")
        return exc_info.value

    # -- B1: mutual exclusion anomaly ----------------------------------------

    def test_b1a_scp_and_idtyp_app_raises_401_ambiguous(self) -> None:
        """B1-a: scp="access_as_user" AND idtyp="app" → AuthError(401) "Ambiguous token..."."""
        from context_intelligence_server.auth import AuthError

        claims = _valid_claims()  # has scp="access_as_user"
        claims["idtyp"] = "app"  # add idtyp=app — anomalous combination

        resolver = _make_service_resolver()
        with patch("context_intelligence_server.auth.jwt") as mock_jwt:
            import jwt as real_jwt

            mock_jwt.PyJWTError = real_jwt.PyJWTError
            mock_jwt.decode.return_value = claims
            with pytest.raises(AuthError) as exc_info:
                resolver.resolve("dummy-token")
        err = exc_info.value
        assert err.status_code == 401
        assert "Ambiguous" in err.reason

    def test_b1b_no_scp_no_idtyp_service_admitted(self) -> None:
        """B1-b: no scp, no idtyp, roles=["Contributor"], appid → service admitted."""
        claims = _service_claims(roles=["Contributor"], appid=FAKE_APPID)
        cid, roles, is_service = self._resolve_service(claims)
        assert cid == FAKE_APPID
        assert roles == ["Contributor"]
        assert is_service is True

    # -- B2: idtyp normalization ---------------------------------------------

    def test_b2a_idtyp_mixed_case_space_service_branch(self) -> None:
        """B2-a: idtyp=" App " (mixed case+space) → normalized "app"; service branch admitted."""
        claims = _service_claims(roles=["Reader"])
        claims["idtyp"] = " App "  # mixed case + surrounding whitespace
        # has_scp=False → B1 check is False → service branch
        cid, roles, is_service = self._resolve_service(
            claims, resolver=_make_service_resolver()
        )
        assert is_service is True
        assert roles == ["Reader"]

    def test_b2b_idtyp_int_normalized_to_empty(self) -> None:
        """B2-b: idtyp=123 (int) → normalized to ""; service branch; admit/deny by roles."""
        claims = _service_claims(roles=["Contributor"])
        claims["idtyp"] = 123  # int, not str → normalized to ""
        cid, roles, is_service = self._resolve_service(claims)
        assert is_service is True
        assert roles == ["Contributor"]

    # -- B6: created_by derivation chain -------------------------------------

    def test_b6a_service_map_wins_over_appid(self) -> None:
        """B6-a: oid in service_identity_map → created_by = mapped contributor id."""
        service_map = {FAKE_SERVICE_OID.lower(): FAKE_SERVICE_CONTRIBUTOR}
        claims = _service_claims(
            oid=FAKE_SERVICE_OID,
            appid=FAKE_APPID,  # map wins over appid
            roles=["Contributor"],
        )
        cid, _, is_service = self._resolve_service(
            claims, resolver=_make_service_resolver(service_map=service_map)
        )
        assert cid == FAKE_SERVICE_CONTRIBUTOR
        assert is_service is True

    def test_b6b_blank_appid_falls_through_to_azp(self) -> None:
        """B6-b: map miss, appid blank → falls through to azp → created_by = azp."""
        claims = _service_claims(
            oid=FAKE_SERVICE_OID,
            appid="  ",  # blank — skipped by _first_nonblank
            azp=FAKE_AZP,
            roles=["Reader"],
        )
        cid, _, _ = self._resolve_service(claims)
        assert cid == FAKE_AZP

    def test_b6c_oid_last_resort(self) -> None:
        """B6-c: map miss, no appid, no azp → created_by = oid (last resort)."""
        claims = _service_claims(oid=FAKE_SERVICE_OID, roles=["Contributor"])
        # No appid, no azp in claims
        cid, _, is_service = self._resolve_service(claims)
        assert cid == FAKE_SERVICE_OID
        assert is_service is True

    # -- B8: anti-spoof — app_displayname must never be used -----------------

    def test_b8_anti_spoof_app_displayname_not_used(self) -> None:
        """B8: app_displayname="alice@contoso.com" (a human UPN) → created_by = appid, NOT display name.

        app_displayname is operator-mutable in Entra (spoofable).  It is deliberately
        excluded from the derivation chain.  A service whose app_displayname happens
        to look like a human UPN must never inherit that UPN as its created_by.
        """
        claims = _service_claims(
            oid=FAKE_SERVICE_OID,
            appid=FAKE_APPID,
            roles=["Contributor"],
        )
        claims["app_displayname"] = "alice@contoso.com"  # human-looking display name
        cid, _, _ = self._resolve_service(claims)
        assert cid == FAKE_APPID, "created_by must be appid, not app_displayname"
        assert cid != "alice@contoso.com", (
            "app_displayname must never become created_by"
        )

    # -- B7: aud / iss enforced in shared validation (no new code) -----------

    def test_b7_aud_wrong_raises_401_before_service_branch(self) -> None:
        """B7-aud: wrong aud → AuthError(401) at jwt.decode; service branch never reached."""
        from context_intelligence_server.auth import AuthError

        resolver = _make_service_resolver()
        with patch("context_intelligence_server.auth.jwt") as mock_jwt:
            import jwt as real_jwt

            mock_jwt.PyJWTError = real_jwt.PyJWTError
            mock_jwt.decode.side_effect = real_jwt.InvalidAudienceError("bad aud")
            with pytest.raises(AuthError) as exc_info:
                resolver.resolve("dummy-token")
        assert exc_info.value.status_code == 401

    def test_b7_iss_wrong_raises_401_before_service_branch(self) -> None:
        """B7-iss: wrong iss → AuthError(401) at jwt.decode; service branch never reached."""
        from context_intelligence_server.auth import AuthError

        resolver = _make_service_resolver()
        with patch("context_intelligence_server.auth.jwt") as mock_jwt:
            import jwt as real_jwt

            mock_jwt.PyJWTError = real_jwt.PyJWTError
            mock_jwt.decode.side_effect = real_jwt.InvalidIssuerError("bad iss")
            with pytest.raises(AuthError) as exc_info:
                resolver.resolve("dummy-token")
        assert exc_info.value.status_code == 401

    # -- RG: role gate -------------------------------------------------------

    def test_rg_none_no_roles_raises_403_naming_roles(self) -> None:
        """RG-none: service token, roles=[] → AuthError(403) naming Contributor/Reader + principal.

        Security R1: the raw roles list must NOT appear in the reason.
        User-advocate: the rejected principal identifier (appid or oid) must appear.
        _service_claims(roles=[]) has no appid → oid (FAKE_SERVICE_OID) is used.
        """
        err = self._resolve_raises(_service_claims(roles=[]))
        assert err.status_code == 403
        assert "Contributor" in err.reason
        assert "Reader" in err.reason
        # Principal identifier (oid as fallback since no appid in this call).
        assert FAKE_SERVICE_OID in err.reason, (
            f"403 reason must name the rejected principal oid {FAKE_SERVICE_OID!r}: "
            f"{err.reason!r}"
        )
        # Raw roles list must NOT be echoed (security R1).
        assert "roles=" not in err.reason, (
            f"403 reason must NOT echo the raw roles claim: {err.reason!r}"
        )

    def test_rg_unknown_unrecognized_role_raises_403(self) -> None:
        """RG-unknown: service token, roles=["SomethingElse"] → AuthError(403)."""
        err = self._resolve_raises(_service_claims(roles=["SomethingElse"]))
        assert err.status_code == 403

    def test_rg_reader_admitted(self) -> None:
        """RG-reader: service token, roles=["Reader"] → admitted (…, ["Reader"], True)."""
        claims = _service_claims(roles=["Reader"], appid=FAKE_APPID)
        cid, roles, is_service = self._resolve_service(claims)
        assert is_service is True
        assert "Reader" in roles

    def test_rg_disabled_empty_reader_role_raises_403(self) -> None:
        """RG-disabled: reader_role="" disables that role → AuthError(403) for a Reader token."""
        claims = _service_claims(roles=["Reader"], appid=FAKE_APPID)
        # Build resolver with reader_role disabled
        resolver = _make_service_resolver(
            reader_role="", service_data_role="Contributor"
        )
        err = self._resolve_raises(claims, resolver=resolver)
        assert err.status_code == 403

    # -- SV-ADM: service IdentityAdmin path ----------------------------------

    def test_sv_adm_identity_admin_admitted(self) -> None:
        """SV-ADM: service token with IdentityAdmin role admitted; (…, ["IdentityAdmin"], True)."""
        claims = _service_claims(roles=["IdentityAdmin"], appid=FAKE_APPID)
        cid, roles, is_service = self._resolve_service(claims)
        assert is_service is True
        assert "IdentityAdmin" in roles

    # -- HR: human regression (delegated path unchanged) ----------------------

    def test_hr1_human_unmapped_oid_raises_403(self) -> None:
        """HR1: human token, oid NOT in identity_map → AuthError(403) — unchanged from V1."""
        err = self._resolve_raises(_valid_claims(oid=FAKE_OID_2))
        assert err.status_code == 403
        assert FAKE_OID_2.lower() in err.reason.lower()

    def test_hr2_human_happy_path_returns_false_is_service(self) -> None:
        """HR2: human token, mapped oid, roles=["x"] → (contributor, ["x"], False)."""
        claims = _valid_claims(oid=FAKE_OID_1)
        claims["roles"] = ["x"]
        cid, roles, is_service = self._resolve_service(claims)
        assert cid == FAKE_CONTRIBUTOR
        assert roles == ["x"]
        assert is_service is False  # human path: is_service=False

    def test_hr3_human_bad_scp_stays_in_user_branch_401(self) -> None:
        """HR3: scp="other" (present, not access_as_user) → stays in user branch → AuthError(401)."""
        err = self._resolve_raises(_valid_claims(scp="other"))
        assert err.status_code == 401

    # -- ST1: StaticKeyResolver returns is_service=False ---------------------

    def test_st1_static_resolver_returns_3tuple_is_service_false(self) -> None:
        """ST1: StaticKeyResolver returns (contributor_id, [], False); miss → None."""
        import hashlib

        from context_intelligence_server.auth import StaticKeyResolver

        token = "test-static-token"
        digest = hashlib.sha256(token.encode()).hexdigest()
        resolver = StaticKeyResolver({digest: "static-contributor"})

        result = resolver.resolve(token)
        assert result is not None
        cid, roles, is_service = result
        assert cid == "static-contributor"
        assert roles == []
        assert is_service is False  # ST1: static tokens always write-capable

        # Miss → None (backward-compat)
        assert resolver.resolve("wrong-token") is None


# ===========================================================================
# M2 SERVICE BRANCH — EDGE CASE TESTS (§4 of security fix)
# ===========================================================================


class TestM2ServiceBranchEdgeCases:
    """Four targeted edge-case tests for the service (app-token) branch.

    These tests use the mock-seam pattern (jwt.decode patched) for speed
    and to exercise specific claim combinations.

    B1-collision — runtime oid collision: no-scp SERVICE token whose oid
                   also exists in the human identity_map → service branch wins.
    foreign-tid  — SERVICE token with wrong tid → AuthError(401).
    roles-case   — roles=["contributor"] (wrong case) vs "Contributor" → 403
                   (membership is case-sensitive; pin so a future lowercasing
                   helper cannot silently leak privilege).
    roles-nonlist — roles as a bare string → normalized to [] → 403.
    """

    def _resolve_service(
        self,
        claims: dict[str, Any],
        resolver: "Any" = None,
    ) -> "tuple[str, list[str], bool]":
        """Patch jwt.decode → claims; call resolve(); return 3-tuple."""
        if resolver is None:
            resolver = _make_service_resolver()
        with patch("context_intelligence_server.auth.jwt") as mock_jwt:
            import jwt as real_jwt

            mock_jwt.PyJWTError = real_jwt.PyJWTError
            mock_jwt.decode.return_value = claims
            result = resolver.resolve("dummy-token")
        assert result is not None
        return result  # type: ignore[return-value]

    def _resolve_raises(
        self,
        claims: dict[str, Any],
        resolver: "Any" = None,
    ) -> "Any":
        """Patch jwt.decode → claims; call resolve(); return raised AuthError."""
        from context_intelligence_server.auth import AuthError

        if resolver is None:
            resolver = _make_service_resolver()
        with patch("context_intelligence_server.auth.jwt") as mock_jwt:
            import jwt as real_jwt

            mock_jwt.PyJWTError = real_jwt.PyJWTError
            mock_jwt.decode.return_value = claims
            with pytest.raises(AuthError) as exc_info:
                resolver.resolve("dummy-token")
        return exc_info.value

    # -- B1-collision: service branch wins even when oid is in human map -----

    def test_b1_collision_oid_in_human_map_routes_to_service_branch(self) -> None:
        """B1 runtime collision: no-scp token whose oid is in the human map → service branch.

        The discriminator is scp-ONLY: no scp → service branch, regardless of
        whether the oid appears in the human identity_map.  The human map lookup
        only happens in the user branch (has_scp=True).

        This proves that even a misconfigured deployment (same oid in both maps,
        which B4 prevents at boot) routes correctly at request time:
        the service branch is taken before the human map is ever consulted.
        """
        # FAKE_OID_1 is in FAKE_IDENTITY_MAP (human map) — deliberate collision.
        claims = _service_claims(
            oid=FAKE_OID_1,  # exists in human identity_map
            roles=["Contributor"],
            appid=FAKE_APPID,
        )
        # Build resolver where the SAME oid is in both human map and service map.
        # (This bypasses B4 which only runs at create_asgi_app time.)
        resolver = _make_service_resolver(
            service_map={FAKE_OID_1.lower(): "svc-identity"}
        )
        cid, roles, is_service = self._resolve_service(claims, resolver=resolver)

        assert is_service is True, (
            "No-scp token must route to the SERVICE branch (is_service=True) "
            "regardless of the oid appearing in the human identity_map."
        )
        assert cid == "svc-identity", (
            f"created_by must come from the service map, not the human map. Got {cid!r}"
        )
        assert FAKE_CONTRIBUTOR not in [cid], (
            f"Human map value {FAKE_CONTRIBUTOR!r} must never be used for a service token."
        )

    # -- foreign-tid: AuthError(401) at shared validation --------------------

    def test_foreign_tid_service_token_raises_401(self) -> None:
        """Foreign-tid SERVICE token → AuthError(401) at shared validation (before service branch).

        The tid check happens before the scp discriminator.  A service token
        from a different tenant must be rejected at 401, not 403, and the
        service branch must never be reached.
        """
        claims = _service_claims(
            tid="00000000-0000-0000-0000-999999999999",  # wrong tenant
            roles=["Contributor"],
            appid=FAKE_APPID,
        )
        err = self._resolve_raises(claims)
        assert err.status_code == 401, (
            f"Foreign-tid service token must → 401, got {err.status_code}: {err.reason!r}"
        )

    # -- roles case-variant: case-sensitive membership -----------------------

    def test_roles_lowercase_case_variant_raises_403(self) -> None:
        """Service token roles=["contributor"] (lowercase) vs "Contributor" → 403.

        Role membership is CASE-SENSITIVE and EXACT.  A token with
        roles=["contributor"] (all-lowercase) does NOT match the configured
        service_data_role="Contributor".

        This test PINS the case-sensitive behavior so a future "helpful"
        lowercasing helper cannot silently grant privilege to a mis-cased role.
        """
        claims = _service_claims(
            roles=["contributor"],  # lowercase — wrong case
            appid=FAKE_APPID,
        )
        # Resolver configured with service_data_role="Contributor" (capital C)
        resolver = _make_service_resolver(service_data_role="Contributor")
        err = self._resolve_raises(claims, resolver=resolver)
        assert err.status_code == 403, (
            f"Lowercase 'contributor' must NOT match 'Contributor' (case-sensitive): "
            f"got status={err.status_code}, reason={err.reason!r}"
        )

    # -- roles non-list: normalized to [] → 403 ------------------------------

    def test_roles_as_bare_string_normalized_to_empty_list_403(self) -> None:
        """SERVICE token with roles as a bare string → normalized to [] → 403.

        The roles normalization logic:
            isinstance(_roles_raw, list) → [r for r in list if isinstance(r, str)]
            otherwise                    → []

        A bare string (not a list) is not a legitimate Entra roles value.
        It must be normalized to [] (no roles) and rejected with 403, not
        accidentally treated as a single-element list.
        """
        claims = _service_claims(appid=FAKE_APPID)
        # Inject roles as a bare string — not a list
        claims["roles"] = "Contributor"  # type: ignore[assignment]

        err = self._resolve_raises(claims)
        assert err.status_code == 403, (
            f"roles='Contributor' (bare string) must normalize to [] and → 403, "
            f"got status={err.status_code}, reason={err.reason!r}"
        )
