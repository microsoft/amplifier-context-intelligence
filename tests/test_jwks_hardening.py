"""T5: JWKS hardening tests — lifespan pin, distinct log tags, post-startup fail-closed.

Three areas validated here (council-ordered, trimmed from the full T5 plan):

  1) JWKS_CACHE_LIFESPAN_SECONDS constant + explicit lifespan kwarg to PyJWKClient
     RED against old code: constant doesn't exist → AttributeError on import.
     GREEN after: constant added, wired to PyJWKClient(lifespan=...).

  2) Distinct greppable log tags in BearerTokenMiddleware:
       auth_event=auth_denied          (normal AuthError denial — INFO)
       auth_event=resolver_unexpected_exception  (catch-all — ERROR)
     RED against old code: catch-all has no auth_event tag; AuthError path has NO log.
     GREEN after: both paths emit their respective tags.

  3) Post-startup JWKS failure paths → AuthError(401), NOT unhandled 500.
     PyJWKClientConnectionError and PyJWKClientError are both PyJWTError subclasses,
     so EntraResolver.resolve() already converts them to AuthError(401).
     These are characterisation tests — they pin the contract so a future refactor
     cannot accidentally break it.
"""

from __future__ import annotations

import logging
import time
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

# ---------------------------------------------------------------------------
# Fake constants — NEVER real GUIDs / tenant ids / oids
# ---------------------------------------------------------------------------
FAKE_CLIENT_ID = "aaaabbbb-1111-2222-3333-ccccddddeeee"
FAKE_TENANT_ID = "ffffeeee-dddd-cccc-bbbb-aaaa99998888"
FAKE_OID = "11111111-2222-3333-4444-555566667777"
FAKE_IDENTITY_MAP = {FAKE_OID.lower(): "colombod"}
FAKE_ISSUER = f"https://login.microsoftonline.com/{FAKE_TENANT_ID}/v2.0"


# ---------------------------------------------------------------------------
# RSA keypair fixture (local — not imported from test_entra_resolver.py)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def rsa_keypair_local() -> tuple[Any, Any]:
    """2048-bit RSA keypair for the one real-crypto test in this module."""
    from cryptography.hazmat.primitives.asymmetric import rsa

    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return private_key, private_key.public_key()


# ---------------------------------------------------------------------------
# Stub JWKS clients
# ---------------------------------------------------------------------------


class _StubSigningKey:
    def __init__(self, key: Any) -> None:
        self.key = key


class _StubJWKSClient:
    """Always returns a fixed key; fetch_data is a no-op."""

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


class _PostStartupConnectionFailClient:
    """Construction succeeds; every signing-key lookup raises PyJWKClientConnectionError.

    Models: JWKS endpoint alive at startup (fetch_data OK), then network dies
    before any signing-key lookup can succeed (e.g. new kid requires a re-fetch).
    """

    def fetch_data(self) -> None:
        pass  # startup succeeds

    def get_jwk_set(self) -> Any:
        class _FakeJWKSet:
            keys = [object()]  # non-empty — passes the empty-JWKS guard

        return _FakeJWKSet()

    def get_signing_key_from_jwt(self, token: str) -> Any:
        from jwt import PyJWKClientConnectionError

        raise PyJWKClientConnectionError(
            "connection refused — JWKS endpoint unreachable post-startup"
        )


class _PostStartupMalformedJWKSClient:
    """Construction succeeds; get_signing_key_from_jwt raises a non-connection PyJWKClientError.

    Models: JWKS endpoint returns malformed data or has no matching kid after startup.
    """

    def fetch_data(self) -> None:
        pass

    def get_jwk_set(self) -> Any:
        class _FakeJWKSet:
            keys = [object()]

        return _FakeJWKSet()

    def get_signing_key_from_jwt(self, token: str) -> Any:
        from jwt import PyJWKClientError

        raise PyJWKClientError("unable to find a signing key — malformed JWKS response")


class _CachedKeyJWKSClient:
    """Simulates PyJWKClient with a key already in-memory cache.

    After fetch_data() at construction the key is available; get_signing_key_from_jwt
    returns from 'cache' without a network call, modelling the within-lifespan path.
    """

    def __init__(self, key: Any) -> None:
        self._cached = _StubSigningKey(key)

    def fetch_data(self) -> None:
        pass  # populates 'cache' at construction

    def get_jwk_set(self) -> Any:
        _k = self._cached

        class _FakeJWKSet:
            keys = [_k]

        return _FakeJWKSet()

    def get_signing_key_from_jwt(self, token: str) -> _StubSigningKey:
        return self._cached  # pure cache-hit; no network


# ---------------------------------------------------------------------------
# Helper: build an EntraResolver with an injected JWKS client
# ---------------------------------------------------------------------------


def _make_entra_resolver(jwks_client: Any) -> Any:
    from context_intelligence_server.auth import EntraResolver

    return EntraResolver(
        client_id=FAKE_CLIENT_ID,
        tenant_id=FAKE_TENANT_ID,
        identity_map=FAKE_IDENTITY_MAP,
        jwks_client=jwks_client,
    )


# ---------------------------------------------------------------------------
# Helper: run BearerTokenMiddleware and capture HTTP status + log records
# ---------------------------------------------------------------------------


async def _run_middleware_and_capture(
    resolver: Any,
    token: str = "dummy-bearer",
    path: str = "/events",
) -> tuple[int, ...]:
    """Run middleware; return tuple of received HTTP statuses (usually length 1)."""
    from context_intelligence_server.auth import BearerTokenMiddleware

    app = AsyncMock()
    statuses: list[int] = []

    async def capture_send(event: dict[str, Any]) -> None:
        if event.get("type") == "http.response.start":
            statuses.append(event["status"])

    scope = {
        "type": "http",
        "path": path,
        "method": "POST",
        "headers": [(b"authorization", f"Bearer {token}".encode())],
    }
    middleware = BearerTokenMiddleware(app, resolver=resolver)
    await middleware(scope, AsyncMock(), capture_send)
    return tuple(statuses)


# ===========================================================================
# 1. LIFESPAN PIN
# RED against old code: JWKS_CACHE_LIFESPAN_SECONDS doesn't exist.
# GREEN after: constant added + passed to PyJWKClient(lifespan=...).
# ===========================================================================


class TestJWKSLifespanPin:
    """JWKS_CACHE_LIFESPAN_SECONDS constant exists and is wired into PyJWKClient."""

    def test_jwks_cache_lifespan_constant_exists(self) -> None:
        """auth module exposes JWKS_CACHE_LIFESPAN_SECONDS as a positive int.

        RED: AttributeError — module has no attribute 'JWKS_CACHE_LIFESPAN_SECONDS'.
        GREEN: constant is importable.
        """
        import context_intelligence_server.auth as auth_module

        constant = auth_module.JWKS_CACHE_LIFESPAN_SECONDS  # AttributeError if missing
        assert isinstance(constant, int)
        assert constant > 0, "lifespan must be a positive number of seconds"

    def test_jwks_cache_lifespan_is_300_seconds(self) -> None:
        """JWKS_CACHE_LIFESPAN_SECONDS == 300 (matches the PyJWKClient library default).

        Pinning the library default explicitly makes the contract visible in code even
        though it does not change behaviour at runtime.  If the library default ever
        changes, this assertion flags the divergence.
        """
        import context_intelligence_server.auth as auth_module

        assert auth_module.JWKS_CACHE_LIFESPAN_SECONDS == 300

    def test_pyjwkclient_receives_explicit_lifespan_kwarg(self) -> None:
        """EntraResolver passes lifespan=JWKS_CACHE_LIFESPAN_SECONDS to PyJWKClient.

        RED: kwarg is absent — PyJWKClient is constructed without 'lifespan'.
        GREEN: 'lifespan' kwarg is present and matches the constant.
        """
        import context_intelligence_server.auth as auth_module

        captured_kwargs: list[dict[str, Any]] = []

        class _CapturingPyJWKClient:
            def __init__(self, uri: str, **kwargs: Any) -> None:
                captured_kwargs.append(kwargs)

            def fetch_data(self) -> None:
                pass

            def get_jwk_set(self) -> Any:
                class _FJK:
                    keys = [object()]

                return _FJK()

        with patch(
            "context_intelligence_server.auth.PyJWKClient", _CapturingPyJWKClient
        ):
            from context_intelligence_server.auth import EntraResolver

            EntraResolver(
                client_id=FAKE_CLIENT_ID,
                tenant_id=FAKE_TENANT_ID,
                identity_map=FAKE_IDENTITY_MAP,
                # no jwks_client → default build path fires and calls PyJWKClient(...)
            )

        assert len(captured_kwargs) == 1, "PyJWKClient constructed exactly once"
        assert "lifespan" in captured_kwargs[0], (
            "PyJWKClient must receive an explicit 'lifespan' kwarg — "
            "this makes the cache TTL contract visible even though it matches the default"
        )
        assert captured_kwargs[0]["lifespan"] == auth_module.JWKS_CACHE_LIFESPAN_SECONDS

    def test_todo_t5_comment_is_replaced(self) -> None:
        """The '# TODO(T5)' comment is removed from auth.py.

        RED: the TODO comment still exists in source.
        GREEN: comment has been replaced with a note about PyJWKClient handling.
        """
        import inspect

        import context_intelligence_server.auth as auth_module

        source = inspect.getsource(auth_module)
        assert "TODO(T5)" not in source, (
            "T5 is done — the TODO(T5) comment must be removed and replaced with "
            "a note that PyJWKClient handles per-kid caching and lifespan refresh"
        )


# ===========================================================================
# 2. DISTINCT LOG TAGS
# RED against old code: catch-all has no auth_event tag; AuthError path has no log.
# GREEN after: both paths emit distinct tags.
# ===========================================================================


class TestDistinctLogTags:
    """BearerTokenMiddleware emits distinct greppable log tags for the two failure modes."""

    async def test_catch_all_logs_resolver_unexpected_exception_tag(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Unexpected resolver exception → ERROR with auth_event=resolver_unexpected_exception.

        RED: log message has no 'resolver_unexpected_exception' tag.
        GREEN: tag is present in the ERROR log.
        """
        from context_intelligence_server.auth import BearerTokenMiddleware

        class _BrokenResolver:
            @property
            def auth_enabled(self) -> bool:
                return True

            def resolve(self, token: str, *, admin_path: bool = False) -> str:
                _ = admin_path
                raise RuntimeError("simulated unexpected internal resolver bug")

        middleware = BearerTokenMiddleware(AsyncMock(), resolver=_BrokenResolver())
        statuses: list[int] = []

        async def capture_send(event: dict[str, Any]) -> None:
            if event.get("type") == "http.response.start":
                statuses.append(event["status"])

        scope = {
            "type": "http",
            "path": "/events",
            "headers": [(b"authorization", b"Bearer dummy-token")],
        }

        with caplog.at_level(logging.ERROR, logger="context_intelligence_server.auth"):
            await middleware(scope, AsyncMock(), capture_send)

        assert statuses == [401], (
            "unexpected resolver exception must deny with HTTP 401"
        )

        # RED: old message has no 'resolver_unexpected_exception' tag
        all_messages = " ".join(r.getMessage() for r in caplog.records)
        assert "resolver_unexpected_exception" in all_messages, (
            "ERROR log must contain 'resolver_unexpected_exception' so operators can "
            "grep for unexpected resolver crashes distinctly from normal auth denials. "
            f"Got messages: {all_messages!r}"
        )

    async def test_auth_error_logs_auth_denied_tag(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """AuthError from resolver → log entry with auth_event=auth_denied.

        RED: the AuthError path in old code emits NO log at all — zero records.
        GREEN: a log with 'auth_denied' is present at a sub-ERROR level.
        """
        from context_intelligence_server.auth import AuthError, BearerTokenMiddleware

        class _DenyingResolver:
            @property
            def auth_enabled(self) -> bool:
                return True

            def resolve(self, token: str, *, admin_path: bool = False) -> str:
                _ = admin_path
                raise AuthError(401, "token rejected — auth_denied path test")

        middleware = BearerTokenMiddleware(AsyncMock(), resolver=_DenyingResolver())
        statuses: list[int] = []

        async def capture_send(event: dict[str, Any]) -> None:
            if event.get("type") == "http.response.start":
                statuses.append(event["status"])

        scope = {
            "type": "http",
            "path": "/events",
            "headers": [(b"authorization", b"Bearer dummy-token")],
        }

        with caplog.at_level(logging.DEBUG, logger="context_intelligence_server.auth"):
            await middleware(scope, AsyncMock(), capture_send)

        assert statuses == [401]

        # RED: no log at all in the old code → no records with 'auth_denied'
        all_messages = " ".join(r.getMessage() for r in caplog.records)
        assert "auth_denied" in all_messages, (
            "AuthError denial must emit a log containing 'auth_denied' so operators "
            "can distinguish 'bad token rejected' from 'resolver crashed'. "
            f"Got messages: {all_messages!r}"
        )

    async def test_auth_error_log_is_below_error_level(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """AuthError denial log is INFO or below — distinguishable from ERROR catch-all.

        Operators who filter at ERROR+ should see only unexpected crashes, not every
        normal auth rejection (which can be high-volume in production).
        """
        from context_intelligence_server.auth import AuthError, BearerTokenMiddleware

        class _DenyingResolver:
            @property
            def auth_enabled(self) -> bool:
                return True

            def resolve(self, token: str, *, admin_path: bool = False) -> str:
                _ = admin_path
                raise AuthError(401, "normal rejection — severity check")

        middleware = BearerTokenMiddleware(AsyncMock(), resolver=_DenyingResolver())
        statuses: list[int] = []

        async def capture_send(event: dict[str, Any]) -> None:
            if event.get("type") == "http.response.start":
                statuses.append(event["status"])

        scope = {
            "type": "http",
            "path": "/events",
            "headers": [(b"authorization", b"Bearer t")],
        }

        with caplog.at_level(logging.DEBUG, logger="context_intelligence_server.auth"):
            await middleware(scope, AsyncMock(), capture_send)

        auth_denied_records = [
            r for r in caplog.records if "auth_denied" in r.getMessage()
        ]
        assert auth_denied_records, (
            "auth_denied record must be emitted (tested separately)"
        )
        for record in auth_denied_records:
            assert record.levelno < logging.ERROR, (
                f"auth_denied log must be below ERROR level; got {record.levelname}"
            )

    async def test_raw_token_not_in_catch_all_log_message(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """The raw bearer token must never appear in the catch-all log MESSAGE.

        Credential hygiene: the exception message may contain the token (we can't
        control what the resolver puts in RuntimeError), but the middleware's own
        log message must not echo it.  The exc_info traceback is acceptable.
        """
        from context_intelligence_server.auth import BearerTokenMiddleware

        secret_token = "super-secret-bearer-xyz987"

        class _BrokenResolver:
            @property
            def auth_enabled(self) -> bool:
                return True

            def resolve(self, token: str, *, admin_path: bool = False) -> str:
                _ = admin_path
                raise RuntimeError("boom — credentials are safe in this test")

        middleware = BearerTokenMiddleware(AsyncMock(), resolver=_BrokenResolver())
        statuses: list[int] = []

        async def capture_send(event: dict[str, Any]) -> None:
            if event.get("type") == "http.response.start":
                statuses.append(event["status"])

        scope = {
            "type": "http",
            "path": "/events",
            "headers": [(b"authorization", f"Bearer {secret_token}".encode())],
        }

        with caplog.at_level(logging.ERROR, logger="context_intelligence_server.auth"):
            await middleware(scope, AsyncMock(), capture_send)

        for record in caplog.records:
            assert secret_token not in record.getMessage(), (
                f"Raw bearer token must not appear in log MESSAGE; "
                f"found in: {record.getMessage()!r}"
            )


# ===========================================================================
# 3. POST-STARTUP JWKS FAILURE — FAIL-CLOSED (characterisation tests)
# ===========================================================================


class TestJWKSPostStartupFailClosed:
    """Post-startup JWKS failures → AuthError(401), NOT unhandled exception / 500.

    PyJWKClientConnectionError and PyJWKClientError are both subclasses of PyJWTError,
    so the existing ``except jwt.PyJWTError`` in EntraResolver.resolve() already
    converts them to AuthError(401).  These tests PIN that contract.
    """

    def test_connection_error_during_signing_key_lookup_raises_auth_error_401(
        self,
    ) -> None:
        """PyJWKClientConnectionError from get_signing_key_from_jwt → AuthError(401).

        Characterisation: post-startup network loss must NOT propagate as an
        unhandled exception (which the middleware catch-all would turn into 401 anyway,
        but via the wrong code path — this ensures it goes via the PyJWTError branch).
        """
        from context_intelligence_server.auth import AuthError

        resolver = _make_entra_resolver(_PostStartupConnectionFailClient())
        with pytest.raises(AuthError) as exc_info:
            resolver.resolve("any-jwt-token")
        assert exc_info.value.status_code == 401, (
            "PyJWKClientConnectionError must become AuthError(401), not propagate as 500"
        )

    def test_malformed_jwks_response_raises_auth_error_401(self) -> None:
        """Non-connection PyJWKClientError (e.g. no matching kid) → AuthError(401).

        Characterisation: any PyJWKClientError variant must be fail-closed.
        """
        from context_intelligence_server.auth import AuthError

        resolver = _make_entra_resolver(_PostStartupMalformedJWKSClient())
        with pytest.raises(AuthError) as exc_info:
            resolver.resolve("any-jwt-token")
        assert exc_info.value.status_code == 401, (
            "PyJWKClientError (malformed/no-match) must become AuthError(401), not 500"
        )

    def test_cached_key_resolves_after_endpoint_modelled_as_dead(
        self,
        rsa_keypair_local: tuple[Any, Any],
    ) -> None:
        """Keys in cache (within lifespan window) resolve successfully.

        Models the PyJWKClient cache-hit path: fetch_data() populated the key at
        construction; get_signing_key_from_jwt() returns from memory without any
        network call, so resolution succeeds even when the JWKS endpoint is down.
        """
        import jwt as pyjwt

        private_key, public_key = rsa_keypair_local
        now = int(time.time())
        claims: dict[str, Any] = {
            "oid": FAKE_OID,
            "tid": FAKE_TENANT_ID,
            "scp": "access_as_user",
            "aud": FAKE_CLIENT_ID,
            "iss": FAKE_ISSUER,
            "exp": now + 3600,
            "iat": now - 10,
        }
        token = pyjwt.encode(claims, private_key, algorithm="RS256")

        resolver = _make_entra_resolver(_CachedKeyJWKSClient(public_key))
        result = resolver.resolve(token)
        # M2 protocol change: resolve() returns (contributor_id, roles, is_service) 3-tuple.
        assert result is not None
        contributor_id, _roles, _is_service = result
        assert contributor_id == "colombod", (
            "A cached signing key must resolve a valid token even when the endpoint is down"
        )

    async def test_post_startup_connection_error_returns_http_401(self) -> None:
        """End-to-end: PyJWKClientConnectionError → HTTP 401 (not 500) via middleware."""
        statuses = await _run_middleware_and_capture(
            _make_entra_resolver(_PostStartupConnectionFailClient())
        )
        assert statuses == (401,), (
            "Post-startup connection error must produce HTTP 401, not 500"
        )


# ===========================================================================
# 4. PyJWKClientError HIERARCHY SANITY CHECK
# Pins the PyJWT exception hierarchy that our fail-closed contract relies on.
# ===========================================================================


class TestPyJWKClientErrorHierarchy:
    """Pin the PyJWT exception hierarchy that the fail-closed guarantee depends on.

    If a future pyjwt release breaks this hierarchy, these tests fail loudly
    before a silent regression lets 500s through.
    """

    def test_pyjwkclientconnection_error_is_pyjwterror(self) -> None:
        """PyJWKClientConnectionError IS-A PyJWTError → caught by resolve()."""
        from jwt import PyJWKClientConnectionError, PyJWTError

        assert issubclass(PyJWKClientConnectionError, PyJWTError), (
            "EntraResolver.resolve() catches PyJWTError; "
            "PyJWKClientConnectionError must remain a subclass for fail-closed to hold"
        )

    def test_pyjwkclient_error_is_pyjwterror(self) -> None:
        """PyJWKClientError IS-A PyJWTError → caught by resolve()."""
        from jwt import PyJWKClientError, PyJWTError

        assert issubclass(PyJWKClientError, PyJWTError), (
            "EntraResolver.resolve() catches PyJWTError; "
            "PyJWKClientError must remain a subclass for fail-closed to hold"
        )
