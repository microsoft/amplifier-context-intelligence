"""T2: PrincipalResolver seam — pure refactor tests.

These tests prove that the StaticKeyResolver / PrincipalResolver abstraction:

  1. Exists as importable symbols from context_intelligence_server.auth.
  2. Behaves byte-for-byte identically to the inline sha256 lookup it replaces.
  3. Is wired into BearerTokenMiddleware so the middleware delegates resolution.

No behavior change vs. the old code — these tests are the spec for correctness.
"""

import hashlib
from unittest.mock import AsyncMock


# ---------------------------------------------------------------------------
# Helpers (mirrors _keystore() in test_auth.py)
# ---------------------------------------------------------------------------


def _make_scope_with_auth(token: str, path: str = "/events") -> dict:
    return {
        "type": "http",
        "path": path,
        "headers": [(b"authorization", f"Bearer {token}".encode())],
    }


def _make_scope(path: str = "/events") -> dict:
    return {
        "type": "http",
        "path": path,
        "headers": [],
    }


# ---------------------------------------------------------------------------
# StaticKeyResolver unit tests
# ---------------------------------------------------------------------------


class TestStaticKeyResolver:
    """StaticKeyResolver wraps the sha256-hash keystore lookup."""

    def test_importable_from_auth_module(self) -> None:
        """StaticKeyResolver is exported from context_intelligence_server.auth."""
        from context_intelligence_server.auth import StaticKeyResolver  # noqa: PLC0415

        assert StaticKeyResolver is not None

    def test_resolve_known_token_returns_contributor_id(self) -> None:
        """resolve() returns (contributor_id, []) for a known token (T5: tuple protocol)."""
        from context_intelligence_server.auth import StaticKeyResolver  # noqa: PLC0415

        token = "my-api-key"
        digest = hashlib.sha256(token.encode()).hexdigest()
        resolver = StaticKeyResolver({digest: "alice"})

        result = resolver.resolve(token)
        assert result is not None
        contributor_id, roles, is_service = result  # M2: 3-tuple
        assert contributor_id == "alice"
        assert roles == []  # static resolver always returns empty roles
        assert (
            is_service is False
        )  # M2: static resolver is always write-capable (is_service=False)

    def test_resolve_unknown_token_returns_none(self) -> None:
        """resolve() returns None — never 'unknown' — for an unrecognised token."""
        from context_intelligence_server.auth import StaticKeyResolver  # noqa: PLC0415

        digest = hashlib.sha256(b"other-token").hexdigest()
        resolver = StaticKeyResolver({digest: "bob"})

        result = resolver.resolve("wrong-token")
        assert result is None
        assert result != "unknown"

    def test_resolve_empty_keystore_returns_none(self) -> None:
        """resolve() returns None for any token when keystore is empty."""
        from context_intelligence_server.auth import StaticKeyResolver  # noqa: PLC0415

        resolver = StaticKeyResolver({})
        assert resolver.resolve("any-token") is None

    def test_is_empty_true_for_empty_keystore(self) -> None:
        """is_empty is True when the resolver was built with no keys."""
        from context_intelligence_server.auth import StaticKeyResolver  # noqa: PLC0415

        assert StaticKeyResolver({}).is_empty is True

    def test_is_empty_false_for_non_empty_keystore(self) -> None:
        """is_empty is False when the resolver has at least one entry."""
        from context_intelligence_server.auth import StaticKeyResolver  # noqa: PLC0415

        digest = hashlib.sha256(b"t").hexdigest()
        assert StaticKeyResolver({digest: "user"}).is_empty is False

    def test_resolve_multiple_entries_correct_contributor(self) -> None:
        """Multiple keys in the store resolve independently (T5: tuple protocol)."""
        from context_intelligence_server.auth import StaticKeyResolver  # noqa: PLC0415

        d_alice = hashlib.sha256(b"alice-key").hexdigest()
        d_bob = hashlib.sha256(b"bob-key").hexdigest()
        resolver = StaticKeyResolver({d_alice: "alice", d_bob: "bob"})

        alice_result = resolver.resolve("alice-key")
        bob_result = resolver.resolve("bob-key")
        assert alice_result is not None
        assert bob_result is not None
        assert alice_result[0] == "alice"
        assert bob_result[0] == "bob"
        assert resolver.resolve("unknown") is None


# ---------------------------------------------------------------------------
# PrincipalResolver protocol
# ---------------------------------------------------------------------------


class TestPrincipalResolverProtocol:
    """PrincipalResolver is an importable Protocol that StaticKeyResolver satisfies."""

    def test_importable_from_auth_module(self) -> None:
        from context_intelligence_server.auth import PrincipalResolver  # noqa: PLC0415

        assert PrincipalResolver is not None

    def test_static_key_resolver_satisfies_protocol(self) -> None:
        """StaticKeyResolver has resolve() and auth_enabled matching the Protocol signature."""
        from context_intelligence_server.auth import (  # noqa: PLC0415
            StaticKeyResolver,
        )

        resolver = StaticKeyResolver({})
        # Structural checks: must have resolve() callable and auth_enabled property
        assert callable(resolver.resolve)
        assert hasattr(resolver, "auth_enabled")
        # Note: isinstance(resolver, PrincipalResolver) is not used because
        # PrincipalResolver is no longer @runtime_checkable (T7 cleanup).


# ---------------------------------------------------------------------------
# BearerTokenMiddleware — resolver delegation
# ---------------------------------------------------------------------------


class TestBearerTokenMiddlewareResolvesViaResolver:
    """BearerTokenMiddleware delegates token resolution to its resolver."""

    async def test_middleware_calls_resolver_resolve(self) -> None:
        """Middleware calls resolver.resolve(token), not inline sha256 lookup."""
        from context_intelligence_server.auth import BearerTokenMiddleware  # noqa: PLC0415

        calls: list[str] = []

        class TrackingResolver:
            """A stub that records every token it was asked to resolve."""

            # auth_enabled=True so the middleware proceeds past the fail-open gate
            # and calls resolve().  (The middleware now checks resolver.auth_enabled,
            # not isinstance(resolver, StaticKeyResolver) and resolver.is_empty.)
            @property
            def auth_enabled(self) -> bool:
                return True

            def resolve(
                self, token: str, *, admin_path: bool = False
            ) -> tuple[str, list[str], bool] | None:
                # M2: resolve() now returns (contributor_id, roles, is_service) 3-tuple.
                # admin_path is accepted for Protocol compatibility (unused here).
                _ = admin_path
                calls.append(token)
                return ("tracked-user", [], False)

        app = AsyncMock()
        middleware = BearerTokenMiddleware(app, resolver=TrackingResolver())
        scope = _make_scope_with_auth("sentinel-token")

        await middleware(scope, AsyncMock(), AsyncMock())

        assert calls == ["sentinel-token"], (
            "Middleware must call resolver.resolve() with the extracted bearer token"
        )

    async def test_middleware_injects_contributor_id_from_resolver(self) -> None:
        """contributor_id in scope state comes from the resolver return value."""
        from context_intelligence_server.auth import (  # noqa: PLC0415
            BearerTokenMiddleware,
            StaticKeyResolver,
        )

        token = "the-key"
        digest = hashlib.sha256(token.encode()).hexdigest()
        resolver = StaticKeyResolver({digest: "my-contributor"})

        app = AsyncMock()
        middleware = BearerTokenMiddleware(app, resolver=resolver)
        scope = _make_scope_with_auth(token)

        await middleware(scope, AsyncMock(), AsyncMock())

        assert scope.get("state", {}).get("contributor_id") == "my-contributor"

    async def test_middleware_with_keystore_kwarg_uses_static_resolver(self) -> None:
        """keystore= backward-compat path internally creates StaticKeyResolver."""
        from context_intelligence_server.auth import (  # noqa: PLC0415
            BearerTokenMiddleware,
            StaticKeyResolver,
        )

        token = "compat-token"
        digest = hashlib.sha256(token.encode()).hexdigest()

        app = AsyncMock()
        middleware = BearerTokenMiddleware(app, keystore={digest: "compat-user"})

        # resolver attribute must be a StaticKeyResolver
        assert isinstance(middleware.resolver, StaticKeyResolver)

        scope = _make_scope_with_auth(token)
        await middleware(scope, AsyncMock(), AsyncMock())
        assert scope.get("state", {}).get("contributor_id") == "compat-user"

    async def test_empty_static_resolver_fails_closed_by_default(self) -> None:
        """Empty StaticKeyResolver + allow_unauthenticated=False (default) -> 401.

        This used to fail-open (pass through unauthenticated). An empty
        keystore alone no longer opens the server wide up — it now fail-closes
        (a SAFE bootstrap state) unless the operator explicitly opts out via
        allow_unauthenticated=True (see the companion test below).
        """
        from context_intelligence_server.auth import (  # noqa: PLC0415
            BearerTokenMiddleware,
            StaticKeyResolver,
        )

        app = AsyncMock()
        middleware = BearerTokenMiddleware(app, resolver=StaticKeyResolver({}))
        scope = _make_scope("/events")

        receive = AsyncMock()
        send = AsyncMock()
        await middleware(scope, receive, send)

        app.assert_not_called()
        response_start = send.call_args_list[0][0][0]
        assert response_start["status"] == 401

    async def test_empty_static_resolver_passes_through_with_opt_out(self) -> None:
        """Empty StaticKeyResolver + allow_unauthenticated=True -> wide-open pass-through.

        This is the ONLY path that still fails open, and it requires an
        explicit, deliberate opt-out (test/dev only).
        """
        from context_intelligence_server.auth import (  # noqa: PLC0415
            BearerTokenMiddleware,
            StaticKeyResolver,
        )

        app = AsyncMock()
        middleware = BearerTokenMiddleware(
            app, resolver=StaticKeyResolver({}), allow_unauthenticated=True
        )
        scope = _make_scope("/events")

        receive = AsyncMock()
        send = AsyncMock()
        await middleware(scope, receive, send)

        app.assert_called_once_with(scope, receive, send)

    async def test_unknown_token_returns_401_via_resolver(self) -> None:
        """resolver.resolve() returning None causes the middleware to send 401."""
        from context_intelligence_server.auth import (  # noqa: PLC0415
            BearerTokenMiddleware,
            StaticKeyResolver,
        )

        digest = hashlib.sha256(b"other").hexdigest()
        resolver = StaticKeyResolver({digest: "other-user"})

        app = AsyncMock()
        middleware = BearerTokenMiddleware(app, resolver=resolver)
        scope = _make_scope_with_auth("wrong-token")
        send = AsyncMock()

        await middleware(scope, AsyncMock(), send)

        app.assert_not_called()
        response_start = send.call_args_list[0][0][0]
        assert response_start["status"] == 401


# ---------------------------------------------------------------------------
# create_asgi_app wires a StaticKeyResolver
# ---------------------------------------------------------------------------


class TestCreateAsgiAppUsesStaticKeyResolver:
    """create_asgi_app() constructs a StaticKeyResolver and wires it into the middleware."""

    def test_asgi_app_resolver_is_static_key_resolver(self) -> None:
        """asgi_app.resolver is a StaticKeyResolver (not raw keystore in __init__)."""
        from context_intelligence_server.auth import StaticKeyResolver  # noqa: PLC0415
        from context_intelligence_server.main import asgi_app  # noqa: PLC0415

        assert isinstance(asgi_app.resolver, StaticKeyResolver), (
            "create_asgi_app() must explicitly build a StaticKeyResolver "
            "and pass it as resolver= to BearerTokenMiddleware"
        )
