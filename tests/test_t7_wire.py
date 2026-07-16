"""T7: auth_mode switch wire tests — AC8/AC13/TB-11.

Proves:
  1. auth_enabled property on protocol and both implementations (AC13/H2).
  2. BearerTokenMiddleware uses auth_enabled, not isinstance(resolver, StaticKeyResolver).
  3. create_asgi_app() selects the correct resolver by auth_mode (AC8).
  4. create_asgi_app() refuses to start when auth_enabled=False and
     allow_unauthenticated=False (AC13 — the CRITICAL fail-closed gate).
  5. auth_mode=entra + empty identity map is caught by config validator (AC13/config).

RED before implementation: these tests FAIL until production changes are applied.
"""

from __future__ import annotations

import hashlib
import inspect
from typing import Any

import pytest

# Fake constants — NEVER real app-reg IDs or oids (§0.3)
FAKE_CLIENT_ID = "aaaabbbb-1111-2222-3333-ccccddddeeee"
FAKE_TENANT_ID = "ffffeeee-dddd-cccc-bbbb-aaaa99998888"
FAKE_OID_1 = "11111111-2222-3333-4444-555566667777"


# ---------------------------------------------------------------------------
# Stub JWKS client — no network, construction-time only
# ---------------------------------------------------------------------------


class _StubSigningKey:
    """Mimics PyJWKClient.get_signing_key_from_jwt(token).key."""

    def __init__(self, key: Any) -> None:
        self.key = key


class _StubJWKSClient:
    """Stub JWKS client: fetch_data no-op; get_jwk_set returns a non-empty set."""

    def fetch_data(self) -> None:  # noqa: D401
        pass

    def get_signing_key_from_jwt(self, token: str) -> _StubSigningKey:
        raise NotImplementedError("Not needed in construction-time tests")

    def get_jwk_set(self) -> Any:
        class _FakeJWKSet:
            keys = [object()]  # non-empty — passes the empty-JWKS startup guard

        return _FakeJWKSet()


# ---------------------------------------------------------------------------
# 1. auth_enabled property
# ---------------------------------------------------------------------------


class TestAuthEnabledProperty:
    """auth_enabled property on protocol and both implementations (AC13/H2)."""

    def test_static_key_resolver_auth_enabled_true_when_keys_present(self) -> None:
        """StaticKeyResolver.auth_enabled is True when at least one key is configured."""
        from context_intelligence_server.auth import StaticKeyResolver  # noqa: PLC0415

        digest = hashlib.sha256(b"mykey").hexdigest()
        assert StaticKeyResolver({digest: "owner"}).auth_enabled is True

    def test_static_key_resolver_auth_enabled_false_when_empty(self) -> None:
        """StaticKeyResolver.auth_enabled is False when keystore is empty."""
        from context_intelligence_server.auth import StaticKeyResolver  # noqa: PLC0415

        assert StaticKeyResolver({}).auth_enabled is False

    def test_entra_resolver_auth_enabled_always_true(self) -> None:
        """EntraResolver.auth_enabled is always True — Entra mode is always active."""
        from context_intelligence_server.auth import EntraResolver  # noqa: PLC0415

        resolver = EntraResolver(
            FAKE_CLIENT_ID,
            FAKE_TENANT_ID,
            {FAKE_OID_1.lower(): "colombod"},
            jwks_client=_StubJWKSClient(),
        )
        assert resolver.auth_enabled is True

    def test_protocol_declares_auth_enabled(self) -> None:
        """PrincipalResolver protocol source includes auth_enabled."""
        from context_intelligence_server.auth import PrincipalResolver  # noqa: PLC0415

        source = inspect.getsource(PrincipalResolver)
        assert "auth_enabled" in source, (
            "PrincipalResolver protocol must declare auth_enabled property"
        )


# ---------------------------------------------------------------------------
# 2. Middleware uses auth_enabled, not isinstance
# ---------------------------------------------------------------------------


class TestMiddlewareUsesAuthEnabled:
    """Middleware checks resolver.auth_enabled, not isinstance(resolver, StaticKeyResolver)."""

    async def test_middleware_passes_through_when_auth_enabled_false(self) -> None:
        """Middleware passes request through when resolver.auth_enabled is False
        AND allow_unauthenticated=True is explicitly set (fail-open opt-out).

        An empty keystore ALONE no longer fails open — that requires the
        explicit allow_unauthenticated=True opt-out (test/dev only). This test
        still proves the middleware checks resolver.auth_enabled (not
        isinstance(resolver, StaticKeyResolver)) as the gate condition.
        """
        from unittest.mock import AsyncMock  # noqa: PLC0415

        from context_intelligence_server.auth import (  # noqa: PLC0415
            BearerTokenMiddleware,
            StaticKeyResolver,
        )

        app = AsyncMock()
        middleware = BearerTokenMiddleware(
            app, resolver=StaticKeyResolver({}), allow_unauthenticated=True
        )
        scope = {"type": "http", "path": "/events", "headers": []}
        receive = AsyncMock()
        send = AsyncMock()

        await middleware(scope, receive, send)

        app.assert_called_once_with(scope, receive, send)

    async def test_middleware_401s_when_auth_enabled_false_without_opt_out(
        self,
    ) -> None:
        """resolver.auth_enabled=False alone (no allow_unauthenticated) -> 401.

        This is the core security fix: an empty keystore no longer fails open
        by default. It fails CLOSED — a safe bootstrap state.
        """
        from unittest.mock import AsyncMock  # noqa: PLC0415

        from context_intelligence_server.auth import (  # noqa: PLC0415
            BearerTokenMiddleware,
            StaticKeyResolver,
        )

        app = AsyncMock()
        middleware = BearerTokenMiddleware(app, resolver=StaticKeyResolver({}))
        scope = {"type": "http", "path": "/events", "headers": []}
        receive = AsyncMock()
        send = AsyncMock()

        await middleware(scope, receive, send)

        app.assert_not_called()
        response_start = send.call_args_list[0][0][0]
        assert response_start["status"] == 401

    def test_middleware_call_does_not_use_isinstance_on_static_resolver(self) -> None:
        """BearerTokenMiddleware.__call__ must not do isinstance(resolver, StaticKeyResolver).

        The isinstance check leaked the concrete class through the Protocol abstraction —
        killed by replacing it with the protocol property resolver.auth_enabled.
        """
        from context_intelligence_server.auth import BearerTokenMiddleware  # noqa: PLC0415

        source = inspect.getsource(BearerTokenMiddleware.__call__)
        assert "isinstance(self.resolver, StaticKeyResolver)" not in source, (
            "Middleware must use resolver.auth_enabled, not "
            "isinstance(self.resolver, StaticKeyResolver). "
            "The isinstance check leaks the concrete class through the Protocol abstraction."
        )


# ---------------------------------------------------------------------------
# 3. create_asgi_app() wires the correct resolver (AC8 — T7)
# ---------------------------------------------------------------------------


class TestCreateAsgiAppWire:
    """AC8 (T7): create_asgi_app() selects the resolver based on auth_mode."""

    def test_static_mode_builds_static_key_resolver(self) -> None:
        """auth_mode=static + api_keys → create_asgi_app() returns StaticKeyResolver."""
        from context_intelligence_server.auth import StaticKeyResolver  # noqa: PLC0415
        from context_intelligence_server.config import Settings  # noqa: PLC0415
        from context_intelligence_server.main import create_asgi_app  # noqa: PLC0415

        token = "test-wire-key"
        digest = hashlib.sha256(token.encode()).hexdigest()
        settings = Settings(auth_mode="static", api_keys={digest: {"id": "owner"}})

        wrapped = create_asgi_app(settings=settings)

        assert isinstance(wrapped.resolver, StaticKeyResolver), (
            "auth_mode=static must wire a StaticKeyResolver"
        )
        assert wrapped.resolver.auth_enabled is True

    def test_entra_mode_builds_entra_resolver(self) -> None:
        """auth_mode=entra + valid config + stub JWKS → create_asgi_app() returns EntraResolver."""
        from context_intelligence_server.auth import EntraResolver  # noqa: PLC0415
        from context_intelligence_server.config import Settings  # noqa: PLC0415
        from context_intelligence_server.main import create_asgi_app  # noqa: PLC0415

        settings = Settings(
            auth_mode="entra",
            azure_client_id=FAKE_CLIENT_ID,
            azure_tenant_id=FAKE_TENANT_ID,
            entra_identities={FAKE_OID_1: {"id": "colombod"}},
        )
        wrapped = create_asgi_app(settings=settings, _jwks_client=_StubJWKSClient())

        assert isinstance(wrapped.resolver, EntraResolver), (
            "auth_mode=entra must wire an EntraResolver"
        )
        assert wrapped.resolver.auth_enabled is True

    def test_module_level_asgi_app_is_static_resolver_in_test_env(self) -> None:
        """Module-level asgi_app uses StaticKeyResolver (test env has auth_mode=static)."""
        from context_intelligence_server.auth import StaticKeyResolver  # noqa: PLC0415
        from context_intelligence_server.main import asgi_app  # noqa: PLC0415

        assert isinstance(asgi_app.resolver, StaticKeyResolver), (
            "Module-level asgi_app must use StaticKeyResolver when auth_mode=static"
        )


# ---------------------------------------------------------------------------
# 4. Fail-closed startup refusal (AC13)
# ---------------------------------------------------------------------------


class TestCreateAsgiAppFailClosed:
    """AC13 (updated): create_asgi_app() now BOOTS fail-CLOSED with an empty
    keystore instead of refusing to start. Wide-open pass-through is reachable
    ONLY via the explicit allow_unauthenticated=True opt-out."""

    async def test_static_mode_no_keys_boots_fail_closed(
        self, tmp_path: Any, caplog: Any
    ) -> None:
        """auth_mode=static + no api_key/api_keys now BOOTS fail-CLOSED (no RuntimeError).

        NEW contract (empty-map bootstrap fix): an empty keystore is a SUPPORTED
        bootstrap state, not a startup error. create_asgi_app() constructs the
        app, logs a loud empty-keystore warning, and every real request
        fail-CLOSES with 401 until keys are onboarded via the admin API. (This
        replaces the old behavior, which raised RuntimeError to refuse startup.)

        allow_unauthenticated=False is set explicitly because the test harness
        sets the env var AMPLIFIER_..._ALLOW_UNAUTHENTICATED=true; we pin the
        production default here so the fail-CLOSED path (not the wide-open
        opt-out) is what gets proven. api_keys_store_path points at a
        non-existent tmp_path location so the store is guaranteed empty
        regardless of any pre-seeded /data/identity/api-keys.json.
        """
        import logging  # noqa: PLC0415
        from unittest.mock import AsyncMock  # noqa: PLC0415

        from context_intelligence_server.config import Settings  # noqa: PLC0415
        from context_intelligence_server.main import create_asgi_app  # noqa: PLC0415

        settings = Settings(
            auth_mode="static",
            allow_unauthenticated=False,
            api_keys_store_path=str(tmp_path / "api-keys.json"),
        )

        # 1. Construction SUCCEEDS (no RuntimeError) and logs the empty-keystore warning.
        with caplog.at_level(
            logging.WARNING, logger="context_intelligence_server.main"
        ):
            wrapped = create_asgi_app(settings=settings)

        assert wrapped is not None
        messages = [r.getMessage() for r in caplog.records]
        assert any("static keystore is EMPTY" in m for m in messages), (
            f"expected an empty-keystore warning at startup; got {messages!r}"
        )

        # 2. A real request fail-CLOSES with 401 (empty keystore, no opt-out).
        scope = {
            "type": "http",
            "path": "/events",
            "method": "POST",
            "headers": [(b"authorization", b"Bearer some-unregistered-token")],
        }
        send = AsyncMock()
        await wrapped(scope, AsyncMock(), send)

        response_start = send.call_args_list[0][0][0]
        assert response_start["type"] == "http.response.start"
        assert response_start["status"] == 401, (
            "empty keystore + allow_unauthenticated=False must fail-CLOSED (401)"
        )

    def test_allow_unauthenticated_bypasses_refusal(self) -> None:
        """allow_unauthenticated=True allows booting with no keys (explicit opt-out).

        This is how the test suite itself boots main.py with no auth configured.
        Production deployments should never set this.
        """
        from context_intelligence_server.auth import StaticKeyResolver  # noqa: PLC0415
        from context_intelligence_server.config import Settings  # noqa: PLC0415
        from context_intelligence_server.main import create_asgi_app  # noqa: PLC0415

        settings = Settings(auth_mode="static", allow_unauthenticated=True)
        # Must NOT raise
        wrapped = create_asgi_app(settings=settings)
        assert isinstance(wrapped.resolver, StaticKeyResolver)


# ---------------------------------------------------------------------------
# 5. Config validator catches entra with missing identity map (AC13/config path)
# ---------------------------------------------------------------------------


class TestEntraModeConfigValidation:
    """auth_mode=entra with missing/empty identity map is now a SUPPORTED
    bootstrap state — Settings() constructs successfully. client_id/tenant_id
    are still required (their absence still raises)."""

    def test_entra_missing_identities_now_boots(self) -> None:
        """auth_mode=entra + entra_identities=None now BOOTS (no ValidationError).

        NEW contract: an omitted identity map is a supported bootstrap state.
        Settings constructs; the map is populated at runtime via
        PUT /admin/identities. client_id/tenant_id remain required.
        """
        from context_intelligence_server.config import Settings  # noqa: PLC0415

        s = Settings(
            auth_mode="entra",
            azure_client_id=FAKE_CLIENT_ID,
            azure_tenant_id=FAKE_TENANT_ID,
            # entra_identities omitted → now a supported bootstrap state
        )
        assert s.auth_mode == "entra"
        assert s.entra_identities is None
        assert s.build_identity_map() == {}

    def test_entra_empty_identities_dict_now_boots(self) -> None:
        """auth_mode=entra + entra_identities={} now BOOTS and returns {}.

        NEW contract: an explicitly empty map is accepted (allow_empty=True) so
        the server boots on a fresh /data volume.
        """
        from context_intelligence_server.config import Settings  # noqa: PLC0415

        s = Settings(
            auth_mode="entra",
            azure_client_id=FAKE_CLIENT_ID,
            azure_tenant_id=FAKE_TENANT_ID,
            entra_identities={},  # empty dict → now a supported bootstrap state
        )
        assert s.entra_identities == {}
        assert s.build_identity_map() == {}

    def test_entra_missing_client_id_still_raises(self) -> None:
        """Regression guard: client_id/tenant_id are STILL required in entra mode.

        The empty-map relaxation must NOT weaken the azure_client_id /
        azure_tenant_id cross-field requirement.
        """
        from pydantic import ValidationError  # noqa: PLC0415

        from context_intelligence_server.config import Settings  # noqa: PLC0415

        with pytest.raises(ValidationError, match="azure_client_id"):
            Settings(
                auth_mode="entra",
                azure_tenant_id=FAKE_TENANT_ID,
                entra_identities={},
                # azure_client_id omitted → still a startup error
            )
