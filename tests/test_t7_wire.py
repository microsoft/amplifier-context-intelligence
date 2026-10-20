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
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx
import pytest
from gunicorn.app.base import BaseApplication

if TYPE_CHECKING:
    from context_intelligence_server.config import Settings

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


class _RequestTokenValidator:
    """Application-boundary double, not evidence of cryptographic validation."""

    def __init__(
        self,
        failure: Exception | None = None,
        *,
        claims: dict[str, Any] | None = None,
    ) -> None:
        self.failure = failure
        self.calls: list[tuple[str, str, str]] = []
        self.claims = (
            claims
            if claims is not None
            else {
                "tid": FAKE_TENANT_ID,
                "oid": FAKE_OID_1,
                "scp": "access_as_user",
                "roles": ["IdentityAdmin"],
            }
        )

    def validate(
        self, bearer: str, *, original_uri: str, original_method: str
    ) -> dict[str, Any]:
        self.calls.append((bearer, original_uri, original_method))
        if self.failure is not None:
            raise self.failure
        return dict(self.claims)


def _token_validator_settings(tmp_path: Path) -> Settings:
    from context_intelligence_server.config import Settings

    return Settings(
        auth_mode="entra",
        allow_unauthenticated=False,
        azure_client_id=FAKE_CLIENT_ID,
        azure_tenant_id=FAKE_TENANT_ID,
        entra_identities={FAKE_OID_1: {"id": "mapped-contributor"}},
        entra_identities_store_path=str(tmp_path / "entra-identities.json"),
    )


class TestTokenValidatorRequestContext:
    @pytest.mark.parametrize(
        ("scheme", "host", "server", "path", "raw_path", "query", "method", "uri"),
        [
            (
                "https",
                b"ingest.example:8443",
                ("internal.example", 8000),
                "/events",
                b"/events",
                b"trace=a%2Fb&trace=2",
                "POST",
                "https://ingest.example:8443/events?trace=a%2Fb&trace=2",
            ),
            (
                "http",
                b"ingest.example",
                ("internal.example", 8000),
                "/events",
                b"/events",
                b"empty=&q=a+b",
                "DELETE",
                "http://ingest.example/events?empty=&q=a+b",
            ),
            (
                "https",
                b"[2001:db8::1]:8443",
                ("internal.example", 8000),
                "/blobs/a/b c",
                b"/blobs/a%2Fb%20c",
                b"q=%2B",
                "PATCH",
                "https://[2001:db8::1]:8443/blobs/a%2Fb%20c?q=%2B",
            ),
            (
                "http",
                None,
                ("scope-server.example", 9080),
                "/events",
                None,
                b"source=scope",
                "PUT",
                "http://scope-server.example:9080/events?source=scope",
            ),
        ],
        ids=[
            "https-host-port-query",
            "http-method",
            "encoded-path-ipv6",
            "scope-server",
        ],
    )
    async def test_middleware_forwards_inbound_uri_and_method(
        self,
        scheme: str,
        host: bytes | None,
        server: tuple[str, int],
        path: str,
        raw_path: bytes | None,
        query: bytes,
        method: str,
        uri: str,
    ) -> None:
        from unittest.mock import AsyncMock

        from context_intelligence_server.auth import BearerTokenMiddleware

        calls: list[tuple[str, str, str, bool]] = []

        class ContextResolver:
            auth_enabled = True

            def resolve(
                self,
                token: str,
                *,
                original_uri: str,
                original_method: str,
                admin_path: bool = False,
            ) -> tuple[str, list[str], bool]:
                calls.append((token, original_uri, original_method, admin_path))
                return ("context-contributor", [], False)

        app = AsyncMock()
        middleware = BearerTokenMiddleware(app, resolver=ContextResolver())
        scope: dict[str, Any] = {
            "type": "http",
            "scheme": scheme,
            "server": server,
            "path": path,
            "method": method,
            "query_string": query,
            "headers": [
                (b"authorization", b"Bearer opaque.raw+bearer/=="),
                (b"original-uri", b"https://forged.example/ignored"),
                (b"x-original-uri", b"https://forged.example/ignored"),
                (b"x-original-method", b"HEAD"),
                (b"x-forwarded-host", b"forged.example"),
                (b"x-forwarded-proto", b"https"),
            ],
        }
        if host is not None:
            scope["headers"].append((b"host", host))
        if raw_path is not None:
            scope["raw_path"] = raw_path
        receive, send = AsyncMock(), AsyncMock()

        await middleware(scope, receive, send)

        assert scope.get("state", {}).get("contributor_id") == "context-contributor"
        assert calls == [("opaque.raw+bearer/==", uri, method, False)]
        app.assert_awaited_once_with(scope, receive, send)


class TestTokenValidatorFactory:
    async def test_injected_validator_revalidates_after_live_store_deletion(
        self, tmp_path: Path
    ) -> None:
        from context_intelligence_server.main import (
            create_asgi_app,
            get_entra_identity_store,
        )

        validator = _RequestTokenValidator()
        wrapped = create_asgi_app(
            settings=_token_validator_settings(tmp_path), _token_validator=validator
        )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=wrapped),
            base_url="https://ingest.example:8443",
        ) as client:
            headers = {"Authorization": "Bearer opaque.raw+bearer/=="}
            accepted = await client.get("/whoami?trace=a%2Fb", headers=headers)
            assert accepted.status_code == 200
            assert accepted.json() == {"contributor_id": "mapped-contributor"}
            store = get_entra_identity_store()
            assert store is not None
            store.delete(FAKE_OID_1.lower())
            denied = await client.get("/whoami?trace=a%2Fb", headers=headers)

        assert denied.status_code == 403
        expected_call = (
            "opaque.raw+bearer/==",
            "https://ingest.example:8443/whoami?trace=a%2Fb",
            "GET",
        )
        assert validator.calls == [expected_call, expected_call]

    @pytest.mark.parametrize("path", ["/whoami", "/admin/identities"])
    @pytest.mark.parametrize(
        "unexpected", [False, True], ids=["validation", "unexpected"]
    )
    async def test_validation_error_response_and_logs_are_sanitized(
        self,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
        path: str,
        unexpected: bool,
    ) -> None:
        import logging

        from context_intelligence_server.main import create_asgi_app
        from context_intelligence_server.mise_validation import TokenValidationError

        failure_type = RuntimeError if unexpected else TokenValidationError
        validator = _RequestTokenValidator(
            failure_type("native diagnostics containing opaque.raw+bearer/==")
        )
        settings = _token_validator_settings(tmp_path)
        settings.entra_identities = {}
        wrapped = create_asgi_app(settings=settings, _token_validator=validator)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=wrapped),
            base_url="https://ingest.example",
        ) as client:
            with caplog.at_level(
                logging.INFO, logger="context_intelligence_server.auth"
            ):
                response = await client.get(
                    path, headers={"Authorization": "Bearer opaque.raw+bearer/=="}
                )

        assert response.status_code == 401
        assert response.json() == {"detail": "Missing or invalid bearer token"}
        assert "opaque.raw+bearer/==" not in caplog.text
        assert "native diagnostics" not in caplog.text
        assert validator.calls == [
            ("opaque.raw+bearer/==", f"https://ingest.example{path}", "GET")
        ]

    @pytest.mark.parametrize(
        "failure",
        [
            ImportError("MISE initialization failed"),
            OSError("MISE initialization failed"),
            RuntimeError("MISE initialization failed"),
        ],
        ids=["import", "native", "configuration"],
    )
    def test_entra_factory_propagates_validator_construction_failure(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: Exception
    ) -> None:
        from context_intelligence_server import auth
        from context_intelligence_server.main import create_asgi_app

        def refuse_start(client_id: str, tenant_id: str) -> None:
            assert (client_id, tenant_id) == (FAKE_CLIENT_ID, FAKE_TENANT_ID)
            raise failure

        def refuse_network(*args: Any, **kwargs: Any) -> None:
            raise AssertionError("Unexpected legacy JWKS network access")

        monkeypatch.setattr(auth, "MiseTokenValidator", refuse_start, raising=False)
        monkeypatch.setattr("urllib.request.urlopen", refuse_network)
        with pytest.raises(
            type(failure), match="^MISE initialization failed$"
        ) as denied:
            create_asgi_app(settings=_token_validator_settings(tmp_path))

        assert denied.value is failure

    def test_real_adapter_import_failure_refuses_startup_without_diagnostics(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        from context_intelligence_server import mise_validation
        from context_intelligence_server.main import create_asgi_app

        def refuse_import(name: str) -> Any:
            assert name == "mise"
            raise ModuleNotFoundError("sensitive native import diagnostics")

        def refuse_network(*args: Any, **kwargs: Any) -> None:
            raise AssertionError("Unexpected legacy JWKS network access")

        monkeypatch.setattr(mise_validation, "import_module", refuse_import)
        monkeypatch.setattr("urllib.request.urlopen", refuse_network)
        with pytest.raises(
            RuntimeError, match="^MISE initialization failed$"
        ) as denied:
            create_asgi_app(settings=_token_validator_settings(tmp_path))

        assert denied.value.__suppress_context__ is True
        assert "sensitive native import diagnostics" not in caplog.text

    async def test_serving_entrypoint_loads_authenticated_asgi_wrapper(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from context_intelligence_server import main as main_module
        from context_intelligence_server.auth import BearerTokenMiddleware

        settings = _token_validator_settings(tmp_path)
        validator = _RequestTokenValidator()
        factory = main_module.create_asgi_app

        def build_app() -> BearerTokenMiddleware:
            return factory(settings=settings, _token_validator=validator)

        served: list[object] = []

        def load_without_listening(server: BaseApplication) -> None:
            served.append(server.load())

        monkeypatch.setenv("WEB_CONCURRENCY", "1")
        monkeypatch.setattr(main_module, "_settings", settings)
        monkeypatch.setattr(main_module, "_asgi_app", None)
        monkeypatch.setattr(main_module, "create_asgi_app", build_app)
        monkeypatch.setattr(BaseApplication, "run", load_without_listening)

        main_module.main(["serve"])

        assert len(served) == 1
        wrapped = served[0]
        assert wrapped is main_module.asgi_app
        assert isinstance(wrapped, BearerTokenMiddleware)
        assert wrapped.app is main_module.app
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=wrapped),
            base_url="https://ingest.example",
        ) as client:
            assert (await client.get("/whoami")).status_code == 401
            response = await client.get(
                "/whoami", headers={"Authorization": "Bearer opaque.raw+bearer/=="}
            )

        assert response.status_code == 200
        assert response.json() == {"contributor_id": "mapped-contributor"}
        assert validator.calls == [
            ("opaque.raw+bearer/==", "https://ingest.example/whoami", "GET")
        ]


class TestTokenValidatorSingletonRoles:
    """Scalar MISE claims must preserve existing policy, not prove authenticity."""

    @pytest.mark.parametrize(
        ("role", "write_status"), [("Reader", 403), ("Contributor", 202)]
    )
    async def test_service_singleton_role_preserves_read_and_write_capabilities(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        role: str,
        write_status: int,
    ) -> None:
        from context_intelligence_server import main as main_module

        validator = _RequestTokenValidator(
            claims={"tid": FAKE_TENANT_ID, "oid": FAKE_OID_1, "roles": role}
        )
        wrapped = main_module.create_asgi_app(
            settings=_token_validator_settings(tmp_path), _token_validator=validator
        )
        # Keep persistence real; suppress only the drainer that would contact Neo4j.
        monkeypatch.setattr(
            main_module.registry, "get_or_create", lambda *args, **kwargs: None
        )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=wrapped),
            base_url="https://ingest.example",
            headers={"Authorization": "Bearer opaque-bearer"},
        ) as client:
            read = await client.get("/whoami")
            assert read.status_code == 200
            assert read.json() == {"contributor_id": "mapped-contributor"}
            write = await client.post(
                "/events",
                json={
                    "event": "tool_use",
                    "workspace": "/ws",
                    "data": {
                        "session_id": "singleton-role-session",
                        "timestamp": "2026-10-20T00:00:00+00:00",
                    },
                },
            )

        assert write.status_code == write_status
        assert validator.calls == [
            ("opaque-bearer", "https://ingest.example/whoami", "GET"),
            ("opaque-bearer", "https://ingest.example/events", "POST"),
        ]

    @pytest.mark.parametrize("is_service", [False, True], ids=["delegated", "service"])
    async def test_singleton_identity_admin_reaches_admin_route(
        self, tmp_path: Path, is_service: bool
    ) -> None:
        from context_intelligence_server.main import create_asgi_app

        claims: dict[str, Any] = {
            "tid": FAKE_TENANT_ID,
            "oid": FAKE_OID_1,
            "roles": "IdentityAdmin",
        }
        settings = _token_validator_settings(tmp_path)
        if not is_service:
            claims["scp"] = "access_as_user"
            settings.entra_identities = {}  # Delegated bootstrap stays map-exempt.
        validator = _RequestTokenValidator(claims=claims)
        wrapped = create_asgi_app(settings=settings, _token_validator=validator)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=wrapped),
            base_url="https://ingest.example",
        ) as client:
            response = await client.get(
                "/admin/identities",
                headers={"Authorization": "Bearer opaque-bearer"},
            )

        assert response.status_code == 200
        assert "identities" in response.json()
        assert validator.calls == [
            ("opaque-bearer", "https://ingest.example/admin/identities", "GET")
        ]


class TestTokenValidatorUriComponents:
    @pytest.mark.parametrize(
        ("scope_fields", "host", "uri"),
        [
            pytest.param(
                {
                    "path": "/blobs/a?b",
                    "raw_path": b"/blobs/a%3Fb",
                    "query_string": b"q=one%3Ftwo&dup=1&dup=2",
                },
                b"ingest.example:8443",
                "https://ingest.example:8443/blobs/a%3Fb?q=one%3Ftwo&dup=1&dup=2",
                id="encoded-question",
            ),
            pytest.param(
                {
                    "path": "/blobs/a#b",
                    "raw_path": b"/blobs/a%23b",
                    "query_string": b"q=%23keep&empty=",
                },
                b"ingest.example:8443",
                "https://ingest.example:8443/blobs/a%23b?q=%23keep&empty=",
                id="encoded-fragment",
            ),
            pytest.param(
                {
                    "path": "/blobs/a?b#c d%/",
                    "raw_path": b"/blobs/a%3fb%23c%20d%25%2F",
                    "query_string": b"q=a%2fb&literal=%2523",
                },
                b"ingest.example",
                "https://ingest.example/blobs/a%3fb%23c%20d%25%2F?q=a%2fb&literal=%2523",
                id="raw-escapes-not-reparsed",
            ),
            pytest.param(
                {
                    "path": "/blobs/a?b#c d%/café",
                    "query_string": b"q=keep%2F&dup=1&dup=2",
                },
                b"ingest.example",
                "https://ingest.example/blobs/a%3Fb%23c%20d%25/caf%C3%A9?q=keep%2F&dup=1&dup=2",
                id="raw-path-absent",
            ),
            pytest.param(
                {
                    "path": "/blobs/a?b#c d%/café",
                    "raw_path": None,
                    "query_string": b"q=keep%2F&dup=1&dup=2",
                },
                b"ingest.example",
                "https://ingest.example/blobs/a%3Fb%23c%20d%25/caf%C3%A9?q=keep%2F&dup=1&dup=2",
                id="raw-path-none",
            ),
            pytest.param(
                {"path": "/blobs/!$&'()*+,;=:@ test"},
                b"ingest.example",
                "https://ingest.example/blobs/!$&'()*+,;=:@%20test",
                id="decoded-path-pchars",
            ),
            pytest.param(
                {"server": None},
                b"Ingest.Example:443",
                "https://Ingest.Example:443/events",
                id="host-preserved-with-explicit-default-port",
            ),
            pytest.param(
                {"scheme": "http", "server": ("scope-server.example", 80)},
                None,
                "http://scope-server.example/events",
                id="http-default-port",
            ),
            pytest.param(
                {"server": ("scope-server.example", 443)},
                None,
                "https://scope-server.example/events",
                id="https-default-port",
            ),
            pytest.param(
                {"scheme": "http", "server": ("scope-server.example", 443)},
                None,
                "http://scope-server.example:443/events",
                id="http-non-default-port",
            ),
            pytest.param(
                {"server": ("scope-server.example", 80)},
                None,
                "https://scope-server.example:80/events",
                id="https-non-default-port",
            ),
            pytest.param(
                {"server": ("2001:db8::1", 443)},
                None,
                "https://[2001:db8::1]/events",
                id="ipv6-default-port",
            ),
            pytest.param(
                {"server": ("2001:db8::1", 8443)},
                None,
                "https://[2001:db8::1]:8443/events",
                id="ipv6-non-default-port",
            ),
            pytest.param(
                {"server": ("2001:db8::1", None), "raw_path": None},
                None,
                "https://[2001:db8::1]/events",
                id="ipv6-no-port",
            ),
            pytest.param(
                {"server": ("[2001:db8::1]", 8443)},
                None,
                "https://[2001:db8::1]:8443/events",
                id="ipv6-already-bracketed",
            ),
            pytest.param(
                {"server": ("192.0.2.4", None)},
                None,
                "https://192.0.2.4/events",
                id="server-no-port",
            ),
        ],
    )
    async def test_validator_receives_exact_scope_components(
        self, scope_fields: dict[str, Any], host: bytes | None, uri: str
    ) -> None:
        from unittest.mock import AsyncMock

        from context_intelligence_server.auth import (
            BearerTokenMiddleware,
            EntraResolver,
        )

        validator = _RequestTokenValidator()
        resolver = EntraResolver(
            FAKE_CLIENT_ID,
            FAKE_TENANT_ID,
            {FAKE_OID_1.lower(): "mapped-contributor"},
            validator=validator,
        )
        app = AsyncMock()
        wrapped = BearerTokenMiddleware(app, resolver=resolver)
        scope: dict[str, Any] = {
            "type": "http",
            "scheme": "https",
            "server": ("backend.example", 8000),
            "method": "PATCH",
            "path": "/events",
            "query_string": b"",
            "headers": [
                (b"authorization", b"Bearer opaque.raw+bearer/=="),
                (b"original-uri", b"https://forged.example/ignored"),
                (b"x-original-uri", b"https://forged.example/ignored"),
                (b"x-forwarded-host", b"forged.example"),
                (b"x-forwarded-proto", b"http"),
            ],
        }
        scope.update(scope_fields)
        if host is not None:
            scope["headers"].append((b"host", host))
        receive, send = AsyncMock(), AsyncMock()

        await wrapped(scope, receive, send)

        assert validator.calls == [("opaque.raw+bearer/==", uri, "PATCH")]
        assert scope.get("state", {}).get("contributor_id") == "mapped-contributor"
        app.assert_awaited_once_with(scope, receive, send)
