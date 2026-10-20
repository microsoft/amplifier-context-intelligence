"""Safety properties at the application validator boundary (formerly JWKS).

The server no longer owns PyJWKClient cache/rotation/prefetch mechanics. Keep the
load-bearing guarantees here: fatal initialization, one validation per request,
no cached authorization after denial/outage, distinct log severity, and sanitized
errors. These application doubles do NOT prove cryptographic authenticity; that
requires the genuine SDK tier in test_mise_validation.py.
"""

from __future__ import annotations

import logging
import traceback
from typing import Any
from unittest.mock import AsyncMock

import pytest

from context_intelligence_server.auth import (
    AuthError,
    BearerTokenMiddleware,
    EntraResolver,
)
from context_intelligence_server.mise_validation import (
    TokenValidationError,
    TokenValidator,
)
from tests.conftest import ApplicationTokenValidator

FAKE_CLIENT_ID = "aaaabbbb-1111-2222-3333-ccccddddeeee"
FAKE_TENANT_ID = "ffffeeee-dddd-cccc-bbbb-aaaa99998888"
FAKE_OID = "11111111-2222-3333-4444-555566667777"
FAKE_IDENTITY_MAP = {FAKE_OID.lower(): "colombod"}
REQUEST_URI = "https://service.example.test/events?part=%2F"
PRIVATE_DIAGNOSTIC = "synthetic native diagnostic that must not escape"
FAKE_BEARER = "opaque-sensitive-test-handle"


def _claims() -> dict[str, Any]:
    return {"oid": FAKE_OID, "tid": FAKE_TENANT_ID, "scp": "access_as_user"}


def _make_entra_resolver(validator: TokenValidator) -> EntraResolver:
    return EntraResolver(
        client_id=FAKE_CLIENT_ID,
        tenant_id=FAKE_TENANT_ID,
        identity_map=FAKE_IDENTITY_MAP,
        validator=validator,
    )


class _FailingValidator:
    """An application-boundary failure, never a simulated SDK or JWT parser."""

    def __init__(self, failure: Exception) -> None:
        self.failure = failure
        self.calls: list[tuple[str, str, str]] = []

    def validate(
        self, bearer: str, *, original_uri: str, original_method: str
    ) -> dict[str, Any]:
        self.calls.append((bearer, original_uri, original_method))
        raise self.failure


async def _run_middleware_and_capture(
    resolver: EntraResolver,
    token: str = FAKE_BEARER,
    path: str = "/events",
) -> tuple[int, str, bool]:
    """Run the real middleware and capture its response and downstream dispatch."""
    dispatched: list[dict[str, Any]] = []

    async def downstream(scope: dict[str, Any], receive: Any, send: Any) -> None:
        dispatched.append(scope)
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    statuses: list[int] = []
    bodies: list[bytes] = []

    async def capture_send(event: dict[str, Any]) -> None:
        if event.get("type") == "http.response.start":
            statuses.append(event["status"])
        elif event.get("type") == "http.response.body":
            bodies.append(event["body"])

    scope = {
        "type": "http",
        "scheme": "https",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"part=%2F",
        "method": "POST",
        "headers": [
            (b"host", b"service.example.test"),
            (b"authorization", f"Bearer {token}".encode()),
        ],
    }
    middleware = BearerTokenMiddleware(downstream, resolver=resolver)
    await middleware(scope, AsyncMock(), capture_send)
    assert len(statuses) == 1
    return (
        statuses[0],
        b"".join(bodies).decode(),
        len(dispatched) == 1,
    )


class TestValidatorInitialization:
    def test_default_validator_is_constructed_once_and_reused(
        self,
        monkeypatch: pytest.MonkeyPatch,
        application_validator: ApplicationTokenValidator,
    ) -> None:
        """The resolver owns one configured validator, not one per request."""
        from context_intelligence_server import auth

        constructed: list[tuple[str, str]] = []

        def construct(client_id: str, tenant_id: str) -> ApplicationTokenValidator:
            constructed.append((client_id, tenant_id))
            return application_validator

        # A local factory-wiring probe, not a suite-wide production override.
        monkeypatch.setattr(auth, "MiseTokenValidator", construct)
        resolver = EntraResolver(FAKE_CLIENT_ID, FAKE_TENANT_ID, FAKE_IDENTITY_MAP)
        assert application_validator.calls == []
        bearer = application_validator.issue(_claims())
        for _ in range(2):
            assert resolver.resolve(
                bearer, original_uri=REQUEST_URI, original_method="POST"
            ) == ("colombod", [], False)
        assert constructed == [(FAKE_CLIENT_ID, FAKE_TENANT_ID)]
        assert application_validator.calls == [(bearer, REQUEST_URI, "POST")] * 2

    @pytest.mark.parametrize(
        "failure_type",
        [ImportError, OSError],
        ids=["sdk-missing", "native-load-failure"],
    )
    def test_initialization_failure_is_fatal_and_sanitized(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
        failure_type: type[Exception],
    ) -> None:
        """Use the real adapter's failure path; do not install a fake mise module."""
        from context_intelligence_server import mise_validation

        attempts: list[str] = []

        def unavailable(name: str) -> Any:
            attempts.append(name)
            raise failure_type(f"{PRIVATE_DIAGNOSTIC} {FAKE_BEARER}")

        monkeypatch.setattr(mise_validation, "import_module", unavailable)
        with caplog.at_level(logging.DEBUG), pytest.raises(RuntimeError) as failed:
            EntraResolver(FAKE_CLIENT_ID, FAKE_TENANT_ID, FAKE_IDENTITY_MAP)
        assert attempts == ["mise"]
        assert str(failed.value) == "MISE initialization failed"
        assert failed.value.__cause__ is None
        assert failed.value.__suppress_context__
        rendered = "".join(traceback.format_exception(failed.value))
        assert PRIVATE_DIAGNOSTIC not in rendered + caplog.text
        assert FAKE_BEARER not in rendered + caplog.text


class TestValidatorPerRequest:
    async def test_each_request_validates_once_with_its_uri_and_method(
        self, application_validator: ApplicationTokenValidator
    ) -> None:
        resolver = _make_entra_resolver(application_validator)
        bearer = application_validator.issue(_claims())
        assert application_validator.calls == []
        for path in ("/events", "/admin/identities"):
            status, _, called = await _run_middleware_and_capture(
                resolver, bearer, path=path
            )
            assert status == 200
            assert called
        assert application_validator.calls == [
            (bearer, REQUEST_URI, "POST"),
            (bearer, "https://service.example.test/admin/identities?part=%2F", "POST"),
        ]

    async def test_previous_success_is_not_cached_after_validator_denial(
        self, application_validator: ApplicationTokenValidator
    ) -> None:
        resolver = _make_entra_resolver(application_validator)
        bearer = application_validator.issue(_claims())
        assert await _run_middleware_and_capture(resolver, bearer) == (200, "", True)
        application_validator.reject(bearer)
        status, body, called = await _run_middleware_and_capture(resolver, bearer)
        assert status == 401
        assert not called
        assert "Missing or invalid bearer token" in body
        assert application_validator.calls == [(bearer, REQUEST_URI, "POST")] * 2

    async def test_previous_success_is_not_cached_during_validator_outage(
        self,
        monkeypatch: pytest.MonkeyPatch,
        application_validator: ApplicationTokenValidator,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        resolver = _make_entra_resolver(application_validator)
        bearer = application_validator.issue(_claims())
        assert await _run_middleware_and_capture(resolver, bearer) == (200, "", True)

        outage = _FailingValidator(
            TokenValidationError(f"{PRIVATE_DIAGNOSTIC} {bearer}")
        )
        monkeypatch.setattr(application_validator, "validate", outage.validate)
        with caplog.at_level(logging.INFO, logger="context_intelligence_server.auth"):
            status, body, called = await _run_middleware_and_capture(resolver, bearer)
        assert status == 401
        assert not called
        assert application_validator.calls == [(bearer, REQUEST_URI, "POST")]
        assert outage.calls == [(bearer, REQUEST_URI, "POST")]
        assert bearer not in body + caplog.text
        assert PRIVATE_DIAGNOSTIC not in body + caplog.text
        assert "auth_denied" in caplog.text

    @pytest.mark.parametrize("path", ["/status", "/version", "/docs", "/openapi.json"])
    async def test_exact_exempt_paths_do_not_call_validator(
        self, application_validator: ApplicationTokenValidator, path: str
    ) -> None:
        resolver = _make_entra_resolver(application_validator)
        assert await _run_middleware_and_capture(resolver, path=path) == (200, "", True)
        assert application_validator.calls == []

    @pytest.mark.parametrize(
        "path",
        ["/status/extra", "/version/extra", "/docs/extra", "/openapi.json/extra"],
    )
    async def test_exempt_path_prefixes_still_validate(
        self, application_validator: ApplicationTokenValidator, path: str
    ) -> None:
        resolver = _make_entra_resolver(application_validator)
        status, _, called = await _run_middleware_and_capture(resolver, path=path)
        assert status == 401
        assert not called
        assert application_validator.calls == [
            (FAKE_BEARER, f"https://service.example.test{path}?part=%2F", "POST")
        ]


class TestDistinctLogTags:
    async def test_catch_all_logs_resolver_unexpected_exception_tag(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        validator = _FailingValidator(
            RuntimeError(f"{PRIVATE_DIAGNOSTIC} {FAKE_BEARER}")
        )
        with caplog.at_level(logging.ERROR, logger="context_intelligence_server.auth"):
            status, _, called = await _run_middleware_and_capture(
                _make_entra_resolver(validator)
            )
        assert status == 401
        assert not called
        assert validator.calls == [(FAKE_BEARER, REQUEST_URI, "POST")]
        unexpected = [
            r
            for r in caplog.records
            if "resolver_unexpected_exception" in r.getMessage()
        ]
        assert len(unexpected) == 1
        assert unexpected[0].levelno == logging.ERROR

    async def test_auth_error_logs_auth_denied_tag(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        validator = _FailingValidator(TokenValidationError(PRIVATE_DIAGNOSTIC))
        with caplog.at_level(logging.DEBUG, logger="context_intelligence_server.auth"):
            status, _, called = await _run_middleware_and_capture(
                _make_entra_resolver(validator)
            )
        assert status == 401
        assert not called
        assert validator.calls == [(FAKE_BEARER, REQUEST_URI, "POST")]
        assert "auth_denied" in caplog.text
        assert "resolver_unexpected_exception" not in caplog.text

    async def test_auth_error_log_is_below_error_level(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        validator = _FailingValidator(TokenValidationError(PRIVATE_DIAGNOSTIC))
        with caplog.at_level(logging.DEBUG, logger="context_intelligence_server.auth"):
            status, _, called = await _run_middleware_and_capture(
                _make_entra_resolver(validator)
            )
        assert status == 401
        assert not called
        denied = [r for r in caplog.records if "auth_denied" in r.getMessage()]
        assert denied
        assert all(r.levelno < logging.ERROR for r in denied)

    @pytest.mark.parametrize(
        "failure_type",
        [TokenValidationError, ConnectionError, RuntimeError],
        ids=["validator-denial", "unexpected-outage", "unexpected-bug"],
    )
    async def test_raw_token_not_in_errors_logs_or_tracebacks(
        self,
        caplog: pytest.LogCaptureFixture,
        failure_type: type[Exception],
    ) -> None:
        validator = _FailingValidator(
            failure_type(f"{PRIVATE_DIAGNOSTIC} {FAKE_BEARER}")
        )
        with caplog.at_level(logging.DEBUG, logger="context_intelligence_server.auth"):
            status, body, called = await _run_middleware_and_capture(
                _make_entra_resolver(validator)
            )
        assert status == 401
        assert not called
        assert body == '{"detail": "Missing or invalid bearer token"}'
        assert validator.calls == [(FAKE_BEARER, REQUEST_URI, "POST")]
        assert FAKE_BEARER not in body + caplog.text
        assert PRIVATE_DIAGNOSTIC not in body + caplog.text
        assert caplog.records
        assert all(r.exc_info is None and r.stack_info is None for r in caplog.records)


class TestValidatorPostStartupFailClosed:
    def test_validator_denial_becomes_sanitized_auth_error_401(self) -> None:
        validator = _FailingValidator(
            TokenValidationError(f"{PRIVATE_DIAGNOSTIC} {FAKE_BEARER}")
        )
        resolver = _make_entra_resolver(validator)
        with pytest.raises(AuthError) as denied:
            resolver.resolve(
                FAKE_BEARER, original_uri=REQUEST_URI, original_method="POST"
            )
        assert denied.value.status_code == 401
        assert denied.value.reason == "Missing or invalid bearer token"
        assert denied.value.__cause__ is None
        assert denied.value.__suppress_context__
        rendered = "".join(traceback.format_exception(denied.value))
        assert PRIVATE_DIAGNOSTIC not in rendered
        assert FAKE_BEARER not in rendered
        assert validator.calls == [(FAKE_BEARER, REQUEST_URI, "POST")]

    async def test_post_startup_outage_returns_http_401(self) -> None:
        validator = _FailingValidator(ConnectionError(PRIVATE_DIAGNOSTIC))
        status, _, called = await _run_middleware_and_capture(
            _make_entra_resolver(validator)
        )
        assert status == 401
        assert not called
        assert validator.calls == [(FAKE_BEARER, REQUEST_URI, "POST")]
