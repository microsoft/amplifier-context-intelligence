"""Entra application policy with explicit opaque-token validator injection.

These tests prove claim policy, status dispatch, and live identity mapping, NOT
cryptographic authenticity. Signature/algorithm/expiry/audience/issuer rejection
belongs to the genuine SDK tier in test_mise_validation.py. No JWT parser or
fallback authenticity validator is used here.
"""

from __future__ import annotations

import time
from typing import Any

import pytest

from context_intelligence_server.auth import AuthError, EntraResolver
from tests.conftest import ApplicationTokenValidator

# Synthetic constants only; never real application, tenant, or principal IDs.
FAKE_CLIENT_ID = "aaaabbbb-1111-2222-3333-ccccddddeeee"
FAKE_TENANT_ID = "ffffeeee-dddd-cccc-bbbb-aaaa99998888"
FAKE_OID_1 = "11111111-2222-3333-4444-555566667777"
FAKE_OID_2 = "22222222-3333-4444-5555-666677778888"
FAKE_CONTRIBUTOR = "colombod"
FAKE_ISSUER = f"https://login.microsoftonline.com/{FAKE_TENANT_ID}/v2.0"
FAKE_IDENTITY_MAP = {FAKE_OID_1.lower(): FAKE_CONTRIBUTOR}
FAKE_SERVICE_OID = "aaaabbbb-1111-1111-1111-ccccddddffff"
FAKE_APPID = "bbbbcccc-2222-3333-4444-eeeeffff0000"
FAKE_AZP = "ccccdddd-3333-4444-5555-ffff00001111"
FAKE_SERVICE_CONTRIBUTOR = "svc-contributor"
FAKE_SERVICE_MAP = {FAKE_SERVICE_OID.lower(): FAKE_SERVICE_CONTRIBUTOR}
REQUEST_URI = "https://service.example.test/events?part=%2F"


def test_application_validator_detaches_claims_and_records_request_context() -> None:
    validator = ApplicationTokenValidator()
    claims: dict[str, Any] = {
        "oid": FAKE_OID_1,
        "roles": ["Reader"],
        "nested": {"values": [1]},
    }
    bearer = validator.issue(claims)
    assert bearer != validator.issue(claims)
    assert "." not in bearer
    claims["roles"].append("Contributor")
    claims["nested"]["values"].append(2)

    context = {"original_uri": REQUEST_URI, "original_method": "POST"}
    detached = validator.validate(bearer, **context)
    assert detached == {
        "oid": FAKE_OID_1,
        "roles": ["Reader"],
        "nested": {"values": [1]},
    }
    detached["roles"].append("IdentityAdmin")
    detached["nested"]["values"].clear()
    assert validator.validate(bearer, **context) == {
        "oid": FAKE_OID_1,
        "roles": ["Reader"],
        "nested": {"values": [1]},
    }
    assert validator.calls == [(bearer, REQUEST_URI, "POST")] * 2


def test_application_validator_denies_unknown_and_rejected_handles() -> None:
    from context_intelligence_server.mise_validation import TokenValidationError

    validator = ApplicationTokenValidator()
    bearer = validator.issue({"oid": FAKE_OID_1})
    validator.reject(bearer)
    for denied in ("unknown-handle", bearer):
        with pytest.raises(TokenValidationError):
            validator.validate(denied, original_uri=REQUEST_URI, original_method="POST")
    assert validator.calls == [
        ("unknown-handle", REQUEST_URI, "POST"),
        (bearer, REQUEST_URI, "POST"),
    ]


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


def _resolve_claims(
    claims: dict[str, Any],
    identity_map: dict[str, str] | None = None,
    *,
    service_map: dict[str, str] | None = None,
    service_data_role: str = "",
    reader_role: str = "",
    entra_admin_role: str = "",
) -> tuple[str, list[str], bool]:
    """Construct the real resolver with an explicit application-only validator."""
    validator = ApplicationTokenValidator()
    resolver = EntraResolver(
        FAKE_CLIENT_ID,
        FAKE_TENANT_ID,
        identity_map if identity_map is not None else FAKE_IDENTITY_MAP,
        service_identity_map=service_map,
        service_data_role=service_data_role,
        reader_role=reader_role,
        entra_admin_role=entra_admin_role,
        validator=validator,
    )
    return resolver.resolve(
        validator.issue(claims), original_uri=REQUEST_URI, original_method="POST"
    )


def _service_claims(
    oid: str = FAKE_SERVICE_OID,
    tid: str = FAKE_TENANT_ID,
    roles: list[str] | None = None,
    appid: str | None = None,
    azp: str | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    claims = _valid_claims(oid=oid, tid=tid)
    del claims["scp"]
    if roles is not None:
        claims["roles"] = roles
    if appid is not None:
        claims["appid"] = appid
    if azp is not None:
        claims["azp"] = azp
    if extra:
        claims.update(extra)
    return claims


def _resolve_service(
    claims: dict[str, Any],
    service_map: dict[str, str] | None = None,
    *,
    service_data_role: str = "Contributor",
    reader_role: str = "Reader",
    entra_admin_role: str = "IdentityAdmin",
) -> tuple[str, list[str], bool]:
    return _resolve_claims(
        claims,
        service_map=service_map,
        service_data_role=service_data_role,
        reader_role=reader_role,
        entra_admin_role=entra_admin_role,
    )


class TestEntraResolverApplicationPolicy:
    """Validated claims still enforce the delegated-user 401/403 matrix."""

    def test_valid_claims_returns_contributor(self) -> None:
        assert _resolve_claims(_valid_claims()) == (FAKE_CONTRIBUTOR, [], False)

    @pytest.mark.parametrize(
        "oid", [FAKE_OID_1, "aabbccdd-1122-3344-5566-778899aabbcc"]
    )
    def test_oid_lookup_is_case_insensitive(self, oid: str) -> None:
        result = _resolve_claims(
            _valid_claims(oid=oid.upper()),
            identity_map={oid.lower(): FAKE_CONTRIBUTOR},
        )
        assert result == (FAKE_CONTRIBUTOR, [], False)

    def test_unmapped_oid_raises_auth_error_403(self) -> None:
        with pytest.raises(AuthError) as denied:
            _resolve_claims(_valid_claims(oid=FAKE_OID_2))
        assert denied.value.status_code == 403

    def test_403_reason_names_the_oid(self) -> None:
        with pytest.raises(AuthError) as denied:
            _resolve_claims(_valid_claims(oid=FAKE_OID_2))
        assert denied.value.status_code == 403
        assert FAKE_OID_2.lower() in denied.value.reason.lower()

    def test_missing_oid_raises_auth_error_401(self) -> None:
        claims = _valid_claims()
        del claims["oid"]
        with pytest.raises(AuthError) as denied:
            _resolve_claims(claims)
        assert denied.value.status_code == 401

    def test_empty_oid_raises_auth_error_401(self) -> None:
        with pytest.raises(AuthError) as denied:
            _resolve_claims(_valid_claims(oid=""))
        assert denied.value.status_code == 401

    @pytest.mark.parametrize("tid", [None, "", "other-tenant", FAKE_TENANT_ID.upper()])
    def test_wrong_tid_raises_auth_error_401(self, tid: Any) -> None:
        claims = {**_valid_claims(), "tid": tid}
        with pytest.raises(AuthError) as denied:
            _resolve_claims(claims)
        assert denied.value.status_code == 401
        assert denied.value.reason == "Token from wrong tenant"

    def test_missing_scp_raises_auth_error_403(self) -> None:
        """No delegated scope selects service policy; no qualifying role is 403."""
        claims = _valid_claims()
        del claims["scp"]
        with pytest.raises(AuthError) as denied:
            _resolve_claims(claims)
        assert denied.value.status_code == 403

    def test_wrong_scp_raises_auth_error_401(self) -> None:
        with pytest.raises(AuthError) as denied:
            _resolve_claims(_valid_claims(scp="openid profile"))
        assert denied.value.status_code == 401

    def test_scp_with_access_as_user_among_others_succeeds(self) -> None:
        result = _resolve_claims(_valid_claims(scp="openid access_as_user profile"))
        assert result == (FAKE_CONTRIBUTOR, [], False)

    @pytest.mark.parametrize(
        "reject_issued", [False, True], ids=["unknown", "rejected"]
    )
    def test_validator_denial_raises_auth_error_401(
        self, application_validator: ApplicationTokenValidator, reject_issued: bool
    ) -> None:
        bearer = (
            application_validator.issue(_valid_claims())
            if reject_issued
            else "unknown-handle"
        )
        application_validator.reject(bearer)
        resolver = EntraResolver(
            FAKE_CLIENT_ID,
            FAKE_TENANT_ID,
            FAKE_IDENTITY_MAP,
            validator=application_validator,
        )
        with pytest.raises(AuthError) as denied:
            resolver.resolve(bearer, original_uri=REQUEST_URI, original_method="POST")
        assert denied.value.status_code == 401
        assert denied.value.reason == "Missing or invalid bearer token"
        assert application_validator.calls == [(bearer, REQUEST_URI, "POST")]


class TestMiddlewareAuthError:
    """The real resolver/middleware preserves denial status and static behavior."""

    async def _call_middleware(
        self, resolver: Any, token: str | None = "bearer-token"
    ) -> int:
        from unittest.mock import AsyncMock

        from context_intelligence_server.auth import BearerTokenMiddleware

        app = AsyncMock()
        received_status: list[int] = []

        async def capture_send(event: dict[str, Any]) -> None:
            if event.get("type") == "http.response.start":
                received_status.append(event["status"])

        headers = [(b"authorization", f"Bearer {token}".encode())] if token else []
        scope = {
            "type": "http",
            "path": "/events",
            "method": "POST",
            "headers": headers,
        }
        middleware = BearerTokenMiddleware(app, resolver=resolver)
        await middleware(scope, AsyncMock(), capture_send)
        return received_status[0] if received_status else 200

    async def test_auth_error_401_returns_http_401(
        self, application_validator: ApplicationTokenValidator
    ) -> None:
        resolver = EntraResolver(
            FAKE_CLIENT_ID,
            FAKE_TENANT_ID,
            FAKE_IDENTITY_MAP,
            validator=application_validator,
        )
        assert await self._call_middleware(resolver) == 401

    async def test_auth_error_403_returns_http_403(
        self, application_validator: ApplicationTokenValidator
    ) -> None:
        bearer = application_validator.issue(_valid_claims(oid=FAKE_OID_2))
        resolver = EntraResolver(
            FAKE_CLIENT_ID,
            FAKE_TENANT_ID,
            FAKE_IDENTITY_MAP,
            validator=application_validator,
        )
        assert await self._call_middleware(resolver, bearer) == 403

    async def test_resolver_returning_none_still_401(self) -> None:
        from context_intelligence_server.auth import StaticKeyResolver

        resolver = StaticKeyResolver({"some-digest": "alice"})
        assert await self._call_middleware(resolver, token=None) == 401

    async def test_static_resolver_wrong_token_still_401(self) -> None:
        from context_intelligence_server.auth import StaticKeyResolver

        resolver = StaticKeyResolver({"some-digest": "alice"})
        assert await self._call_middleware(resolver, token="wrong-token") == 401

    async def test_unexpected_resolver_exception_returns_401(self) -> None:
        calls: list[tuple[str, str, str, bool]] = []

        class _BrokenResolver:
            @property
            def auth_enabled(self) -> bool:
                return True

            def resolve(
                self,
                token: str,
                *,
                original_uri: str,
                original_method: str,
                admin_path: bool = False,
            ) -> None:
                calls.append((token, original_uri, original_method, admin_path))
                raise RuntimeError("unexpected internal error — simulated bug")

        assert await self._call_middleware(_BrokenResolver()) == 401
        assert calls == [("bearer-token", "/events", "POST", False)]


class TestEntraResolverBugRepros:
    """Malformed claims are policy denials, never accidental Python exceptions."""

    @pytest.mark.parametrize(
        "oid",
        [42, [FAKE_OID_1], "   ", "\t"],
        ids=["integer", "list", "spaces", "tab"],
    )
    def test_invalid_oid_raises_401_not_attribute_error_or_403(self, oid: Any) -> None:
        with pytest.raises(AuthError) as denied:
            _resolve_claims({**_valid_claims(), "oid": oid})
        assert denied.value.status_code == 401

    @pytest.mark.parametrize(
        "scp",
        [["access_as_user"], 42, None, "", " \t "],
        ids=["list", "integer", "null", "empty", "whitespace"],
    )
    def test_invalid_or_empty_scp_selects_unprivileged_service_branch(
        self, scp: Any
    ) -> None:
        with pytest.raises(AuthError) as denied:
            _resolve_claims({**_valid_claims(), "scp": scp})
        assert denied.value.status_code == 403


class TestEntraResolverScpCoverage:
    @pytest.mark.parametrize("scp", ["access_as_user_admin", "xaccess_as_user"])
    def test_scope_substrings_do_not_grant_delegated_access(self, scp: str) -> None:
        with pytest.raises(AuthError) as denied:
            _resolve_claims(_valid_claims(scp=scp))
        assert denied.value.status_code == 401

    def test_app_only_token_with_unrecognized_role_is_rejected(self) -> None:
        claims = _valid_claims()
        del claims["scp"]
        claims["roles"] = ["Directory.Read.All"]
        with pytest.raises(AuthError) as denied:
            _resolve_claims(claims)
        assert denied.value.status_code == 403


class TestM2ServiceBranch:
    """Service authorization and attribution, after validator acceptance."""

    @pytest.mark.parametrize("idtyp", ["app", " App ", "APP", "\tApP\n"])
    def test_b1a_scp_and_idtyp_app_raises_401_ambiguous(self, idtyp: str) -> None:
        claims = {**_valid_claims(), "idtyp": idtyp}
        with pytest.raises(AuthError) as denied:
            _resolve_service(claims)
        assert denied.value.status_code == 401
        assert "Ambiguous" in denied.value.reason

    def test_b1b_no_scp_no_idtyp_service_admitted(self) -> None:
        result = _resolve_service(
            _service_claims(roles=["Contributor"], appid=FAKE_APPID),
            service_map=FAKE_SERVICE_MAP,
        )
        assert result == (FAKE_SERVICE_CONTRIBUTOR, ["Contributor"], True)

    def test_b2a_idtyp_mixed_case_space_service_branch(self) -> None:
        result = _resolve_service(
            _service_claims(roles=["Reader"], extra={"idtyp": " App "}),
            service_map=FAKE_SERVICE_MAP,
        )
        assert result == (FAKE_SERVICE_CONTRIBUTOR, ["Reader"], True)

    def test_b2b_idtyp_int_normalized_to_empty(self) -> None:
        result = _resolve_service(
            _service_claims(roles=["Contributor"], extra={"idtyp": 123}),
            service_map=FAKE_SERVICE_MAP,
        )
        assert result == (FAKE_SERVICE_CONTRIBUTOR, ["Contributor"], True)

    def test_b6a_service_map_wins_over_appid(self) -> None:
        result = _resolve_service(
            _service_claims(roles=["Contributor"], appid=FAKE_APPID, azp=FAKE_AZP),
            service_map=FAKE_SERVICE_MAP,
        )
        assert result == (FAKE_SERVICE_CONTRIBUTOR, ["Contributor"], True)

    def test_b6b_unmapped_oid_blank_appid_raises_403(self) -> None:
        with pytest.raises(AuthError) as denied:
            _resolve_service(
                _service_claims(roles=["Reader"], appid="  ", azp=FAKE_AZP)
            )
        assert denied.value.status_code == 403
        assert FAKE_SERVICE_OID.lower() in denied.value.reason.lower()
        assert FAKE_AZP not in denied.value.reason

    def test_b6c_unmapped_oid_no_appid_no_azp_raises_403(self) -> None:
        with pytest.raises(AuthError) as denied:
            _resolve_service(_service_claims(roles=["Contributor"]))
        assert denied.value.status_code == 403
        assert FAKE_SERVICE_OID.lower() in denied.value.reason.lower()

    def test_b8_anti_spoof_app_displayname_not_used(self) -> None:
        display_name = "spoofed-user@example.test"
        with pytest.raises(AuthError) as denied:
            _resolve_service(
                _service_claims(
                    roles=["Contributor"],
                    appid=FAKE_APPID,
                    extra={"app_displayname": display_name},
                )
            )
        assert denied.value.status_code == 403
        assert display_name not in denied.value.reason

    @pytest.mark.parametrize("mapped", [False, True], ids=["unbound", "mapped"])
    def test_validator_denial_precedes_service_policy(
        self, application_validator: ApplicationTokenValidator, mapped: bool
    ) -> None:
        """No role or identity-map state can turn authenticity denial into 403/200."""
        bearer = application_validator.issue(
            _service_claims(roles=["Contributor"], appid=FAKE_APPID)
        )
        application_validator.reject(bearer)
        resolver = EntraResolver(
            FAKE_CLIENT_ID,
            FAKE_TENANT_ID,
            FAKE_IDENTITY_MAP,
            service_identity_map=FAKE_SERVICE_MAP if mapped else {},
            service_data_role="Contributor",
            validator=application_validator,
        )
        with pytest.raises(AuthError) as denied:
            resolver.resolve(bearer, original_uri=REQUEST_URI, original_method="POST")
        assert denied.value.status_code == 401
        assert application_validator.calls == [(bearer, REQUEST_URI, "POST")]

    def test_rg_none_no_roles_raises_403_naming_roles(self) -> None:
        with pytest.raises(AuthError) as denied:
            _resolve_service(_service_claims(roles=[]))
        assert denied.value.status_code == 403
        assert "Contributor" in denied.value.reason
        assert "Reader" in denied.value.reason
        assert FAKE_SERVICE_OID in denied.value.reason
        assert "roles=" not in denied.value.reason

    def test_rg_unknown_unrecognized_role_raises_403(self) -> None:
        with pytest.raises(AuthError) as denied:
            _resolve_service(_service_claims(roles=["SomethingElse"]))
        assert denied.value.status_code == 403

    def test_rg_reader_admitted(self) -> None:
        result = _resolve_service(
            _service_claims(roles=["Reader"], appid=FAKE_APPID),
            service_map=FAKE_SERVICE_MAP,
        )
        assert result == (FAKE_SERVICE_CONTRIBUTOR, ["Reader"], True)

    def test_rg_disabled_empty_reader_role_raises_403(self) -> None:
        with pytest.raises(AuthError) as denied:
            _resolve_service(
                _service_claims(roles=["Reader"], appid=FAKE_APPID),
                service_map=FAKE_SERVICE_MAP,
                reader_role="",
            )
        assert denied.value.status_code == 403

    def test_sv_adm_identity_admin_admitted(self) -> None:
        result = _resolve_service(
            _service_claims(roles=["IdentityAdmin"], appid=FAKE_APPID),
            service_map=FAKE_SERVICE_MAP,
        )
        assert result == (FAKE_SERVICE_CONTRIBUTOR, ["IdentityAdmin"], True)

    def test_hr1_human_unmapped_oid_raises_403(self) -> None:
        with pytest.raises(AuthError) as denied:
            _resolve_service(_valid_claims(oid=FAKE_OID_2))
        assert denied.value.status_code == 403
        assert FAKE_OID_2.lower() in denied.value.reason.lower()

    def test_hr2_human_happy_path_returns_false_is_service(self) -> None:
        result = _resolve_service({**_valid_claims(), "roles": ["x"]})
        assert result == (FAKE_CONTRIBUTOR, ["x"], False)

    def test_hr3_human_bad_scp_stays_in_user_branch_401(self) -> None:
        with pytest.raises(AuthError) as denied:
            _resolve_service(_valid_claims(scp="other"))
        assert denied.value.status_code == 401

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


class TestM2ServiceBranchEdgeCases:
    def test_b1_collision_oid_in_human_map_routes_to_service_branch(self) -> None:
        result = _resolve_service(
            _service_claims(oid=FAKE_OID_1, roles=["Contributor"], appid=FAKE_APPID),
            service_map={FAKE_OID_1.lower(): "svc-identity"},
        )
        cid, roles, is_service = result
        assert is_service is True
        assert cid == "svc-identity"
        assert FAKE_CONTRIBUTOR not in [cid]
        assert roles == ["Contributor"]

    def test_foreign_tid_service_token_raises_401(self) -> None:
        with pytest.raises(AuthError) as denied:
            _resolve_service(
                _service_claims(
                    tid="00000000-0000-0000-0000-999999999999",
                    roles=["Contributor"],
                    appid=FAKE_APPID,
                )
            )
        assert denied.value.status_code == 401

    def test_roles_lowercase_case_variant_raises_403(self) -> None:
        with pytest.raises(AuthError) as denied:
            _resolve_service(
                _service_claims(roles=["contributor"], appid=FAKE_APPID),
                service_map=FAKE_SERVICE_MAP,
            )
        assert denied.value.status_code == 403

    @pytest.mark.parametrize("roles", [42, {"Contributor": True}, ("Contributor",)])
    def test_roles_non_string_nonlist_normalized_to_empty_list_403(
        self, roles: Any
    ) -> None:
        """Unsupported role shapes stay unprivileged; MISE scalar strings are supported."""
        claims = {**_service_claims(appid=FAKE_APPID), "roles": roles}
        with pytest.raises(AuthError) as denied:
            _resolve_service(claims, service_map=FAKE_SERVICE_MAP)
        assert denied.value.status_code == 403


class _ClaimTokenValidator:
    """Application-boundary double; does not validate cryptographic authenticity."""

    def __init__(
        self, claims: dict[str, Any], failure: Exception | None = None
    ) -> None:
        self.claims = claims
        self.failure = failure
        self.calls: list[tuple[str, str, str]] = []

    def validate(
        self, bearer: str, *, original_uri: str, original_method: str
    ) -> dict[str, Any]:
        self.calls.append((bearer, original_uri, original_method))
        if self.failure is not None:
            raise self.failure
        return dict(self.claims)


class TestEntraResolverTokenValidator:
    @pytest.mark.parametrize("is_service", [False, True], ids=["delegated", "service"])
    def test_revalidates_same_token_against_live_identity_map(
        self, is_service: bool
    ) -> None:
        from context_intelligence_server.auth import AuthError, EntraResolver

        identities: dict[str, str] = {}
        claims = _valid_claims()
        roles = ["Contributor"] if is_service else ["IdentityAdmin"]
        claims["roles"] = roles
        if is_service:
            del claims["scp"]
            claims["idtyp"] = " App "
        validator = _ClaimTokenValidator(claims)
        resolver = EntraResolver(
            FAKE_CLIENT_ID,
            FAKE_TENANT_ID,
            identities,
            service_identity_map=identities,
            service_data_role="Contributor",
            validator=validator,
        )
        identities[FAKE_OID_1.lower()] = "mapped-contributor"
        uri = "https://ingest.example:8443/whoami?trace=a%2Fb&trace=2"

        assert resolver.resolve(
            "opaque.raw+bearer/==", original_uri=uri, original_method="GET"
        ) == (
            "mapped-contributor",
            roles,
            is_service,
        )
        del identities[FAKE_OID_1.lower()]
        with pytest.raises(AuthError) as denied:
            resolver.resolve(
                "opaque.raw+bearer/==", original_uri=uri, original_method="GET"
            )

        assert denied.value.status_code == 403
        expected_call = ("opaque.raw+bearer/==", uri, "GET")
        assert validator.calls == [expected_call, expected_call]

    @pytest.mark.parametrize("admin_path", [False, True], ids=["data", "admin"])
    def test_validation_failure_is_sanitized_401_even_on_admin_path(
        self, admin_path: bool
    ) -> None:
        from context_intelligence_server.auth import AuthError, EntraResolver
        from context_intelligence_server.mise_validation import TokenValidationError

        validator = _ClaimTokenValidator(
            _valid_claims(),
            TokenValidationError("native diagnostics containing opaque.raw+bearer/=="),
        )
        resolver = EntraResolver(
            FAKE_CLIENT_ID, FAKE_TENANT_ID, {}, validator=validator
        )

        with pytest.raises(AuthError) as denied:
            resolver.resolve(
                "opaque.raw+bearer/==",
                admin_path=admin_path,
                original_uri="https://ingest.example/admin/identities",
                original_method="GET",
            )

        assert denied.value.status_code == 401
        assert denied.value.reason == "Missing or invalid bearer token"
        assert denied.value.__suppress_context__ is True
        assert len(validator.calls) == 1

    @pytest.mark.parametrize(
        "overrides",
        [
            {"tid": "other-tenant"},
            {"scp": "openid profile"},
            {"idtyp": " App "},
            {"oid": None},
        ],
        ids=["tenant", "scope", "ambiguous-app", "missing-oid"],
    )
    def test_validated_claims_still_enforce_existing_user_checks(
        self, overrides: dict[str, Any]
    ) -> None:
        from context_intelligence_server.auth import AuthError, EntraResolver

        claims = _valid_claims()
        claims.update(overrides)
        validator = _ClaimTokenValidator(claims)
        resolver = EntraResolver(
            FAKE_CLIENT_ID,
            FAKE_TENANT_ID,
            dict(FAKE_IDENTITY_MAP),
            validator=validator,
        )

        with pytest.raises(AuthError) as denied:
            resolver.resolve(
                "opaque-bearer",
                original_uri="https://ingest.example/events",
                original_method="POST",
            )

        assert denied.value.status_code == 401
        assert len(validator.calls) == 1

    def test_static_resolver_ignores_request_context(self) -> None:
        import hashlib

        from context_intelligence_server.auth import StaticKeyResolver

        digest = hashlib.sha256(b"static-context-token").hexdigest()
        resolver = StaticKeyResolver({digest: "static-contributor"})
        assert resolver.resolve(
            "static-context-token",
            original_uri="https://ingest.example/whoami?source=test",
            original_method="GET",
        ) == ("static-contributor", [], False)


class TestEntraResolverRoleClaimCompatibility:
    @pytest.mark.parametrize("is_service", [False, True], ids=["delegated", "service"])
    def test_repeated_roles_keep_their_order(self, is_service: bool) -> None:
        from context_intelligence_server.auth import EntraResolver

        claims = _valid_claims()
        claims["roles"] = ["IdentityAdmin", "Reader", "Contributor", "Reader"]
        if is_service:
            del claims["scp"]
        identities = {FAKE_OID_1.lower(): "mapped-contributor"}
        resolver = EntraResolver(
            FAKE_CLIENT_ID,
            FAKE_TENANT_ID,
            identities,
            service_identity_map=identities,
            service_data_role="Contributor",
            reader_role="Reader",
            entra_admin_role="IdentityAdmin",
            validator=_ClaimTokenValidator(claims),
        )

        assert resolver.resolve(
            "opaque-bearer",
            original_uri="https://ingest.example/whoami",
            original_method="GET",
        ) == (
            "mapped-contributor",
            ["IdentityAdmin", "Reader", "Contributor", "Reader"],
            is_service,
        )

    @pytest.mark.parametrize(
        "role_claims",
        [
            {},
            {"roles": None},
            {"roles": 42},
            {"roles": ("Contributor",)},
            {"roles": {"Contributor": True}},
            {"roles": [None, 42]},
            {"roles": ""},
            {"roles": "reader"},
            {"roles": "Reader Contributor"},
        ],
        ids=[
            "absent",
            "null",
            "integer",
            "tuple",
            "mapping",
            "non-string-list",
            "empty-string",
            "wrong-case",
            "not-space-separated",
        ],
    )
    def test_missing_or_malformed_roles_stay_unprivileged(
        self, role_claims: dict[str, Any]
    ) -> None:
        from context_intelligence_server.auth import AuthError, EntraResolver

        claims = {"tid": FAKE_TENANT_ID, "oid": FAKE_OID_1, **role_claims}
        identities = {FAKE_OID_1.lower(): "mapped-contributor"}
        resolver = EntraResolver(
            FAKE_CLIENT_ID,
            FAKE_TENANT_ID,
            identities,
            service_identity_map=identities,
            service_data_role="Contributor",
            reader_role="Reader",
            entra_admin_role="IdentityAdmin",
            validator=_ClaimTokenValidator(claims),
        )

        with pytest.raises(AuthError) as denied:
            resolver.resolve(
                "opaque-bearer",
                original_uri="https://ingest.example/whoami",
                original_method="GET",
            )

        assert denied.value.status_code == 403
