"""Adapter-logic doubles are not evidence of token authenticity.

SDK-free checks explicitly select ``-m 'not mise'``. The ``mise`` tier requires
the genuine 2.7.0 distribution, Linux x86_64 and ICU; missing prerequisites fail.
It proves native configuration/rejection only, not positive live-token validation.
"""

import json
import platform
import subprocess
import sys
import traceback
from importlib import import_module
from importlib.metadata import version
from types import SimpleNamespace
from typing import Any, Self

import pytest

CLIENT_ID = "aaaaaaaa-0000-0000-0000-000000000001"
TENANT_ID = "bbbbbbbb-0000-0000-0000-000000000002"
NATIVE_DETAIL = f"private native diagnostic {CLIENT_ID} {TENANT_ID}"
REQUEST_URI = "https://service.example.test/events?part=%2F&part=two"


class _OwnedResult:
    def __init__(self) -> None:
        self.active = False
        self.closed = False
        self.exit_error: Exception | None = None

    def __enter__(self) -> Self:
        self.active = True
        return self

    def __exit__(self, *args: object) -> None:
        self.active = False
        self.closed = True
        if self.exit_error is not None:
            raise self.exit_error

    @property
    def error_description(self) -> str:
        raise AssertionError("The adapter must not read native diagnostics")


class _ControlResult(_OwnedResult):
    def __init__(self, error_code: int = 0) -> None:
        super().__init__()
        self._error_code = error_code

    @property
    def error_code(self) -> int:
        assert self.active, "Control result must be read inside its context"
        return self._error_code


class _Claim:
    def __init__(self, owner: "_ValidationResult", name: Any, value: Any) -> None:
        self.owner = owner
        self._name = name
        self._value = value

    @property
    def name(self) -> Any:
        assert self.owner.active, "Claim name must be copied before release"
        return self._name

    @property
    def value(self) -> Any:
        assert self.owner.active, "Claim value must be copied before release"
        return self._value


class _ValidationResult(_OwnedResult):
    def __init__(
        self,
        status: int = 200,
        pairs: list[tuple[Any, Any]] | None = None,
    ) -> None:
        super().__init__()
        self.status = status
        self.pairs = pairs if pairs is not None else [("oid", "subject")]
        self.claims_reads = 0
        self.status_error: Exception | None = None
        self.claims_error: Exception | None = None

    @property
    def http_response_status_code(self) -> int:
        assert self.active, "Validation result must be read inside its context"
        if self.status_error is not None:
            raise self.status_error
        return self.status

    @property
    def subject_claims(self) -> list[_Claim]:
        assert self.active, "Subject claims must be read inside the result context"
        self.claims_reads += 1
        if self.claims_error is not None:
            raise self.claims_error
        return [_Claim(self, name, value) for name, value in self.pairs]

    @property
    def actor_claims(self) -> list[_Claim]:
        raise AssertionError("Actor claims must never supply subject identity")


class _Client:
    def __init__(self) -> None:
        self.control = _ControlResult()
        self.configure_calls: list[tuple[str, str]] = []
        self.result = _ValidationResult()
        self.validation_inputs: list[SimpleNamespace] = []

    def configure(
        self, configuration_json: str, configuration_section: str
    ) -> _ControlResult:
        self.configure_calls.append((configuration_json, configuration_section))
        return self.control

    def validate(self, *, validation_input: SimpleNamespace) -> _ValidationResult:
        assert self.control.closed, "Configuration must finish before validation"
        self.validation_inputs.append(validation_input)
        return self.result


class _Sdk:
    def __init__(self) -> None:
        self.client = _Client()
        self.clients_created = 0
        self.inputs_created = 0

    def Mise(self) -> _Client:
        self.clients_created += 1
        return self.client

    def MiseValidationInput(self) -> SimpleNamespace:
        assert self.clients_created == 1, "Mise() must initialize native structures"
        self.inputs_created += 1
        return SimpleNamespace(
            authorization_header=None,
            original_uri_header=None,
            original_method_header=None,
        )


def _assert_sanitized(error: BaseException, message: str) -> None:
    assert str(error) == message
    assert error.__cause__ is None
    assert error.__suppress_context__
    rendered = "".join(traceback.format_exception(error))
    assert NATIVE_DETAIL not in rendered
    assert CLIENT_ID not in rendered
    assert TENANT_ID not in rendered


def test_constructor_configures_exact_single_tenant_inbound_policy() -> None:
    from context_intelligence_server.mise_validation import MiseTokenValidator

    sdk = _Sdk()
    MiseTokenValidator(CLIENT_ID, TENANT_ID, _mise_module=sdk)

    assert sdk.clients_created == 1
    assert len(sdk.client.configure_calls) == 1
    configuration_json, section = sdk.client.configure_calls[0]
    assert section == "AzureAd"
    assert json.loads(configuration_json) == {
        "MiseVersion": "2.0",
        "AzureAd": {
            "Instance": "https://login.microsoftonline.com/",
            "ClientId": CLIENT_ID,
            "TenantId": TENANT_ID,
            "ValidTenantIds": [TENANT_ID],
            "Audiences": [CLIENT_ID, f"api://{CLIENT_ID}"],
            "Protocols": {
                "Bearer": {
                    "TokenTypes": {"AccessToken": {"UserToken": True, "AppToken": True}}
                }
            },
        },
    }
    assert sdk.client.control.closed


def test_module_import_does_not_load_mise() -> None:
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys\n"
                "from context_intelligence_server.mise_validation import "
                "MiseTokenValidator, TokenValidationError, TokenValidator\n"
                "assert 'mise' not in sys.modules\n"
            ),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr


def test_default_constructor_imports_mise_dynamically(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from context_intelligence_server import mise_validation

    sdk = _Sdk()
    imports: list[str] = []

    def load_module(name: str) -> _Sdk:
        imports.append(name)
        return sdk

    monkeypatch.setattr(mise_validation, "import_module", load_module)
    mise_validation.MiseTokenValidator(CLIENT_ID, TENANT_ID)
    assert imports == ["mise"]
    assert sdk.clients_created == 1
    assert sdk.client.control.closed


@pytest.mark.parametrize("error_code", [-1, 1, 200])
def test_nonzero_configuration_result_fails_startup_and_is_released(
    error_code: int,
) -> None:
    from context_intelligence_server.mise_validation import MiseTokenValidator

    sdk = _Sdk()
    sdk.client.control = _ControlResult(error_code)
    with pytest.raises(RuntimeError) as caught:
        MiseTokenValidator(CLIENT_ID, TENANT_ID, _mise_module=sdk)
    _assert_sanitized(caught.value, "MISE initialization failed")
    assert sdk.client.control.closed


@pytest.mark.parametrize("stage", ["import", "native", "configure", "cleanup"])
def test_initialization_exceptions_are_sanitized(
    monkeypatch: pytest.MonkeyPatch, stage: str
) -> None:
    from context_intelligence_server import mise_validation

    sdk = _Sdk()

    def fail(*args: Any, **kwargs: Any) -> Any:
        raise OSError(NATIVE_DETAIL)

    if stage == "import":
        monkeypatch.setattr(mise_validation, "import_module", fail)
    elif stage == "native":
        monkeypatch.setattr(sdk, "Mise", fail)
    elif stage == "configure":
        monkeypatch.setattr(sdk.client, "configure", fail)
    else:
        sdk.client.control.exit_error = OSError(NATIVE_DETAIL)

    with pytest.raises(RuntimeError) as caught:
        if stage == "import":
            mise_validation.MiseTokenValidator(CLIENT_ID, TENANT_ID)
        else:
            mise_validation.MiseTokenValidator(CLIENT_ID, TENANT_ID, _mise_module=sdk)
    _assert_sanitized(caught.value, "MISE initialization failed")
    if stage == "cleanup":
        assert sdk.client.control.closed


def test_validation_passes_exact_request_fields_and_reuses_configured_client() -> None:
    from context_intelligence_server.mise_validation import (
        MiseTokenValidator,
        TokenValidator,
    )

    sdk = _Sdk()
    validator: TokenValidator = MiseTokenValidator(
        CLIENT_ID, TENANT_ID, _mise_module=sdk
    )
    first_result = sdk.client.result
    assert validator.validate(
        "opaque-token", original_uri=REQUEST_URI, original_method="PATCH"
    ) == {"oid": "subject"}
    assert first_result.closed

    sdk.client.result = _ValidationResult(pairs=[])
    assert (
        validator.validate(
            "other-opaque-token",
            original_uri="https://service.example.test/blobs/item",
            original_method="GET",
        )
        == {}
    )
    assert sdk.client.result.closed
    assert [vars(value) for value in sdk.client.validation_inputs] == [
        {
            "authorization_header": "Bearer opaque-token",
            "original_uri_header": REQUEST_URI,
            "original_method_header": "PATCH",
        },
        {
            "authorization_header": "Bearer other-opaque-token",
            "original_uri_header": "https://service.example.test/blobs/item",
            "original_method_header": "GET",
        },
    ]
    assert sdk.clients_created == 1
    assert sdk.inputs_created == 2
    assert len(sdk.client.configure_calls) == 1


@pytest.mark.parametrize("status", [0, 199, 201, 204, 299, 302, 400, 401, 403, 500])
def test_only_200_is_accepted_and_rejected_results_are_released(status: int) -> None:
    from context_intelligence_server.mise_validation import (
        MiseTokenValidator,
        TokenValidationError,
    )

    sdk = _Sdk()
    sdk.client.result = _ValidationResult(status=status)
    validator = MiseTokenValidator(CLIENT_ID, TENANT_ID, _mise_module=sdk)
    with pytest.raises(TokenValidationError) as caught:
        validator.validate(
            "opaque-token", original_uri=REQUEST_URI, original_method="POST"
        )
    _assert_sanitized(caught.value, "Token validation failed")
    assert sdk.client.result.closed
    assert sdk.client.result.claims_reads == 0


@pytest.mark.parametrize(
    ("pairs", "expected"),
    [
        (
            [
                ("roles", "Reader"),
                ("oid", "subject"),
                ("roles", "Contributor"),
                ("roles", "Reader"),
            ],
            {"roles": ["Reader", "Contributor", "Reader"], "oid": "subject"},
        ),
        (
            [("oid", "subject"), ("roles", "Reader"), ("idtyp", "app")],
            {"oid": "subject", "roles": "Reader", "idtyp": "app"},
        ),
        (
            [("scp", ""), ("idtyp", ""), ("roles", "")],
            {"scp": "", "idtyp": "", "roles": ""},
        ),
        (
            [("scp", ""), ("scp", "access_as_user"), ("scp", "")],
            {"scp": ["", "access_as_user", ""]},
        ),
        ([("", "")], {"": ""}),
        ([], {}),
    ],
)
def test_copies_subject_claims_with_repetition_absence_and_empty_values(
    pairs: list[tuple[str, str]], expected: dict[str, Any]
) -> None:
    from context_intelligence_server.mise_validation import MiseTokenValidator

    sdk = _Sdk()
    sdk.client.result = _ValidationResult(pairs=pairs)
    validator = MiseTokenValidator(CLIENT_ID, TENANT_ID, _mise_module=sdk)
    claims = validator.validate(
        "not-a-jwt", original_uri=REQUEST_URI, original_method="POST"
    )
    assert sdk.client.result.closed
    assert claims == expected


@pytest.mark.parametrize(
    "pair", [(None, "subject"), ("oid", None), ("roles", ["Reader"])]
)
def test_malformed_native_claims_fail_closed(pair: tuple[Any, Any]) -> None:
    from context_intelligence_server.mise_validation import (
        MiseTokenValidator,
        TokenValidationError,
    )

    sdk = _Sdk()
    sdk.client.result = _ValidationResult(pairs=[pair])
    validator = MiseTokenValidator(CLIENT_ID, TENANT_ID, _mise_module=sdk)
    with pytest.raises(TokenValidationError) as caught:
        validator.validate(
            "opaque-token", original_uri=REQUEST_URI, original_method="POST"
        )
    _assert_sanitized(caught.value, "Token validation failed")
    assert sdk.client.result.closed


@pytest.mark.parametrize("stage", ["input", "validate", "status", "claims", "cleanup"])
def test_validation_exceptions_are_sanitized_and_owned_results_released(
    monkeypatch: pytest.MonkeyPatch, stage: str
) -> None:
    from context_intelligence_server.mise_validation import (
        MiseTokenValidator,
        TokenValidationError,
    )

    sdk = _Sdk()
    validator = MiseTokenValidator(CLIENT_ID, TENANT_ID, _mise_module=sdk)

    def fail(*args: Any, **kwargs: Any) -> Any:
        raise ValueError(NATIVE_DETAIL)

    if stage == "input":
        monkeypatch.setattr(sdk, "MiseValidationInput", fail)
    elif stage == "validate":
        monkeypatch.setattr(sdk.client, "validate", fail)
    elif stage == "status":
        sdk.client.result.status_error = ValueError(NATIVE_DETAIL)
    elif stage == "claims":
        sdk.client.result.claims_error = UnicodeError(NATIVE_DETAIL)
    else:
        sdk.client.result.exit_error = OSError(NATIVE_DETAIL)

    with pytest.raises(TokenValidationError) as caught:
        validator.validate(
            "opaque-token", original_uri=REQUEST_URI, original_method="POST"
        )
    _assert_sanitized(caught.value, "Token validation failed")
    if stage in {"status", "claims", "cleanup"}:
        assert sdk.client.result.closed


@pytest.mark.mise
def test_mise_270_native_configuration_and_malformed_token_rejection() -> None:
    """Run with the real release installed; no importorskip or fake acceptance."""
    from context_intelligence_server.mise_validation import (
        MiseTokenValidator,
        TokenValidationError,
    )

    assert sys.platform == "linux" and platform.machine() == "x86_64", (
        "The trusted MISE tier requires Linux x86_64 with ICU"
    )
    assert version("mise") == "2.7.0"
    # These identifiers and request target are local rejection fixtures, not a
    # deployed service or a positive token fixture. Construction must configure
    # the genuine native client successfully before the rejection is exercised.
    validator = MiseTokenValidator(CLIENT_ID, TENANT_ID)
    # Check the real response on that same client: an unexpected SDK exception
    # must not masquerade as evidence of a native token rejection.
    native_input = import_module("mise").MiseValidationInput()
    native_input.authorization_header = "Bearer not-a-jwt"
    native_input.original_uri_header = REQUEST_URI
    native_input.original_method_header = "POST"
    with validator._client.validate(validation_input=native_input) as native_result:
        assert native_result.http_response_status_code == 401
        assert native_result.subject_claims == []

    with pytest.raises(TokenValidationError) as caught:
        validator.validate(
            "not-a-jwt", original_uri=REQUEST_URI, original_method="POST"
        )
    _assert_sanitized(caught.value, "Token validation failed")
