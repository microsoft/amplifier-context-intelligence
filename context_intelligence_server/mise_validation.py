"""Inbound token-authenticity boundary backed by the MISE Python adapter."""

import json
from importlib import import_module
from typing import Any, Protocol


class TokenValidationError(Exception):
    """Token validation failed without exposing native diagnostics."""


class TokenValidator(Protocol):
    def validate(
        self, bearer: str, *, original_uri: str, original_method: str
    ) -> dict[str, Any]: ...


class MiseTokenValidator:
    """Own one configured MISE client for the lifetime of a server worker."""

    def __init__(
        self, client_id: str, tenant_id: str, *, _mise_module: Any = None
    ) -> None:
        try:
            self._mise = import_module("mise") if _mise_module is None else _mise_module
            # Mise() initializes the native interface before any input/result exists.
            self._client = self._mise.Mise()
            configuration = {
                "MiseVersion": "2.0",
                "AzureAd": {
                    "Instance": "https://login.microsoftonline.com/",
                    "ClientId": client_id,
                    "TenantId": tenant_id,
                    "ValidTenantIds": [tenant_id],
                    "Audiences": [client_id, f"api://{client_id}"],
                    "Protocols": {
                        "Bearer": {
                            "TokenTypes": {
                                "AccessToken": {"UserToken": True, "AppToken": True}
                            }
                        }
                    },
                },
            }
            with self._client.configure(json.dumps(configuration), "AzureAd") as result:
                if result.error_code != 0:
                    raise RuntimeError("MISE initialization failed")
        except Exception:  # noqa: BLE001 - Do not leak SDK/native diagnostics.
            raise RuntimeError("MISE initialization failed") from None

    def validate(
        self, bearer: str, *, original_uri: str, original_method: str
    ) -> dict[str, Any]:
        """Return copied subject claims only after MISE reports success."""
        try:
            validation_input = self._mise.MiseValidationInput()
            validation_input.authorization_header = f"Bearer {bearer}"
            validation_input.original_uri_header = original_uri
            validation_input.original_method_header = original_method
            with self._client.validate(validation_input=validation_input) as result:
                if result.http_response_status_code != 200:
                    raise TokenValidationError("Token validation failed")
                claims: dict[str, list[str]] = {}
                for claim in result.subject_claims:
                    # Native-backed wrappers cannot outlive the result context.
                    name, value = claim.name, claim.value
                    if not isinstance(name, str) or not isinstance(value, str):
                        raise TokenValidationError("Token validation failed")
                    claims.setdefault(name, []).append(value)
                return {
                    name: values[0] if len(values) == 1 else values
                    for name, values in claims.items()
                }
        except Exception:  # noqa: BLE001 - Do not leak SDK/native diagnostics.
            raise TokenValidationError("Token validation failed") from None
