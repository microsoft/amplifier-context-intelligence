"""Inbound token-authenticity boundary to the fixed localhost MISE sidecar."""

import base64
import re
import socket
import time
from http.client import HTTPConnection, HTTPResponse
from threading import Timer
from typing import Any, Protocol

_HTTP_TIMEOUT = 1.0
_STARTUP_TIMEOUT = 20.0
_POLL_INTERVAL = 0.25
_MAX_RESPONSE_HEADER_BYTES = 64 * 1024
_MAX_BEARER_BYTES = 32 * 1024
_MAX_URI_BYTES = 16 * 1024
_MAX_METHOD_BYTES = 32
_PLAIN_PREFIX = "subject-token-claim-"
_ENCODED_PREFIX = "subject-token-encoded-claim-"
_KNOWN_CLAIMS = frozenset({"tid", "oid", "scp", "idtyp", "roles", "appid", "azp"})
_METHOD = re.compile(r"[!#$%&'*+\-.^_`|~0-9A-Za-z]+")


class TokenValidationError(Exception):
    """Token validation failed without exposing transport or token diagnostics."""


class TokenValidator(Protocol):
    def validate(
        self, bearer: str, *, original_uri: str, original_method: str
    ) -> dict[str, Any]: ...


def _has_controls(value: str) -> bool:
    return any(ord(char) < 32 or ord(char) == 127 for char in value)


def _validation_headers(
    bearer: str, original_uri: str, original_method: str
) -> dict[str, str]:
    # Validate before constructing any connection. No stripping, repair or truncation.
    if (
        not isinstance(bearer, str)
        or not bearer
        or len(bearer.encode("ascii")) > _MAX_BEARER_BYTES
        or any(ord(char) <= 32 or ord(char) >= 127 for char in bearer)
        or not isinstance(original_uri, str)
        or not original_uri
        or len(original_uri.encode("latin-1")) > _MAX_URI_BYTES
        or _has_controls(original_uri)
        or not isinstance(original_method, str)
        or len(original_method.encode("ascii")) > _MAX_METHOD_BYTES
        or _METHOD.fullmatch(original_method) is None
    ):
        raise ValueError("Invalid validation input")
    return {
        "Authorization": "Bearer " + bearer,
        "original-uri": original_uri,
        "original-method": original_method,
        "return-all-subject-token-claims": "1",
        "Content-Length": "0",
        "Connection": "close",
    }


def _exchange(
    method: str, path: str, headers: dict[str, str], *, deadline: float
) -> tuple[int, list[tuple[str, str]]]:
    """One bounded exchange; callers own status interpretation, never retries."""
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError
    connection = HTTPConnection(
        "127.0.0.1", 5000, timeout=min(_HTTP_TIMEOUT, remaining)
    )
    response: HTTPResponse | None = None
    timer: Timer | None = None
    timer_started = False
    try:
        connection.connect()
        connected_socket = connection.sock
        if connected_socket is None:
            raise ConnectionError
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError
        connected_socket.settimeout(min(_HTTP_TIMEOUT, remaining))

        def expire() -> None:
            # Capture the actual socket: getresponse() can set connection.sock=None
            # while the response still owns a blocking socket-backed file.
            try:
                connected_socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

        timer = Timer(remaining, expire)
        timer.start()
        timer_started = True
        connection.request(method, path, body=b"", headers=headers)
        response = connection.getresponse()
        pairs = response.getheaders()  # A list, preserving repeated claim headers.
        if (
            response.headers.defects
            or sum(
                len(name.encode("latin-1")) + len(value.encode("latin-1")) + 4
                for name, value in pairs
            )
            > _MAX_RESPONSE_HEADER_BYTES
        ):
            raise ValueError("Invalid response headers")
        if any(_has_controls(name) or _has_controls(value) for name, value in pairs):
            raise ValueError("Invalid response headers")
        # Never consume denial diagnostics/bodies. Success must carry no body.
        if response.status == 200 and response.read(1):
            raise ValueError("Unexpected response body")
        if time.monotonic() >= deadline:
            raise TimeoutError
        return response.status, pairs
    except Exception:
        if time.monotonic() >= deadline:
            raise TimeoutError from None
        raise
    finally:
        try:
            if timer is not None:
                try:
                    timer.cancel()
                finally:
                    if timer_started:
                        timer.join()
        finally:
            # Timer setup/cleanup failures must never bypass socket ownership.
            try:
                if response is not None:
                    response.close()
            finally:
                connection.close()


def _claim_name(header: str) -> tuple[str, bool] | None:
    lower = header.lower()
    if lower.startswith(_ENCODED_PREFIX):
        return header[len(_ENCODED_PREFIX) :], True
    if lower.startswith(_PLAIN_PREFIX):
        return header[len(_PLAIN_PREFIX) :], False
    return None


def _subject_claims(headers: list[tuple[str, str]]) -> dict[str, Any]:
    values: dict[str, list[str]] = {}
    spellings: dict[str, str] = {}
    for header, value in headers:
        claim = _claim_name(header)
        if claim is None:
            continue  # Actor claims, challenges and diagnostics are never identity.
        name, encoded = claim
        if not name:
            raise ValueError("Empty claim name")
        canonical = name.lower()
        if canonical in _KNOWN_CLAIMS:
            name = canonical
        elif canonical in spellings and spellings[canonical] != name:
            raise ValueError("Ambiguous claim name")
        spellings[canonical] = name
        if encoded:
            encoded_value = value.encode("ascii")
            decoded = base64.b64decode(encoded_value, validate=True)
            # Strict decoding alone permits extra padding on Python 3.11 and
            # nonzero pad bits. Require the canonical standard-base64 spelling.
            if base64.b64encode(decoded) != encoded_value:
                raise ValueError("Invalid claim encoding")
            value = decoded.decode("utf-8")
        if _has_controls(value):
            raise ValueError("Invalid claim value")
        values.setdefault(name, []).append(value)
    for name in _KNOWN_CLAIMS - {"roles"}:
        if name in values and len(values[name]) != 1:
            raise ValueError("Ambiguous singleton claim")
    for name in ("tid", "oid"):
        if name not in values or not values[name][0].strip():
            raise ValueError("Missing identity")
    return {
        name: items if name == "roles" or len(items) > 1 else items[0]
        for name, items in values.items()
    }


class MiseTokenValidator:
    """Stateless localhost client; startup proves connectivity/negative contract only.

    The sidecar owns authenticity configuration. These startup probes do not prove
    signing-key readiness, correct tenant/audience configuration or positive tokens.
    """

    def __init__(self) -> None:
        try:
            deadline = time.monotonic() + _STARTUP_TIMEOUT
            while time.monotonic() < deadline:
                try:
                    status, _ = _exchange(
                        "GET",
                        "/readyz",
                        {"Connection": "close"},
                        deadline=min(deadline, time.monotonic() + _HTTP_TIMEOUT),
                    )
                    if status == 503:
                        raise ConnectionRefusedError
                    if status != 200:
                        raise ValueError("Unexpected readiness status")
                    status, headers = _exchange(
                        "POST",
                        "/ValidateRequest",
                        _validation_headers(
                            "not-a-jwt", "http://127.0.0.1:8000/events", "POST"
                        ),
                        deadline=min(deadline, time.monotonic() + _HTTP_TIMEOUT),
                    )
                    if any(_claim_name(name) is not None for name, _ in headers):
                        raise ValueError("Unexpected negative validation claims")
                    if status == 503:
                        raise ConnectionRefusedError
                    if status != 401:
                        raise ValueError("Unexpected negative validation contract")
                    if time.monotonic() >= deadline:
                        raise TimeoutError
                    return
                except (ConnectionRefusedError, ConnectionResetError, TimeoutError):
                    time.sleep(min(_POLL_INTERVAL, max(0, deadline - time.monotonic())))
            raise TimeoutError
        except Exception:  # noqa: BLE001 - Never expose transport diagnostics.
            raise RuntimeError("MISE initialization failed") from None

    def validate(
        self, bearer: str, *, original_uri: str, original_method: str
    ) -> dict[str, Any]:
        """Return subject claims only on exact 200 with an empty response body."""
        try:
            headers = _validation_headers(bearer, original_uri, original_method)
            status, response_headers = _exchange(
                "POST",
                "/ValidateRequest",
                headers,
                deadline=time.monotonic() + _HTTP_TIMEOUT,
            )
            if status != 200:
                raise ValueError("Unexpected validation status")
            return _subject_claims(response_headers)
        except Exception:  # noqa: BLE001 - Never expose token/claim/transport diagnostics.
            raise TokenValidationError("Token validation failed") from None
