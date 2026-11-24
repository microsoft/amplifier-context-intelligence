"""Azure Container Apps job adapter for the existing scheduled retention CLI."""

from __future__ import annotations

import argparse
import http.client
import ipaddress
import json
import os
import sys
import tempfile
import time
from pathlib import Path
from urllib.parse import urlencode, urlsplit
from uuid import UUID

from context_intelligence_server import cli
from context_intelligence_server.retention_client import ClientError, server_url

_WORK_SECONDS = 1200
_CANCEL_SECONDS = 120
_TOKEN_MARGIN_SECONDS = 60
_MAX_RESPONSE_BYTES = 32768


def _managed_identity_token(audience: str) -> str:
    resource = f"api://{UUID(audience.removeprefix('api://'))}"
    if audience != resource:
        raise ValueError("Invalid API audience")
    client_id = str(UUID(os.environ["AZURE_CALLER_CLIENT_ID"]))
    url = urlsplit(os.environ["IDENTITY_ENDPOINT"])
    host = url.hostname
    if (
        url.scheme != "http"
        or not host
        or url.username is not None
        or url.password is not None
        or url.query
        or url.fragment
        or not url.path.startswith("/")
        or url.path.startswith("//")
    ):
        raise ValueError("Invalid managed identity endpoint")
    if host == "localhost":
        host = "127.0.0.1"
    ip = ipaddress.ip_address(host)
    if not (ip.is_loopback or ip.is_link_local) or "%" in host:
        raise ValueError("Managed identity endpoint must be local")

    query = urlencode(
        {"resource": resource, "api-version": "2019-08-01", "client_id": client_id}
    )
    # Direct transport: do not use HTTP proxies or follow redirects with this header.
    connection = http.client.HTTPConnection(host, url.port, timeout=10)
    try:
        connection.request(
            "GET",
            f"{url.path}?{query}",
            headers={"X-IDENTITY-HEADER": os.environ["IDENTITY_HEADER"]},
        )
        with connection.getresponse() as response:
            if response.status != 200:
                raise ValueError("Managed identity request failed")
            data = response.read(_MAX_RESPONSE_BYTES + 1)
    finally:
        connection.close()
    if len(data) > _MAX_RESPONSE_BYTES:
        raise ValueError("Oversized managed identity response")
    payload = json.loads(data)
    if not isinstance(payload, dict):
        raise ValueError("Invalid managed identity response")
    token = payload.get("access_token")
    if (
        not isinstance(token, str)
        or not 0 < len(token) <= 8192
        or any(not 32 < ord(char) < 127 for char in token)
    ):
        raise ValueError("Invalid managed identity token")
    # ACA reports expires_on as Unix seconds (typically a decimal string).
    expiry = payload.get("expires_on")
    if type(expiry) is not int and not (
        isinstance(expiry, str) and expiry.isascii() and expiry.isdecimal()
    ):
        raise ValueError("Invalid managed identity expiry")
    if int(expiry) <= time.time() + _WORK_SECONDS + _CANCEL_SECONDS + _TOKEN_MARGIN_SECONDS:
        raise ValueError("Managed identity token expires too soon")
    return token


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Run the 60-day online Azure blob-retention job (preview by default).",
        allow_abbrev=False,
    )
    parser.add_argument("--apply", action="store_true", help="Enable payload expiry.")
    args = parser.parse_args(argv)
    try:
        url = server_url(os.environ["API_URL"])
        if not url.startswith("https://"):
            raise ValueError("The Azure job requires an HTTPS API URL")
        token = _managed_identity_token(os.environ["API_AUDIENCE"])
        with tempfile.TemporaryDirectory(prefix="ci-retention-") as scratch:
            token_path = Path(scratch, "token")
            fd = os.open(token_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "w", encoding="ascii") as key_file:
                key_file.write(token)
            del token
            command = [
                "blobs", "prune-scheduled", "--online", "--older-than", "60d",
                "--server-url", url, "--admin-key-file", str(token_path),
                "--max-runtime", str(_WORK_SECONDS), "--cancel-timeout", str(_CANCEL_SECONDS),
            ]
            if not args.apply:
                command.append("--dry-run")
            cli.main(command)
    except (ClientError, OSError, KeyError, ValueError, http.client.HTTPException):
        # Do not surface credentials, endpoint details, or response bodies.
        print('{"outcome":"failed","error_code":"azure_retention_setup_failed"}', file=sys.stderr)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()