"""Hermetic contract checks for the opt-in Azure retention job adapter.

All identifiers and credentials are synthetic. Identity HTTP is stubbed and the
scheduled CLI is intercepted; these tests never contact Azure or prune storage.
"""

from __future__ import annotations

import http.client
import io
import json
import os
import socket
import stat
import sys
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest

from context_intelligence_server import retention_job

API_URL = "https://retention.example.invalid"
AUDIENCE = "api://aaaaaaaa-0000-0000-0000-000000000001"
CALLER = "bbbbbbbb-0000-0000-0000-000000000002"
TOKEN = "synthetic-retention-bearer"
IDENTITY_HEADER = "synthetic-identity-header"
NOW = 1_700_000_000
MIN_LIFETIME = 1200 + 120 + 60


@pytest.fixture(autouse=True)
def isolated_job(monkeypatch, tmp_path):
    """Supply only test configuration and forbid unstubbed network or CLI work."""
    for name, value in {
        "API_URL": API_URL,
        "API_AUDIENCE": AUDIENCE,
        "AZURE_CALLER_CLIENT_ID": CALLER,
        "IDENTITY_ENDPOINT": "http://127.0.0.1:43210/msi/token",
        "IDENTITY_HEADER": IDENTITY_HEADER,
        # Ambient static credentials must never become an identity fallback.
        "AZURE_CLIENT_SECRET": "synthetic-fallback-secret",
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(retention_job.time, "time", lambda: NOW)
    monkeypatch.setattr(retention_job.tempfile, "tempdir", str(tmp_path))

    def forbidden(*args, **kwargs):
        pytest.fail("Unexpected network or scheduled CLI invocation")

    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(retention_job.cli, "main", forbidden)


@pytest.fixture
def identity_http(monkeypatch):
    """Model HTTPConnection without DNS, sockets, proxies, or redirects."""
    state = SimpleNamespace(
        body=json.dumps(
            {"access_token": TOKEN, "expires_on": str(NOW + MIN_LIFETIME + 1)}
        ).encode(),
        status=200,
        connections=[],
        requests=[],
        read_sizes=[],
        closed=0,
        error=None,
    )

    class Response:
        def __init__(self):
            self.status = state.status
            self.stream = io.BytesIO(state.body)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.stream.close()

        def read(self, amount=None):
            state.read_sizes.append(amount)
            return self.stream.read(amount)

    class Connection:
        def __init__(self, host, port=None, *, timeout=None):
            state.connections.append((host, port, timeout))

        def request(self, method, url, body=None, headers=None):
            state.requests.append((method, url, body, headers))
            if state.error is not None:
                raise state.error

        def getresponse(self):
            return Response()

        def close(self):
            state.closed += 1

    monkeypatch.setattr(http.client, "HTTPConnection", Connection)
    return state


def assert_setup_failure(capsys, tmp_path):
    with pytest.raises(SystemExit) as caught:
        retention_job.main([])
    assert caught.value.code == 1
    output = capsys.readouterr()
    assert output.err.strip()
    combined = output.out + output.err
    for sensitive in (
        TOKEN,
        IDENTITY_HEADER,
        "synthetic-fallback-secret",
        API_URL,
        AUDIENCE,
        CALLER,
        "Traceback",
    ):
        assert sensitive not in combined
    assert not list(tmp_path.iterdir()), "No credential file may survive failure"


@pytest.mark.parametrize("apply", [False, True], ids=["preview-default", "explicit-apply"])
def test_fixed_online_policy_and_private_ephemeral_token(
    apply, monkeypatch, identity_http, capsys, tmp_path
):
    paths = []
    pid = os.getpid()

    def cli(argv):
        assert os.getpid() == pid, "CLI must run in the same process"
        assert isinstance(argv, list)
        path = Path(argv[argv.index("--admin-key-file") + 1])
        paths.append(path)
        assert path.is_file()
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
        assert path.read_text(encoding="ascii") == TOKEN
        expected = [
            "blobs",
            "prune-scheduled",
            "--online",
            "--older-than",
            "60d",
            "--server-url",
            API_URL,
            "--admin-key-file",
            str(path),
            "--max-runtime",
            "1200",
            "--cancel-timeout",
            "120",
        ]
        if not apply:
            expected.append("--dry-run")
        assert argv == expected
        assert all(TOKEN not in argument for argument in argv)

    monkeypatch.setattr(retention_job.cli, "main", cli)
    assert retention_job.main(["--apply"] if apply else []) is None
    assert len(paths) == 1
    assert not paths[0].exists()
    assert not paths[0].parent.exists()
    assert not list(tmp_path.iterdir())
    output = capsys.readouterr()
    assert TOKEN not in output.out + output.err
    assert IDENTITY_HEADER not in output.out + output.err


@pytest.mark.parametrize("exit_code", [0, 1, 130])
def test_token_is_removed_when_cli_exits(exit_code, monkeypatch, identity_http, tmp_path):
    paths = []

    def cli(argv):
        path = Path(argv[argv.index("--admin-key-file") + 1])
        assert path.read_text() == TOKEN
        paths.append(path)
        raise SystemExit(exit_code)

    monkeypatch.setattr(retention_job.cli, "main", cli)
    with pytest.raises(SystemExit) as caught:
        retention_job.main(["--apply"])
    assert caught.value.code == exit_code
    assert len(paths) == 1 and not paths[0].exists()
    assert not list(tmp_path.iterdir())


def test_cli_oserror_is_sanitized_and_token_removed(
    monkeypatch, identity_http, capsys, tmp_path
):
    def cli(argv):
        path = Path(argv[argv.index("--admin-key-file") + 1])
        assert path.read_text() == TOKEN
        raise OSError(f"{TOKEN} {IDENTITY_HEADER}")

    monkeypatch.setattr(retention_job.cli, "main", cli)
    assert_setup_failure(capsys, tmp_path)


def test_none_argv_reads_process_arguments(monkeypatch, identity_http):
    calls = []
    monkeypatch.setattr(sys, "argv", ["retention-job", "--apply"])
    monkeypatch.setattr(retention_job.cli, "main", calls.append)
    retention_job.main()
    assert len(calls) == 1 and "--dry-run" not in calls[0]


@pytest.mark.parametrize(
    "argv",
    [["--app"], ["--apply=true"], ["--older-than", "1d"], ["--max-runtime", "9999"]],
)
def test_apply_requires_exact_opt_in_and_policy_cannot_be_overridden(argv, identity_http):
    with pytest.raises(SystemExit) as caught:
        retention_job.main(argv)
    assert caught.value.code == 2
    assert not identity_http.connections


@pytest.mark.parametrize(
    ("endpoint", "host"),
    [
        ("http://127.0.0.1:43210/msi/token", "127.0.0.1"),
        ("http://localhost:43210/msi/token", "127.0.0.1"),
        ("http://169.254.169.254:43210/msi/token", "169.254.169.254"),
    ],
)
def test_identity_request_uses_direct_local_http_and_caller_identity(
    endpoint, host, monkeypatch, identity_http
):
    monkeypatch.setenv("IDENTITY_ENDPOINT", endpoint)
    monkeypatch.setenv("HTTP_PROXY", "http://proxy.example.invalid:3128")
    monkeypatch.setenv("http_proxy", "http://proxy.example.invalid:3128")
    assert retention_job._managed_identity_token(AUDIENCE) == TOKEN
    assert len(identity_http.connections) == 1
    actual_host, port, timeout = identity_http.connections[0]
    assert actual_host == host and port == 43210
    assert timeout is not None and 0 < timeout <= 60
    assert len(identity_http.requests) == 1
    method, target, body, headers = identity_http.requests[0]
    assert method == "GET" and body is None
    assert headers == {"X-IDENTITY-HEADER": IDENTITY_HEADER}
    parsed = urlsplit(target)
    assert not parsed.scheme and not parsed.netloc
    assert parsed.path == "/msi/token"
    assert parse_qs(parsed.query) == {
        "resource": [AUDIENCE],
        "client_id": [CALLER],
        "api-version": ["2019-08-01"],
    }
    assert identity_http.closed == 1
    assert len(identity_http.read_sizes) == 1
    assert 0 < identity_http.read_sizes[0] <= 1024 * 1024


@pytest.mark.parametrize("status", [301, 302, 307, 400, 401, 403, 500])
def test_identity_http_failure_does_not_retry_redirect_or_fallback(
    status, identity_http, capsys, tmp_path
):
    identity_http.status = status
    identity_http.body = f"{TOKEN} {IDENTITY_HEADER}".encode()
    assert_setup_failure(capsys, tmp_path)
    assert len(identity_http.requests) == 1
    assert len(identity_http.connections) == 1
    assert identity_http.closed == 1


@pytest.mark.parametrize("error_type", [OSError, http.client.HTTPException])
def test_identity_transport_failure_is_sanitized(
    error_type, identity_http, capsys, tmp_path
):
    identity_http.error = error_type(f"{TOKEN} {IDENTITY_HEADER}")
    assert_setup_failure(capsys, tmp_path)
    assert len(identity_http.requests) == 1
    assert identity_http.closed == 1


@pytest.mark.parametrize("lifetime", [-1, 0, 1200, MIN_LIFETIME - 1, MIN_LIFETIME])
def test_identity_token_must_outlive_work_cleanup_and_margin(
    lifetime, identity_http, capsys, tmp_path
):
    identity_http.body = json.dumps(
        {"access_token": TOKEN, "expires_on": str(NOW + lifetime)}
    ).encode()
    assert_setup_failure(capsys, tmp_path)


@pytest.mark.parametrize("expiry", [NOW + MIN_LIFETIME + 1, str(NOW + MIN_LIFETIME + 1)])
def test_identity_accepts_sufficient_unix_seconds_expiry(expiry, identity_http):
    identity_http.body = json.dumps(
        {"access_token": TOKEN, "expires_on": expiry}
    ).encode()
    assert retention_job._managed_identity_token(AUDIENCE) == TOKEN


@pytest.mark.parametrize(
    "payload",
    [
        None,
        [],
        "synthetic-retention-bearer",
        {},
        {"access_token": TOKEN},
        {"access_token": TOKEN, "expires_on": None},
        {"access_token": TOKEN, "expires_on": True},
        {"access_token": TOKEN, "expires_on": "not-an-expiry"},
        {"access_token": TOKEN, "expires_on": "Infinity"},
        {"access_token": TOKEN, "expires_on": 1e100},
        {"access_token": TOKEN, "expires_on": {"seconds": 1e100}},
        {"access_token": None, "expires_on": NOW + 3600},
        {"access_token": 123, "expires_on": NOW + 3600},
        {"access_token": "", "expires_on": NOW + 3600},
        {"access_token": "bad\ntoken", "expires_on": NOW + 3600},
    ],
)
def test_malformed_identity_payload_fails_closed(payload, identity_http, capsys, tmp_path):
    identity_http.body = json.dumps(payload).encode()
    assert_setup_failure(capsys, tmp_path)
    assert identity_http.closed == 1


@pytest.mark.parametrize("body", [b"{invalid-json", b"\xff", b" " * (1024 * 1024)])
def test_invalid_or_oversized_identity_body_is_bounded_and_sanitized(
    body, identity_http, capsys, tmp_path
):
    identity_http.body = body
    assert_setup_failure(capsys, tmp_path)
    assert identity_http.closed == 1
    assert len(identity_http.read_sizes) == 1
    assert 0 < identity_http.read_sizes[0] < 1024 * 1024


@pytest.mark.parametrize(
    "endpoint",
    [
        "http://identity.example.invalid/msi/token",
        "http://10.0.0.1/msi/token",
        "http://192.0.2.1/msi/token",
        "https://127.0.0.1/msi/token",
        "http://localhost.example.invalid/msi/token",
        "http://localhost@identity.example.invalid/msi/token",
        "http://user:password@127.0.0.1/msi/token",
        "http://127.0.0.1/msi/token?resource=other",
        "http://127.0.0.1/msi/token#fragment",
        "http://127.0.0.1//identity.example.invalid/token",
    ],
)
def test_remote_or_unsafe_identity_endpoint_rejected_before_http(
    endpoint, monkeypatch, identity_http, capsys, tmp_path
):
    monkeypatch.setenv("IDENTITY_ENDPOINT", endpoint)
    assert_setup_failure(capsys, tmp_path)
    assert not identity_http.connections


@pytest.mark.parametrize(
    "url",
    [
        "http://retention.example.invalid",
        "http://localhost:8000",
        "http://127.0.0.1:8000",
        "https://user:password@retention.example.invalid",
    ],
)
def test_plaintext_or_credentialed_api_url_rejected_before_identity(
    url, monkeypatch, identity_http, capsys, tmp_path
):
    monkeypatch.setenv("API_URL", url)
    assert_setup_failure(capsys, tmp_path)
    assert not identity_http.connections


@pytest.mark.parametrize(
    "audience",
    [
        "aaaaaaaa-0000-0000-0000-000000000001",
        "api://not-a-uuid",
        "api://AAAAAAAA-0000-0000-0000-000000000001",
        "api://aaaaaaaa-0000-0000-0000-000000000001/.default",
    ],
)
def test_noncanonical_audience_rejected_before_http(
    audience, monkeypatch, identity_http, capsys, tmp_path
):
    monkeypatch.setenv("API_AUDIENCE", audience)
    assert_setup_failure(capsys, tmp_path)
    assert not identity_http.connections


@pytest.mark.parametrize(
    "name",
    [
        "API_URL",
        "API_AUDIENCE",
        "AZURE_CALLER_CLIENT_ID",
        "IDENTITY_ENDPOINT",
        "IDENTITY_HEADER",
    ],
)
def test_missing_configuration_fails_without_credential_fallback(
    name, monkeypatch, identity_http, capsys, tmp_path
):
    monkeypatch.delenv(name)
    assert_setup_failure(capsys, tmp_path)
    assert not identity_http.requests