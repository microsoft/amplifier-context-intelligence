"""Real HTTP client and subprocess/PTY CLI checks, all against scratch servers."""

from __future__ import annotations

import hashlib
import os
import shlex
import socket
import subprocess
import sys
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
import uvicorn
from fastapi import FastAPI
from starlette.responses import RedirectResponse

from context_intelligence_server.auth import BearerTokenMiddleware
from context_intelligence_server.retention_client import (
    ClientError, RetentionClient, admin_credential, server_url,
)
from context_intelligence_server.retention_service import RetentionService
from context_intelligence_server.routers.admin import router

CLI = Path(sys.executable).parent / "context-intelligence-server"
FIXTURE_KEY = "online-prune-fixture-admin"


@pytest.mark.parametrize("url", [
    "ftp://localhost", "https://", "https://host:bad", "https://host:99999",
    "https://u:p@host", "https://host?key=raw", "https://host/#fragment",
    "https://host/\npath", "https://host\\@evil", "http://10.0.0.1",
    "http://example.com", "http://127.0.0.1.evil", "https://[broken",
    "https://host/a/../b", "https://host/%2e%2e", "https://ho%73t", "https://host:0",
])
def test_reject_malformed_or_insecure_urls(url):
    with pytest.raises(ClientError):
        server_url(url)


@pytest.mark.parametrize("url", [
    "http://localhost:8000", "http://127.0.0.1:8000", "http://[::1]:8000",
    "https://example.com/ci", "https://example.com",
])
def test_safe_urls(url):
    assert server_url(url + "/") == url


def test_missing_digest_only_and_no_data_key_fallback(monkeypatch):
    monkeypatch.delenv("ADMIN_API_KEY", raising=False)
    monkeypatch.delenv("AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_ADMIN_API_KEY", raising=False)
    monkeypatch.setenv("AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_API_KEY", "fixture-data-only")
    settings = SimpleNamespace(admin_api_key=None, admin_api_key_sha256="a" * 64, api_key="fixture-data-only")
    with pytest.raises(ClientError, match="digest-only"):
        admin_credential(settings=settings)
    monkeypatch.setenv("ADMIN_API_KEY", FIXTURE_KEY)
    assert admin_credential(settings=settings) == FIXTURE_KEY


@pytest.mark.parametrize("mode", [0o400, 0o600, 0o644, 0o660, 0o700, 0o000])
def test_keyfile_permissions(tmp_path, mode):
    key = tmp_path / "admin"
    key.write_text(FIXTURE_KEY + "\n")
    key.chmod(mode)
    try:
        if mode in {0o400, 0o600}:
            assert admin_credential(key) == FIXTURE_KEY
        else:
            with pytest.raises((ClientError, OSError)):
                admin_credential(key)
    finally:
        key.chmod(0o600)  # leave scratch fixtures readable by normal teardown tools


def test_keyfile_symlink_fifo_hardlink_size_owner_and_bad_content(tmp_path, monkeypatch):
    key = tmp_path / "admin"
    key.write_text(FIXTURE_KEY)
    key.chmod(0o600)
    link = tmp_path / "link"
    link.symlink_to(key)
    with pytest.raises(OSError):
        admin_credential(link)
    fifo = tmp_path / "fifo"
    os.mkfifo(fifo, 0o600)
    with pytest.raises(ClientError):
        admin_credential(fifo)
    hardlink = tmp_path / "hardlink"
    os.link(key, hardlink)
    with pytest.raises(ClientError):
        admin_credential(hardlink)
    hardlink.unlink()
    with monkeypatch.context() as patch:
        patch.setattr(os, "geteuid", lambda: key.stat().st_uid + 1)
        with pytest.raises(ClientError):
            admin_credential(key)
    for value in ["x" * 8193, "", "abc\nbad", "\N{SNOWMAN}"]:
        key.write_text(value)
        with pytest.raises(ClientError):
            admin_credential(key)


@pytest.fixture
def live_retention(tmp_path):
    root = tmp_path / "blobs"
    old = root / "session" / "blobs" / "old.json"
    old.parent.mkdir(parents=True)
    old.write_text('{"old":true}')
    then = time.time_ns() - 90 * 86400 * 10**9
    os.utime(old, ns=(then, then))

    @asynccontextmanager
    async def lifespan(app):
        service = await RetentionService.open(root)
        app.state.retention_service = service
        try:
            yield
        finally:
            await service.close()

    app = FastAPI(lifespan=lifespan)
    app.state.auth_mode = "static"
    app.state.admin_api_key_configured = True
    app.include_router(router)

    @app.get("/redirect/admin/blobs/prune")
    def redirect():
        return RedirectResponse("/admin/blobs/prune", status_code=307)

    middleware = BearerTokenMiddleware(
        app, keystore={hashlib.sha256(b"client-fixture-data").hexdigest(): "fixture"},
        admin_api_key_digest=hashlib.sha256(FIXTURE_KEY.encode()).hexdigest(),
    )
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    url = f"http://127.0.0.1:{sock.getsockname()[1]}"
    server = uvicorn.Server(uvicorn.Config(middleware, host="127.0.0.1", loop="asyncio", ws="none",
                                         access_log=False, log_config=None, log_level="critical"))
    thread = threading.Thread(target=lambda: server.run(sockets=[sock]), name="retention-test-http")
    thread.start()
    deadline = time.monotonic() + 5
    try:
        while not server.started:
            assert thread.is_alive() and time.monotonic() < deadline
            time.sleep(0.005)
        yield url, old, app.state.retention_service
    finally:
        server.should_exit = True
        thread.join(8)
        sock.close()
        assert not thread.is_alive(), "scratch HTTP server did not stop"


def run_cli(url, *args, tty_input=None, env=None):
    command = [str(CLI), "blobs", *args, "--online", "--server-url", url]
    if tty_input is not None:
        command = ["script", "-q", "-e", "-c", shlex.join(command), "/dev/null"]
    return subprocess.run(
        command, input=tty_input or "", capture_output=True, text=True, timeout=10,
        env=env or {**os.environ, "ADMIN_API_KEY": FIXTURE_KEY},
    )


def test_real_client_auth_redirect_refusal_and_remote_state(live_retention):
    url, old, service = live_retention
    client = RetentionClient(url, FIXTURE_KEY)
    for wrong in ["wrong", "client-fixture-data"]:
        with pytest.raises(ClientError, match="HTTP (401|403)"):
            RetentionClient(url, wrong).preview("30d")
    with pytest.raises(ClientError, match="redirect"):
        RetentionClient(url + "/redirect", "client-fixture-data").status()
    job = client.wait_preview(client.preview("30d"), timeout=3, interval=0.005)
    assert job["state"] == "ready" and old.exists()
    with pytest.raises(ClientError, match="UUID"):
        client.cancel("../arbitrary")
    assert client.start(job["id"], "prune")["state"] == "running"
    deadline = time.monotonic() + 5
    while (status := client.status(job["id"]))["state"] == "running":
        assert time.monotonic() < deadline
        time.sleep(0.005)
    assert status["state"] == "completed" and not old.exists()


@pytest.mark.parametrize("answer", ["prune\n", "decline\n", "\x04", None])
def test_real_cli_preview_confirmation_and_decline(live_retention, answer):
    url, old, service = live_retention
    result = run_cli(url, "prune", "--older-than", "30d", "--apply", tty_input=answer)
    output = result.stdout + result.stderr
    assert FIXTURE_KEY not in output
    assert "Eligible files: 1" in output and "Live estimate" in output
    if answer == "prune\n":
        assert result.returncode == 0 and "Disconnecting does NOT cancel" in output
        deadline = time.monotonic() + 5
        while old.exists():
            assert time.monotonic() < deadline
            time.sleep(0.005)
        status = run_cli(url, "prune-status")
        assert status.returncode == 0 and "completed" in status.stdout
    elif answer is None:
        assert result.returncode == 1 and "interactive terminal" in output
        assert old.exists()
    else:
        assert result.returncode == 0 and "not confirmed" in output
        assert old.exists() and service.status()["state"] == "cancelled"


def test_cli_reuses_same_preview_after_bounded_poll_timeout(live_retention):
    url, old, service = live_retention
    client = RetentionClient(url, FIXTURE_KEY)
    preview = client.preview("30d")
    # Bounded wait returns the still-running job, never invents ready.
    delayed = {**preview, "state": "previewing"}
    assert client.wait_preview(delayed, timeout=0)["state"] == "previewing"
    job = client.wait_preview(preview, timeout=3, interval=0.005)
    result = run_cli(url, "prune", "--job-id", job["id"], "--apply", tty_input="prune\n")
    assert result.returncode == 0, result.stdout + result.stderr
    assert f"Apply accepted: {job['id']}" in result.stdout
    assert service.status()["id"] == job["id"]
    deadline = time.monotonic() + 5
    while old.exists():
        assert time.monotonic() < deadline
        time.sleep(0.005)


def test_cli_private_keyfile_status_cancel_and_no_yes(live_retention, tmp_path):
    url, old, service = live_retention
    key = tmp_path / "admin-token"
    key.write_text(FIXTURE_KEY)
    key.chmod(0o600)
    env = dict(os.environ)
    env.pop("ADMIN_API_KEY", None)
    env.pop("AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_ADMIN_API_KEY", None)
    preview = run_cli(url, "prune", "--admin-key-file", str(key), env=env)
    assert preview.returncode == 0 and "ready" in preview.stdout
    job_id = service.status()["id"]
    cancel = run_cli(url, "prune-cancel", job_id, "--admin-key-file", str(key), env=env)
    assert cancel.returncode == 0 and "cancelled" in cancel.stdout
    assert old.exists()
    refused = run_cli(url, "prune", "--apply", "--yes")
    assert refused.returncode != 0 and "unrecognized arguments" in refused.stderr