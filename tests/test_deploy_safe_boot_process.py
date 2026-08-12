"""REAL-PROCESS deploy-safe boot regression tests.

Why this file exists (and why the unit tests in ``test_main.py`` were not
enough): the unit tests call ``lifespan()`` as a function inside the test
process, where ``/data`` and logging already work and the module is already
imported. That harness CANNOT catch a crash-loop caused by a step that runs
*before* the B1 try/except boundary -- e.g. ``setup_logging()`` doing
``mkdir('/data')`` on an unwritable path, or driver construction on a
misconfigured URL. A live boot test proved exactly that hole: the real
gunicorn worker died with ``PermissionError: '/data'`` from ``setup_logging``,
never reaching the boundary -> ``Worker failed to boot`` -> systemd
restart-loop, with NO Neo4j involvement at all.

These tests launch the ACTUAL server entrypoint
(``context_intelligence_server.main:main`` -> ``run()`` -> gunicorn +
UvicornWorker -- the real deploy path) as a subprocess, pointed at an
UNREACHABLE Neo4j and with ALL data/log paths redirected under a tmp dir,
and assert the process stays alive and serves ``GET /status`` with a
non-green ``schema_health``. They need NO Neo4j -- that is the whole point:
a deploy against an unreachable graph must boot and serve, never crash-loop.

Marked ``deploy_safe_boot`` so CI can select/deselect them explicitly. They
are self-contained (spare port, tmp paths) and tear down the subprocess.
"""

from __future__ import annotations

import contextlib
import json
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.deploy_safe_boot

_ENV_PREFIX = "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_"
# Unreachable Neo4j: nothing listens on this localhost port, so a connection
# attempt fails fast (ECONNREFUSED) rather than hanging -- exactly the
# "graph unreachable at boot" scenario (ACA cold-start race / wrong URL).
_UNREACHABLE_NEO4J = "bolt://127.0.0.1:59999"

# How long to give the real gunicorn worker to boot + serve. Generous because
# a real process fork + import + lifespan (with one fast-failing Neo4j probe)
# is slower than an in-process call, but bounded so a genuine crash-loop
# still fails the test quickly.
_BOOT_DEADLINE_S = 25.0


def _free_port() -> int:
    """Return a currently-free localhost TCP port."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _base_env(tmp_path: Path, port: int, *, log_path: str) -> dict[str, str]:
    """Build a subprocess env: all data/log paths under tmp, Neo4j unreachable.

    Every path the server writes to is redirected under *tmp_path* so the
    real defaults (``/data/...``) are never touched, and ``log_path`` is
    supplied by the caller so a variant can point it at a deliberately
    unwritable location.
    """
    env = dict(os.environ)
    # WEB_CONCURRENCY must be unset/1 or run()'s _validate_single_worker trips.
    env.pop("WEB_CONCURRENCY", None)
    env.update(
        {
            f"{_ENV_PREFIX}SERVER_HOST": "127.0.0.1",
            f"{_ENV_PREFIX}SERVER_PORT": str(port),
            f"{_ENV_PREFIX}NEO4J_URL": _UNREACHABLE_NEO4J,
            f"{_ENV_PREFIX}LOG_PATH": log_path,
            f"{_ENV_PREFIX}BLOB_PATH": str(tmp_path / "blobs"),
            f"{_ENV_PREFIX}QUEUES_PATH": str(tmp_path / "queues"),
            f"{_ENV_PREFIX}API_KEYS_STORE_PATH": str(tmp_path / "api-keys.json"),
            f"{_ENV_PREFIX}ENTRA_IDENTITIES_STORE_PATH": str(
                tmp_path / "entra-identities.json"
            ),
            # Boot wide-open so no auth misconfig can mask the boot outcome;
            # /status is auth-exempt anyway, but this removes a variable.
            f"{_ENV_PREFIX}ALLOW_UNAUTHENTICATED": "true",
        }
    )
    return env


def _spawn_server(env: dict[str, str], repo_root: Path) -> subprocess.Popen[bytes]:
    """Launch the REAL server entrypoint (gunicorn) as a subprocess.

    ``main()`` -> ``run()`` -> gunicorn + UvicornWorker is the exact deploy
    path; the lifespan runs inside the worker, so this reproduces the real
    boot sequence a systemd/ACA restart would exercise.
    """
    return subprocess.Popen(
        [sys.executable, "-c", "from context_intelligence_server.main import main; main()"],
        cwd=str(repo_root),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        # New session so we can signal the whole gunicorn process group on teardown.
        start_new_session=True,
    )


def _terminate(proc: subprocess.Popen[bytes]) -> bytes:
    """Terminate the process group and return captured output (best-effort)."""
    if proc.poll() is None:
        with contextlib.suppress(ProcessLookupError, OSError):
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        try:
            proc.wait(timeout=8)
        except subprocess.TimeoutExpired:
            with contextlib.suppress(ProcessLookupError, OSError):
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            with contextlib.suppress(subprocess.TimeoutExpired):
                proc.wait(timeout=5)
    try:
        out = proc.stdout.read() if proc.stdout else b""
    except Exception:  # noqa: BLE001 - teardown diagnostics only
        out = b""
    return out or b""


def _poll_status(port: int, proc: subprocess.Popen[bytes]) -> dict[str, Any]:
    """Poll GET /status until 200 or the boot deadline; assert liveness.

    Fails loudly (with captured subprocess output) if the process dies or
    never serves -- that is the crash-loop this whole file guards against.
    """
    url = f"http://127.0.0.1:{port}/status"
    deadline = time.monotonic() + _BOOT_DEADLINE_S
    last_err: str = "no attempt made"
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            out = _read_available(proc)
            raise AssertionError(
                "Server process EXITED during boot (crash-loop!) with code "
                f"{proc.returncode}. Captured output:\n{out.decode(errors='replace')}"
            )
        try:
            with urllib.request.urlopen(url, timeout=2.0) as resp:
                if resp.status == 200:
                    return json.loads(resp.read().decode())
                last_err = f"status {resp.status}"
        except (urllib.error.URLError, ConnectionError, OSError) as exc:
            last_err = str(exc)
        time.sleep(0.5)

    out = _read_available(proc)
    raise AssertionError(
        f"Server did not serve GET /status within {_BOOT_DEADLINE_S}s "
        f"(last error: {last_err}). Process alive={proc.poll() is None}. "
        f"Captured output:\n{out.decode(errors='replace')}"
    )


def _read_available(proc: subprocess.Popen[bytes]) -> bytes:
    """Best-effort read of whatever the process has emitted so far."""
    # The process may still be running; do a non-blocking-ish drain by
    # terminating first if needed is the caller's job. Here we only read if
    # the stream is already at EOF (process exited); otherwise return empty
    # to avoid blocking.
    if proc.poll() is None:
        return b""
    try:
        return proc.stdout.read() if proc.stdout else b""
    except Exception:  # noqa: BLE001
        return b""


@pytest.mark.timeout(60)
def test_boot_serves_status_against_unreachable_neo4j(tmp_path: Path) -> None:
    """The REAL server process boots and serves /status against an
    UNREACHABLE Neo4j -- schema_health is unknown/degraded, NEVER healthy,
    and the process stays alive (no crash-loop)."""
    repo_root = Path(__file__).resolve().parent.parent
    port = _free_port()
    log_path = str(tmp_path / "logs" / "server.jsonl")  # writable tmp path
    env = _base_env(tmp_path, port, log_path=log_path)

    proc = _spawn_server(env, repo_root)
    try:
        data = _poll_status(port, proc)
        assert data["schema_health"] in {"unknown", "degraded"}, (
            f"schema_health must be unknown/degraded against an unreachable "
            f"Neo4j, never healthy. Got: {data.get('schema_health')!r}"
        )
        # Still alive after serving -- not a one-shot that then dies.
        assert proc.poll() is None, "Server exited right after serving /status."
    finally:
        _terminate(proc)


@pytest.mark.timeout(60)
def test_boot_serves_status_with_unwritable_log_path(tmp_path: Path) -> None:
    """The REAL server process boots and serves /status even when the
    configured log_path is UNWRITABLE (its parent cannot be created).

    This is the EXACT real-process failure a live boot test caught:
    setup_logging() used to mkdir the log dir BEFORE the B1 boundary, and a
    PermissionError there sank the worker with no Neo4j involvement. We force
    the mkdir to fail deterministically -- even when the test runs as root,
    where filesystem permission bits are bypassed -- by making the log path's
    parent a REGULAR FILE, so mkdir(parents=True) raises NotADirectoryError.
    setup_logging must fall back to console-only and boot must still serve.
    """
    repo_root = Path(__file__).resolve().parent.parent
    port = _free_port()

    # Create a regular file, then point log_path UNDER it. mkdir(parents=True)
    # on ".../blocker_file/logs" fails with NotADirectoryError (an OSError)
    # regardless of uid -- a root-proof way to force the log dir unwritable.
    blocker = tmp_path / "blocker_file"
    blocker.write_text("not a directory", encoding="utf-8")
    log_path = str(blocker / "logs" / "server.jsonl")
    env = _base_env(tmp_path, port, log_path=log_path)

    proc = _spawn_server(env, repo_root)
    try:
        data = _poll_status(port, proc)
        assert data["schema_health"] in {"unknown", "degraded"}, (
            f"schema_health must be unknown/degraded (unreachable Neo4j + "
            f"unwritable log). Got: {data.get('schema_health')!r}"
        )
        assert proc.poll() is None, "Server exited right after serving /status."
    finally:
        _terminate(proc)
