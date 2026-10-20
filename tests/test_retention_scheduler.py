"""Fault-injected control plane plus real HTTP/service/non-TTY boundary checks."""

from __future__ import annotations

import copy
import json
import os
import signal
import subprocess
import threading
import time
import uuid
from collections import defaultdict, deque
from dataclasses import asdict
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace

import pytest

from context_intelligence_server import retention_scheduler as s
from context_intelligence_server.blob_retention import Report, duration_ns
from context_intelligence_server.retention_client import ClientError, RetentionClient
from tests.test_retention_client import CLI, FIXTURE_KEY, live_retention  # noqa: F401


def job(state="ready", *, confirmed=False):
    now = time.time_ns()
    cutoff = now - duration_ns("30d")
    return {
        "id": str(uuid.uuid4()), "root": "/scratch/blobs", "older_than": "30d",
        "preview_started_ns": now, "cutoff_ns": cutoff, "state": state,
        "confirmed": confirmed, "error": None, "expires_ns": now + 900 * 10**9,
        "report": asdict(Report("/scratch/blobs", cutoff, files=3, logical_bytes=123)),
    }


class Fake:
    def __init__(self, current=None):
        self.current = current
        self.job = None
        self.calls = []
        self.faults = defaultdict(deque)
        self.timeout = 30
        self.keep_running = False

    def invoke(self, name, args, default):
        self.calls.append((name, args, self.timeout))
        if self.faults[name]:
            value = self.faults[name].popleft()
            if isinstance(value, BaseException):
                raise value
            return value(self) if callable(value) else copy.deepcopy(value)
        return default()

    def status(self, job_id=None):
        def get():
            if job_id is None:
                if self.current is None:
                    raise ClientError("no current", status_code=404)
                return copy.deepcopy(self.current)
            assert job_id == self.job["id"]
            if self.job["state"] == "running" and not self.keep_running:
                self.job["state"] = "completed"
            return copy.deepcopy(self.job)
        return self.invoke("status", (job_id,), get)

    def preview(self, age):
        def create():
            assert age == "30d"
            self.job = job()
            return copy.deepcopy(self.job)
        return self.invoke("preview", (age,), create)

    def start(self, job_id, confirmation):
        def start():
            assert job_id == self.job["id"] and confirmation == "prune"
            self.job.update(state="running", confirmed=True)
            return copy.deepcopy(self.job)
        return self.invoke("start", (job_id, confirmation), start)

    def cancel(self, job_id):
        def cancel():
            assert job_id == self.job["id"]
            if self.job["state"] not in s.TERMINAL:
                self.job["state"] = "cancelled"
            return copy.deepcopy(self.job)
        return self.invoke("cancel", (job_id,), cancel)


@pytest.fixture
def clock(monkeypatch):
    class Clock:
        now = 100.0

        def monotonic(self):
            return self.now

        def sleep(self, seconds):
            self.now += seconds

    clock = Clock()
    monkeypatch.setattr(s, "time", SimpleNamespace(
        monotonic=clock.monotonic, sleep=clock.sleep, time_ns=time.time_ns,
    ))
    return clock


def run(client, **kwargs):
    return s.run_scheduled(client, older_than="30d", poll_interval=0.01, **kwargs)


def names(client):
    return [name for name, _, _ in client.calls]


@pytest.mark.parametrize("state", sorted(s.BUSY))
@pytest.mark.parametrize("dry_run", [False, True])
def test_busy_manual_job_untouched(state, dry_run):
    client = Fake(job(state, confirmed=state == "running"))
    result = run(client, dry_run=dry_run)
    assert result.outcome == "skipped_busy" and result.exit_code == 0
    assert result.job_id is None and names(client) == ["status"]


@pytest.mark.parametrize("state", sorted(s.TERMINAL))
def test_terminal_permits_one_preview_and_success(state, clock):
    client = Fake(job(state, confirmed=state == "completed"))
    result = run(client)
    assert result.outcome == "completed" and result.exit_code == 0 and result.confirmed
    assert names(client) == ["status", "preview", "start", "status"]
    assert client.timeout == 30
    assert result.cutoff_ns == client.job["cutoff_ns"]
    for timestamp in (result.started_at, result.ended_at):
        assert datetime.fromisoformat(timestamp).utcoffset().total_seconds() in {-25200, -28800}


def test_creation_409_is_busy_race_without_retry():
    client = Fake()
    client.faults["preview"].append(ClientError("busy", status_code=409))
    result = run(client)
    assert result.outcome == "skipped_busy" and result.exit_code == 0
    assert names(client) == ["status", "preview"]


@pytest.mark.parametrize("status", [401, 403, 409, 500, 503, None])
def test_unavailable_current_not_confused_with_no_job(status):
    client = Fake()
    client.faults["status"].append(ClientError("HTTP 404 secret text", status_code=status))
    result = run(client)
    assert result.exit_code == 1 and names(client) == ["status"]
    assert result.error_code == (f"http_{status}" if status else "client_error")


@pytest.mark.parametrize("bad", [None, [], {}, {"state": "future"}, "credential-secret"])
def test_bad_current_fails_without_creation(bad):
    client = Fake()
    client.faults["status"].append(bad)
    assert run(client).exit_code == 1 and names(client) == ["status"]


def test_unknown_state_fails_closed():
    client = Fake(job("future"))
    assert run(client).error_code == "invalid_response"
    assert names(client) == ["status"]


def test_lost_preview_reply_never_retries_or_cancels_unknown_job():
    client = Fake()

    def lost(c):
        c.job = job()
        raise ClientError("lost preview")

    client.faults["preview"].append(lost)
    result = run(client)
    assert result.exit_code == 1 and result.job_id is None
    assert names(client) == ["status", "preview"]
    assert client.job["state"] == "ready"


@pytest.mark.parametrize("field,value", [
    ("id", "not-a-uuid"), ("older_than", "20d"), ("root", "../elsewhere"),
    ("root", "/scratch/../elsewhere"), ("cutoff_ns", True), ("cutoff_ns", 5),
    ("preview_started_ns", -1),
])
def test_creation_identity_validation_grants_no_ownership(field, value):
    client = Fake()
    bad = job()
    bad[field] = value
    client.faults["preview"].append(bad)
    result = run(client)
    assert result.exit_code == 1 and result.job_id is None
    assert names(client) == ["status", "preview"]


def test_correct_cutoff_but_wrong_age_refused():
    client = Fake()
    bad = job()
    bad["older_than"] = "20d"
    bad["cutoff_ns"] = bad["preview_started_ns"] - duration_ns("20d")
    client.faults["preview"].append(bad)
    result = run(client)
    assert result.error_code == "age_changed" and result.job_id is None


def test_current_root_cannot_change_on_creation():
    client = Fake(job("completed", confirmed=True))
    bad = job()
    bad["root"] = "/other"
    client.faults["preview"].append(bad)
    result = run(client)
    assert result.error_code == "root_changed" and result.job_id is None


@pytest.mark.parametrize("field,value", [
    ("id", str(uuid.uuid4())), ("root", "/other"), ("cutoff_ns", 1),
    ("older_than", "20d"), ("preview_started_ns", 100),
])
def test_subsequent_identity_never_adopted(field, value, clock):
    client = Fake()

    def changed(c):
        other = copy.deepcopy(c.job)
        other[field] = value
        return other

    client.faults["status"].extend([ClientError(status_code=404), changed])
    result = run(client)
    assert result.exit_code == 1 and result.job_id == client.job["id"]
    assert names(client).count("start") == 1 and names(client).count("cancel") == 1
    assert all(args[0] == result.job_id for name, args, _ in client.calls if name in {"start", "cancel"})
    assert client.timeout == 30


@pytest.mark.parametrize("status", [None, 409])
@pytest.mark.parametrize("reconciled_state", ["running", "completed"])
def test_start_reply_lost_or_409_reconciles_exact_uuid_once(status, reconciled_state, clock):
    client = Fake()

    def lost(c):
        c.job.update(state="running", confirmed=True)
        raise ClientError("lost start", status_code=status)

    client.faults["start"].append(lost)
    def reconciled(c):
        c.job["state"] = reconciled_state
        return copy.deepcopy(c.job)

    client.faults["status"].extend([ClientError(status_code=404), reconciled])
    result = run(client)
    assert result.outcome == "completed" and result.exit_code == 0
    assert names(client).count("start") == 1 and names(client).count("preview") == 1
    assert "cancel" not in names(client)
    assert all(args == (result.job_id,) for name, args, _ in client.calls[2:] if name == "status")


def test_start_409_not_confirmation_when_still_ready():
    client = Fake()
    client.faults["start"].append(ClientError(status_code=409))
    result = run(client)
    assert result.exit_code == 1 and result.error_code == "apply_state"
    assert client.job["state"] == "cancelled" and not result.confirmed
    assert names(client).count("start") == 1


def test_dry_run_cancels_owned_preview_preserves_estimate():
    client = Fake()
    result = run(client, dry_run=True)
    assert result.outcome == "dry_run" and result.exit_code == 0
    assert result.cleanup_state == "cancelled" and result.counts["files"] == 3
    assert result.counts["logical_bytes"] == 123 and not result.confirmed
    assert names(client) == ["status", "preview", "cancel"]


@pytest.mark.parametrize("fault", ["error", "errors", "changed", "expired", "missing_expiry",
                                  "confirmed", "failed", "interrupted", "cancelled"])
def test_bad_preview_never_started_and_owned_cancelled(fault):
    client = Fake()

    def bad(c):
        c.job = job()
        if fault == "error":
            c.job["error"] = "secret server body"
        elif fault in {"errors", "changed"}:
            c.job["report"][fault] = 1
        elif fault == "expired":
            c.job["expires_ns"] = 1
        elif fault == "missing_expiry":
            del c.job["expires_ns"]
        elif fault == "confirmed":
            c.job["confirmed"] = True
        else:
            c.job["state"] = fault
        return copy.deepcopy(c.job)

    client.faults["preview"].append(bad)
    result = run(client)
    assert result.exit_code == 1 and "start" not in names(client)
    assert names(client).count("cancel") == 1


@pytest.mark.parametrize("field,value", [
    ("errors", 1), ("changed", 1), ("files", -1), ("deleted", True),
    ("errors", "0"), ("skipped", 0.5), ("retained", None),
])
def test_completed_report_must_be_clean_nonnegative_integers(field, value, clock):
    client = Fake()

    def completed(c):
        answer = copy.deepcopy(c.job)
        answer["state"] = "completed"
        answer["report"][field] = value
        return answer

    client.faults["status"].extend([ClientError(status_code=404), completed])
    result = run(client)
    assert result.exit_code == 1 and result.outcome != "completed"


@pytest.mark.parametrize("missing", ["errors", "changed", "files", "deleted", "cutoff_ns", "root"])
def test_missing_report_fields_fail_closed(missing, clock):
    client = Fake()

    def completed(c):
        answer = copy.deepcopy(c.job)
        answer["state"] = "completed"
        del answer["report"][missing]
        return answer

    client.faults["status"].extend([ClientError(status_code=404), completed])
    result = run(client)
    assert result.exit_code == 1 and result.error_code == "invalid_response"


@pytest.mark.parametrize("state", ["failed", "interrupted", "cancelled", "cancelling"])
def test_noncompleted_apply_nonzero(state, clock):
    client = Fake()

    def failed(c):
        c.job["state"] = state
        return copy.deepcopy(c.job)

    client.faults["status"].extend([ClientError(status_code=404), failed])
    result = run(client)
    assert result.exit_code == 1 and result.outcome != "completed"
    assert names(client).count("cancel") == 1


def test_work_deadline_cancels_then_waits_for_terminal_with_separate_budget(clock):
    client = Fake()
    client.keep_running = True

    def cancelling(c):
        c.job["state"] = "cancelling"
        return copy.deepcopy(c.job)

    def cancelled(c):
        c.job["state"] = "cancelled"
        return copy.deepcopy(c.job)

    client.faults["cancel"].append(cancelling)
    # First status is current. Running status consumes work; cleanup observes cancelled.
    client.faults["status"].extend([
        ClientError(status_code=404), lambda c: copy.deepcopy(c.job), cancelled,
    ])
    result = run(client, max_runtime=0.02, cancel_timeout=0.1)
    assert result.error_code == "deadline" and result.exit_code == 1
    assert result.cleanup_state == "cancelled" and clock.now < 100.12
    assert names(client).count("cancel") == 1
    assert client.timeout == 30
    assert all(0 < timeout <= 0.1 + 1e-9 for _, _, timeout in client.calls)


def test_cleanup_deadline_cancelling_is_not_cancelled(clock):
    client = Fake()
    client.keep_running = True

    def cancelling(c):
        c.job["state"] = "cancelling"
        return copy.deepcopy(c.job)

    client.faults["cancel"].append(cancelling)
    result = run(client, max_runtime=0.01, cancel_timeout=0.03)
    assert result.outcome == "cancel_unconfirmed" and result.exit_code == 1
    assert result.cleanup_state == "unconfirmed" and result.state == "cancelling"
    assert result.job_id == client.job["id"] and clock.now <= 100.041


def test_monitor_disconnection_and_cancel_uncertainty(clock):
    client = Fake()
    client.faults["status"].extend([
        ClientError(status_code=404), ClientError("secret"), ClientError("secret"),
    ])
    client.faults["cancel"].append(ClientError("secret"))
    result = run(client)
    assert result.outcome == "cancel_unconfirmed" and result.exit_code == 1
    assert result.job_id == client.job["id"] and result.state == "running"
    assert client.timeout == 30


def test_lost_cancel_reply_reconciles_owned_job_only():
    client = Fake()

    def lost(c):
        c.job["state"] = "cancelled"
        raise ClientError("lost")

    client.faults["cancel"].append(lost)
    result = run(client, dry_run=True)
    assert result.outcome == "dry_run" and result.cleanup_state == "cancelled"
    assert names(client) == ["status", "preview", "cancel", "status"]
    assert client.calls[-1][1] == (result.job_id,)


def test_completed_after_deadline_not_reported_as_clean_success(clock):
    client = Fake()

    def late(c):
        clock.now += 2
        c.job["state"] = "completed"
        return copy.deepcopy(c.job)

    client.faults["status"].extend([ClientError(status_code=404), late])
    result = run(client, max_runtime=1)
    assert result.error_code == "deadline" and result.exit_code == 1
    assert result.cleanup_state == "completed" and result.outcome != "completed"


@pytest.mark.parametrize("interrupt,code,exit_code", [
    (KeyboardInterrupt(), "interrupted", 130), (s.ScheduledTermination(), "sigterm", 143),
])
def test_injected_interrupt_cancels_only_owned_job(interrupt, code, exit_code, clock):
    client = Fake()
    client.faults["status"].extend([ClientError(status_code=404), interrupt])
    result = run(client)
    assert result.error_code == code and result.exit_code == exit_code
    assert result.cleanup_state == "cancelled"
    assert names(client).count("cancel") == 1 and client.timeout == 30


@pytest.mark.parametrize("option", ["max_runtime", "cancel_timeout", "poll_interval"])
@pytest.mark.parametrize("value", [0, -1, float("nan"), float("inf"), float("-inf"), True])
def test_limits_positive_finite_no_requests(option, value):
    client = Fake()
    kwargs = {"older_than": "30d", option: value}
    result = s.run_scheduled(client, **kwargs)
    assert result.error_code == "invalid_arguments" and result.exit_code == 1
    assert not client.calls


def test_client_timeout_restored_on_failure_and_remaining_budget(clock):
    client = Fake()
    client.faults["preview"].append(ClientError("secret"))
    result = run(client, max_runtime=0.2)
    assert result.exit_code == 1 and client.timeout == 30
    assert all(0 < timeout <= 0.2 + 1e-9 for _, _, timeout in client.calls)


@pytest.mark.parametrize("where", ["cancel", "poll"])
@pytest.mark.parametrize("interrupt", [KeyboardInterrupt(), s.ScheduledTermination()])
def test_interrupt_during_cleanup_finishes_original_budget(where, interrupt, clock):
    client = Fake()
    if where == "cancel":
        client.faults["cancel"].append(interrupt)
    else:
        def cancelling(c):
            c.job["state"] = "cancelling"
            return copy.deepcopy(c.job)

        def cancelled(c):
            c.job["state"] = "cancelled"
            return copy.deepcopy(c.job)

        client.faults["cancel"].append(cancelling)
        client.faults["status"].extend([ClientError(status_code=404), interrupt, cancelled])
    result = run(client, dry_run=True, cancel_timeout=0.1)
    assert result.exit_code in {130, 143} and result.outcome != "dry_run"
    assert result.cleanup_state == "cancelled"
    assert clock.now < 100.1 and client.timeout == 30
    assert all(args[0] == result.job_id for name, args, _ in client.calls if name == "cancel")


def test_previewing_waits_for_ready_without_adopting_current(clock):
    client = Fake()

    def previewing(c):
        c.job = job("previewing")
        return copy.deepcopy(c.job)

    def ready(c):
        c.job["state"] = "ready"
        return copy.deepcopy(c.job)

    client.faults["preview"].append(previewing)
    client.faults["status"].extend([ClientError(status_code=404), ready])
    result = run(client, dry_run=True)
    assert result.exit_code == 0 and result.outcome == "dry_run"
    assert client.calls[2][1] == (result.job_id,)


@pytest.mark.parametrize("during", ["work_body", "work_headers", "cancel_body"])
def test_trickling_http_peer_cannot_extend_work_or_cleanup_budget(during):
    """Exercise actual network reads, not just the timeout assignment."""
    state = job()
    stopped = threading.Event()
    cancel_seen = threading.Event()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def answer(self, status, body, *, slow=False, headers=False):
            data = json.dumps(body).encode()
            try:
                if headers:
                    for char in b"HTTP/1.1 200 OK\r\nX-Slow: unlimited":
                        self.wfile.write(bytes([char]))
                        self.wfile.flush()
                        if stopped.wait(0.02):
                            return
                    return
                self.send_response(status)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                if slow:
                    for char in data:
                        self.wfile.write(bytes([char]))
                        self.wfile.flush()
                        if stopped.wait(0.02):
                            return
                else:
                    self.wfile.write(data)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def do_GET(self):
            if self.path == "/admin/blobs/prune":
                self.answer(404, {})
            else:
                self.answer(200, state, slow=during == "work_body",
                            headers=during == "work_headers")

        def do_POST(self):
            self.rfile.read(int(self.headers["Content-Length"]))
            if self.path.endswith("/cancel"):
                cancel_seen.set()
                state["state"] = "cancelled"
                self.answer(200, state, slow=during == "cancel_body")
            elif self.path.endswith("/start"):
                state.update(state="running", confirmed=True)
                self.answer(202, state)
            else:
                self.answer(202, state)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = False
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
    thread.start()
    client = RetentionClient(f"http://127.0.0.1:{server.server_port}", "fixture", timeout=30)
    started = time.monotonic()
    try:
        result = run(client, dry_run=during == "cancel_body", max_runtime=0.15, cancel_timeout=0.15)
        elapsed = time.monotonic() - started
        assert elapsed < 0.8, f"request overran budget: {elapsed}"
        assert cancel_seen.is_set() and result.exit_code == 1
        if during == "cancel_body":
            assert result.outcome == "cancel_unconfirmed"
        else:
            assert result.cleanup_state == "cancelled" and result.error_code == "deadline"
        assert client.timeout == 30
    finally:
        stopped.set()
        server.shutdown()
        server.server_close()
        thread.join(2)
        assert not thread.is_alive()


def test_tcp_addresses_share_one_remaining_budget(monkeypatch):
    from context_intelligence_server import retention_client as c
    now = [100.0]
    timeouts, closed = [], []
    addresses = [(2, 1, 6, "", ("192.0.2.1", 443)), (2, 1, 6, "", ("192.0.2.2", 443))]

    class Socket:
        def __init__(self, *args):
            pass

        def settimeout(self, seconds):
            self.timeout = seconds
            timeouts.append(seconds)

        def connect(self, address):
            now[0] += min(0.1, self.timeout)
            raise TimeoutError()

        def close(self):
            closed.append(True)

    monkeypatch.setattr(c, "time", SimpleNamespace(monotonic=lambda: now[0]))
    monkeypatch.setattr(c.socket, "getaddrinfo", lambda *args: addresses)
    monkeypatch.setattr(c.socket, "socket", Socket)
    with pytest.raises(TimeoutError):
        c._connect(("fixture.invalid", 443), 0.15, None, deadline=100.15)
    assert timeouts == pytest.approx([0.15, 0.05]) and len(closed) == 2
    assert now[0] == pytest.approx(100.15)


def test_tcp_unsupported_address_family_still_falls_back(monkeypatch):
    import errno
    from context_intelligence_server import retention_client as c
    now = [100.0]
    attempts, timeouts = [], []
    addresses = [(10, 1, 6, "", ("::1", 443)), (2, 1, 6, "", ("127.0.0.1", 443))]

    class Socket:
        def __init__(self, family, *args):
            attempts.append(family)
            if family == 10:
                now[0] += 0.01
                raise OSError(errno.EAFNOSUPPORT, "fixture unsupported family")

        def settimeout(self, seconds):
            timeouts.append(seconds)

        def connect(self, address):
            self.address = address

    monkeypatch.setattr(c, "time", SimpleNamespace(monotonic=lambda: now[0]))
    monkeypatch.setattr(c.socket, "getaddrinfo", lambda *args: addresses)
    monkeypatch.setattr(c.socket, "socket", Socket)
    connected = c._connect(("fixture.invalid", 443), 0.15, None, deadline=100.15)
    assert attempts == [10, 2] and connected.address == ("127.0.0.1", 443)
    assert timeouts == pytest.approx([0.14])


def test_receipt_allowlist_never_server_body_root_uri_or_secret(clock):
    client = Fake()

    def tainted(c):
        answer = copy.deepcopy(c.job)
        answer["state"] = "completed"
        answer["report"]["samples"] = ["ci-blob://private/session"]
        answer["error"] = FIXTURE_KEY + " body\nlog injection"
        return answer

    client.faults["status"].extend([ClientError(status_code=404), tainted])
    result = run(client)
    text = json.dumps(asdict(result))
    assert result.exit_code == 1 and result.error_code == "job_error"
    assert all(secret not in text for secret in [FIXTURE_KEY, "ci-blob://", "/scratch", "injection"])


def scheduled_command(url, *args):
    return [str(CLI), "blobs", "prune-scheduled", "--online", "--older-than", "30d",
            "--server-url", url, *args]


def test_real_http_dry_run_next_cycle_apply_non_tty_and_expiry(live_retention, tmp_path):
    url, old, service = live_retention
    key = tmp_path / "private-admin"
    key.write_text(FIXTURE_KEY)
    key.chmod(0o600)
    command = scheduled_command(url, "--admin-key-file", str(key),
                                "--max-runtime", "5", "--cancel-timeout", "2")
    dry = subprocess.run([*command, "--dry-run"], input="", capture_output=True, text=True, timeout=10)
    dry_receipt = json.loads(dry.stdout)
    assert dry.returncode == 0 and not dry.stderr
    assert dry_receipt["outcome"] == "dry_run" and dry_receipt["counts"]["files"] == 1
    assert dry_receipt["cleanup_state"] == "cancelled" and old.exists()
    assert not (old.parents[2] / ".ci-blob-retention").exists()
    applied = subprocess.run(command, input="", capture_output=True, text=True, timeout=10)
    receipt = json.loads(applied.stdout)
    assert applied.returncode == 0 and not applied.stderr, applied.stdout + applied.stderr
    assert receipt["outcome"] == "completed" and receipt["confirmed"]
    assert receipt["counts"]["deleted"] == 1 and not old.exists()
    assert receipt["job_id"] != dry_receipt["job_id"]
    assert service.status()["state"] == "completed"
    # Assert real durable expiry, not just an absent physical file.
    import sqlite3
    from context_intelligence_server.blob_retention import DATABASE, METADATA
    ledger = old.parents[2] / METADATA / DATABASE
    assert ledger.is_file()
    with sqlite3.connect(f"file:{ledger}?mode=ro", uri=True) as db:
        assert db.execute("SELECT count(*) FROM expiry WHERE pending = 0").fetchone()[0] == 1
    assert FIXTURE_KEY not in dry.stdout + applied.stdout
    print("scratch dry-run receipt:", dry.stdout.strip())
    print("scratch apply receipt:", applied.stdout.strip())


def test_real_http_status_codes_and_manual_busy_untouched(live_retention):
    url, old, service = live_retention
    with pytest.raises(ClientError) as absent:
        RetentionClient(url, FIXTURE_KEY).status()
    assert absent.value.status_code == 404
    with pytest.raises(ClientError) as forbidden:
        RetentionClient(url, "bad-fixture-key").status()
    assert forbidden.value.status_code == 401
    client = RetentionClient(url, FIXTURE_KEY)
    manual = client.wait_preview(client.preview("30d"), timeout=3, interval=0.01)
    for dry_run in (True, False):
        result = s.run_scheduled(client, older_than="30d", dry_run=dry_run)
        assert result.outcome == "skipped_busy" and result.exit_code == 0
        assert client.status()["id"] == manual["id"] and client.status()["state"] == "ready"
    assert old.exists()


@pytest.mark.parametrize("trigger", ["running", "cleanup"])
def test_real_sigterm_running_non_tty_explicit_cancel(live_retention, monkeypatch, trigger):
    url, old, service = live_retention
    # Hold a REAL admitted storage batch. Cancel must wait, not invent cancelled.
    from context_intelligence_server import blob_retention as r
    entered, release = threading.Event(), threading.Event()
    original = r.apply_batch

    def held_batch(*args, **kwargs):
        entered.set()
        assert release.wait(8)
        return original(*args, **kwargs)

    monkeypatch.setattr(r, "apply_batch", held_batch)
    process = subprocess.Popen(
        scheduled_command(url, "--cancel-timeout", "5", "--max-runtime",
                          "2.5" if trigger == "cleanup" else "10"),
        stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, env={**os.environ, "ADMIN_API_KEY": FIXTURE_KEY},
    )
    try:
        assert entered.wait(5), "scheduled process did not enter real apply batch"
        if trigger == "cleanup":
            deadline = time.monotonic() + 4
            while service.status()["state"] != "cancelling":
                assert process.poll() is None and time.monotonic() < deadline
                time.sleep(0.005)
        process.send_signal(signal.SIGTERM)
        deadline = time.monotonic() + 3
        while service.status()["state"] != "cancelling":
            assert process.poll() is None and time.monotonic() < deadline
            time.sleep(0.005)
        assert process.poll() is None, "cancelling is not terminal"
        release.set()
        stdout, stderr = process.communicate(timeout=8)
        receipt = json.loads(stdout)
        assert process.returncode == 143 and not stderr
        assert receipt["error_code"] == ("deadline" if trigger == "cleanup" else "sigterm")
        assert receipt["cleanup_state"] == "cancelled"
        assert receipt["confirmed"] and service.status()["state"] == "cancelled"
        assert receipt["counts"]["deleted"] == 1  # admitted batch may finish
        print("scratch SIGTERM receipt:", stdout.strip())
    finally:
        release.set()
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=8)


@pytest.mark.parametrize("flag", ["--blob-root", "--job-id", "--apply", "--yes"])
def test_scheduled_command_has_no_offline_or_interactive_bypass(flag):
    result = subprocess.run(
        scheduled_command("http://127.0.0.1:1", flag), input="", capture_output=True, text=True, timeout=5,
    )
    assert result.returncode == 2 and "unrecognized arguments" in result.stderr


@pytest.mark.parametrize("missing", ["--online", "--older-than", "--server-url"])
def test_scheduled_requires_explicit_transport_age_and_online(missing):
    command = scheduled_command("http://127.0.0.1:1")
    index = command.index(missing)
    del command[index:index + (1 if missing == "--online" else 2)]
    result = subprocess.run(command, input="", capture_output=True, text=True, timeout=5)
    assert result.returncode == 2 and "required" in result.stderr


def test_scoped_sigterm_handler_restored_and_setup_errors_sanitized(monkeypatch, capsys):
    from context_intelligence_server.cli import main
    previous = signal.getsignal(signal.SIGTERM)
    with pytest.raises(SystemExit) as exit:
        main(["blobs", "prune-scheduled", "--online", "--older-than", "30d",
              "--server-url", "http://remote.invalid", "--admin-key-file", "/private/do-not-read"])
    assert exit.value.code == 1 and signal.getsignal(signal.SIGTERM) == previous
    receipt = capsys.readouterr()
    assert json.loads(receipt.out)["error_code"] == "client_setup_failed"
    assert not receipt.err and "private" not in receipt.out and "remote.invalid" not in receipt.out