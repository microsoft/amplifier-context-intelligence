"""Bounded unattended client of the existing online API, never a storage writer."""

from __future__ import annotations

import math
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import PurePosixPath
from zoneinfo import ZoneInfo

from context_intelligence_server.blob_retention import duration_ns
from context_intelligence_server.retention_client import ClientError, RetentionClient

BUSY = frozenset({"previewing", "ready", "running", "cancelling"})
TERMINAL = frozenset({"completed", "cancelled", "failed", "interrupted"})
COUNTS = ("files", "logical_bytes", "allocated_bytes", "retained", "skipped",
          "changed", "errors", "deleted")


class ScheduledTermination(KeyboardInterrupt):
    """Raised only by the scheduled CLI's scoped SIGTERM handler."""


def _pacific_now() -> str:
    return datetime.now(ZoneInfo("America/Los_Angeles")).isoformat()


@dataclass
class ScheduledResult:
    """Allow-listed receipt: no server text, paths, sample URIs or credentials."""

    outcome: str = "failed"
    exit_code: int = 1
    job_id: str | None = None
    state: str | None = None
    confirmed: bool = False
    cutoff_ns: int | None = None
    counts: dict[str, int] = field(default_factory=dict)
    cleanup_state: str = "not_requested"
    error_code: str | None = None
    started_at: str = field(default_factory=_pacific_now)
    ended_at: str | None = None


class _Problem(Exception):
    pass


@dataclass(frozen=True)
class _Identity:
    id: str
    root: str
    age: str
    cutoff: int
    started: int


def _integer(value) -> bool:
    return type(value) is int and 0 <= value <= 2**63 - 1


def _identity(job) -> _Identity:
    try:
        root = job["root"]
        identity = _Identity(job["id"], root, job["older_than"],
                             job["cutoff_ns"], job["preview_started_ns"])
        if (str(uuid.UUID(identity.id)) != identity.id
                or not isinstance(root, str) or not root.startswith("/")
                or str(PurePosixPath(root)) != root or ".." in PurePosixPath(root).parts
                or any(ord(c) < 32 for c in root)
                or not _integer(identity.cutoff) or not _integer(identity.started)
                or identity.started - duration_ns(identity.age) != identity.cutoff):
            raise ValueError()
        return identity
    except (KeyError, TypeError, ValueError, AttributeError):
        raise _Problem("invalid_identity") from None


def _validate(job) -> tuple[str, dict[str, int]]:
    try:
        state = job["state"]
        report = job["report"]
        if (state not in BUSY | TERMINAL or type(job["confirmed"]) is not bool
                or "error" not in job
                or report["root"] != job["root"] or report["cutoff_ns"] != job["cutoff_ns"]
                or not _integer(report["cutoff_ns"])
                or any(not _integer(report[key]) for key in COUNTS)
                or (state in {"previewing", "ready"} and job["confirmed"])
                or (state in {"running", "completed"} and not job["confirmed"])):
            raise ValueError()
        return state, {key: report[key] for key in COUNTS}
    except (KeyError, TypeError, ValueError):
        raise _Problem("invalid_response") from None


def _error_code(exc: Exception) -> str:
    if isinstance(exc, _Problem):
        return str(exc)  # Only fixed internal codes, never remote text.
    if isinstance(exc, ClientError):
        code = exc.status_code
        return f"http_{code}" if type(code) is int and 100 <= code <= 599 else "client_error"
    return "client_failure"


def run_scheduled(
    client: RetentionClient, *, older_than: str, dry_run: bool = False,
    max_runtime: float = 3600, cancel_timeout: float = 120, poll_interval: float = 1,
) -> ScheduledResult:
    """Own at most one preview. Start it once, or explicitly cancel and observe.

    Work and cleanup have separate monotonic budgets. Each request receives at
    most the remaining socket budget; the caller's timeout is always restored.
    OS hostname resolution can delay deadline checks (prefer numeric loopback).
    Cancellation cannot be guaranteed after network loss or SIGKILL; a nonzero
    cancel_unconfirmed receipt names only the owned UUID.
    """
    result = ScheduledResult()
    owned: _Identity | None = None
    current_root: str | None = None
    deadline = 0.0

    def remaining(until: float) -> float:
        left = until - time.monotonic()
        if left <= 0:
            raise _Problem("deadline")
        return left

    def call(operation, *args, until: float):
        left = remaining(until)
        previous = client.timeout
        try:
            client.timeout = min(previous, left)
            return operation(*args)
        finally:
            client.timeout = previous

    def pause(until: float):
        time.sleep(min(poll_interval, remaining(until)))

    def observe(job):
        if _identity(job) != owned:
            raise _Problem("identity_changed")
        state, counts = _validate(job)
        if result.confirmed and not job["confirmed"]:
            raise _Problem("confirmation_changed")
        result.state, result.counts = state, counts
        result.confirmed = job["confirmed"]
        return job

    def clean_preview(job):
        if job["confirmed"]:
            raise _Problem("unexpected_confirmation")
        if job["error"] is not None or result.counts["errors"] or result.counts["changed"]:
            raise _Problem("preview_errors")
        if job["state"] not in {"previewing", "ready"}:
            raise _Problem("preview_state")
        if job["state"] == "ready":
            expiry = job.get("expires_ns")
            if not _integer(expiry) or expiry <= time.time_ns():
                raise _Problem("preview_expired")

    def cleanup() -> bool:
        """Never inspect or cancel 'current'; every request pins the owned UUID."""
        until = time.monotonic() + cancel_timeout
        result.cleanup_state = "unconfirmed"
        job = None
        acknowledged = False
        while time.monotonic() < until:
            try:
                if job is None or (
                    not acknowledged
                    and job["state"] in {"previewing", "ready", "running"}
                ):
                    try:
                        job = observe(call(client.cancel, owned.id, until=until))
                        acknowledged = True
                    except ClientError:
                        # The request or its reply can be lost. Observe only this UUID;
                        # if it is still active, cancel is retried within this deadline.
                        job = observe(call(client.status, owned.id, until=until))
                remaining(until)
                if job["state"] in TERMINAL:
                    result.cleanup_state = job["state"]
                    return True
                pause(until)
                job = observe(call(client.status, owned.id, until=until))
            except KeyboardInterrupt as exc:
                # A first signal may arrive DURING cleanup, before cancel was
                # sent. Continue within the original budget, not a new 120s.
                result.error_code = result.error_code or (
                    "sigterm" if isinstance(exc, ScheduledTermination) else "interrupted"
                )
                result.exit_code = 143 if isinstance(exc, ScheduledTermination) else 130
            except Exception:
                return False
        return False

    try:
        duration_ns(older_than)
        for limit in (max_runtime, cancel_timeout, poll_interval, client.timeout):
            if isinstance(limit, bool) or not math.isfinite(limit) or limit <= 0:
                raise ValueError()
        if type(dry_run) is not bool:
            raise ValueError()
        deadline = time.monotonic() + max_runtime
        if not math.isfinite(deadline):
            raise ValueError()
        try:
            current = call(client.status, until=deadline)
        except ClientError as exc:
            if exc.status_code != 404:
                raise
        else:
            current_root = _identity(current).root
            state, _ = _validate(current)
            if state in BUSY:
                result.outcome, result.exit_code = "skipped_busy", 0
                return result
        try:
            job = call(client.preview, older_than, until=deadline)
        except ClientError as exc:
            if exc.status_code == 409:
                result.outcome, result.exit_code = "skipped_busy", 0
                return result
            raise
        identity = _identity(job)
        if identity.age != older_than:
            raise _Problem("age_changed")
        if current_root is not None and identity.root != current_root:
            raise _Problem("root_changed")
        # Ownership is granted only by a validated successful creation reply.
        owned = identity
        result.job_id, result.cutoff_ns = owned.id, owned.cutoff
        observe(job)
        while True:
            remaining(deadline)
            clean_preview(job)
            if job["state"] == "ready":
                break
            pause(deadline)
            job = observe(call(client.status, owned.id, until=deadline))
        if dry_run:
            estimate = result.counts.copy()
            if not cleanup():
                raise _Problem("cancel_unconfirmed")
            if result.state != "cancelled" or result.confirmed:
                raise _Problem("dry_run_not_cancelled")
            result.counts = estimate
            if result.error_code is not None:
                return result
            result.outcome, result.exit_code = "dry_run", 0
            return result
        try:
            job = call(client.start, owned.id, "prune", until=deadline)
        except ClientError:
            # POST may have taken effect. Never retry start or create another job.
            job = call(client.status, owned.id, until=deadline)
        observe(job)
        while True:
            remaining(deadline)
            if job["error"] is not None:
                raise _Problem("job_error")
            if not job["confirmed"] or job["state"] not in {"running", "completed"}:
                raise _Problem("apply_state")
            if job["state"] == "completed":
                if result.counts["errors"] or result.counts["changed"]:
                    raise _Problem("apply_errors")
                result.outcome, result.exit_code = "completed", 0
                return result
            pause(deadline)
            job = observe(call(client.status, owned.id, until=deadline))
    except ScheduledTermination:
        result.error_code, result.exit_code = "sigterm", 143
    except KeyboardInterrupt:
        result.error_code, result.exit_code = "interrupted", 130
    except (ValueError, TypeError, OverflowError):
        result.error_code = "invalid_arguments"
    except Exception as exc:
        result.error_code = (
            "deadline" if deadline and time.monotonic() >= deadline else _error_code(exc)
        )
    finally:
        # Returns still run finally. Successful runs and busy skips need no cancel.
        if result.exit_code and owned is not None and result.cleanup_state == "not_requested":
            if not cleanup():
                result.outcome = "cancel_unconfirmed"
        if result.cleanup_state == "unconfirmed":
            result.outcome = "cancel_unconfirmed"
        result.ended_at = _pacific_now()
    return result