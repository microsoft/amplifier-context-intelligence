"""Logging configuration with stdout and rotating file handlers."""

import json
import logging
import logging.handlers
import sys
from pathlib import Path

from context_intelligence_server.config import get_settings

_MAX_BYTES = 10 * 1024 * 1024
_BACKUP_COUNT = 5

# Third-party loggers (gunicorn + uvicorn.workers.UvicornWorker) that install
# their OWN handlers and do NOT propagate by default. Left alone, their lines
# (startup, access, errors) reach stdout as PLAIN TEXT, which Azure Log Analytics
# cannot parse. We strip those handlers and force propagation so every record
# bubbles up to the root logger's JsonFormatter-wired handlers as one-line JSON.
#
# Boundary (confirmed by live gunicorn+UvicornWorker boot): the dividing line is
# the moment the worker runs setup_logging() inside the FastAPI lifespan. EVERY
# line emitted BEFORE that point is plain text and outside this function's reach
# — this spans both the gunicorn MASTER lines ("Starting gunicorn", "Listening
# at", "Booting worker", "Worker exited", "Shutting down") AND the early uvicorn
# *worker* lines that fire before lifespan startup completes ("Started server
# process", "Waiting for application startup"). Those plain-text lines are only
# reachable via gunicorn's own --log-config / logconfig_dict. EVERYTHING emitted
# AFTER setup_logging() runs — app logs, neo4j_store logs, uvicorn.error/access
# at runtime, gunicorn.error worker events — is one-line JSON.
_THIRD_PARTY_LOGGER_NAMES = (
    "uvicorn",
    "uvicorn.error",
    "uvicorn.access",
    "gunicorn.error",
    "gunicorn.access",
)


def _route_third_party_loggers_to_root() -> None:
    """Strip third-party loggers' own handlers and force propagation to root.

    Idempotent: clearing handlers and setting propagate=True can be repeated
    safely. This is the standard way to unify framework logging onto a single
    formatter wired on the root logger.
    """
    for name in _THIRD_PARTY_LOGGER_NAMES:
        lg = logging.getLogger(name)
        lg.handlers.clear()
        lg.propagate = True


class JsonFormatter(logging.Formatter):
    """Serialize each log record to exactly one physical JSON line.

    Promotes session_id (and only session_id) to a top-level key. Folds any
    exc_info into a single-line 'exc' string field so multi-line tracebacks
    never break a record across physical lines. Never raises: on any failure it
    emits a minimal valid-JSON record instead.
    """

    def format(self, record: logging.LogRecord) -> str:
        try:
            obj = {
                "time": self.formatTime(record),
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),
            }
            session_id = getattr(record, "session_id", None)
            if session_id is not None:
                obj["session_id"] = session_id
            if record.exc_info:
                obj["exc"] = self.formatException(record.exc_info)
            return json.dumps(obj, default=str)
        except Exception:
            fallback = {
                "time": "",
                "level": getattr(record, "levelname", "ERROR"),
                "logger": getattr(record, "name", ""),
                "message": "log record formatting failed",
            }
            return json.dumps(fallback)


class _DemoteToDebugFilter(logging.Filter):
    """Rewrite a record to DEBUG level.

    Attached to the per-request HTTP access loggers (uvicorn.access /
    gunicorn.access). uvicorn emits one access line PER REQUEST at INFO; those
    routine 2xx lines flood the log stream and bury the ingest-pipeline signals.
    A 2xx request is not an INFO-worthy event, so we demote it to DEBUG: hidden
    at the default INFO level, visible only when the level is lowered to DEBUG.
    WARNING stays reserved for genuine problems.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        record.levelno = logging.DEBUG
        record.levelname = "DEBUG"
        return True


# Module-level singleton. logging.Logger.addFilter() de-duplicates by identity,
# so reusing one instance keeps setup_logging() idempotent — a fresh instance per
# call would stack filters on the process-global access loggers (harmless but
# unbounded, notably in tests where the handler guard does not short-circuit).
_DEMOTE_TO_DEBUG = _DemoteToDebugFilter()


def setup_logging() -> None:
    """Configure root logger with stdout StreamHandler and RotatingFileHandler.

    Reads log_path and log_level from get_settings(). Creates the parent
    directory of log_path if it does not already exist.

    Idempotent: if the root logger already has handlers attached (e.g. because
    the application lifespan is exercised multiple times in tests) this function
    returns immediately without adding duplicate handlers.

    Deploy-safe boot (council amendment, 2026-08-12): this function MUST NEVER
    raise. The stdout/stderr StreamHandler is configured FIRST and
    unconditionally, so console logging always exists to report a degraded
    state. The RotatingFileHandler (and its parent-directory mkdir) is
    best-effort and wrapped in its own try/except: on failure (e.g. the
    `/data` volume not yet mounted, or a read-only path on Azure Container
    Apps) it falls back to console-only and logs a loud WARNING, rather than
    crash-looping the worker. This is the exact real-process failure a
    live boot test caught: `setup_logging()` used to `mkdir('/data')` before
    the lifespan try/except boundary and a `PermissionError` there sank the
    whole worker with no Neo4j involvement at all.
    """
    settings = get_settings()
    log_path = Path(settings.log_path)
    # If the user supplied a directory path (no suffix), default to server.jsonl
    if not log_path.suffix:
        log_path = log_path / "server.jsonl"
    log_level = settings.log_level

    formatter = JsonFormatter()

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Guard: skip handler registration if our StreamHandler is already present.
    # Checking for our own console StreamHandler (rather than any handler)
    # avoids false positives from pytest's log-capture handler which is always
    # present during tests. We key off the console handler (not the file
    # handler) because the file handler may legitimately be ABSENT in a
    # degraded boot -- if we keyed off the file handler, a degraded first call
    # would re-run on the next call and stack duplicate console handlers.
    if any(getattr(h, "_ci_console_handler", False) for h in root_logger.handlers):
        return

    # stdout stream handler -- configured FIRST and unconditionally, so console
    # logging ALWAYS exists (deploy-safe boot: we must be able to report a
    # degraded state even when file logging is unavailable).
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(log_level)
    # Tag so the idempotency guard above can recognise our own console handler.
    stream_handler._ci_console_handler = True  # type: ignore[attr-defined]
    root_logger.addHandler(stream_handler)

    # rotating file handler -- best-effort. A failure here (unwritable /data,
    # volume not yet mounted, wrong perms) MUST NOT crash boot; fall back to
    # console-only. Both the parent-dir mkdir and the handler construction can
    # raise OSError/PermissionError, so both live inside this guard.
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            filename=str(log_path),
            maxBytes=_MAX_BYTES,
            backupCount=_BACKUP_COUNT,
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(log_level)
        root_logger.addHandler(file_handler)
    except OSError as exc:
        # Console logging is already wired above, so this warning is delivered.
        root_logger.warning(
            "file logging unavailable at %s, using console only: %s",
            log_path,
            exc,
        )

    # Route uvicorn/gunicorn loggers up to the root JsonFormatter so every line
    # from those frameworks is one-line JSON (not plain text) for Azure Log
    # Analytics. See _THIRD_PARTY_LOGGER_NAMES for the master-process boundary.
    _route_third_party_loggers_to_root()

    # Use levels correctly so the ingest-pipeline signals stay visible in the
    # stream the dashboard tails:
    #  - per-request HTTP access logs (uvicorn.access / gunicorn.access) are
    #    routine 2xx noise -> demote to DEBUG (hidden at INFO, shown at DEBUG).
    #  - the neo4j driver's chatty INFO schema "notifications" ("index already
    #    exists") are suppressed below WARNING.
    #
    # INVARIANT: the demote filter MUST live on the LOGGER, not a handler. Logger
    # filters run in Logger.handle() BEFORE callHandlers(), so the record is
    # rewritten to DEBUG before the handler-level gate (set above) evaluates it.
    # Moved to a handler it would run AFTER that gate and access logs would
    # reappear at INFO. The handler.setLevel(log_level) above is the gate that
    # actually drops the demoted records — both lines are load-bearing.
    for name in ("uvicorn.access", "gunicorn.access"):
        logging.getLogger(name).addFilter(_DEMOTE_TO_DEBUG)
    logging.getLogger("neo4j.notifications").setLevel(logging.WARNING)
