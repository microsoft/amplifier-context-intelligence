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


def setup_logging() -> None:
    """Configure root logger with stdout StreamHandler and RotatingFileHandler.

    Reads log_path and log_level from get_settings(). Creates the parent
    directory of log_path if it does not already exist.

    Idempotent: if the root logger already has handlers attached (e.g. because
    the application lifespan is exercised multiple times in tests) this function
    returns immediately without adding duplicate handlers.
    """
    settings = get_settings()
    log_path = Path(settings.log_path)
    # If the user supplied a directory path (no suffix), default to server.jsonl
    if not log_path.suffix:
        log_path = log_path / "server.jsonl"
    log_level = settings.log_level

    # Ensure parent directory exists
    log_path.parent.mkdir(parents=True, exist_ok=True)

    formatter = JsonFormatter()

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Guard: skip handler registration if our RotatingFileHandler is already present.
    # Checking for a RotatingFileHandler (rather than any handler) avoids false
    # positives from pytest's log-capture handler which is always present during tests.
    if any(
        isinstance(h, logging.handlers.RotatingFileHandler)
        for h in root_logger.handlers
    ):
        return

    # stdout stream handler
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    root_logger.addHandler(stream_handler)

    # rotating file handler
    file_handler = logging.handlers.RotatingFileHandler(
        filename=str(log_path),
        maxBytes=_MAX_BYTES,
        backupCount=_BACKUP_COUNT,
    )
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    # Route uvicorn/gunicorn loggers up to the root JsonFormatter so every line
    # from those frameworks is one-line JSON (not plain text) for Azure Log
    # Analytics. See _THIRD_PARTY_LOGGER_NAMES for the master-process boundary.
    _route_third_party_loggers_to_root()
