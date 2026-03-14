"""Logging configuration with stdout and rotating file handlers."""

import logging
import logging.handlers
import sys
from pathlib import Path

from context_intelligence_server.config import get_settings

_LOG_FORMAT = '{"time": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}'
_MAX_BYTES = 10 * 1024 * 1024
_BACKUP_COUNT = 5


def setup_logging() -> None:
    """Configure root logger with stdout StreamHandler and RotatingFileHandler.

    Reads log_path and log_level from get_settings(). Creates the parent
    directory of log_path if it does not already exist.
    """
    settings = get_settings()
    log_path = Path(settings.log_path)
    log_level = settings.log_level

    # Ensure parent directory exists
    log_path.parent.mkdir(parents=True, exist_ok=True)

    formatter = logging.Formatter(_LOG_FORMAT)

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

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
