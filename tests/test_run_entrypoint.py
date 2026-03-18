"""Tests for the run() entrypoint function in main.py."""

from unittest.mock import patch

from context_intelligence_server.config import get_settings


def test_run_calls_uvicorn_with_settings() -> None:
    """run() should delegate to uvicorn.run with host/port/log_level from settings."""
    settings = get_settings()

    with patch("uvicorn.run") as mock_run:
        from context_intelligence_server.main import run

        run()

        mock_run.assert_called_once_with(
            "context_intelligence_server.main:app",
            host=settings.server_host,
            port=settings.server_port,
            log_level=settings.log_level.lower(),
        )
