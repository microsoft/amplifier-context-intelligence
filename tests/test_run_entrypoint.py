"""Tests for the run() entrypoint function in main.py."""

from unittest.mock import patch

from gunicorn.app.base import BaseApplication
from uvicorn.workers import UvicornWorker

from context_intelligence_server.config import get_settings
from context_intelligence_server.main import run


def test_run_uses_gunicorn_with_settings() -> None:
    """run() should configure gunicorn with correct host/port/worker settings."""
    settings = get_settings()
    instances: list[BaseApplication] = []

    def _capture(self: BaseApplication) -> None:
        instances.append(self)

    with patch.object(BaseApplication, "run", _capture):
        run()

    assert len(instances) == 1
    cfg = instances[0].cfg
    assert f"{settings.server_host}:{settings.server_port}" in cfg.bind
    assert cfg.workers == 1
    assert cfg.graceful_timeout == 10
    assert cfg.worker_class is UvicornWorker
    assert cfg.timeout == 30
