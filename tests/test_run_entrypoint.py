"""Tests for the run() entrypoint function in main.py."""

from unittest.mock import patch

import context_intelligence_server.main as main_module
import pytest
from context_intelligence_server.config import get_settings
from context_intelligence_server.main import run
from gunicorn.app.base import BaseApplication
from uvicorn.workers import UvicornWorker


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


# ---------------------------------------------------------------------------
# Change 3: configurable gunicorn worker timeout / graceful_timeout
# ---------------------------------------------------------------------------


def test_run_gunicorn_timeouts_default_to_previous_hardcoded_values() -> None:
    """No config set -> gunicorn sees the SAME 30s/10s that used to be
    hardcoded (no-op default, verified end-to-end through run())."""
    instances: list[BaseApplication] = []

    def _capture(self: BaseApplication) -> None:
        instances.append(self)

    with patch.object(BaseApplication, "run", _capture):
        run()

    cfg = instances[0].cfg
    assert cfg.timeout == 30
    assert cfg.graceful_timeout == 10


def test_run_gunicorn_timeouts_respect_settings_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A configured gunicorn_worker_timeout / gunicorn_graceful_timeout flows
    through run() into the actual gunicorn config -- an operator can
    accommodate a legitimately slow, large-backlog boot (see
    crash_recovery_respawn_limit) without gunicorn's own watchdog SIGKILLing
    the worker mid-startup."""
    monkeypatch.setattr(main_module._settings, "gunicorn_worker_timeout", 300)
    monkeypatch.setattr(main_module._settings, "gunicorn_graceful_timeout", 45)

    instances: list[BaseApplication] = []

    def _capture(self: BaseApplication) -> None:
        instances.append(self)

    with patch.object(BaseApplication, "run", _capture):
        run()

    cfg = instances[0].cfg
    assert cfg.timeout == 300
    assert cfg.graceful_timeout == 45
