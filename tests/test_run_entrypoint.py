"""Tests for the run() entrypoint function in main.py."""

from unittest.mock import patch

import context_intelligence_server.main as main_module
import pytest
from context_intelligence_server.config import get_settings
from context_intelligence_server.main import run
from gunicorn.app.base import BaseApplication
from uvicorn_worker import UvicornWorker


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


# ---------------------------------------------------------------------------
# gunicorn control socket must stay OFF (fork-deadlock guard, gunicorn #3529)
# ---------------------------------------------------------------------------


def test_run_disables_the_gunicorn_control_socket() -> None:
    """The `gunicornc` control socket must never be enabled.

    gunicorn 25.1.0 runs that control socket as an asyncio event loop in a
    DAEMON THREAD inside the arbiter, started immediately before the first
    os.fork(). A fork landing while that thread is inside a blocking write()
    on the shared stderr stream leaves the io.BufferedWriter's internal lock
    held in the child by a thread that does not exist there -- CPython's
    os.register_at_fork resets logging.Handler locks but not that one -- so
    the child deadlocks forever on gunicorn's own "Booting worker with pid"
    log line: no output, no traceback, no exit.

    This is a REGRESSION GUARD, not a style check. The failure it prevents is
    intermittent and silent (observed twice in CI as an opaque 90s hang), so
    nothing else in the suite would notice the flag being dropped -- and a
    gunicorn upgrade past the upstream fix (25.2.0, #3520) does not make the
    flag redundant: we do not use `gunicornc`, and enabling it also writes a
    socket file into the working directory.
    """
    instances: list[BaseApplication] = []

    def _capture(self: BaseApplication) -> None:
        instances.append(self)

    with patch.object(BaseApplication, "run", _capture):
        run()

    assert instances[0].cfg.control_socket_disable is True
