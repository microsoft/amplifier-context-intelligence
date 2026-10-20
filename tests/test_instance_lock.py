"""Regression tests for the process-wide durable queue writer lock."""

from __future__ import annotations

import asyncio
import multiprocessing
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import FastAPI

import context_intelligence_server.main as main_module
from context_intelligence_server.instance_lock import (
    InstanceLockUnavailable,
    ServerInstanceLock,
)


def _child_lock_attempt(queues_path: str, result: Any) -> None:
    """Report whether an independently spawned process acquired the lock."""
    lock = ServerInstanceLock(queues_path)
    try:
        lock.acquire()
    except InstanceLockUnavailable:
        result.put(False)
    else:
        try:
            result.put(True)
        finally:
            lock.release()


def test_second_server_instance_cannot_lock_the_same_queue_path(
    tmp_path: Path,
) -> None:
    """An independently spawned process fails before writing shared queues."""
    queues_path = tmp_path / "queues"
    first = ServerInstanceLock(queues_path)
    first.acquire()
    try:
        context = multiprocessing.get_context("spawn")
        result = context.Queue()
        child = context.Process(
            target=_child_lock_attempt, args=(str(queues_path), result)
        )
        child.start()
        child.join(timeout=5)
        assert child.exitcode == 0
        assert result.get(timeout=1) is False
    finally:
        first.release()


def test_releasing_server_instance_lock_allows_a_new_owner(tmp_path: Path) -> None:
    """A clean shutdown releases the shared queue writer lock to another process."""
    queues_path = tmp_path / "queues"
    first = ServerInstanceLock(queues_path)
    first.acquire()
    first.release()

    context = multiprocessing.get_context("spawn")
    result = context.Queue()
    child = context.Process(target=_child_lock_attempt, args=(str(queues_path), result))
    child.start()
    child.join(timeout=5)
    assert child.exitcode == 0
    assert result.get(timeout=1) is True


def test_recovery_registry_defers_queue_initialization_until_lifespan(
    tmp_path: Path,
) -> None:
    """ASGI construction records a recovery path but does not touch its queue."""
    from context_intelligence_server.registry import LiveFirstFlushGate, SessionRegistry

    queue_path = tmp_path / "state" / "recovery"
    registry = SessionRegistry.for_recovery(
        queues_path=queue_path,
        shared_write_semaphore=asyncio.Semaphore(1),
        max_delivery_attempts=1,
        flush_gate=LiveFirstFlushGate(1),
    )

    assert registry._queue_manager is None
    assert not queue_path.exists()


@asynccontextmanager
async def _empty_server_lifespan(_app: FastAPI) -> Any:
    """Avoid unrelated Neo4j setup while exercising the outer ASGI lifespan."""
    yield


@pytest.mark.asyncio
async def test_direct_asgi_lifespan_refuses_second_server_and_releases(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Direct uvicorn/gunicorn ASGI loading cannot bypass the writer lock."""
    state_root = tmp_path / "state"
    settings = SimpleNamespace(stateful_root=lambda: state_root)
    first, second = FastAPI(), FastAPI()
    first.state.settings = settings
    second.state.settings = settings
    monkeypatch.setattr(main_module, "_server_lifespan", _empty_server_lifespan)

    async with main_module.lifespan(first):
        with pytest.raises(RuntimeError, match="another server holds"):
            async with main_module.lifespan(second):
                raise AssertionError("the second lifespan must not start")

    # The lock lifetime ends at ASGI shutdown, so a clean direct restart works.
    async with main_module.lifespan(second):
        assert second.state.server_instance_lock.path == (
            state_root / ".context-intelligence-server.lock"
        )


@pytest.mark.asyncio
async def test_direct_asgi_lifespan_refuses_split_queue_roots_sharing_api_key_store(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Lifespan validates the active identity store before taking either lock."""
    from context_intelligence_server.config import Settings

    first_root = tmp_path / "first-state"
    shared_store = first_root / "identity" / "api-keys.json"
    key_map = {"a" * 64: {"id": "writer"}}
    first_settings = Settings(
        queues_path=str(first_root / "queues"),
        api_keys=key_map,
        api_keys_store_path=str(shared_store),
    )
    second_settings = Settings(
        queues_path=str(tmp_path / "second-state" / "queues"),
        api_keys=key_map,
        api_keys_store_path=str(shared_store),
    )
    first, second = FastAPI(), FastAPI()
    first.state.settings = first_settings
    second.state.settings = second_settings
    monkeypatch.setattr(main_module, "_server_lifespan", _empty_server_lifespan)

    async with main_module.lifespan(first):
        with pytest.raises(
            RuntimeError,
            match="invalid durable state-root configuration: .*API-key identity store",
        ):
            async with main_module.lifespan(second):
                raise AssertionError("the split identity-store lifespan must not start")


def test_run_refuses_multiworker_before_acquiring_instance_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Normal startup retains the WEB_CONCURRENCY=1 safety gate."""
    monkeypatch.setenv("WEB_CONCURRENCY", "2")

    with pytest.raises(RuntimeError, match="requires exactly one worker"):
        main_module.run()
