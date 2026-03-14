"""Tests for SessionRegistry and SessionWorker."""

import asyncio

import pytest

from context_intelligence_server.registry import SessionRegistry, SessionWorker


@pytest.fixture
def registry() -> SessionRegistry:
    return SessionRegistry()


@pytest.mark.asyncio
async def test_get_or_create_new_worker(registry: SessionRegistry) -> None:
    worker = registry.get_or_create("session-1", "/workspace/a")

    assert isinstance(worker, SessionWorker)
    assert worker.session_id == "session-1"
    assert worker.workspace == "/workspace/a"
    assert isinstance(worker.queue, asyncio.Queue)
    assert worker.queue.empty()
    assert isinstance(worker.task, asyncio.Task)


@pytest.mark.asyncio
async def test_get_or_create_returns_same_worker(registry: SessionRegistry) -> None:
    worker_a = registry.get_or_create("session-1", "/workspace/a")
    worker_b = registry.get_or_create("session-1", "/workspace/a")

    assert worker_a is worker_b


@pytest.mark.asyncio
async def test_active_count(registry: SessionRegistry) -> None:
    assert registry.active_count() == 0

    registry.get_or_create("session-1", "/workspace/a")
    assert registry.active_count() == 1

    registry.get_or_create("session-2", "/workspace/b")
    assert registry.active_count() == 2


@pytest.mark.asyncio
async def test_active_sessions(registry: SessionRegistry) -> None:
    registry.get_or_create("session-b", "/workspace/b")
    registry.get_or_create("session-a", "/workspace/a")

    sessions = registry.active_sessions()
    assert sorted(sessions) == sessions
    assert sessions == ["session-a", "session-b"]


@pytest.mark.asyncio
async def test_remove(registry: SessionRegistry) -> None:
    registry.get_or_create("session-1", "/workspace/a")
    assert registry.active_count() == 1

    registry.remove("session-1")
    assert registry.active_count() == 0


@pytest.mark.asyncio
async def test_remove_nonexistent_is_noop(registry: SessionRegistry) -> None:
    # Should not raise any exception
    registry.remove("nonexistent-session")
    assert registry.active_count() == 0


@pytest.mark.asyncio
async def test_queue_put_get(registry: SessionRegistry) -> None:
    worker = registry.get_or_create("session-1", "/workspace/a")

    event = ("tool_call", {"tool": "bash", "result": "ok"})
    await worker.queue.put(event)

    # get() on a non-empty queue returns without yielding — drain task does not interpose
    retrieved = await worker.queue.get()
    assert retrieved == event
