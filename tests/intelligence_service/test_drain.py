"""Tests for the DrainManager graceful-shutdown component.

Covers:
 1. Initial state (accepting=True, active_count=0)
 2. register / unregister lifecycle
 3. start_drain() behaviour (stops accepting, waits for sessions, timeout)
"""

import asyncio

from intelligence_service.drain import DrainManager


# ---------------------------------------------------------------------------
# Test 1: drain manager starts in accepting state
# ---------------------------------------------------------------------------


def test_drain_starts_accepting() -> None:
    """DrainManager.accepting is True on creation."""
    dm = DrainManager()

    assert dm.accepting is True


# ---------------------------------------------------------------------------
# Test 2: drain manager starts with zero active sessions
# ---------------------------------------------------------------------------


def test_drain_starts_with_zero_active() -> None:
    """DrainManager.active_count is 0 on creation."""
    dm = DrainManager()

    assert dm.active_count == 0


# ---------------------------------------------------------------------------
# Test 3: register increments active_count
# ---------------------------------------------------------------------------


def test_register_increments_active() -> None:
    """register() increments active_count: 0 -> 1 -> 2."""
    dm = DrainManager()

    dm.register("session-1")
    assert dm.active_count == 1

    dm.register("session-2")
    assert dm.active_count == 2


# ---------------------------------------------------------------------------
# Test 4: unregister decrements active_count
# ---------------------------------------------------------------------------


def test_unregister_decrements_active() -> None:
    """register then unregister: active_count goes 1 -> 0."""
    dm = DrainManager()

    dm.register("session-1")
    assert dm.active_count == 1

    dm.unregister("session-1")
    assert dm.active_count == 0


# ---------------------------------------------------------------------------
# Test 5: unregister of unknown session is a no-op
# ---------------------------------------------------------------------------


def test_unregister_nonexistent_is_noop() -> None:
    """unregister() on an unknown session ID does not raise."""
    dm = DrainManager()

    # Should not raise
    dm.unregister("ghost-session")

    assert dm.active_count == 0


# ---------------------------------------------------------------------------
# Test 6: start_drain() stops accepting new connections
# ---------------------------------------------------------------------------


async def test_start_drain_stops_accepting() -> None:
    """After start_drain(), accepting is False."""
    dm = DrainManager()

    await dm.start_drain(timeout=1)

    assert dm.accepting is False


# ---------------------------------------------------------------------------
# Test 7: start_drain() returns True immediately when no active sessions
# ---------------------------------------------------------------------------


async def test_start_drain_returns_true_when_no_sessions() -> None:
    """start_drain(timeout=1) returns True immediately when active_count==0."""
    dm = DrainManager()

    result = await dm.start_drain(timeout=1)

    assert result is True


# ---------------------------------------------------------------------------
# Test 8: start_drain() waits for sessions to unregister
# ---------------------------------------------------------------------------


async def test_start_drain_waits_for_sessions_to_unregister() -> None:
    """start_drain() waits for an in-flight session and returns True on clean drain."""
    dm = DrainManager()
    dm.register("session-a")

    async def delayed_unregister() -> None:
        await asyncio.sleep(0.1)
        dm.unregister("session-a")

    task = asyncio.create_task(delayed_unregister())

    result = await dm.start_drain(timeout=5)

    assert result is True
    await task  # ensure task is fully awaited


# ---------------------------------------------------------------------------
# Test 9: start_drain() returns False when timeout expires with active sessions
# ---------------------------------------------------------------------------


async def test_start_drain_returns_false_on_timeout() -> None:
    """start_drain() returns False when timeout expires with stuck sessions."""
    dm = DrainManager()
    dm.register("stuck-session")

    result = await dm.start_drain(timeout=0.1)

    assert result is False
