"""Tests for the session manager Protocol and StubSessionManager implementation."""

from intelligence_service.session_manager import StubSessionManager


# ---------------------------------------------------------------------------
# Test 1: create_session returns a non-empty string ID
# ---------------------------------------------------------------------------


async def test_create_session_returns_id() -> None:
    """StubSessionManager.create_session() returns a non-empty string."""
    manager = StubSessionManager()

    session_id = await manager.create_session()

    assert isinstance(session_id, str)
    assert len(session_id) > 0


# ---------------------------------------------------------------------------
# Test 2: active_count increments with each create_session call
# ---------------------------------------------------------------------------


async def test_create_session_increments_count() -> None:
    """active_count goes 0 -> 1 -> 2 as sessions are created."""
    manager = StubSessionManager()

    assert manager.active_count == 0

    await manager.create_session()
    assert manager.active_count == 1

    await manager.create_session()
    assert manager.active_count == 2


# ---------------------------------------------------------------------------
# Test 3: destroy_session decrements active_count
# ---------------------------------------------------------------------------


async def test_destroy_session_decrements_count() -> None:
    """create then destroy: active_count goes 1 -> 0."""
    manager = StubSessionManager()

    session_id = await manager.create_session()
    assert manager.active_count == 1

    await manager.destroy_session(session_id)
    assert manager.active_count == 0


# ---------------------------------------------------------------------------
# Test 4: destroying a nonexistent session is a no-op
# ---------------------------------------------------------------------------


async def test_destroy_nonexistent_session_is_noop() -> None:
    """destroy_session('nonexistent') does not raise and active_count stays 0."""
    manager = StubSessionManager()

    # Should not raise
    await manager.destroy_session("nonexistent")

    assert manager.active_count == 0


# ---------------------------------------------------------------------------
# Test 5: get_session returns metadata dict for a known session
# ---------------------------------------------------------------------------


async def test_get_session_returns_metadata() -> None:
    """After create, get_session returns dict with session_id and status='active'."""
    manager = StubSessionManager()

    session_id = await manager.create_session()
    metadata = await manager.get_session(session_id)

    assert metadata is not None
    assert metadata["session_id"] == session_id
    assert metadata["status"] == "active"


# ---------------------------------------------------------------------------
# Test 6: get_session returns None for an unknown session ID
# ---------------------------------------------------------------------------


async def test_get_session_returns_none_for_unknown() -> None:
    """get_session('unknown') returns None."""
    manager = StubSessionManager()

    result = await manager.get_session("unknown")

    assert result is None


# ---------------------------------------------------------------------------
# Test 7: reset_session returns a new ID, active_count stays 1, old ID gone
# ---------------------------------------------------------------------------


async def test_reset_session_returns_new_id() -> None:
    """After create, reset_session returns different ID, active_count stays 1,
    old ID not found, new ID found."""
    manager = StubSessionManager()

    old_id = await manager.create_session()
    assert manager.active_count == 1

    new_id = await manager.reset_session(old_id)

    assert new_id != old_id
    assert manager.active_count == 1
    assert await manager.get_session(old_id) is None
    assert await manager.get_session(new_id) is not None
