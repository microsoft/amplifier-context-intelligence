"""Tests for AmplifierSessionManager."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from intelligence_service.amplifier_session_manager import AmplifierSessionManager
from intelligence_service.session_manager import SessionManager


def make_manager(mock_app: MagicMock) -> AmplifierSessionManager:
    """Return an AmplifierSessionManager with standard test configuration."""
    return AmplifierSessionManager(
        amplifier_app=mock_app, workspace="myproject", amplifier_home="/data/home"
    )


# ---------------------------------------------------------------------------
# Test 1: protocol conformance (isinstance check)
# ---------------------------------------------------------------------------


def test_protocol_conformance() -> None:
    """AmplifierSessionManager satisfies the SessionManager protocol."""
    mock_app = MagicMock()
    manager = make_manager(mock_app)

    assert isinstance(manager, SessionManager)


# ---------------------------------------------------------------------------
# Test 2: create_session returns a string id
# ---------------------------------------------------------------------------


async def test_create_session_returns_string_id() -> None:
    """create_session() returns a non-empty string session ID."""
    mock_app = MagicMock()
    mock_app.prepared.create_session = AsyncMock(return_value=MagicMock())
    manager = make_manager(mock_app)

    session_id = await manager.create_session()

    assert isinstance(session_id, str)
    assert len(session_id) > 0


# ---------------------------------------------------------------------------
# Test 3: create_session delegates to prepared.create_session
# ---------------------------------------------------------------------------


async def test_create_session_delegates_to_prepared() -> None:
    """create_session() calls amplifier_app.prepared.create_session with correct kwargs."""
    mock_app = MagicMock()
    mock_session = MagicMock()
    mock_app.prepared.create_session = AsyncMock(return_value=mock_session)

    manager = make_manager(mock_app)
    session_id = await manager.create_session()

    mock_app.prepared.create_session.assert_called_once_with(
        session_id=session_id,
        session_cwd="/data/home/myproject",
    )


# ---------------------------------------------------------------------------
# Test 4: create_session increments active_count
# ---------------------------------------------------------------------------


async def test_create_session_increments_active_count() -> None:
    """active_count increments with each create_session call."""
    mock_app = MagicMock()
    mock_app.prepared.create_session = AsyncMock(return_value=MagicMock())

    manager = make_manager(mock_app)

    assert manager.active_count == 0
    await manager.create_session()
    assert manager.active_count == 1
    await manager.create_session()
    assert manager.active_count == 2


# ---------------------------------------------------------------------------
# Test 5: destroy_session decrements count
# ---------------------------------------------------------------------------


async def test_destroy_session_decrements_count() -> None:
    """destroy_session() removes the session and decrements active_count."""
    mock_app = MagicMock()
    mock_app.prepared.create_session = AsyncMock(return_value=MagicMock())

    manager = make_manager(mock_app)
    session_id = await manager.create_session()
    assert manager.active_count == 1

    await manager.destroy_session(session_id)
    assert manager.active_count == 0


# ---------------------------------------------------------------------------
# Test 6: destroy_session on nonexistent id is a no-op
# ---------------------------------------------------------------------------


async def test_destroy_nonexistent_session_is_noop() -> None:
    """destroy_session() on an unknown ID does not raise and count stays 0."""
    mock_app = MagicMock()
    manager = make_manager(mock_app)

    # Should not raise
    await manager.destroy_session("nonexistent-id")

    assert manager.active_count == 0


# ---------------------------------------------------------------------------
# Test 7: reset_session returns new id with same count
# ---------------------------------------------------------------------------


async def test_reset_session_returns_new_id_with_same_count() -> None:
    """reset_session() destroys old + creates new, active_count unchanged."""
    mock_app = MagicMock()
    mock_app.prepared.create_session = AsyncMock(return_value=MagicMock())

    manager = make_manager(mock_app)
    old_id = await manager.create_session()
    assert manager.active_count == 1

    new_id = await manager.reset_session(old_id)

    assert new_id != old_id
    assert manager.active_count == 1


# ---------------------------------------------------------------------------
# Test 8: get_session returns metadata
# ---------------------------------------------------------------------------


async def test_get_session_returns_metadata() -> None:
    """get_session() returns {'session_id': id, 'status': 'active'} for known session."""
    mock_app = MagicMock()
    mock_app.prepared.create_session = AsyncMock(return_value=MagicMock())

    manager = make_manager(mock_app)
    session_id = await manager.create_session()

    metadata = await manager.get_session(session_id)

    assert metadata is not None
    assert metadata["session_id"] == session_id
    assert metadata["status"] == "active"


# ---------------------------------------------------------------------------
# Test 9: get_session returns None for unknown session
# ---------------------------------------------------------------------------


async def test_get_session_returns_none_for_unknown() -> None:
    """get_session() returns None for an unknown session ID."""
    mock_app = MagicMock()
    manager = make_manager(mock_app)

    result = await manager.get_session("unknown-session-id")

    assert result is None


# ---------------------------------------------------------------------------
# Test 10: execute calls session.execute
# ---------------------------------------------------------------------------


async def test_execute_calls_session_execute() -> None:
    """execute() calls session.execute(prompt) on the stored session object."""
    mock_session = MagicMock()
    mock_session.execute = AsyncMock(return_value="response text")

    mock_app = MagicMock()
    mock_app.prepared.create_session = AsyncMock(return_value=mock_session)

    manager = make_manager(mock_app)
    session_id = await manager.create_session()

    await manager.execute(session_id, "hello")

    mock_session.execute.assert_called_once_with("hello")


# ---------------------------------------------------------------------------
# Test 11: execute returns {'text': ..., 'a2ui': []}
# ---------------------------------------------------------------------------


async def test_execute_returns_text_and_a2ui() -> None:
    """execute() returns dict with 'text' and 'a2ui' keys."""
    mock_session = MagicMock()
    mock_session.execute = AsyncMock(return_value="the answer")

    mock_app = MagicMock()
    mock_app.prepared.create_session = AsyncMock(return_value=mock_session)

    manager = make_manager(mock_app)
    session_id = await manager.create_session()

    result = await manager.execute(session_id, "what is 2+2?")

    assert result == {"text": "the answer", "a2ui": []}


# ---------------------------------------------------------------------------
# Test 12: execute unknown session raises KeyError
# ---------------------------------------------------------------------------


async def test_execute_unknown_session_raises_key_error() -> None:
    """execute() raises KeyError for an unknown session_id."""
    mock_app = MagicMock()
    manager = make_manager(mock_app)

    with pytest.raises(KeyError):
        await manager.execute("nonexistent-id", "some prompt")


# ---------------------------------------------------------------------------
# Test 13: close_all clears all sessions
# ---------------------------------------------------------------------------


async def test_close_all_clears_all_sessions() -> None:
    """close_all() removes all sessions and active_count becomes 0."""
    mock_app = MagicMock()
    mock_app.prepared.create_session = AsyncMock(return_value=MagicMock())

    manager = make_manager(mock_app)
    await manager.create_session()
    await manager.create_session()
    await manager.create_session()
    assert manager.active_count == 3

    await manager.close_all()

    assert manager.active_count == 0


# ---------------------------------------------------------------------------
# Test 14: create_session uses amplifier_home for cwd
# ---------------------------------------------------------------------------


async def test_create_session_uses_amplifier_home_for_cwd() -> None:
    """create_session() builds session_cwd from amplifier_home, not a hardcoded path."""
    mock_app = MagicMock()
    mock_session = MagicMock()
    mock_app.prepared.create_session = AsyncMock(return_value=mock_session)

    manager = AmplifierSessionManager(
        amplifier_app=mock_app,
        workspace="myproject",
        amplifier_home="/custom/data/dir",
    )
    session_id = await manager.create_session()

    mock_app.prepared.create_session.assert_called_once_with(
        session_id=session_id,
        session_cwd="/custom/data/dir/myproject",
    )


# ---------------------------------------------------------------------------
# Test 15: destroy_session() closes the Amplifier session
# ---------------------------------------------------------------------------


async def test_destroy_session_closes_amplifier_session() -> None:
    """destroy_session() calls close() on the Amplifier session if available."""
    mock_session = MagicMock()
    mock_session.close = AsyncMock()
    mock_app = MagicMock()
    mock_app.prepared.create_session = AsyncMock(return_value=mock_session)
    manager = make_manager(mock_app)
    session_id = await manager.create_session()
    await manager.destroy_session(session_id)
    mock_session.close.assert_called_once()


# ---------------------------------------------------------------------------
# Test 16: close_all() closes each session before clearing
# ---------------------------------------------------------------------------


async def test_close_all_closes_each_session() -> None:
    """close_all() calls close() on every registered session."""
    mock_session_a = MagicMock()
    mock_session_a.close = AsyncMock()
    mock_session_b = MagicMock()
    mock_session_b.close = AsyncMock()

    mock_app = MagicMock()
    mock_app.prepared.create_session = AsyncMock(
        side_effect=[mock_session_a, mock_session_b]
    )

    manager = make_manager(mock_app)
    await manager.create_session()
    await manager.create_session()
    assert manager.active_count == 2

    await manager.close_all()

    mock_session_a.close.assert_called_once()
    mock_session_b.close.assert_called_once()
    assert manager.active_count == 0


# ---------------------------------------------------------------------------
# Test 17: create_session when prepared is None raises AttributeError
# ---------------------------------------------------------------------------


async def test_create_session_when_prepared_is_none_raises() -> None:
    """create_session() raises AttributeError when prepared is None."""
    mock_app = MagicMock()
    mock_app.prepared = None
    manager = make_manager(mock_app)
    with pytest.raises(AttributeError):
        await manager.create_session()


# ---------------------------------------------------------------------------
# Test 18: execute propagates errors from the underlying session
# ---------------------------------------------------------------------------


async def test_execute_propagates_session_error() -> None:
    """When the underlying session.execute() raises, the error propagates."""
    mock_session = MagicMock()
    mock_session.execute = AsyncMock(side_effect=RuntimeError("LLM timeout"))
    mock_app = MagicMock()
    mock_app.prepared.create_session = AsyncMock(return_value=mock_session)
    manager = make_manager(mock_app)
    session_id = await manager.create_session()
    with pytest.raises(RuntimeError, match="LLM timeout"):
        await manager.execute(session_id, "hello")
