"""Tests for HookStateService.touch_session — direct session last_updated behavior."""

from __future__ import annotations

from context_intelligence_server.services import HookStateService


async def test_touch_session_sets_last_updated_when_null(services: HookStateService) -> None:
    """First event on a session sets last_updated from NULL.

    Upserts a Session node with no last_updated, calls touch_session, and asserts
    that last_updated is set.
    """
    await services.graph.upsert_node(
        "session-1",
        {"labels": ["Session"], "session_id": "session-1"},
    )

    await services.touch_session("session-1", "2026-01-01T00:00:01Z")

    node = await services.graph.get_node("session-1")
    assert node is not None
    assert node.get("last_updated") == "2026-01-01T00:00:01Z"


async def test_touch_session_advances_with_newer_timestamp(services: HookStateService) -> None:
    """A newer timestamp advances last_updated.

    Upserts a Session node with last_updated='2026-01-01T00:00:01Z', calls
    touch_session with '2026-01-01T00:00:05Z', and asserts last_updated advanced.
    """
    await services.graph.upsert_node(
        "session-2",
        {
            "labels": ["Session"],
            "session_id": "session-2",
            "last_updated": "2026-01-01T00:00:01Z",
        },
    )

    await services.touch_session("session-2", "2026-01-01T00:00:05Z")

    node = await services.graph.get_node("session-2")
    assert node is not None
    assert node.get("last_updated") == "2026-01-01T00:00:05Z"


async def test_touch_session_ignores_older_timestamp(services: HookStateService) -> None:
    """An older timestamp must NOT regress last_updated.

    Upserts a Session node with last_updated='2026-01-01T00:00:05Z', calls
    touch_session with '2026-01-01T00:00:01Z', and asserts last_updated remains
    unchanged.
    """
    await services.graph.upsert_node(
        "session-3",
        {
            "labels": ["Session"],
            "session_id": "session-3",
            "last_updated": "2026-01-01T00:00:05Z",
        },
    )

    await services.touch_session("session-3", "2026-01-01T00:00:01Z")

    node = await services.graph.get_node("session-3")
    assert node is not None
    assert node.get("last_updated") == "2026-01-01T00:00:05Z"
