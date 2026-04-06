"""Tests for HookStateService.touch_session — direct session last_updated behavior."""

from __future__ import annotations

from context_intelligence_server.services import HookStateService


async def test_touch_session_sets_last_updated_when_null(
    services: HookStateService,
) -> None:
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


async def test_touch_session_advances_with_newer_timestamp(
    services: HookStateService,
) -> None:
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


async def test_touch_session_ignores_older_timestamp(
    services: HookStateService,
) -> None:
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


# Ancestor propagation — child activity updates parent and grandparent


async def test_touch_session_propagates_to_parent(services: HookStateService) -> None:
    """Child activity propagates last_updated to the parent session node.

    Creates a parent Session node (no parent_id) and a child Session node with
    parent_id='prop-parent'.  Calls touch_session on the child and asserts that the
    parent node's last_updated is also updated.
    """
    await services.graph.upsert_node(
        "prop-parent",
        {"labels": ["Session"], "session_id": "prop-parent"},
    )
    await services.graph.upsert_node(
        "prop-child",
        {"labels": ["Session"], "session_id": "prop-child", "parent_id": "prop-parent"},
    )

    await services.touch_session("prop-child", "2026-01-01T00:00:10Z")

    child_node = await services.graph.get_node("prop-child")
    parent_node = await services.graph.get_node("prop-parent")

    assert child_node is not None
    assert child_node.get("last_updated") == "2026-01-01T00:00:10Z"

    assert parent_node is not None
    assert parent_node.get("last_updated") == "2026-01-01T00:00:10Z"


async def test_touch_session_propagates_to_grandparent(
    services: HookStateService,
) -> None:
    """Child activity propagates last_updated through the full ancestor chain.

    Creates a prop-grandparent→prop-parent→prop-grandchild chain (3 Session nodes
    with parent_id links).  Calls touch_session on the grandchild and asserts that
    all three nodes (grandchild, parent, grandparent) have last_updated updated.
    """
    await services.graph.upsert_node(
        "prop-grandparent",
        {"labels": ["Session"], "session_id": "prop-grandparent"},
    )
    await services.graph.upsert_node(
        "prop-parent",
        {
            "labels": ["Session"],
            "session_id": "prop-parent",
            "parent_id": "prop-grandparent",
        },
    )
    await services.graph.upsert_node(
        "prop-grandchild",
        {
            "labels": ["Session"],
            "session_id": "prop-grandchild",
            "parent_id": "prop-parent",
        },
    )

    await services.touch_session("prop-grandchild", "2026-01-01T00:00:20Z")

    grandchild_node = await services.graph.get_node("prop-grandchild")
    parent_node = await services.graph.get_node("prop-parent")
    grandparent_node = await services.graph.get_node("prop-grandparent")

    assert grandchild_node is not None
    assert grandchild_node.get("last_updated") == "2026-01-01T00:00:20Z"

    assert parent_node is not None
    assert parent_node.get("last_updated") == "2026-01-01T00:00:20Z"

    assert grandparent_node is not None
    assert grandparent_node.get("last_updated") == "2026-01-01T00:00:20Z"
