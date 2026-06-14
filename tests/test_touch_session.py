"""Tests for HookStateService.touch_session — direct session last_updated behavior."""

from __future__ import annotations

from datetime import datetime, timezone

from unittest.mock import AsyncMock

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


# Single-node contract — child activity NEVER propagates to ancestors
#
# Phase A removed the parent_id ancestor walk: it SET last_updated on the shared
# root :Session node for every child event, so many independent writers contended
# on that one node's exclusive lock — the Neo4j deadlock that silently dropped
# events.  touch_session now updates only the direct session node.  These tests
# were previously "propagation" tests; they now assert the single-node contract.


async def test_touch_session_does_not_propagate_to_parent(
    services: HookStateService,
) -> None:
    """Child activity must NOT touch the parent session node (single-node contract).

    Creates a parent Session node (no parent_id) and a child Session node with
    parent_id='prop-parent'.  Calls touch_session on the child and asserts that the
    child is updated but the parent's last_updated is left untouched.
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
    assert parent_node.get("last_updated") is None


async def test_touch_session_does_not_propagate_to_grandparent(
    services: HookStateService,
) -> None:
    """Child activity must NOT walk the ancestor chain (single-node contract).

    Creates a prop-grandparent→prop-parent→prop-grandchild chain (3 Session nodes
    with parent_id links).  Calls touch_session on the grandchild and asserts that
    only the grandchild is updated — parent and grandparent are left untouched.
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
    assert parent_node.get("last_updated") is None

    assert grandparent_node is not None
    assert grandparent_node.get("last_updated") is None


async def test_touch_session_ignores_parent_id_no_cycle_traversal(
    services: HookStateService,
) -> None:
    """touch_session never traverses parent_id, so cyclic chains are irrelevant.

    Creates a cycle-a → cycle-b → cycle-a parent_id loop.  Touching cycle-a updates
    only cycle-a; cycle-b is never visited because there is no ancestor walk.  This
    confirms the single-node contract makes the old infinite-loop concern moot.
    """
    await services.graph.upsert_node(
        "cycle-a",
        {"labels": ["Session"], "session_id": "cycle-a", "parent_id": "cycle-b"},
    )
    await services.graph.upsert_node(
        "cycle-b",
        {"labels": ["Session"], "session_id": "cycle-b", "parent_id": "cycle-a"},
    )

    await services.touch_session("cycle-a", "2026-01-01T00:00:30Z")

    node_a = await services.graph.get_node("cycle-a")
    node_b = await services.graph.get_node("cycle-b")
    assert node_a is not None
    assert node_a.get("last_updated") == "2026-01-01T00:00:30Z"
    assert node_b is not None
    assert node_b.get("last_updated") is None


# Edge cases — no-op and error isolation


async def test_touch_session_noop_when_session_absent(
    services: HookStateService,
) -> None:
    """touch_session is a no-op when the target session node does not exist.

    Calls touch_session with a session_id that was never upserted into the graph.
    Asserts that no exception is raised and that get_node still returns None
    (i.e. no spurious node was created for the missing session).
    """
    # No session node created beforehand — touching should be a silent no-op
    await services.touch_session("nonexistent", "2026-01-01T00:00:01Z")

    node = await services.graph.get_node("nonexistent")
    assert node is None


async def test_touch_session_swallows_graph_exception(
    services: HookStateService,
) -> None:
    """touch_session swallows exceptions raised by the underlying graph store.

    Replaces services.graph.get_node with an AsyncMock that raises RuntimeError.
    Verifies that touch_session completes normally without propagating the error,
    honouring the fault-tolerance contract.
    """
    services.graph.get_node = AsyncMock(side_effect=RuntimeError("db down"))

    # Must not raise despite the graph error
    await services.touch_session("s1", "2026-01-01T00:00:01Z")


async def test_touch_session_compares_against_datetime_last_updated(
    services: HookStateService,
) -> None:
    """Comparison works when last_updated is a Python datetime (as normalized read path returns).

    The store's read path normalises neo4j.time.DateTime to Python datetime.  The
    in-memory GraphState returns whatever was written (often a str), so touch_session
    must handle both sides of the comparison gracefully.  This test proves that a
    datetime last_updated does not cause TypeError — the timestamp must still advance.
    """
    older = datetime(2026, 1, 1, 0, 0, 1, tzinfo=timezone.utc)
    services.graph.get_node = AsyncMock(
        return_value={
            "labels": ["Session"],
            "session_id": "s-dt",
            "last_updated": older,  # datetime, as normalized read path returns
            # no parent_id so walk stops after one iteration
        }
    )
    upserts: list[tuple[str, dict]] = []

    async def _capture(node_id: str, data: dict) -> None:
        upserts.append((node_id, data))

    services.graph.upsert_node = AsyncMock(side_effect=_capture)

    await services.touch_session("s-dt", "2026-01-01T00:00:05Z")

    assert upserts, "expected last_updated to advance, but no upsert occurred"
    assert upserts[0][1]["last_updated"] == "2026-01-01T00:00:05Z"
