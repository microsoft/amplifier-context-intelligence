"""Tier 3 - Neo4j integration proof for IncompleteSession heal-forward (Part 1).

Encodes the out-of-order race that produces the ~99% false-positive
IncompleteSession population: a forked sub-session's session:end drains
(independent per-session queue) BEFORE its session:fork/session:start.
SessionLabelStateMachine.classify() must strip the stale IncompleteSession
marker the moment the real start/fork is processed (see
docs/issues/incomplete-session-mislabeling.md and
docs/plans/2026-08-12-incomplete-session-relabel-spec.md, Part 1).

This closes a real coverage gap: no prior real-Neo4j test exercised
set_labels() with a non-empty remove_labels list. It runs the real
SessionHandler (not just the pure classify() unit) against a live Neo4j
container, flushes, and reads back with a fresh Cypher query (bypassing the
in-memory buffer) to prove the label is PHYSICALLY removed from the node.

It also proves non-interaction with the terminal-label lattice
normalization (_LATTICE_NORMALIZATION in neo4j_store.py): removing
IncompleteSession must not disturb the RootSession/SubSession/ForkedSession
convergence guarantee, since IncompleteSession is not a member of
_TERMINAL_LABELS.

Run: uv run pytest tests/neo4j/test_incomplete_session_heal_forward.py -v -m neo4j
"""

from __future__ import annotations

from typing import Any

import pytest

from context_intelligence_server.handlers.data_layer_2.session import SessionHandler

pytestmark = pytest.mark.neo4j


async def _neo4j_labels(services: Any, node_id: str) -> list[str]:
    """Read labels directly from Neo4j (bypasses the in-memory buffer)."""
    rows = await services.graph.execute_query(
        "MATCH (n) WHERE n.node_id = $id AND n.workspace = $workspace "
        "RETURN labels(n) AS lbls",
        {"id": node_id, "workspace": services.graph.workspace},
        workspace="*",
    )
    return list(rows[0]["lbls"]) if rows else []


@pytest.mark.neo4j
class TestIncompleteSessionHealForward:
    """Out-of-order end -> fork/start must physically strip IncompleteSession."""

    async def test_out_of_order_end_then_fork_strips_incomplete_session_in_neo4j(
        self, neo4j_services: Any
    ) -> None:
        """end (bare, stamps IncompleteSession) -> flush -> fork -> flush:
        the real Neo4j node must end with ForkedSession only, no
        IncompleteSession, proving REMOVE n:IncompleteSession actually ran.
        """
        handler = SessionHandler(neo4j_services)
        session_id = "child-heal-fork-001"

        # session:end drains first (out-of-order race) -- stamps IncompleteSession
        await handler(
            "session:end",
            {"session_id": session_id, "timestamp": "2026-01-01T10:00:00Z"},
        )
        await neo4j_services.graph.flush()

        mid_labels = await _neo4j_labels(neo4j_services, session_id)
        assert "IncompleteSession" in mid_labels, (
            f"precondition: out-of-order end must stamp IncompleteSession in "
            f"Neo4j; got {mid_labels}"
        )

        # The real session:fork arrives late, in its own flush cycle.
        await handler(
            "session:fork",
            {
                "session_id": session_id,
                "parent_id": "parent-heal-fork-001",
                "timestamp": "2026-01-01T09:59:59Z",
            },
        )
        await neo4j_services.graph.flush()

        final_labels = await _neo4j_labels(neo4j_services, session_id)
        assert "ForkedSession" in final_labels, (
            f"expected ForkedSession in {final_labels}"
        )
        assert "IncompleteSession" not in final_labels, (
            f"heal-forward must physically REMOVE n:IncompleteSession at "
            f"flush; still present in Neo4j: {final_labels}"
        )

    async def test_out_of_order_end_then_start_strips_incomplete_session_in_neo4j(
        self, neo4j_services: Any
    ) -> None:
        """end (bare, stamps IncompleteSession) -> flush -> start (no parent)
        -> flush: the real Neo4j node must end with RootSession only.
        """
        handler = SessionHandler(neo4j_services)
        session_id = "root-heal-start-001"

        await handler(
            "session:end",
            {"session_id": session_id, "timestamp": "2026-01-01T10:00:00Z"},
        )
        await neo4j_services.graph.flush()

        mid_labels = await _neo4j_labels(neo4j_services, session_id)
        assert "IncompleteSession" in mid_labels, (
            f"precondition: out-of-order end must stamp IncompleteSession in "
            f"Neo4j; got {mid_labels}"
        )

        await handler(
            "session:start",
            {"session_id": session_id, "timestamp": "2026-01-01T09:59:59Z"},
        )
        await neo4j_services.graph.flush()

        final_labels = await _neo4j_labels(neo4j_services, session_id)
        assert "RootSession" in final_labels, f"expected RootSession in {final_labels}"
        assert "IncompleteSession" not in final_labels, (
            f"heal-forward must physically REMOVE n:IncompleteSession at "
            f"flush; still present in Neo4j: {final_labels}"
        )

    async def test_heal_forward_does_not_disturb_terminal_lattice_normalization(
        self, neo4j_services: Any
    ) -> None:
        """Healing IncompleteSession must not interact with, or break, the
        RootSession/SubSession/ForkedSession lattice-normalization guarantee.

        Drives: end (bare, stamps IncompleteSession) -> flush -> start WITH a
        parent (assigns SubSession) -> flush -> fork (reclassifies to
        ForkedSession, the lattice's specificity ordering) -> flush. The node
        must converge to exactly ONE terminal label (ForkedSession) with
        IncompleteSession and the stale SubSession both absent -- proving the
        IncompleteSession REMOVE and the terminal-lattice REMOVE/SET both ran
        correctly and did not clobber each other.
        """
        handler = SessionHandler(neo4j_services)
        session_id = "lattice-heal-001"
        parent_id = "lattice-heal-parent-001"

        await handler(
            "session:end",
            {"session_id": session_id, "timestamp": "2026-01-01T10:00:00Z"},
        )
        await neo4j_services.graph.flush()

        await handler(
            "session:start",
            {
                "session_id": session_id,
                "parent_id": parent_id,
                "timestamp": "2026-01-01T09:59:58Z",
            },
        )
        await neo4j_services.graph.flush()

        mid_labels = await _neo4j_labels(neo4j_services, session_id)
        assert "SubSession" in mid_labels, f"expected SubSession in {mid_labels}"
        assert "IncompleteSession" not in mid_labels, (
            f"IncompleteSession must already be healed after start: {mid_labels}"
        )

        await handler(
            "session:fork",
            {
                "session_id": session_id,
                "parent_id": parent_id,
                "timestamp": "2026-01-01T09:59:59Z",
            },
        )
        await neo4j_services.graph.flush()

        final_labels = await _neo4j_labels(neo4j_services, session_id)
        terminals = [
            lbl
            for lbl in final_labels
            if lbl in ("RootSession", "SubSession", "ForkedSession")
        ]
        assert terminals == ["ForkedSession"], (
            f"lattice must converge to exactly one terminal (ForkedSession); "
            f"got {terminals} in {final_labels}"
        )
        assert "IncompleteSession" not in final_labels, (
            f"IncompleteSession must remain healed through the lattice "
            f"reclassification: {final_labels}"
        )
