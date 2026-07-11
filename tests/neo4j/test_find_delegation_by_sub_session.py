"""Tier 3 — Neo4j integration proof for find_delegation_by_sub_session (Brick 3).

find_delegation_by_sub_session is the store capability the self-delegation
resolver depends on: it locates the parent Delegation node D where
``D.sub_session_id == sub_session_id`` so the resolver can read the real agent
behind an ``agent == "self"`` delegation (the correct source of truth is the
parent Delegation node, never the parent Session node — see design doc §3,
Defect B).

The in-memory GraphState parity test lives in
``tests/handlers/data_layer_3/test_delegation.py`` (TestFindDelegationBySubSessionParity).
This module proves the PRODUCTION path — the Neo4jGraphStore Cypher fallback —
against a real disposable Neo4j container, and mirrors the GraphState
assertions so the two stores are demonstrably at parity.

Both read paths are exercised:
- BUFFERED path: the Delegation node is still in the in-memory node buffer
  (pre-flush), so find returns it without hitting Neo4j.
- FLUSHED / Cypher path: after flush() the buffer is empty, so find falls
  through to the ``MATCH (d:Delegation {sub_session_id, workspace})`` query —
  the real production code path.

Run: uv run pytest tests/neo4j/test_find_delegation_by_sub_session.py -v -m neo4j
"""

from __future__ import annotations

from typing import Any

import pytest

from context_intelligence_server.neo4j_store import Neo4jGraphStore

_DELEGATION_DATA: dict[str, Any] = {
    "labels": ["Delegation", "SST_EVENT"],
    "agent": "foundation:explorer",
    "parent_session_id": "ps1",
    "sub_session_id": "ss-target",
    "tool_call_id": "tc1",
    "started_at": "2026-01-01T00:00:00Z",
    "is_self_delegation": False,
}


@pytest.mark.neo4j
class TestFindDelegationBySubSessionNeo4j:
    """Neo4jGraphStore.find_delegation_by_sub_session against a real Neo4j."""

    async def test_finds_delegation_in_buffer_before_flush(
        self, neo4j_services: Any
    ) -> None:
        """BUFFERED path: node still in the in-memory buffer is returned (no flush)."""
        store = neo4j_services.graph
        await store.upsert_node("ps1::delegation::tc1", dict(_DELEGATION_DATA))

        found = await store.find_delegation_by_sub_session("ss-target", store.workspace)
        assert found is not None
        assert found["agent"] == "foundation:explorer"
        assert found["sub_session_id"] == "ss-target"

    async def test_finds_delegation_via_cypher_after_flush(
        self, neo4j_services: Any
    ) -> None:
        """FLUSHED / Cypher path: after flush the buffer is empty, so the fallback
        MATCH (d:Delegation {sub_session_id, workspace}) query must find it."""
        store = neo4j_services.graph
        await store.upsert_node("ps1::delegation::tc1", dict(_DELEGATION_DATA))
        await store.flush()

        # Buffer is now empty — this exercises the real production Cypher path.
        assert not store._node_buffer, "buffer must be empty after flush"

        found = await store.find_delegation_by_sub_session("ss-target", store.workspace)
        assert found is not None, "Cypher fallback must locate the flushed Delegation"
        assert found["agent"] == "foundation:explorer"
        assert found["sub_session_id"] == "ss-target"

    async def test_returns_none_when_no_matching_sub_session(
        self, neo4j_services: Any
    ) -> None:
        """No Delegation with that sub_session_id (post-flush) -> None."""
        store = neo4j_services.graph
        await store.upsert_node("ps1::delegation::tc1", dict(_DELEGATION_DATA))
        await store.flush()

        found = await store.find_delegation_by_sub_session(
            "nonexistent-sub-session", store.workspace
        )
        assert found is None

    async def test_workspace_scoping_excludes_other_workspace(
        self, neo4j_services: Any, neo4j_container: dict[str, Any]
    ) -> None:
        """A Delegation with the SAME sub_session_id but a DIFFERENT workspace
        must NOT be returned when scoping to this store's workspace.

        Seeds a Delegation in workspace 'other-workspace' via an independent
        store against the same Neo4j, then proves:
        - scoping to 'test' returns None (cross-workspace node is invisible), and
        - scoping to 'other-workspace' DOES return it (the node really exists;
          workspace is the discriminator, not absence).
        """
        # Seed a Delegation with sub_session_id='ss-target' in a DIFFERENT workspace.
        other_store = Neo4jGraphStore(
            uri=neo4j_container["bolt_url"],
            auth=(neo4j_container["user"], neo4j_container["password"]),
            workspace="other-workspace",
        )
        try:
            await other_store.upsert_node(
                "other-ps::delegation::other-tc", dict(_DELEGATION_DATA)
            )
            await other_store.flush()
        finally:
            await other_store.close()

        store = neo4j_services.graph  # workspace == "test", empty buffer

        # Scoped to THIS store's workspace ("test"): the other-workspace node is invisible.
        found_test = await store.find_delegation_by_sub_session("ss-target", "test")
        assert found_test is None, (
            "workspace scoping must exclude a Delegation from another workspace"
        )

        # Scoped to 'other-workspace': the same call DOES return it — proving the
        # node exists and workspace (not absence) is what excluded it above.
        found_other = await store.find_delegation_by_sub_session(
            "ss-target", "other-workspace"
        )
        assert found_other is not None
        assert found_other["agent"] == "foundation:explorer"
        assert found_other["sub_session_id"] == "ss-target"
