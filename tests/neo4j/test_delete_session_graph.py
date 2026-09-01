"""Tier 3 - Neo4j integration proof for Neo4jGraphStore.delete_session_graph.

Ingests a multi-session graph (root + 2 subsessions + 1 fork) that shares a
:SST_CONCEPT node (Agent) with a SEPARATE, unrelated graph, then deletes via
a SUB-session id and proves:

  (a) every owned graph node is gone (root + all descendants + their
      Events/ToolCalls/Delegation);
  (b) the shared :SST_CONCEPT node STILL EXISTS;
  (c) the unrelated graph reachable only through that shared concept node is
      UNTOUCHED (its own nodes and its edge to the concept node both survive);
  (d) the returned counts (nodes_deleted / relationships_deleted) match what
      was actually removed;
  (e) deleting an unknown session id returns None / no-op (nothing deleted).

Run: uv run --group dev pytest tests/neo4j/test_delete_session_graph.py -v -m neo4j
"""

from __future__ import annotations

from typing import Any

import pytest

pytestmark = pytest.mark.neo4j


async def _build_graph(store: Any) -> None:
    """Seed a root + 2 subsessions + 1 fork, with blobs on several sessions.

    Tree shape:  root -[HAS_SUBSESSION]-> sub1 -[HAS_SUBSESSION]-> sub2
                 root -[FORKED]-> fork1

    A shared Agent (:SST_CONCEPT) is reached from fork1's Delegation. A
    completely SEPARATE, unrelated session graph also reaches the SAME
    shared Agent, proving the delete of THIS graph must not touch the
    other one.
    """
    store.created_by = "colombod"
    await store.upsert_node(
        "df-fam-root",
        {"labels": ["Session", "RootSession"], "started_at": "2026-01-01T00:00:00Z"},
    )
    await store.upsert_node(
        "df-fam-sub1",
        {
            "labels": ["Session", "SubSession"],
            "parent_id": "df-fam-root",
            "started_at": "2026-01-01T00:01:00Z",
        },
    )
    await store.upsert_edge("df-fam-root", "df-fam-sub1", {"type": "HAS_SUBSESSION"})
    await store.upsert_node(
        "df-fam-sub2",
        {
            "labels": ["Session", "SubSession"],
            "parent_id": "df-fam-sub1",
            "started_at": "2026-01-01T00:02:00Z",
        },
    )
    await store.upsert_edge("df-fam-sub1", "df-fam-sub2", {"type": "HAS_SUBSESSION"})
    await store.upsert_node(
        "df-fam-fork1",
        {
            "labels": ["Session", "ForkedSession"],
            "parent_id": "df-fam-root",
            "started_at": "2026-01-01T00:03:00Z",
        },
    )
    await store.upsert_edge("df-fam-root", "df-fam-fork1", {"type": "FORKED"})

    await store.upsert_node(
        "df-fam-root::orch::1",
        {
            "labels": ["OrchestratorRun", "SST_EVENT"],
            "raw": {"$blob_ref": "ci-blob://df-fam-root/orch1"},
        },
    )
    await store.upsert_edge(
        "df-fam-root", "df-fam-root::orch::1", {"type": "HAS_EXECUTION"}
    )

    await store.upsert_node(
        "df-fam-sub2::tool::1",
        {
            "labels": ["ToolCall", "SST_EVENT"],
            "result": {"$blob_ref": "ci-blob://df-fam-sub2/tool1"},
        },
    )
    await store.upsert_edge(
        "df-fam-sub2", "df-fam-sub2::tool::1", {"type": "HAS_TOOL_CALL"}
    )

    await store.upsert_node(
        "df-fam-fork1::delegation::1",
        {
            "labels": ["Delegation", "SST_EVENT"],
            "messages": {"$blob_ref": "ci-blob://df-fam-fork1/del1"},
        },
    )
    await store.upsert_edge(
        "df-fam-fork1", "df-fam-fork1::delegation::1", {"type": "TRIGGERED"}
    )

    # Shared concept node -- must survive the delete of df-fam-* below.
    await store.upsert_node("df-agent-shared", {"labels": ["Agent", "SST_CONCEPT"]})
    await store.upsert_edge(
        "df-fam-fork1::delegation::1", "df-agent-shared", {"type": "HAS_AGENT"}
    )

    await store.flush()


async def _build_unrelated_graph_sharing_concept(store: Any) -> None:
    """A SEPARATE, unrelated graph reaching the SAME shared Agent node.

    Reachable ONLY through the shared concept -- deleting df-fam-* must not
    touch any of this.
    """
    await store.upsert_node(
        "other-fam-root",
        {"labels": ["Session", "RootSession"], "started_at": "2026-02-01T00:00:00Z"},
    )
    await store.upsert_node(
        "other-fam-root::delegation::1",
        {"labels": ["Delegation", "SST_EVENT"]},
    )
    await store.upsert_edge(
        "other-fam-root", "other-fam-root::delegation::1", {"type": "TRIGGERED"}
    )
    await store.upsert_edge(
        "other-fam-root::delegation::1", "df-agent-shared", {"type": "HAS_AGENT"}
    )
    await store.flush()


class TestDeleteSessionGraphNeo4j:
    """Neo4jGraphStore.delete_session_graph against a real Neo4j."""

    async def test_returns_none_for_unknown_session(self, neo4j_services: Any) -> None:
        store = neo4j_services.graph
        assert await store.delete_session_graph("does-not-exist") is None

    async def test_unknown_session_deletes_nothing(self, neo4j_services: Any) -> None:
        store = neo4j_services.graph
        await _build_graph(store)
        await _build_unrelated_graph_sharing_concept(store)

        assert await store.delete_session_graph("does-not-exist") is None

        # Nothing from either graph was touched.
        assert await store.get_node("df-fam-root") is not None
        assert await store.get_node("other-fam-root") is not None

    async def test_owned_graph_nodes_all_removed(self, neo4j_services: Any) -> None:
        store = neo4j_services.graph
        await _build_graph(store)
        await _build_unrelated_graph_sharing_concept(store)

        result = await store.delete_session_graph("df-fam-sub2")
        assert result is not None
        assert result.root_id == "df-fam-root"

        for node_id in (
            "df-fam-root",
            "df-fam-sub1",
            "df-fam-sub2",
            "df-fam-fork1",
            "df-fam-root::orch::1",
            "df-fam-sub2::tool::1",
            "df-fam-fork1::delegation::1",
        ):
            assert await store.get_node(node_id) is None, (
                f"{node_id} should have been deleted"
            )

    async def test_shared_concept_node_survives(self, neo4j_services: Any) -> None:
        store = neo4j_services.graph
        await _build_graph(store)
        await _build_unrelated_graph_sharing_concept(store)

        await store.delete_session_graph("df-fam-root")

        agent = await store.get_node("df-agent-shared")
        assert agent is not None
        assert "SST_CONCEPT" in agent.get("labels", [])

    async def test_unrelated_graph_reachable_only_via_shared_concept_untouched(
        self, neo4j_services: Any
    ) -> None:
        store = neo4j_services.graph
        await _build_graph(store)
        await _build_unrelated_graph_sharing_concept(store)

        await store.delete_session_graph("df-fam-fork1")

        assert await store.get_node("other-fam-root") is not None
        assert await store.get_node("other-fam-root::delegation::1") is not None
        assert (
            await store.get_edge("other-fam-root::delegation::1", "df-agent-shared")
            is not None
        )

    async def test_counts_match_deleted_nodes_and_relationships(
        self, neo4j_services: Any
    ) -> None:
        store = neo4j_services.graph
        await _build_graph(store)
        await _build_unrelated_graph_sharing_concept(store)

        result = await store.delete_session_graph("df-fam-root")
        assert result is not None
        # Owned = 4 sessions + orch + tool + delegation = 7 (agent excluded).
        assert result.nodes_deleted == 7
        # root->sub1, sub1->sub2, root->fork1, root->orch, sub2->tool,
        # fork1->delegation, delegation->agent = 7 (agent->other-delegation
        # edge belongs to the unrelated graph and is excluded).
        assert result.relationships_deleted == 7

    async def test_sub_session_and_root_delete_identical_graph(
        self, neo4j_services: Any
    ) -> None:
        """A sub-session id and its root id must delete the identical graph."""
        store_a = neo4j_services.graph
        await _build_graph(store_a)
        await _build_unrelated_graph_sharing_concept(store_a)
        result_a = await store_a.delete_session_graph("df-fam-sub1")
        assert result_a is not None
        assert result_a.root_id == "df-fam-root"
        assert result_a.nodes_deleted == 7
        assert result_a.relationships_deleted == 7
