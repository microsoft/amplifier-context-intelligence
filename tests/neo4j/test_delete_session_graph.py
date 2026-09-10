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


async def _build_large_adverse_graph(store: Any) -> tuple[int, str]:
    """Build a deliberately adverse graph for the whole-graph traversal:
    a branchy session tree, several events per session, and a DIAMOND where
    every event points at one shared :SST_CONCEPT agent (many paths converging
    on the boundary node). Returns (owned_node_count, root_id).

    This is the shape that stresses ``_GRAPH_SUBGRAPH_CYPHER`` /
    ``_GRAPH_NODE_PARTITION_CYPHER``: the traversal must fan out over the whole
    tree, stop AT (not past) the shared concept despite countless paths reaching
    it, and still resolve + delete correctly. A regression here is a hang or a
    wrong count, not a subtle value.
    """
    store.created_by = "colombod"
    root = "big-root"
    await store.upsert_node(root, {"labels": ["Session", "RootSession"]})
    await store.upsert_node("big-agent", {"labels": ["Agent", "SST_CONCEPT"]})
    sessions = [root]
    frontier = [root]
    sid = 0
    # depth 4, branch 3 -> 121 sessions (bounded so the throwaway CI Neo4j stays fast)
    for _ in range(4):
        nxt: list[str] = []
        for parent in frontier:
            for _b in range(3):
                sid += 1
                child = f"big-s{sid}"
                await store.upsert_node(
                    child, {"labels": ["Session", "SubSession"], "parent_id": parent}
                )
                await store.upsert_edge(parent, child, {"type": "HAS_SUBSESSION"})
                sessions.append(child)
                nxt.append(child)
        frontier = nxt
    owned = len(sessions)  # sessions
    # 2 events per session, each also edged to the ONE shared agent (the diamond).
    for i, ses in enumerate(sessions):
        for e in range(2):
            ev = f"big-e{i}-{e}"
            await store.upsert_node(ev, {"labels": ["Event"]})
            await store.upsert_edge(ses, ev, {"type": "HAS_EVENT"})
            await store.upsert_edge(ev, "big-agent", {"type": "USED_AGENT"})
            owned += 1  # event (the agent is a boundary concept, NOT owned)
    # One k-way parallel group written as a transitive tournament (each member
    # edged to every PRIOR member), exactly how data_layer_2/tool_call.py and
    # data_layer_3/delegation.py record a parallel batch. A tournament on k
    # nodes has Theta(2^k) directed paths, so a path-enumerating traversal
    # would hang here; a frontier BFS costs the k(k-1)/2 edges. k=24 is well
    # within the 5-30 parallel units mass-change / ten-lane produce by design.
    k = 24
    for j in range(k):
        node = f"big-par-{j}"
        await store.upsert_node(node, {"labels": ["Event"]})
        await store.upsert_edge(root, node, {"type": "HAS_EVENT"})
        for prior in range(j):
            await store.upsert_edge(node, f"big-par-{prior}", {"type": "CAUSED"})
        owned += 1  # each parallel-group member is owned
    await store.flush()
    return owned, root


class TestDeleteLargeAdverseGraph:
    """Whole-graph resolve + delete on a real Neo4j, at a size and shape that
    exercises the unbounded traversal rather than a 7-node toy graph."""

    async def test_resolves_and_deletes_a_large_diamond_graph(
        self, neo4j_services: Any
    ) -> None:
        store = neo4j_services.graph
        owned, root = await _build_large_adverse_graph(store)

        # Resolve returns the whole owned graph plus the one boundary concept.
        graph = await store.resolve_session_graph(root)
        assert graph is not None
        assert graph.node_count == owned + 1  # + the shared agent (boundary, included)

        # Delete removes every owned node, keeps the shared concept, and the
        # boundary-pruned traversal terminates (a hang would fail the suite).
        result = await store.delete_session_graph(root, graph=graph)
        assert result is not None
        assert result.nodes_deleted == owned
        assert await store.get_node(root) is None
        agent = await store.get_node("big-agent")
        assert agent is not None
        assert "SST_CONCEPT" in agent.get("labels", [])
