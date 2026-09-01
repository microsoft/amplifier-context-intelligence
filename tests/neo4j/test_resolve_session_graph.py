"""Tier 3 - Neo4j integration proof for Neo4jGraphStore.resolve_session_graph.

Ingests a multi-session graph (root + 2 subsessions + 1 fork) with a few
extra event nodes attached to nodes across several of those sessions (used
to build up node/edge counts -- blobs themselves are not this store's job,
see BlobStore.list instead), then proves:

  (a) resolving from a SUB-session id yields the SAME graph (same session-id
      set) as resolving from the root id;
  (b) started_at and last_change are present, and last_change reflects the
      MAX across the graph (not just the root -- touch_session only ever
      updates the direct node's last_updated, per services.py);
  (c) node/edge counts match the constructed graph exactly.

Run: uv run pytest tests/neo4j/test_resolve_session_graph.py -v -m neo4j
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

pytestmark = pytest.mark.neo4j


async def _build_graph(store: Any) -> None:
    """Seed a root + 2 subsessions + 1 fork, with extra event nodes on several sessions.

    Tree shape:  root -[HAS_SUBSESSION]-> sub1 -[HAS_SUBSESSION]-> sub2
                 root -[FORKED]-> fork1

    Event nodes attached at three different graph sessions (root, sub2,
    fork1), plus a shared Agent (:SST_CONCEPT) reached from fork1's
    Delegation, with an out-of-graph node hanging off the Agent to prove
    traversal stops there.
    """
    store.created_by = "colombod"
    await store.upsert_node(
        "nf-fam-root",
        {
            "labels": ["Session", "RootSession"],
            "started_at": "2026-01-01T00:00:00Z",
            "working_dir": "/mnt/workspaces/project-2501",
        },
    )
    await store.upsert_node(
        "nf-fam-sub1",
        {
            "labels": ["Session", "SubSession"],
            "parent_id": "nf-fam-root",
            "started_at": "2026-01-01T00:01:00Z",
        },
    )
    await store.upsert_edge("nf-fam-root", "nf-fam-sub1", {"type": "HAS_SUBSESSION"})
    await store.upsert_node(
        "nf-fam-sub2",
        {
            "labels": ["Session", "SubSession"],
            "parent_id": "nf-fam-sub1",
            "started_at": "2026-01-01T00:02:00Z",
            "last_updated": "2026-01-01T00:05:00Z",
        },
    )
    await store.upsert_edge("nf-fam-sub1", "nf-fam-sub2", {"type": "HAS_SUBSESSION"})
    await store.upsert_node(
        "nf-fam-fork1",
        {
            "labels": ["Session", "ForkedSession"],
            "parent_id": "nf-fam-root",
            "started_at": "2026-01-01T00:03:00Z",
        },
    )
    await store.upsert_edge("nf-fam-root", "nf-fam-fork1", {"type": "FORKED"})

    # Extra event nodes across several graph sessions.
    await store.upsert_node(
        "nf-fam-root::orch::1",
        {"labels": ["OrchestratorRun", "SST_EVENT"]},
    )
    await store.upsert_edge(
        "nf-fam-root", "nf-fam-root::orch::1", {"type": "HAS_EXECUTION"}
    )

    await store.upsert_node(
        "nf-fam-sub2::tool::1",
        {"labels": ["ToolCall", "SST_EVENT"]},
    )
    await store.upsert_edge(
        "nf-fam-sub2", "nf-fam-sub2::tool::1", {"type": "HAS_TOOL_CALL"}
    )

    await store.upsert_node(
        "nf-fam-fork1::delegation::1",
        {"labels": ["Delegation", "SST_EVENT"]},
    )
    await store.upsert_edge(
        "nf-fam-fork1", "nf-fam-fork1::delegation::1", {"type": "TRIGGERED"}
    )

    # Shared concept node -- traversal must stop here, not continue past it.
    await store.upsert_node("nf-agent-shared", {"labels": ["Agent", "SST_CONCEPT"]})
    await store.upsert_edge(
        "nf-fam-fork1::delegation::1", "nf-agent-shared", {"type": "HAS_AGENT"}
    )
    await store.upsert_node(
        "nf-other-session-xyz::leak",
        {"labels": ["ToolCall", "SST_EVENT"]},
    )
    await store.upsert_edge(
        "nf-agent-shared", "nf-other-session-xyz::leak", {"type": "SOME_EDGE"}
    )

    await store.flush()


class TestResolveSessionGraphNeo4j:
    """Neo4jGraphStore.resolve_session_graph against a real Neo4j."""

    async def test_returns_none_for_unknown_session(self, neo4j_services: Any) -> None:
        store = neo4j_services.graph
        assert await store.resolve_session_graph("does-not-exist") is None

    async def test_sub_session_and_root_resolve_identical_graph(
        self, neo4j_services: Any
    ) -> None:
        store = neo4j_services.graph
        await _build_graph(store)

        from_root = await store.resolve_session_graph("nf-fam-root")
        from_sub = await store.resolve_session_graph("nf-fam-sub2")
        from_fork = await store.resolve_session_graph("nf-fam-fork1")

        assert from_root is not None
        expected_ids = frozenset(
            {"nf-fam-root", "nf-fam-sub1", "nf-fam-sub2", "nf-fam-fork1"}
        )
        assert from_root.root_id == "nf-fam-root"
        assert from_root.session_ids == expected_ids

        assert from_sub is not None
        assert from_sub.root_id == "nf-fam-root"
        assert from_sub.session_ids == expected_ids

        assert from_fork is not None
        assert from_fork.root_id == "nf-fam-root"
        assert from_fork.session_ids == expected_ids

    async def test_subsession_count_excludes_root(self, neo4j_services: Any) -> None:
        store = neo4j_services.graph
        await _build_graph(store)
        graph = await store.resolve_session_graph("nf-fam-root")
        assert graph is not None
        assert graph.subsession_count == 3

    async def test_started_at_and_last_change_present(
        self, neo4j_services: Any
    ) -> None:
        """(b) started_at/last_change present; last_change is the graph MAX."""
        store = neo4j_services.graph
        await _build_graph(store)
        graph = await store.resolve_session_graph("nf-fam-root")
        assert graph is not None

        assert graph.started_at is not None
        assert graph.started_at == datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        # fam-sub2's last_updated (00:05) is later than root's own timestamps,
        # proving last_change is computed across the graph, not just root.
        assert graph.last_change is not None
        assert graph.last_change == datetime(2026, 1, 1, 0, 5, 0, tzinfo=timezone.utc)

    async def test_node_and_edge_counts_match_constructed_graph(
        self, neo4j_services: Any
    ) -> None:
        """(c) node/edge counts match the constructed graph exactly."""
        store = neo4j_services.graph
        await _build_graph(store)
        graph = await store.resolve_session_graph("nf-fam-root")
        assert graph is not None

        # 4 sessions + orchestrator run + tool call + delegation + agent
        # (boundary node, included but not expanded past) = 8.
        assert graph.node_count == 8
        # root->sub1, sub1->sub2, root->fork1, root->orch, sub2->tool,
        # fork1->delegation, delegation->agent = 7 (agent->leak excluded).
        assert graph.edge_count == 7

    async def test_created_by_from_root(self, neo4j_services: Any) -> None:
        store = neo4j_services.graph
        await _build_graph(store)
        graph = await store.resolve_session_graph("nf-fam-fork1")
        assert graph is not None
        assert graph.created_by == "colombod"

    async def test_working_dir_from_root(self, neo4j_services: Any) -> None:
        store = neo4j_services.graph
        await _build_graph(store)
        graph = await store.resolve_session_graph("nf-fam-fork1")
        assert graph is not None
        assert graph.working_dir == "/mnt/workspaces/project-2501"

    async def test_resolves_regardless_of_callers_bound_workspace(
        self, neo4j_services: Any, neo4j_container: dict[str, Any]
    ) -> None:
        """resolve_session_graph takes no workspace argument -- it discovers
        the workspace from the session id itself. A store instance bound to
        a DIFFERENT workspace at construction must still resolve the graph,
        and must report the workspace the session actually lives in (not
        the store's own bound workspace)."""
        from context_intelligence_server.neo4j_store import Neo4jGraphStore

        store = neo4j_services.graph
        await _build_graph(store)

        other_store = Neo4jGraphStore(
            uri=neo4j_container["bolt_url"],
            auth=(neo4j_container["user"], neo4j_container["password"]),
            workspace="other-workspace",
        )
        try:
            found = await other_store.resolve_session_graph("nf-fam-root")
            assert found is not None, (
                "the workspace is discovered from the session id, not "
                "supplied by the caller, so a differently-bound store must "
                "still find it"
            )
            assert found.workspace == "test", (
                "the reported workspace must be the one the session "
                "actually lives in, not other_store's own bound workspace"
            )
        finally:
            await other_store.close()

    async def test_ambiguous_session_id_across_workspaces_raises(
        self, neo4j_services: Any, neo4j_container: dict[str, Any]
    ) -> None:
        """The SAME session id used in TWO DIFFERENT workspaces is refused
        loudly rather than silently resolving one of them.

        Session ids are supposed to be unique, so this should not happen in
        practice -- but if it ever does, resolving (or deleting) by id alone
        must raise instead of guessing which workspace was meant.
        """
        from context_intelligence_server.graph_store import AmbiguousSessionError
        from context_intelligence_server.neo4j_store import Neo4jGraphStore

        store = neo4j_services.graph
        await _build_graph(store)

        other_store = Neo4jGraphStore(
            uri=neo4j_container["bolt_url"],
            auth=(neo4j_container["user"], neo4j_container["password"]),
            workspace="other-workspace",
        )
        try:
            # Put a session with the SAME node_id "nf-fam-root" into a
            # SECOND, different workspace.
            await other_store.upsert_node(
                "nf-fam-root",
                {
                    "labels": ["Session", "RootSession"],
                    "started_at": "2026-05-01T00:00:00Z",
                },
            )
            await other_store.flush()

            with pytest.raises(AmbiguousSessionError, match="more than one workspace"):
                await store.resolve_session_graph("nf-fam-root")
            with pytest.raises(AmbiguousSessionError, match="more than one workspace"):
                await other_store.resolve_session_graph("nf-fam-root")
        finally:
            await other_store.close()
