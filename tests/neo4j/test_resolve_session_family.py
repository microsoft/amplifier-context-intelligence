"""Tier 3 - Neo4j integration proof for Neo4jGraphStore.resolve_session_family.

Ingests a multi-session family (root + 2 subsessions + 1 fork) with $blob_ref
values attached to nodes across several of those sessions, then proves:

  (a) resolving from a SUB-session id yields the SAME family (same session-id
      set) as resolving from the root id;
  (b) the whole-family blob count = the distinct $blob_ref URIs across the
      family (not just one session's), and a blob reachable only through a
      shared :SST_CONCEPT node is correctly excluded;
  (c) started_at and last_change are present, and last_change reflects the
      MAX across the family (not just the root -- touch_session only ever
      updates the direct node's last_updated, per services.py);
  (d) node/edge counts match the constructed family exactly.

Run: uv run pytest tests/neo4j/test_resolve_session_family.py -v -m neo4j
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

pytestmark = pytest.mark.neo4j


async def _build_family(store: Any) -> None:
    """Seed a root + 2 subsessions + 1 fork, with blobs on several sessions.

    Tree shape:  root -[HAS_SUBSESSION]-> sub1 -[HAS_SUBSESSION]-> sub2
                 root -[FORKED]-> fork1

    Blob-bearing event nodes attached at three different family sessions
    (root, sub2, fork1), plus a shared Agent (:SST_CONCEPT) reached from
    fork1's Delegation, with an out-of-family node hanging off the Agent to
    prove traversal stops there (its blob must NOT be counted).
    """
    store.created_by = "colombod"
    await store.upsert_node(
        "nf-fam-root",
        {"labels": ["Session", "RootSession"], "started_at": "2026-01-01T00:00:00Z"},
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

    # Blob-bearing nodes across several family sessions.
    await store.upsert_node(
        "nf-fam-root::orch::1",
        {
            "labels": ["OrchestratorRun", "SST_EVENT"],
            "raw": {"$blob_ref": "ci-blob://nf-fam-root/orch1"},
        },
    )
    await store.upsert_edge(
        "nf-fam-root", "nf-fam-root::orch::1", {"type": "HAS_EXECUTION"}
    )

    await store.upsert_node(
        "nf-fam-sub2::tool::1",
        {
            "labels": ["ToolCall", "SST_EVENT"],
            "result": {"$blob_ref": "ci-blob://nf-fam-sub2/tool1"},
        },
    )
    await store.upsert_edge(
        "nf-fam-sub2", "nf-fam-sub2::tool::1", {"type": "HAS_TOOL_CALL"}
    )

    await store.upsert_node(
        "nf-fam-fork1::delegation::1",
        {
            "labels": ["Delegation", "SST_EVENT"],
            "messages": {"$blob_ref": "ci-blob://nf-fam-fork1/del1"},
        },
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
        {
            "labels": ["ToolCall", "SST_EVENT"],
            "raw": {"$blob_ref": "ci-blob://nf-other-session-xyz/leak"},
        },
    )
    await store.upsert_edge(
        "nf-agent-shared", "nf-other-session-xyz::leak", {"type": "SOME_EDGE"}
    )

    await store.flush()


class TestResolveSessionFamilyNeo4j:
    """Neo4jGraphStore.resolve_session_family against a real Neo4j."""

    async def test_returns_none_for_unknown_session(self, neo4j_services: Any) -> None:
        store = neo4j_services.graph
        assert await store.resolve_session_family("does-not-exist") is None

    async def test_sub_session_and_root_resolve_identical_family(
        self, neo4j_services: Any
    ) -> None:
        store = neo4j_services.graph
        await _build_family(store)

        from_root = await store.resolve_session_family("nf-fam-root")
        from_sub = await store.resolve_session_family("nf-fam-sub2")
        from_fork = await store.resolve_session_family("nf-fam-fork1")

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
        await _build_family(store)
        family = await store.resolve_session_family("nf-fam-root")
        assert family is not None
        assert family.subsession_count == 3

    async def test_blob_count_is_whole_family_not_one_session(
        self, neo4j_services: Any
    ) -> None:
        """(b) blob count = distinct $blob_ref URIs across the WHOLE family."""
        store = neo4j_services.graph
        await _build_family(store)
        family = await store.resolve_session_family("nf-fam-sub2")
        assert family is not None
        assert family.blob_refs == {
            "ci-blob://nf-fam-root/orch1",
            "ci-blob://nf-fam-sub2/tool1",
            "ci-blob://nf-fam-fork1/del1",
        }
        assert len(family.blob_refs) == 3
        # Reachable only via the shared concept node -- must be excluded.
        assert "ci-blob://nf-other-session-xyz/leak" not in family.blob_refs

    async def test_started_at_and_last_change_present(
        self, neo4j_services: Any
    ) -> None:
        """(c) started_at/last_change present; last_change is the family MAX."""
        store = neo4j_services.graph
        await _build_family(store)
        family = await store.resolve_session_family("nf-fam-root")
        assert family is not None

        assert family.started_at is not None
        assert family.started_at == datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        # fam-sub2's last_updated (00:05) is later than root's own timestamps,
        # proving last_change is computed across the family, not just root.
        assert family.last_change is not None
        assert family.last_change == datetime(2026, 1, 1, 0, 5, 0, tzinfo=timezone.utc)

    async def test_node_and_edge_counts_match_constructed_family(
        self, neo4j_services: Any
    ) -> None:
        """(d) node/edge counts match the constructed family exactly."""
        store = neo4j_services.graph
        await _build_family(store)
        family = await store.resolve_session_family("nf-fam-root")
        assert family is not None

        # 4 sessions + orchestrator run + tool call + delegation + agent
        # (boundary node, included but not expanded past) = 8.
        assert family.node_count == 8
        # root->sub1, sub1->sub2, root->fork1, root->orch, sub2->tool,
        # fork1->delegation, delegation->agent = 7 (agent->leak excluded).
        assert family.edge_count == 7

    async def test_created_by_from_root(self, neo4j_services: Any) -> None:
        store = neo4j_services.graph
        await _build_family(store)
        family = await store.resolve_session_family("nf-fam-fork1")
        assert family is not None
        assert family.created_by == "colombod"

    async def test_workspace_scoping_excludes_other_workspace(
        self, neo4j_services: Any, neo4j_container: dict[str, Any]
    ) -> None:
        """A same-named root in a DIFFERENT workspace must not be resolved."""
        from context_intelligence_server.neo4j_store import Neo4jGraphStore

        store = neo4j_services.graph
        await _build_family(store)

        other_store = Neo4jGraphStore(
            uri=neo4j_container["bolt_url"],
            auth=(neo4j_container["user"], neo4j_container["password"]),
            workspace="other-workspace",
        )
        try:
            found = await other_store.resolve_session_family("nf-fam-root")
            assert found is None, (
                "workspace scoping must exclude a family from another workspace"
            )
        finally:
            await other_store.close()
