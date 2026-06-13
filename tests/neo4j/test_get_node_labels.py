"""Tier 3 - Neo4j integration proof for the get_node labels fix (Component A).

Encodes the Wave-3 antagonist scenario against a real disposable Neo4j:
after a flush, get_node() must return the node's labels. Before the
neo4j_store fix this returned a dict without a 'labels' key, which caused
session dual-labeling. Requires a Neo4j container (neo4j_services fixture).

Run: uv run pytest tests/neo4j/test_get_node_labels.py -v -m neo4j
"""

from __future__ import annotations

from typing import Any

import pytest


@pytest.mark.neo4j
class TestGetNodeReturnsLabelsAfterFlush:
    """get_node() must include labels on the Neo4j fallback path."""

    async def test_labels_present_after_flush(self, neo4j_services: Any) -> None:
        store = neo4j_services.graph
        session_id = "child-session-001"

        # Create a node and add the ForkedSession terminal label, as the fork
        # handler would, then flush so the in-memory buffer is cleared.
        await store.upsert_node(
            session_id,
            {
                "labels": ["Session"],
                "session_id": session_id,
                "started_at": "2026-01-01T00:00:00Z",
            },
        )
        await store.set_labels(
            session_id,
            remove_labels=[],
            add_labels=["Session", "ForkedSession", "SST_EVENT"],
        )
        await store.flush()

        # get_node() now misses the buffer and falls back to Neo4j.
        node = await store.get_node(session_id)

        assert node is not None
        assert "labels" in node, (
            f"fallback must return labels; got keys {sorted(node.keys())}"
        )
        assert "ForkedSession" in node["labels"]


from context_intelligence_server.handlers.data_layer_2.session import (  # noqa: E402
    SessionHandler,
)


async def _neo4j_labels(services: Any, node_id: str) -> list[str]:
    rows = await services.graph.execute_query(
        "MATCH (n) WHERE n.node_id = $id AND n.workspace = $workspace "
        "RETURN labels(n) AS lbls",
        {"id": node_id, "workspace": services.graph.workspace},
        workspace="*",
    )
    return list(rows[0]["lbls"]) if rows else []


@pytest.mark.neo4j
class TestForkFlushStartSingleLabel:
    """fork -> flush -> start must leave exactly one terminal label."""

    async def test_fork_flush_start_yields_single_label(
        self, neo4j_services: Any
    ) -> None:
        handler = SessionHandler(neo4j_services)
        parent_id = "parent-session-t02"
        child_id = "child-session-t02"

        await handler(
            "session:fork",
            {
                "session_id": child_id,
                "parent_id": parent_id,
                "timestamp": "2026-01-01T10:00:00Z",
            },
        )
        # Flush lands between the two OS-process events in production.
        await neo4j_services.graph.flush()

        await handler(
            "session:start",
            {
                "session_id": child_id,
                "parent_id": parent_id,
                "timestamp": "2026-01-01T10:00:01Z",
            },
        )
        await neo4j_services.graph.flush()

        labels = await _neo4j_labels(neo4j_services, child_id)
        assert "ForkedSession" in labels, f"expected ForkedSession in {labels}"
        assert "SubSession" not in labels, (
            f"dual-label regression: node carries both terminal labels: {labels}"
        )
