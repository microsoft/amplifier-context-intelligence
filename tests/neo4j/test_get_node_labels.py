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
