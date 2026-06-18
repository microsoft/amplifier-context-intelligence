"""Live cross-chunk referential integrity + large-buffer happy path.

Proves that nodes-before-edges ordering holds across chunk boundaries
and that a large multi-chunk buffer drains with no residue and exact counts.

Uses the shared session neo4j_container fixture (no cap needed).

Run explicitly:
    uv run pytest tests/neo4j/test_chunked_flush.py -v -m neo4j
"""

from __future__ import annotations

from typing import Any

import pytest

from context_intelligence_server.neo4j_store import Neo4jGraphStore

pytestmark = pytest.mark.neo4j


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _store(container: dict[str, Any], *, rows: int, byts: int) -> Neo4jGraphStore:
    """Construct a Neo4jGraphStore against the test container with given bounds."""
    return Neo4jGraphStore(
        uri=container["bolt_url"],
        auth=(container["user"], container["password"]),
        workspace="chunk-test",
        flush_chunk_rows=rows,
        flush_chunk_bytes=byts,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_cross_chunk_referential_integrity(
    neo4j_container: dict[str, Any],
) -> None:
    """Cross-chunk edge writes: all edges queryable after flush.

    With rows=2 each flush chunk holds at most 2 nodes, so 10 nodes
    produce 5 node chunks.  Three edges span non-adjacent chunk pairs:
    (c-0, c-9), (c-1, c-8), (c-2, c-7).  After flush the nodes-before-edges
    invariant guarantees all three endpoint nodes are committed before the
    edge phase runs, so every edge must be queryable via get_edge().
    """
    store = _store(neo4j_container, rows=2, byts=10_000_000)
    try:
        # Buffer 10 Event nodes c-0 .. c-9
        for i in range(10):
            await store.upsert_node(f"c-{i}", {"labels": ["Event"]})

        # Buffer 3 cross-chunk edges
        cross_pairs = [("c-0", "c-9"), ("c-1", "c-8"), ("c-2", "c-7")]
        for src, dst in cross_pairs:
            await store.upsert_edge(src, dst, {"type": "RELATED"})

        # Flush all buffered writes (nodes first, then edges across chunk boundaries)
        await store.flush()

        # Assert 10 c-* Event nodes were persisted
        records = await store.execute_query(
            "MATCH (n:Event) WHERE n.node_id STARTS WITH 'c-' "
            "AND n.workspace = $workspace RETURN count(n) AS cnt",
            {},
        )
        node_count = records[0]["cnt"]
        assert node_count == 10, (
            f"Expected 10 c-* Event nodes after flush, got {node_count}"
        )

        # Assert each cross-chunk edge is queryable (not None)
        for src, dst in cross_pairs:
            edge = await store.get_edge(src, dst)
            assert edge is not None, (
                f"Cross-chunk edge ({src!r} -> {dst!r}) not found after flush; "
                "nodes-before-edges invariant may have failed across chunk boundaries"
            )
    finally:
        await store._driver.close()


async def test_large_buffer_happy_path_no_residue(
    neo4j_container: dict[str, Any],
) -> None:
    """Large multi-chunk buffer drains completely with no residue.

    500 nodes + 499 NEXT edges are buffered into a store with default
    production bounds (rows=100, byts=4_194_304).  After flush:
    - _node_buffer and _edge_buffer are both empty (no residue).
    - Exactly 500 h-* nodes are persisted.
    - Exactly 499 NEXT relationships are persisted.
    """
    store = _store(neo4j_container, rows=100, byts=4_194_304)
    n = 500
    try:
        # Buffer 500 Event nodes h-0 .. h-499
        for i in range(n):
            await store.upsert_node(f"h-{i}", {"labels": ["Event"]})

        # Buffer 499 NEXT edges h-i -> h-(i+1)
        for i in range(n - 1):
            await store.upsert_edge(f"h-{i}", f"h-{i + 1}", {"type": "NEXT"})

        # Flush all buffered writes
        await store.flush()

        # Assert buffers are empty (no residue)
        assert not store._node_buffer, (
            f"_node_buffer must be empty after flush, got {len(store._node_buffer)} entries"
        )
        assert not store._edge_buffer, (
            f"_edge_buffer must be empty after flush, got {len(store._edge_buffer)} entries"
        )

        # Assert exactly 500 h-* nodes were persisted
        node_records = await store.execute_query(
            "MATCH (n) WHERE n.node_id STARTS WITH 'h-' "
            "AND n.workspace = $workspace RETURN count(n) AS cnt",
            {},
        )
        node_count = node_records[0]["cnt"]
        assert node_count == 500, (
            f"Expected 500 h-* nodes after large-buffer flush, got {node_count}"
        )

        # Assert exactly 499 NEXT edges were persisted
        edge_records = await store.execute_query(
            "MATCH ()-[r:NEXT]->() WHERE r.workspace = $workspace "
            "AND r.src_id STARTS WITH 'h-' RETURN count(r) AS cnt",
            {},
        )
        edge_count = edge_records[0]["cnt"]
        assert edge_count == 499, (
            f"Expected 499 NEXT edges after large-buffer flush, got {edge_count}"
        )
    finally:
        await store._driver.close()
