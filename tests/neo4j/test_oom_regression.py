"""OOM regression tests for Neo4j memory-cap behaviour.

Proves that the 2 MiB per-transaction cap (NEO4J_db_memory_transaction_max=2m)
is correctly enforced by the capped container fixture.  The test suite has two
layers:

  calibration guard  — a tiny single-node write SUCCEEDS under the cap,
                        establishing that a later "stall" provably means OOM,
                        not a broken fixture.

All tests in this module require the ``docker`` package and are marked
``neo4j`` so they opt-out of the default run.

Run explicitly:
    uv run pytest tests/neo4j/test_oom_regression.py -v -m neo4j
"""

from __future__ import annotations

from typing import Any

import pytest
from neo4j import AsyncGraphDatabase
from neo4j.exceptions import TransientError

from context_intelligence_server.neo4j_store import Neo4jGraphStore

pytestmark = pytest.mark.neo4j

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

_OOM_CODE = "Neo.TransientError.General.MemoryPoolOutOfMemoryError"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _low_retry_store(
    container: dict[str, Any],
    *,
    rows: int,
    byts: int,
) -> Neo4jGraphStore:
    """Create a Neo4jGraphStore with a 2s max_transaction_retry_time.

    Constructs the store against the capped container, then:
      1. Closes the original 30s-retry driver (no async driver leak).
      2. Replaces store._driver with a new driver with max_transaction_retry_time=2.0.

    Args:
        container:  Connection info dict from neo4j_container_capped.
        rows:       flush_chunk_rows for the store.
        byts:       flush_chunk_bytes for the store.

    Returns:
        A Neo4jGraphStore ready for use against the capped container.
    """
    store = Neo4jGraphStore(
        uri=container["bolt_url"],
        auth=(container["user"], container["password"]),
        workspace="oom-test",
        flush_chunk_rows=rows,
        flush_chunk_bytes=byts,
    )
    # Close the default 30s-retry driver before swapping to avoid an async
    # driver leak (open handles left behind).
    await store._driver.close()
    store._driver = AsyncGraphDatabase.driver(
        container["bolt_url"],
        auth=(container["user"], container["password"]),
        max_transaction_retry_time=2.0,
    )
    return store


async def _buffer_fat_nodes(
    store: Neo4jGraphStore,
    *,
    n: int,
    blob_bytes: int,
    prefix: str,
) -> None:
    """Buffer *n* single-phase node rows, each carrying a ~blob_bytes blob.

    Each node gets a UNIQUE prefix-scoped node_id so test runs don't collide.
    Only nodes are buffered (no edges, no label patches — single phase).

    Args:
        store:      The Neo4jGraphStore to buffer into.
        n:          Number of nodes to buffer.
        blob_bytes: Approximate size of the ``blob`` property in bytes.
        prefix:     Namespace prefix for node_ids (e.g. ``"calib"``, ``"big"``).
    """
    blob = "x" * blob_bytes
    for i in range(n):
        node_id = f"{prefix}-{i}"
        await store.upsert_node(node_id, {"blob": blob})


async def _purge_prefix(store: Neo4jGraphStore, prefix: str) -> None:
    """DETACH DELETE all nodes whose node_id starts with *prefix*.

    Runs at the top of each OOM test so tests are order-independent.
    """
    await store.execute_query(
        "MATCH (n) WHERE n.node_id STARTS WITH $prefix DETACH DELETE n",
        {"prefix": prefix},
        workspace="*",
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_calibration_guard_tiny_write_succeeds(
    neo4j_container_capped: dict[str, Any],
) -> None:
    """A tiny single-node write SUCCEEDS under the 2 MiB cap.

    This calibration guard proves the fixture is functional: if the 2 MiB cap
    were so tight that even a minimal write failed, every subsequent OOM test
    would be meaningless (it could be a broken fixture, not real OOM).

    Pass condition: flush does NOT raise, and exactly 1 node is persisted.
    """
    store = await _low_retry_store(
        neo4j_container_capped,
        rows=500,
        byts=2_000_000,
    )
    try:
        # Buffer one tiny node — well under 2 MiB.
        await _buffer_fat_nodes(store, n=1, blob_bytes=64, prefix="calib")

        # Flush must NOT raise under the 2 MiB cap.
        await store.flush()

        # Exactly one node was persisted.
        records = await store.execute_query(
            "MATCH (n) WHERE n.node_id STARTS WITH $prefix RETURN count(n) AS cnt",
            {"prefix": "calib"},
            workspace="*",
        )
        count = records[0]["cnt"]
        assert count == 1, (
            f"Expected exactly 1 node after calibration flush, got {count}"
        )
    finally:
        await store._driver.close()


async def test_unbounded_single_phase_flush_ooms(
    neo4j_container_capped: dict[str, Any],
) -> None:
    """Enormous chunk bounds ⇒ single-phase flush ⇒ OOM on the 2 MiB cap.

    Calibration: enormous flush_chunk_rows / flush_chunk_bytes (10 M rows,
    10 GB bytes) means all 400 fat nodes (each ~20 KB blob ⇒ ~8 MB total)
    are sent in one transaction, which is 4× over the 2 MiB per-transaction
    cap ⇒ Neo4j raises MemoryPoolOutOfMemoryError.

    Assertions:
    - flush() raises TransientError with code == _OOM_CODE (positively
      identifies the cause, not just "something failed").
    - MATCH count under prefix == 0 after the failed flush (the failed
      transaction committed nothing; _flush_body restored the buffer).
    """
    store = await _low_retry_store(
        neo4j_container_capped,
        rows=10_000_000,
        byts=10_000_000_000,
    )
    try:
        prefix = "oom-unbounded"
        await _purge_prefix(store, prefix)
        await _buffer_fat_nodes(store, n=400, blob_bytes=20_000, prefix=prefix)

        with pytest.raises(TransientError) as exc_info:
            await store.flush()
        assert exc_info.value.code == _OOM_CODE, (
            f"Expected OOM code {_OOM_CODE!r}, got {exc_info.value.code!r}"
        )

        # Nothing was committed — the failed transaction is fully rolled back.
        records = await store.execute_query(
            "MATCH (n) WHERE n.node_id STARTS WITH $prefix RETURN count(n) AS cnt",
            {"prefix": prefix},
            workspace="*",
        )
        count = records[0]["cnt"]
        assert count == 0, (
            f"Expected 0 nodes after OOM flush (nothing should commit), got {count}"
        )
    finally:
        await store._driver.close()


async def test_chunked_flush_drains_same_single_phase_buffer(
    neo4j_container_capped: dict[str, Any],
) -> None:
    """Small chunk bounds ⇒ chunked flush ⇒ all 400 nodes drained (RED).

    This is the genuine RED test: the same 400 fat nodes that OOM the capped
    container in a single transaction MUST drain successfully when
    flush_chunk_rows=50 / flush_chunk_bytes=262_144 (256 KB) keeps each
    chunk well under the 2 MiB cap.

    RED state: against the current unchunked _flush_body the store ignores
    flush_chunk_rows / flush_chunk_bytes and sends all 400 nodes in one
    transaction ⇒ TransientError / MemoryPoolOutOfMemoryError.

    GREEN state (after the fix): flush() must NOT raise; _node_buffer must
    be empty; MATCH count under prefix must equal 400.

    Calibration note: each chunk is ~50 × 20 KB = ~1 MB ⇒ 4-8× UNDER the
    2 MiB cap. Production defaults (100 rows / 4 MB) are deliberately NOT
    used because a 4 MB chunk would itself exceed a 2 MiB cap.
    """
    store = await _low_retry_store(
        neo4j_container_capped,
        rows=50,
        byts=262_144,
    )
    try:
        prefix = "oom-drain"
        await _purge_prefix(store, prefix)
        await _buffer_fat_nodes(store, n=400, blob_bytes=20_000, prefix=prefix)

        # Must NOT raise once chunking is implemented (RED: raises OOM now).
        await store.flush()

        assert store._node_buffer == {}, (
            "_node_buffer must be empty after a successful chunked flush"
        )

        records = await store.execute_query(
            "MATCH (n) WHERE n.node_id STARTS WITH $prefix RETURN count(n) AS cnt",
            {"prefix": prefix},
            workspace="*",
        )
        count = records[0]["cnt"]
        assert count == 400, f"Expected 400 nodes after chunked flush, got {count}"
    finally:
        await store._driver.close()
