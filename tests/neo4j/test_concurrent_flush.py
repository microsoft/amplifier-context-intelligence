"""Tier 3 — Neo4j-backed tests for concurrent flush durability.

These tests validate the schema and write-path changes that make concurrent
``MERGE`` operations atomic under multi-writer load. They require a live Neo4j
container (provided by the ``neo4j_container`` fixture) and are marked with
``@pytest.mark.neo4j``.

Run explicitly:
    uv run pytest tests/neo4j/test_concurrent_flush.py -v -m neo4j
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

import pytest
from neo4j import AsyncGraphDatabase

from context_intelligence_server.neo4j_store import (
    Neo4jGraphStore,
    ensure_neo4j_schema,
)
from context_intelligence_server.utils import make_node_id

pytestmark = pytest.mark.neo4j

# ---------------------------------------------------------------------------
# Phase A concurrent-flush acceptance test parameters
# ---------------------------------------------------------------------------
ROOT_ID = "root-session"
N_WRITERS = 8
EVENTS_PER_WRITER = 25


def _ts(i: int) -> str:
    """Distinct ISO-8601 timestamps so make_node_id yields distinct node ids."""
    return datetime(2026, 6, 11, 12, 0, 0, i * 1000, tzinfo=timezone.utc).isoformat()


async def test_event_uniqueness_constraint_created(
    neo4j_container: dict[str, Any],
) -> None:
    """ensure_neo4j_schema creates a uniqueness constraint on :Event(node_id, workspace).

    Mirrors the existing :Session uniqueness constraint so that idempotent
    ``MERGE (n:Event ...)`` under concurrency is atomic.
    """
    driver = AsyncGraphDatabase.driver(
        neo4j_container["bolt_url"],
        auth=(neo4j_container["user"], neo4j_container["password"]),
    )
    try:
        await ensure_neo4j_schema(driver)

        async with driver.session() as session:
            result = await session.run("SHOW CONSTRAINTS")
            rows = [record async for record in result]

        names = {row["name"] for row in rows}
        assert "event_node_id_workspace_unique" in names
    finally:
        await driver.close()


async def test_concurrent_flush_zero_event_loss(
    neo4j_container: dict[str, Any],
) -> None:
    """N concurrent writers sharing one hot root :Session node must not drop events.

    Phase A acceptance test (committed with the fix in a later task). Each of
    ``N_WRITERS`` independent ``Neo4jGraphStore`` instances upserts the SAME root
    :Session node (the cross-writer contention source) plus ``EVENTS_PER_WRITER``
    child :Event nodes connected by ``HAS_EVENT`` edges, then all stores flush
    concurrently. The test asserts conservation: every accepted :Event node is
    persisted (zero drops).

    Why independent stores: ``_flush_lock`` only serializes flushes *within* one
    store instance, so distinct stores reproduce real cross-writer contention on
    the shared root node.

    Against the current raw-``begin_transaction()``/``commit()`` write path (no
    managed-transaction retry), this is the documented RED reproduction: a flush
    raises ``Neo.TransientError.Transaction.DeadlockDetected`` (captured in
    ``results``) and/or the conservation assertion finds fewer :Event nodes than
    were accepted.
    """
    auth = (neo4j_container["user"], neo4j_container["password"])
    bolt = neo4j_container["bolt_url"]

    stores = [
        Neo4jGraphStore(uri=bolt, auth=auth, workspace="test") for _ in range(N_WRITERS)
    ]
    expected_ids: set[str] = set()

    try:
        for w, store in enumerate(stores):
            # Shared hot root node — the cross-writer contention source. Every
            # writer MERGEs the same (node_id, workspace) Session node.
            await store.upsert_node(
                ROOT_ID,
                {"labels": ["Session", "RootSession"], "last_updated": _ts(w)},
            )
            for e in range(EVENTS_PER_WRITER):
                ts = _ts(w * EVENTS_PER_WRITER + e + 1)
                node_id = make_node_id(f"sess-{w}", "user:prompt", ts)
                expected_ids.add(node_id)
                await store.upsert_node(
                    node_id,
                    {
                        "labels": ["Event"],
                        "session_id": f"sess-{w}",
                        "timestamp": ts,
                    },
                )
                await store.upsert_edge(ROOT_ID, node_id, {"type": "HAS_EVENT"})

        # Concurrent flush across independent stores — _flush_lock only serializes
        # within a single store, so this reproduces the real cross-writer contention.
        results = await asyncio.gather(
            *(store.flush() for store in stores), return_exceptions=True
        )
        failures = [r for r in results if isinstance(r, BaseException)]
        assert not failures, f"flush(es) raised under contention: {failures!r}"

        # Conservation: every accepted :Event node is present (count by exact ids so
        # the assertion is robust to any leftover data in the shared container).
        rows = await stores[0].execute_query(
            "MATCH (n:Event) WHERE n.node_id IN $ids AND n.workspace = $ws "
            "RETURN count(n) AS c",
            {"ids": list(expected_ids), "ws": "test"},
            workspace="*",
        )
        assert rows[0]["c"] == len(expected_ids), (
            f"event loss: expected {len(expected_ids)} :Event nodes, found {rows[0]['c']}"
        )
    finally:
        for store in stores:
            await store.close()
