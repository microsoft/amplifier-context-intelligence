"""Tier 3 — Neo4j-backed tests for concurrent flush durability.

These tests validate the schema and write-path changes that make concurrent
``MERGE`` operations atomic under multi-writer load. They require a live Neo4j
container (provided by the ``neo4j_container`` fixture) and are marked with
``@pytest.mark.neo4j``.

Run explicitly:
    uv run pytest tests/neo4j/test_concurrent_flush.py -v -m neo4j
"""

from __future__ import annotations

from typing import Any

import pytest
from neo4j import AsyncGraphDatabase

from context_intelligence_server.neo4j_store import ensure_neo4j_schema

pytestmark = pytest.mark.neo4j


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
