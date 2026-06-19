"""Live E2E test: the universal :Node identity migration in ensure_neo4j_schema.

Ships with the B' silent-edge-drop fix.  On a graph dirtied by the #19 dead-backfill
bug (duplicate (node_id, workspace) nodes; legacy nodes with NO :Node label),
``ensure_neo4j_schema`` must, in order, leave the graph in the state the re-keyed
writers + the :Node uniqueness constraint require:

  1. dedup duplicate (node_id, workspace) nodes (global, keep the richest),
  2. backfill the :Node label onto every untagged node,
  3. create the :Node(node_id, workspace) uniqueness constraint.

See docs/node-identity-migration.md.

Run explicitly:
    cd amplifier-context-intelligence
    uv run pytest tests/neo4j/test_node_identity_migration.py -v -m neo4j
"""

from __future__ import annotations

from typing import Any

import pytest
from neo4j import GraphDatabase

from context_intelligence_server.neo4j_store import (
    Neo4jGraphStore,
    ensure_neo4j_schema,
)

pytestmark = pytest.mark.neo4j

_WS = "test"


def _wipe(container: dict[str, Any]) -> None:
    """Reset the shared, session-scoped neo4j_container to a fresh-DB state.

    This test makes GLOBAL graph assertions, drives a global schema migration, and
    must SEED duplicate (node_id, workspace) nodes — which is impossible while a
    prior test's :Session/:Event uniqueness constraint is still present. So we drop
    every node AND every constraint/index, both before (clean slate) and after (no
    pollution). Subsequent tests re-create schema idempotently on their next flush.
    """
    driver = GraphDatabase.driver(
        container["bolt_url"],
        auth=(container["user"], container["password"]),
    )
    try:
        with driver.session() as s:
            s.run("MATCH (n) DETACH DELETE n")
            # Drop constraints first (removes their backing indexes), then any
            # remaining standalone indexes. IF EXISTS keeps each drop idempotent.
            for rec in list(s.run("SHOW CONSTRAINTS YIELD name RETURN name")):
                s.run(f"DROP CONSTRAINT {rec['name']} IF EXISTS")
            for rec in list(s.run("SHOW INDEXES YIELD name RETURN name")):
                try:
                    s.run(f"DROP INDEX {rec['name']} IF EXISTS")
                except Exception:  # noqa: BLE001 - constraint-backed/lookup indexes
                    pass
    finally:
        driver.close()


def _seed_dirty_graph(container: dict[str, Any]) -> None:
    """Seed the exact dirty state #19's dead backfill leaves behind."""
    driver = GraphDatabase.driver(
        container["bolt_url"],
        auth=(container["user"], container["password"]),
    )
    try:
        with driver.session() as s:
            # Two duplicate :Event nodes, same (node_id, workspace), NO :Node label
            # (an indexed MERGE (n:Node {..}) duplicated a legacy untagged node).
            s.run("CREATE (:Event {node_id: 'dup-1', workspace: $ws, v: 1})", ws=_WS)
            s.run("CREATE (:Event {node_id: 'dup-1', workspace: $ws, v: 2})", ws=_WS)
            # A legacy :Session node written before the :Node label existed.
            s.run("CREATE (:Session {node_id: 'legacy-sess', workspace: $ws})", ws=_WS)
    finally:
        driver.close()


async def test_schema_migration_dedups_backfills_and_constrains(
    neo4j_container: dict[str, Any],
) -> None:
    # Hermetic: start from a clean graph (this test makes GLOBAL assertions and
    # drives a global migration) and DETACH DELETE everything afterwards so it
    # does not pollute the shared session-scoped container.
    _wipe(neo4j_container)
    try:
        await _run_migration_assertions(neo4j_container)
    finally:
        _wipe(neo4j_container)


async def _run_migration_assertions(neo4j_container: dict[str, Any]) -> None:
    _seed_dirty_graph(neo4j_container)

    # Run the migration via a store's async driver (the real production path).
    store = Neo4jGraphStore(
        uri=neo4j_container["bolt_url"],
        auth=(neo4j_container["user"], neo4j_container["password"]),
        workspace=_WS,
    )
    try:
        established = await ensure_neo4j_schema(store._driver, store._database)
    finally:
        await store.close()

    assert established, (
        "ensure_neo4j_schema did not fully establish — the :Node uniqueness "
        "constraint was not created (dirty graph not migrated)."
    )

    driver = GraphDatabase.driver(
        neo4j_container["bolt_url"],
        auth=(neo4j_container["user"], neo4j_container["password"]),
    )
    try:
        with driver.session() as s:
            dup_count = s.run(
                "MATCH (n {node_id: 'dup-1', workspace: $ws}) RETURN count(n) AS c",
                ws=_WS,
            ).single()["c"]
            untagged = s.run(
                "MATCH (n) WHERE NOT n:Node RETURN count(n) AS c"
            ).single()["c"]
            constraint_present = s.run(
                "SHOW CONSTRAINTS YIELD name "
                "WHERE name = 'node_node_id_workspace_unique' RETURN count(*) AS c"
            ).single()["c"]
            legacy_tagged = s.run(
                "MATCH (n:Node {node_id: 'legacy-sess', workspace: $ws}) "
                "RETURN count(n) AS c",
                ws=_WS,
            ).single()["c"]
    finally:
        driver.close()

    assert dup_count == 1, (
        f"global dedup failed: expected exactly one node for dup-1, found {dup_count}"
    )
    assert untagged == 0, (
        f"backfill incomplete: {untagged} node(s) still lack the :Node label"
    )
    assert legacy_tagged == 1, (
        "legacy :Session node was not adopted by the :Node backfill"
    )
    assert constraint_present == 1, (
        "the :Node(node_id, workspace) uniqueness constraint was not created"
    )
