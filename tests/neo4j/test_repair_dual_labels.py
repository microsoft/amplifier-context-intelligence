"""Tier 3 Neo4j integration test module — dual-label repair scaffolding.

Seeds nodes carrying BOTH the stray label (SubSession/ForkedSession) AND the
canonical label (SST_EVENT) to mirror the live corruption produced by the
session-labeling bug.  Every helper in this module targets the isolated test
container provided by the ``neo4j_container`` fixture and never touches a live
instance.

Requires: the ``neo4j_container`` session-scoped fixture (tests/neo4j/conftest.py).

This module contains ONLY seed/helper functions.  No test functions are
defined here.
"""

from __future__ import annotations

import pytest
from neo4j import GraphDatabase

WORKSPACE = "test"

pytestmark = pytest.mark.neo4j


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _driver(neo4j_container: dict) -> GraphDatabase:
    """Return a synchronous Neo4j driver for the test container."""
    return GraphDatabase.driver(
        neo4j_container["bolt_url"],
        auth=(neo4j_container["user"], neo4j_container["password"]),
    )


def _seed_dual_node(session, child_id: str, parent_id: str) -> None:
    """Seed a parent and child node that mirror the live dual-label corruption.

    Creates:
    - A parent ``:Session`` node (merged by node_id + workspace).
    - A child ``:Session`` node with the extra labels
      ``SubSession``, ``ForkedSession``, and ``SST_EVENT`` — exactly as the
      bug produces.
    - TWO edges from parent to child:
        * ``(p)-[:HAS_SUBSESSION {sst_semantic:'LEADS_TO'}]->(c)`` — the stray
          edge left behind by the bug.
        * ``(p)-[:FORKED {sst_semantic:'LEADS_TO'}]->(c)`` — the canonical
          edge that should be the only one.
    """
    session.run(
        """
        MERGE (p:Session {node_id: $parent_id, workspace: $workspace})
        MERGE (c:Session {node_id: $child_id,  workspace: $workspace})
        SET   c:SubSession, c:ForkedSession, c:SST_EVENT
        MERGE (p)-[:HAS_SUBSESSION {sst_semantic: 'LEADS_TO'}]->(c)
        MERGE (p)-[:FORKED       {sst_semantic: 'LEADS_TO'}]->(c)
        """,
        parent_id=parent_id,
        child_id=child_id,
        workspace=WORKSPACE,
    )


def _labels(session, node_id: str) -> list[str]:
    """Return the labels of a node matched by node_id + workspace, or []."""
    result = session.run(
        """
        MATCH (n {node_id: $node_id, workspace: $workspace})
        RETURN labels(n) AS lbls
        """,
        node_id=node_id,
        workspace=WORKSPACE,
    )
    record = result.single()
    if record is None:
        return []
    return list(record["lbls"])


def _edge_count(session, rel: str, child_id: str) -> int:
    """Return the count of (p)-[r:<rel>]->(c) edges pointing at child_id.

    Uses f-string interpolation for the relationship type because Cypher does
    not support parameterised relationship types.  ``rel`` must be a safe
    identifier (uppercase letters only) — never pass user-controlled input.
    """
    result = session.run(
        f"""
        MATCH (p)-[r:{rel}]->(c {{node_id: $child_id, workspace: $workspace}})
        RETURN count(r) AS cnt
        """,
        child_id=child_id,
        workspace=WORKSPACE,
    )
    record = result.single()
    if record is None:
        return 0
    return int(record["cnt"])
