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

import json
from typing import Any

import pytest
from neo4j import GraphDatabase
from neo4j.time import DateTime as Neo4jDateTime

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


from scripts import repair_dual_labels as repair  # noqa: E402


# ---------------------------------------------------------------------------
# Per-test isolation
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clear_neo4j(neo4j_container: dict) -> None:  # type: ignore[return]
    """Delete all nodes and edges before each test for complete isolation.

    The ``neo4j_container`` fixture is session-scoped (one container per test
    run), so data seeded in one test accumulates and can break later tests.
    This autouse fixture runs BEFORE every test in this module and wipes the
    container clean so each test starts from an empty database.
    """
    driver = GraphDatabase.driver(
        neo4j_container["bolt_url"],
        auth=(neo4j_container["user"], neo4j_container["password"]),
    )
    try:
        with driver.session() as s:
            s.run("MATCH (n) DETACH DELETE n")
    finally:
        driver.close()


@pytest.mark.neo4j
class TestDryRun:
    """Dry-run reports counts and session_ids and mutates nothing."""

    def test_dry_run_reports_and_does_not_mutate(
        self, neo4j_container: dict[str, Any]
    ) -> None:
        driver = _driver(neo4j_container)
        try:
            with driver.session() as s:
                _seed_dual_node(s, "child-a", "parent-a")
                _seed_dual_node(s, "child-b", "parent-b")
                report = repair.count_dual(s, WORKSPACE)
                assert report["node_count"] == 2
                assert report["edge_count"] == 2
                assert sorted(report["session_ids"]) == ["child-a", "child-b"]
            # assert nothing changed
            with driver.session() as s:
                assert "SubSession" in _labels(s, "child-a")
                assert "ForkedSession" in _labels(s, "child-a")
                assert _edge_count(s, "HAS_SUBSESSION", "child-a") == 1
        finally:
            driver.close()


@pytest.mark.neo4j
class TestApplyPostState:
    """apply_repair heals a dual node to the canonical single label and single edge."""

    def test_apply_strips_subsession_and_deletes_has_subsession(
        self, neo4j_container: dict[str, Any]
    ) -> None:
        driver = _driver(neo4j_container)
        try:
            with driver.session() as s:
                _seed_dual_node(s, "child-a", "parent-a")
                repair.snapshot(s, WORKSPACE)
                healed = repair.apply_repair(s, WORKSPACE)
                assert healed == 1
                labels = _labels(s, "child-a")
                assert "ForkedSession" in labels
                assert "SubSession" not in labels
                assert _edge_count(s, "HAS_SUBSESSION", "child-a") == 0
                assert _edge_count(s, "FORKED", "child-a") == 1
                assert repair.count_dual(s, WORKSPACE)["node_count"] == 0
        finally:
            driver.close()


@pytest.mark.neo4j
class TestSnapshotRestore:
    """restore_snapshot recovers BOTH the SubSession label AND the HAS_SUBSESSION edge."""

    def test_restore_recovers_relationships(
        self, neo4j_container: dict[str, Any]
    ) -> None:
        driver = _driver(neo4j_container)
        try:
            with driver.session() as s:
                _seed_dual_node(s, "child-a", "parent-a")
                snap = repair.snapshot(s, WORKSPACE)
                repair.apply_repair(s, WORKSPACE)
                # Post-heal: stray label and edge must be gone
                assert _edge_count(s, "HAS_SUBSESSION", "child-a") == 0
                assert "SubSession" not in _labels(s, "child-a")
                # Restore from snapshot
                repair.restore_snapshot(s, WORKSPACE, snap)
                # Both the label and the non-recomputable relationship must be back
                assert "SubSession" in _labels(s, "child-a")
                assert _edge_count(s, "HAS_SUBSESSION", "child-a") == 1
        finally:
            driver.close()


@pytest.mark.neo4j
class TestIdempotencyAndDryRunNoop:
    """apply_repair is idempotent; dry-run after a successful apply reports zero."""

    def test_apply_twice_second_is_noop(self, neo4j_container: dict[str, Any]) -> None:
        driver = _driver(neo4j_container)
        try:
            with driver.session() as s:
                _seed_dual_node(s, "child-a", "parent-a")
                first = repair.apply_repair(s, WORKSPACE)
                second = repair.apply_repair(s, WORKSPACE)
                assert first == 1
                assert second == 0
                assert repair.count_dual(s, WORKSPACE)["node_count"] == 0
        finally:
            driver.close()

    def test_dry_run_after_apply_reports_zero(
        self, neo4j_container: dict[str, Any]
    ) -> None:
        driver = _driver(neo4j_container)
        try:
            with driver.session() as s:
                _seed_dual_node(s, "child-a", "parent-a")
                repair.apply_repair(s, WORKSPACE)
                assert repair.run_dry_run(s, WORKSPACE) == 0
        finally:
            driver.close()


@pytest.mark.neo4j
class TestCompareAndSetGuard:
    """apply_repair skips nodes whose label set changed after the snapshot was taken.

    Deterministically exercises the read-modify-write race: between snapshot()
    and apply_repair() a concurrent writer changes a node's label set so it no
    longer satisfies the 'both labels' predicate.  The WHERE guard
    ``n:SubSession AND n:ForkedSession`` inside apply_repair must skip the node
    so the repair never clobbers a live write.
    """

    def test_guard_skips_node_whose_labels_changed_after_snapshot(
        self, neo4j_container: dict[str, Any]
    ) -> None:
        driver = _driver(neo4j_container)
        try:
            with driver.session() as s:
                # Seed a dual-labelled node exactly as the live bug produces.
                _seed_dual_node(s, "child-a", "parent-a")

                # Take a snapshot while the node still carries both labels.
                snap = repair.snapshot(s, WORKSPACE)
                assert "child-a" in snap["nodes"]

                # Simulate a concurrent writer that changes the label set
                # AFTER the snapshot but BEFORE apply_repair runs.
                # Removing :ForkedSession means child-a no longer satisfies the
                # 'both labels' predicate that apply_repair uses as its guard.
                s.run(
                    "MATCH (n {node_id: 'child-a', workspace: $ws})"
                    " REMOVE n:ForkedSession",
                    {"ws": WORKSPACE},
                )

                # apply_repair must skip child-a because the label set changed.
                healed = repair.apply_repair(s, WORKSPACE)

                # Guard skipped the node: nothing was healed.
                assert healed == 0

                # :SubSession was NOT stripped — the concurrent writer's state
                # is intact.
                assert "SubSession" in _labels(s, "child-a")

                # HAS_SUBSESSION edge was NOT deleted out from under the writer.
                assert _edge_count(s, "HAS_SUBSESSION", "child-a") == 1
        finally:
            driver.close()


@pytest.mark.neo4j
class TestSnapshotTemporalSerialization:
    """snapshot() must JSON-serialize, and restore must rebuild, temporal edge props.

    Mirrors the live data shape: a HAS_SUBSESSION edge carries ``occurred_at``
    as a Neo4j ZONED DATETIME.  The earlier seed helper set no temporal prop, so
    json.dump never saw a Neo4j temporal and the bug stayed hidden.  This test
    seeds the real shape and exercises the full
    snapshot -> json.dump -> json.load -> restore round-trip.
    """

    def test_snapshot_json_round_trip_restores_datetime_edge(
        self, neo4j_container: dict[str, Any], tmp_path: Any
    ) -> None:
        driver = _driver(neo4j_container)
        try:
            with driver.session() as s:
                _seed_dual_node(s, "child-a", "parent-a")
                # Mirror production: HAS_SUBSESSION carries occurred_at as a
                # ZONED DATETIME, not a string.
                s.run(
                    "MATCH (p)-[r:HAS_SUBSESSION]->"
                    "(c {node_id: 'child-a', workspace: $ws})"
                    " SET r.occurred_at = datetime('2026-06-13T10:00:00Z')",
                    {"ws": WORKSPACE},
                )

                snap = repair.snapshot(s, WORKSPACE)

                # The actual bug: json.dump must not choke on a Neo4j temporal.
                snap_path = tmp_path / "snap.json"
                with open(snap_path, "w") as fh:
                    json.dump(snap, fh)
                loaded = json.loads(snap_path.read_text())

                repair.apply_repair(s, WORKSPACE)
                assert _edge_count(s, "HAS_SUBSESSION", "child-a") == 0

                repair.restore_snapshot(s, WORKSPACE, loaded)
                assert _edge_count(s, "HAS_SUBSESSION", "child-a") == 1

                # occurred_at must come back as a real ZONED DATETIME, not a
                # plain string (AGENTS.md temporal convention).
                rec = s.run(
                    "MATCH (p)-[r:HAS_SUBSESSION]->"
                    "(c {node_id: 'child-a', workspace: $ws})"
                    " RETURN r.occurred_at AS oa",
                    {"ws": WORKSPACE},
                ).single()
                assert isinstance(rec["oa"], Neo4jDateTime)
                assert (rec["oa"].year, rec["oa"].month, rec["oa"].day) == (
                    2026,
                    6,
                    13,
                )
        finally:
            driver.close()
