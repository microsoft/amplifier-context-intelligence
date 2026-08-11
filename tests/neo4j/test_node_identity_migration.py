"""Live E2E tests: the universal :Node identity migration and the PR #67 blocker fix.

Ships with the B' silent-edge-drop fix. On a graph dirtied by the #19 dead-backfill
bug (duplicate (node_id, workspace) nodes; legacy nodes with NO :Node label), the
graph must eventually reach the state the re-keyed writers + the :Node uniqueness
constraint require:

  1. dedup duplicate (node_id, workspace) nodes (global, keep the richest),
  2. backfill the :Node label onto every untagged node,
  3. create the :Node(node_id, workspace) uniqueness constraint.

IMPORTANT (post-PR-#67): this migration no longer runs inside ``ensure_neo4j_schema``
at cold start. It was extracted into ``run_repair`` -- the logic behind
``context-intelligence-server doctor --fix`` -- and now runs ONLY when explicitly
invoked via the doctor CLI, never automatically on every boot (that was pure
dead-weight O(graph-size) cost on an already-migrated graph). ``ensure_neo4j_schema``
now only creates cheap, idempotent indexes/constraints; if the graph still has
untagged/duplicate legacy data, constraint creation (Step 6) either raises
(``fail_on_data_conflict=True``, the ``doctor --fix``/``run_repair`` contract --
a lingering conflict AFTER dedup+backfill is a genuine repair failure) or logs a
WARNING and returns ``False`` (the default -- correct for BOTH cold start, which
must never fail due to graph state, and the mid-flight flush path; see
``Neo4jGraphStore._ensure_schema``, ``main.py``'s ``lifespan()``, and the PR #67
review discussion, reviewer Salil, commit 14a6d30).

See docs/node-identity-migration.md.

Run explicitly:
    cd amplifier-context-intelligence
    uv run pytest tests/neo4j/test_node_identity_migration.py -v -m neo4j
"""

from __future__ import annotations

import logging
from typing import Any

import pytest
from context_intelligence_server.neo4j_store import (
    Neo4jGraphStore,
    ensure_neo4j_schema,
    run_repair,
)
from neo4j import AsyncGraphDatabase, GraphDatabase

pytestmark = pytest.mark.neo4j

_WS = "test"


def _wipe(container: dict[str, Any]) -> None:
    """Reset the shared, session-scoped neo4j_container to a fresh-DB state.

    This test makes GLOBAL graph assertions, drives a global schema migration, and
    must SEED duplicate (node_id, workspace) nodes -- which is impossible while a
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


def _seed_node_constraint_conflict(container: dict[str, Any]) -> None:
    """Seed a genuine LIVE :Node uniqueness-constraint data conflict.

    Two nodes already carrying the universal ``:Node`` label -- plus a domain
    label (``OrchestratorRun``) that is NOT covered by the Session/Event
    uniqueness constraints (Steps 3 & 4), so this conflict is isolated to the
    ``:Node`` constraint step (Step 6) -- share the same (node_id, workspace)
    identity. This is the exact "duplicate legacy :Node" shape
    ``_is_constraint_data_conflict`` exists to detect.

    Unlike ``_seed_dirty_graph`` (untagged legacy nodes, used by the
    run_repair migration test below), these nodes ALREADY carry the :Node
    label. That distinction matters post-PR-#67: ``ensure_neo4j_schema`` no
    longer backfills the :Node label before attempting the constraint, so
    merely-untagged nodes do not by themselves violate a :Node-scoped
    uniqueness constraint (the constraint only governs nodes that already
    have the label). Attempting ``CREATE CONSTRAINT
    node_node_id_workspace_unique`` against this seed raises a genuine
    ``Neo.ClientError.Schema.ConstraintValidationFailed`` from a REAL Neo4j
    server (not a synthesized error) -- exactly the conflict the PR #67
    blocker fix (reviewer Salil) must survive without dead-lettering.
    """
    driver = GraphDatabase.driver(
        container["bolt_url"],
        auth=(container["user"], container["password"]),
    )
    try:
        with driver.session() as s:
            s.run(
                "CREATE (:Node:OrchestratorRun "
                "{node_id: 'dup-node', workspace: $ws, v: 1})",
                ws=_WS,
            )
            s.run(
                "CREATE (:Node:OrchestratorRun "
                "{node_id: 'dup-node', workspace: $ws, v: 2})",
                ws=_WS,
            )
    finally:
        driver.close()


def _read_graph_state(container: dict[str, Any]) -> dict[str, int]:
    """Read the four post-condition counts this migration must satisfy."""
    driver = GraphDatabase.driver(
        container["bolt_url"],
        auth=(container["user"], container["password"]),
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
    return {
        "dup_count": dup_count,
        "untagged": untagged,
        "constraint_present": constraint_present,
        "legacy_tagged": legacy_tagged,
    }


async def test_run_repair_dedups_backfills_and_constrains(
    neo4j_container: dict[str, Any],
) -> None:
    """The migration (dedup + backfill + constrain) now lives in run_repair --
    the ``doctor --fix`` entrypoint -- not in ``ensure_neo4j_schema``. This test
    drives the CURRENT migration entrypoint and keeps the exact post-conditions
    the pre-PR-#67 version of this test asserted.
    """
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

    # Run the migration via a store's async driver (the real production path) --
    # run_repair is what `context-intelligence-server doctor --fix` invokes.
    store = Neo4jGraphStore(
        uri=neo4j_container["bolt_url"],
        auth=(neo4j_container["user"], neo4j_container["password"]),
        workspace=_WS,
    )
    try:
        result = await run_repair(store._driver, store._database)
    finally:
        await store.close()

    assert result["duplicates_removed"] >= 1, (
        "run_repair did not report removing the seeded duplicate 'dup-1' node."
    )
    assert result["nodes_tagged"] >= 1, (
        "run_repair did not report tagging any legacy untagged nodes."
    )

    state = _read_graph_state(neo4j_container)

    assert state["dup_count"] == 1, (
        f"global dedup failed: expected exactly one node for dup-1, "
        f"found {state['dup_count']}"
    )
    assert state["untagged"] == 0, (
        f"backfill incomplete: {state['untagged']} node(s) still lack the :Node label"
    )
    assert state["legacy_tagged"] == 1, (
        "legacy :Session node was not adopted by the :Node backfill"
    )
    assert state["constraint_present"] == 1, (
        "the :Node(node_id, workspace) uniqueness constraint was not created "
        "by run_repair"
    )


async def test_flush_path_self_heals_and_repair_converges_e2e(
    neo4j_container: dict[str, Any],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Live E2E regression for the PR #67 merge-blocker (reviewer Salil, commit
    14a6d30): a genuine :Node constraint data conflict against a REAL Neo4j must
    NOT dead-letter in-flight activity records on the flush path, must self-heal
    once ``doctor --fix`` (``run_repair``) repairs the graph, and the
    ``fail_on_data_conflict=True`` (doctor/repair) contract must still fail
    closed against a genuinely unrepairable graph.

    Phases (against the exact dirty state #19's dead backfill leaves behind):

    A. ``ensure_neo4j_schema`` with the DEFAULT (``fail_on_data_conflict=False``)
       on the dirty graph does NOT raise, returns ``False``, and logs a WARNING
       naming ``doctor --fix``. This is the contract BOTH the cold-start lifespan
       handler (``main.py``, which never opts into ``fail_on_data_conflict=True``
       -- startup must never fail due to graph state) and the mid-flight flush
       path rely on.
    B. A real ``store.flush()`` against the same dirty graph does NOT raise and
       does NOT dead-letter -- the buffered node is written and preserved, not
       lost, and the schema flag stays ``False`` so the next flush retries.
    C. ``run_repair`` (the ``doctor --fix`` path) converges the graph: dedup,
       backfill, and the :Node constraint all succeed.
    D. ``ensure_neo4j_schema`` on the now-clean graph returns ``True``.
    E. The doctor/repair contract still fails closed: seeding a FRESH dirty
       graph and calling ``ensure_neo4j_schema(..., fail_on_data_conflict=True)``
       -- exactly what ``run_repair`` does post-dedup/backfill -- raises the
       ``RuntimeError`` naming ``doctor --fix``, byte-for-byte the pre-existing
       contract. (NOT the lifespan/cold-start path -- that now always uses the
       default ``False``, per Phase A above.)

    Mirrors the existing test's hermetic pattern (_wipe before/after, seed dirty
    graph fresh for each phase that needs one).
    """
    _wipe(neo4j_container)
    try:
        await _phase_a_through_d_self_heal(neo4j_container, caplog)
        await _phase_e_doctor_contract_fails_closed(neo4j_container)
    finally:
        _wipe(neo4j_container)


async def _phase_a_through_d_self_heal(
    neo4j_container: dict[str, Any],
    caplog: pytest.LogCaptureFixture,
) -> None:
    _seed_node_constraint_conflict(neo4j_container)

    store = Neo4jGraphStore(
        uri=neo4j_container["bolt_url"],
        auth=(neo4j_container["user"], neo4j_container["password"]),
        workspace=_WS,
    )
    try:
        # --- Phase A: default ensure_neo4j_schema on the dirty graph ---------
        with caplog.at_level(logging.WARNING):
            established = await ensure_neo4j_schema(store._driver, store._database)

        assert established is False, (
            "ensure_neo4j_schema must report failure (not raise) when the "
            ":Node constraint hits a genuine data conflict at its default "
            "fail_on_data_conflict=False."
        )
        assert any(
            record.levelno == logging.WARNING and "doctor --fix" in record.getMessage()
            for record in caplog.records
        ), (
            "Expected a WARNING naming `doctor --fix` from the live Neo4j "
            f"constraint-creation failure. Records: "
            f"{[(r.levelname, r.getMessage()) for r in caplog.records]}"
        )
        caplog.clear()

        # --- Phase B: a real flush() against the still-dirty graph ----------
        await store.upsert_node("evt-e2e", {"labels": ["Event"], "v": 1})

        with caplog.at_level(logging.WARNING):
            await store.flush()  # MUST NOT raise -- MUST NOT dead-letter

        assert store._node_buffer == {}, (
            "Buffer must be cleared (flush succeeded) -- NOT restored-on-failure. "
            "A non-empty buffer here would mean flush() re-raised and the "
            "caller (registry._handle_exhausted_batch) would dead-letter this "
            "real activity record."
        )
        assert store._schema_initialized is False, (
            "Schema flag must stay False after a genuine data conflict so the "
            "NEXT flush retries schema init once the graph has been repaired "
            "-- this is the self-healing contract the fix restores."
        )

        # Data preserved (not dead-lettered): the node actually landed in Neo4j.
        written = await store.execute_query(
            "MATCH (n {node_id: 'evt-e2e', workspace: $workspace}) "
            "RETURN count(n) AS c",
            workspace=_WS,
        )
        assert written[0]["c"] == 1, (
            "The flushed node must be present in Neo4j -- data must not be "
            "lost/dead-lettered when the schema constraint hits a data conflict."
        )

        # --- Phase C: run_repair (doctor --fix) converges the graph ----------
        repair_result = await run_repair(store._driver, store._database)
        assert repair_result["duplicates_removed"] >= 1, (
            "run_repair did not report removing the seeded 'dup-node' duplicate."
        )
        # nodes_tagged may legitimately be 0 here: the seeded duplicate already
        # carried the :Node label (this scenario tests the constraint conflict
        # specifically, not the untagged-backfill case covered by the other test).

        dup_remaining = await store.execute_query(
            "MATCH (n {node_id: 'dup-node', workspace: $workspace}) "
            "RETURN count(n) AS c",
            workspace=_WS,
        )
        assert dup_remaining[0]["c"] == 1, (
            f"dedup did not converge to a single dup-node, found "
            f"{dup_remaining[0]['c']}"
        )

        constraint_rows = await store.execute_query(
            "SHOW CONSTRAINTS YIELD name "
            "WHERE name = 'node_node_id_workspace_unique' RETURN count(*) AS c",
            workspace="*",
        )
        assert constraint_rows[0]["c"] == 1, (
            "the :Node uniqueness constraint was not established after repair"
        )

        # --- Phase D: ensure_neo4j_schema now succeeds on the clean graph -----
        established_after_repair = await ensure_neo4j_schema(
            store._driver, store._database
        )
        assert established_after_repair is True, (
            "ensure_neo4j_schema must fully establish the schema once the "
            "graph has been repaired by run_repair."
        )
    finally:
        await store.close()


async def _phase_e_doctor_contract_fails_closed(
    neo4j_container: dict[str, Any],
) -> None:
    """Re-dirty a clean slate and confirm the doctor/repair contract
    (``fail_on_data_conflict=True``) still fails closed on an unrepairable
    graph. NOT the lifespan/cold-start path -- that always uses the default
    ``False`` (never raises due to graph state; see Phase A)."""
    _wipe(neo4j_container)
    _seed_node_constraint_conflict(neo4j_container)

    driver = AsyncGraphDatabase.driver(
        neo4j_container["bolt_url"],
        auth=(neo4j_container["user"], neo4j_container["password"]),
    )
    try:
        with pytest.raises(RuntimeError, match="doctor --fix"):
            await ensure_neo4j_schema(driver, fail_on_data_conflict=True)
    finally:
        await driver.close()
