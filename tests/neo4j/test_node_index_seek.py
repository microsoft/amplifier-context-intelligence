"""Live E2E tests: the non-Session node MERGE is index-backed, not a full scan.

Root cause being guarded here (confirmed on the live 1.3M-node graph via
``SHOW TRANSACTIONS`` + ``SHOW INDEXES``):

  ``_write_batch`` wrote non-Session nodes with a **label-free** MERGE:

      UNWIND $rows AS row
      MERGE (n {node_id: row.node_id, workspace: row.props.workspace})
      SET n += row.props

  A label-free MERGE cannot use any index (Neo4j property indexes are
  label-scoped), so every row drives an ``AllNodesScan`` over the whole graph.
  At 1.3M nodes each non-Session flush transaction ran 25-30s and was killed by
  the 30s Layer-B ``unit_of_work`` timeout, collapsing drain throughput.

The fix (Option 2 — universal label): every node carries a ``:Node`` label, a
composite ``:Node(node_id, workspace)`` index exists, and the non-Session MERGE
targets ``(n:Node {node_id, workspace})`` so the planner emits a
``NodeIndexSeek`` instead of an ``AllNodesScan``.  Existing nodes are tagged by
a batched, idempotent backfill in ``ensure_neo4j_schema`` so the indexed MERGE
finds them rather than creating duplicates.

Requires Docker and the docker Python package.  Skip-if-absent via the
``neo4j_container`` fixture in tests/neo4j/conftest.py.

Run explicitly:
    cd amplifier-context-intelligence
    uv run pytest tests/neo4j/test_node_index_seek.py -v -m neo4j
"""

from __future__ import annotations

from typing import Any, LiteralString, cast

import pytest
from neo4j import GraphDatabase

from context_intelligence_server.neo4j_store import Neo4jGraphStore

pytestmark = pytest.mark.neo4j


# The EXACT non-Session node MERGE the production code issues.  Imported from
# the module so this test exercises the real query, not a copy.  The fallback
# is byte-identical to the *unfixed* source (neo4j_store.py before the fix) so
# that, when the fix is stashed, the test still reconstructs the genuine
# pre-fix query and fails for the right reason (AllNodesScan) rather than at
# import time.
try:  # pragma: no cover - import resolution differs pre/post fix
    from context_intelligence_server.neo4j_store import _NODE_MERGE_CYPHER
except ImportError:  # pragma: no cover - exercised only against unfixed code
    _NODE_MERGE_CYPHER = (
        "UNWIND $rows AS row "
        "MERGE (n {node_id: row.node_id, workspace: row.props.workspace}) "
        "SET n += row.props"
    )

# The EXACT edge-MERGE builder and single-node MATCH prefix the production code
# issues, imported so these tests assert the real query plan (not a copy).  The
# fallbacks are byte-identical to the *unfixed* (label-free) source so that, if
# the fix is reverted, the tests still reconstruct the genuine pre-fix queries
# and fail for the right reason (AllNodesScan) rather than at import time.
try:  # pragma: no cover - import resolution differs pre/post fix
    from context_intelligence_server.neo4j_store import (
        _NODE_MATCH_BY_ID,
        _edge_merge_cypher,
    )
except ImportError:  # pragma: no cover - exercised only against unfixed code
    _NODE_MATCH_BY_ID = "MATCH (n {node_id: $node_id, workspace: $workspace})"

    def _edge_merge_cypher(edge_type: str) -> str:
        return (
            "UNWIND $rows AS row "
            "MATCH (src {node_id: row.src_id, workspace: $workspace}) "
            "MATCH (dst {node_id: row.dst_id, workspace: $workspace}) "
            f"MERGE (src)-[r:{edge_type}]->(dst) "
            "SET r += row.props"
        )


def _collect_operators(plan: dict[str, Any]) -> list[str]:
    """Recursively collect every operatorType in a Neo4j EXPLAIN/PROFILE plan."""
    ops: list[str] = []
    if not plan:
        return ops
    op = plan.get("operatorType") or plan.get("operator_type")
    if op:
        ops.append(op)
    for child in plan.get("children", []) or []:
        ops.extend(_collect_operators(child))
    return ops


async def _flush_one_non_session_node(
    container: dict[str, Any], node_id: str, props: dict[str, Any]
) -> None:
    """Drive a single non-Session node through the real flush path.

    Exercises ``ensure_neo4j_schema`` (which, in the fixed code, creates the
    ``:Node`` index and runs the backfill) followed by the production
    ``_write_batch`` non-Session MERGE.
    """
    store = Neo4jGraphStore(
        uri=container["bolt_url"],
        auth=(container["user"], container["password"]),
        workspace="test",
    )
    try:
        await store.upsert_node(node_id, {"labels": ["Event"], **props})
        await store.flush()
    finally:
        await store.close()


async def test_non_session_node_merge_uses_index_seek_not_allnodesscan(
    neo4j_container: dict[str, Any],
) -> None:
    """The non-Session node MERGE must plan as an index seek, never AllNodesScan.

    RED  (unfixed): query is label-free, no usable index -> plan contains
                    AllNodesScan -> this assertion fails.
    GREEN (fixed):  query is ``MERGE (n:Node {...})`` against the composite
                    ``:Node(node_id, workspace)`` index -> NodeIndexSeek.
    """
    # Run a real flush so ensure_neo4j_schema executes (creates the :Node index
    # + backfill in the fixed code) and a node exists for the planner.
    await _flush_one_non_session_node(neo4j_container, "evt-plan-1", {"v": 1})

    driver = GraphDatabase.driver(
        neo4j_container["bolt_url"],
        auth=(neo4j_container["user"], neo4j_container["password"]),
    )
    try:
        with driver.session() as session:
            result = session.run(
                "EXPLAIN " + _NODE_MERGE_CYPHER,
                rows=[{"node_id": "evt-plan-1", "props": {"workspace": "test"}}],
            )
            summary = result.consume()
            plan = summary.plan or {}
        ops = _collect_operators(plan)
    finally:
        driver.close()

    assert ops, f"EXPLAIN returned no plan operators (plan={plan!r})"
    # Operator types carry a planner-version suffix (e.g. "AllNodesScan@neo4j"),
    # so match on substring, not exact equality.
    assert not any("AllNodesScan" in op for op in ops), (
        "Non-Session node MERGE still does a full-graph AllNodesScan "
        f"(the 1.3M-node stall). Plan operators: {ops}"
    )
    assert any("IndexSeek" in op for op in ops), (
        "Non-Session node MERGE is not index-backed — expected a NodeIndexSeek / "
        f"NodeUniqueIndexSeek in the plan. Plan operators: {ops}"
    )


async def test_non_session_node_merge_idempotent_no_duplicates(
    neo4j_container: dict[str, Any],
) -> None:
    """Same node_id written twice (across a label change) yields exactly one node.

    Guards the Option-2 invariant: switching the MERGE to the universal ``:Node``
    label must NOT split a node's identity.  Mirrors the real "bare node written
    with one type label, then a different type label on a later flush" scenario.
    """
    # Flush 1: node carries Event label.
    await _flush_one_non_session_node(neo4j_container, "dup-1", {"v": 1})

    # Flush 2: SAME node_id, a *different* type label and updated prop.
    store = Neo4jGraphStore(
        uri=neo4j_container["bolt_url"],
        auth=(neo4j_container["user"], neo4j_container["password"]),
        workspace="test",
    )
    try:
        await store.upsert_node("dup-1", {"labels": ["ToolExecution"], "v": 2})
        await store.flush()
    finally:
        await store.close()

    driver = GraphDatabase.driver(
        neo4j_container["bolt_url"],
        auth=(neo4j_container["user"], neo4j_container["password"]),
    )
    try:
        with driver.session() as session:
            rec = session.run(
                "MATCH (n {node_id: $nid, workspace: $ws}) "
                "RETURN count(n) AS c, collect(n.v) AS vs",
                nid="dup-1",
                ws="test",
            ).single()
        assert rec is not None
        count = rec["c"]
        vs = rec["vs"]
    finally:
        driver.close()

    assert count == 1, (
        f"Expected exactly one node for dup-1, found {count} (duplicate!)"
    )
    assert vs == [2], (
        f"Expected the single node to carry the updated prop v=2, got {vs}"
    )


async def test_preexisting_unlabeled_node_not_duplicated_after_backfill(
    neo4j_container: dict[str, Any],
) -> None:
    """A legacy node (no :Node label) must be adopted by the backfill, not duplicated.

    Simulates the live graph: nodes written before the :Node label existed.  The
    indexed MERGE on (n:Node {...}) would CREATE a duplicate of such a node
    unless the ensure_neo4j_schema backfill tags it with :Node first.  This test
    fails (count == 2) if the fix ships the index without the backfill.
    """
    # Seed a legacy node directly: has its type label but NOT :Node.
    seed_driver = GraphDatabase.driver(
        neo4j_container["bolt_url"],
        auth=(neo4j_container["user"], neo4j_container["password"]),
    )
    try:
        with seed_driver.session() as session:
            session.run(
                "CREATE (n:Event {node_id: $nid, workspace: $ws, v: 1})",
                nid="legacy-1",
                ws="test",
            )
    finally:
        seed_driver.close()

    # Write the SAME node_id through the fixed store (ensure_neo4j_schema runs the
    # backfill, then the indexed :Node MERGE must adopt the legacy node).
    store = Neo4jGraphStore(
        uri=neo4j_container["bolt_url"],
        auth=(neo4j_container["user"], neo4j_container["password"]),
        workspace="test",
    )
    try:
        await store.upsert_node("legacy-1", {"labels": ["Event"], "v": 2})
        await store.flush()
    finally:
        await store.close()

    driver = GraphDatabase.driver(
        neo4j_container["bolt_url"],
        auth=(neo4j_container["user"], neo4j_container["password"]),
    )
    try:
        with driver.session() as session:
            rec = session.run(
                "MATCH (n {node_id: $nid, workspace: $ws}) RETURN count(n) AS c",
                nid="legacy-1",
                ws="test",
            ).single()
        assert rec is not None
        count = rec["c"]
    finally:
        driver.close()

    assert count == 1, (
        f"Legacy (pre-:Node) node was duplicated by the indexed MERGE: found "
        f"{count} nodes for legacy-1 — the backfill did not adopt it."
    )


def _explain_ops(container: dict[str, Any], query: str, **params: Any) -> list[str]:
    """EXPLAIN *query* against the container and return its plan operators."""
    driver = GraphDatabase.driver(
        container["bolt_url"],
        auth=(container["user"], container["password"]),
    )
    try:
        with driver.session() as session:
            summary = session.run(
                cast(LiteralString, "EXPLAIN " + query), **params
            ).consume()
            plan = summary.plan or {}
        return _collect_operators(plan)
    finally:
        driver.close()


async def test_edge_merge_match_uses_index_seek_not_allnodesscan(
    neo4j_container: dict[str, Any],
) -> None:
    """The edge MERGE's src/dst MATCH must plan as index seeks, never AllNodesScan.

    RED  (unfixed): MATCH (src {...}) / MATCH (dst {...}) are label-free -> the
                    plan contains AllNodesScan (the multi-second "Running" edge
                    transactions on the 1.3M-node graph) -> this assertion fails.
    GREEN (fixed):  MATCH (src:Node {...}) / (dst:Node {...}) -> NodeIndexSeek
                    against idx_node_universal.
    """
    # Real flush so ensure_neo4j_schema runs (creates idx_node_universal).
    await _flush_one_non_session_node(neo4j_container, "edge-plan-1", {"v": 1})

    ops = _explain_ops(
        neo4j_container,
        _edge_merge_cypher("HAS_EVENT"),
        rows=[],
        workspace="test",
    )

    assert ops, "EXPLAIN returned no plan operators for the edge MERGE query"
    assert not any("AllNodesScan" in op for op in ops), (
        "Edge MERGE src/dst MATCH still does a full-graph AllNodesScan "
        f"(the 1.3M-node edge stall). Plan operators: {ops}"
    )
    assert any("IndexSeek" in op for op in ops), (
        "Edge MERGE src/dst MATCH is not index-backed — expected a NodeIndexSeek "
        f"against idx_node_universal. Plan operators: {ops}"
    )


async def test_label_patch_match_uses_index_seek_not_allnodesscan(
    neo4j_container: dict[str, Any],
) -> None:
    """The label-write MATCH (label SET / patch add/remove) must seek, not scan.

    Exercises the shared single-node MATCH prefix (_NODE_MATCH_BY_ID) used by
    the per-node label SET and by the label-patch add/remove queries in
    _write_batch.

    RED  (unfixed): MATCH (n {node_id, workspace}) is label-free -> AllNodesScan
                    -> this assertion fails.
    GREEN (fixed):  MATCH (n:Node {node_id, workspace}) -> NodeIndexSeek.
    """
    await _flush_one_non_session_node(neo4j_container, "patch-plan-1", {"v": 1})

    # Append a representative SET so the prefix forms a complete statement —
    # identical shape to the production label-write queries.
    ops = _explain_ops(
        neo4j_container,
        f"{_NODE_MATCH_BY_ID} SET n:RootSession",
        node_id="patch-plan-1",
        workspace="test",
    )

    assert ops, "EXPLAIN returned no plan operators for the label-patch query"
    assert not any("AllNodesScan" in op for op in ops), (
        "Label-write MATCH still does a full-graph AllNodesScan "
        f"(the 1.3M-node stall). Plan operators: {ops}"
    )
    assert any("IndexSeek" in op for op in ops), (
        "Label-write MATCH is not index-backed — expected a NodeIndexSeek "
        f"against idx_node_universal. Plan operators: {ops}"
    )
