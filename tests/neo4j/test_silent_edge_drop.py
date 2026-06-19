"""Live E2E test: a cross-session edge whose endpoint is absent must NEVER be
dropped silently (Issue 1 — the HAS_SUBSESSION parent-absent race).

Root cause being guarded here (Cypher semantics, confirmed against current main
post-#19):

    ``_write_batch`` writes every cross-session edge via ``_edge_merge_cypher``:

        UNWIND $rows AS row
        MATCH (src:Node {node_id: row.src_id, workspace: $workspace})
        MATCH (dst:Node {node_id: row.dst_id, workspace: $workspace})
        MERGE (src)-[r:TYPE]->(dst)
        SET r += row.props

    The two ``MATCH`` clauses are an inner join: if EITHER endpoint is not yet
    committed, that row produces zero bindings, the ``MERGE`` never runs for it,
    and the edge is **silently dropped** — no exception, no log, no warning,
    ``res.consume()`` reports success.

This is exactly the W1 race: ``handlers/data_layer_2/session.py:196`` calls
``ensure_session_node(parent_id)`` immediately before buffering the
``HAS_SUBSESSION`` edge ``upsert_edge(parent_id, session_id, ...)`` precisely to
guarantee the parent (``src``) endpoint exists.  When the parent ensure lives in
a *different* drainer/flush than the child's edge (the cross-session case), the
parent node can be absent at the child's edge-flush time and the
parent->child ``HAS_SUBSESSION`` edge is lost forever.

This test forces the parent-absent race against a real Neo4j and asserts the
**never-silent invariant**: writing an edge whose ``src`` endpoint is absent must
not BOTH (a) raise nothing AND (b) leave no edge.  Current behaviour violates it
(no error + no edge).  The fail-loud / merge-endpoint fix makes it pass.

  RED  (current main):  flush succeeds, no edge created -> invariant violated.
  GREEN (after fix):    flush raises loudly OR the edge is present (endpoint
                        merged) -> invariant holds.

Requires Docker and the docker Python package.  Skip-if-absent via the
``neo4j_container`` fixture in tests/neo4j/conftest.py.

Run explicitly:
    cd amplifier-context-intelligence
    uv run pytest tests/neo4j/test_silent_edge_drop.py -v -m neo4j
"""

from __future__ import annotations

from typing import Any

import pytest
from neo4j import GraphDatabase

from context_intelligence_server.neo4j_store import Neo4jGraphStore

pytestmark = pytest.mark.neo4j

# These tests PROVE the silent cross-session edge drop is real on current main
# (#19). They are strict-xfail because the fix is NOT a simple fail-loud raise:
# a blanket raise on a missing endpoint breaks the normal ingest pipeline — the
# generic edge writer legitimately writes edges whose endpoints are not yet
# committed (SOURCED_FROM cross-layer bridges appear 27x; HAS_PART, TRIGGERED,
# HAS_STEP, etc.). Measured on real Neo4j: a fail-loud raise regressed 18
# integration tests (apples-to-apples vs #19 baseline: 1 fail -> 19 fail).
# The correct never-silent fix (self-healing MERGE-endpoints with unified :Node
# identity, OR surface-don't-abort) is pending design approval — see
# docs/issue1-edge-fix-design.md. When it lands, drop the xfail markers.

_WS = "test"
_PARENT_ABSENT = "root-parent-absent"
_CHILD = "child-subsession"


def _count_node(container: dict[str, Any], node_id: str) -> int:
    driver = GraphDatabase.driver(
        container["bolt_url"],
        auth=(container["user"], container["password"]),
    )
    try:
        with driver.session() as session:
            rec = session.run(
                "MATCH (n:Node {node_id: $nid, workspace: $ws}) RETURN count(n) AS c",
                nid=node_id,
                ws=_WS,
            ).single()
        return int(rec["c"]) if rec is not None else 0
    finally:
        driver.close()


def _count_has_subsession_into(container: dict[str, Any], child_id: str) -> int:
    """Count HAS_SUBSESSION edges pointing at *child_id* (any source)."""
    driver = GraphDatabase.driver(
        container["bolt_url"],
        auth=(container["user"], container["password"]),
    )
    try:
        with driver.session() as session:
            rec = session.run(
                "MATCH ()-[r:HAS_SUBSESSION]->(dst {node_id: $cid, workspace: $ws}) "
                "RETURN count(r) AS c",
                cid=child_id,
                ws=_WS,
            ).single()
        return int(rec["c"]) if rec is not None else 0
    finally:
        driver.close()


def _count_edge(
    container: dict[str, Any], edge_type: str, src_id: str, dst_id: str
) -> int:
    """Count *edge_type* edges between the exact (src_id -> dst_id) pair."""
    driver = GraphDatabase.driver(
        container["bolt_url"],
        auth=(container["user"], container["password"]),
    )
    try:
        with driver.session() as session:
            rec = session.run(
                f"MATCH (s {{node_id: $s, workspace: $ws}})-[r:{edge_type}]->"
                "(d {node_id: $d, workspace: $ws}) RETURN count(r) AS c",
                s=src_id,
                d=dst_id,
                ws=_WS,
            ).single()
        return int(rec["c"]) if rec is not None else 0
    finally:
        driver.close()


async def _commit_node(store: Neo4jGraphStore, node_id: str) -> None:
    await store.upsert_node(
        node_id, {"labels": ["Session"], "session_id": node_id, "workspace": _WS}
    )
    await store.flush()


async def test_cross_session_edge_with_absent_endpoint_is_never_silent(
    neo4j_container: dict[str, Any],
) -> None:
    """A HAS_SUBSESSION edge to an absent parent must fail loud or not be lost.

    Models the cross-session W1 race exactly: the child SubSession node is
    committed, but the parent RootSession node (the edge's ``src``) is NOT yet in
    the graph when the edge flush runs.
    """
    # --- Arrange: commit ONLY the child (dst) endpoint; parent (src) is absent.
    store = Neo4jGraphStore(
        uri=neo4j_container["bolt_url"],
        auth=(neo4j_container["user"], neo4j_container["password"]),
        workspace=_WS,
    )
    try:
        await store.upsert_node(
            _CHILD,
            {"labels": ["Session"], "session_id": _CHILD, "workspace": _WS},
        )
        await store.flush()

        # Sanity: the race precondition holds — child present, parent absent.
        assert _count_node(neo4j_container, _CHILD) == 1, (
            "test setup broken: child endpoint was not committed"
        )
        assert _count_node(neo4j_container, _PARENT_ABSENT) == 0, (
            "test setup broken: parent endpoint should be absent for the race"
        )

        # --- Act: write the parent->child HAS_SUBSESSION edge with parent ABSENT.
        await store.upsert_edge(
            _PARENT_ABSENT,
            _CHILD,
            {"type": "HAS_SUBSESSION", "sst_semantic": "LEADS_TO"},
        )

        raised: Exception | None = None
        try:
            await store.flush()
        except Exception as exc:  # noqa: BLE001 - we are characterising loudness
            raised = exc
    finally:
        await store.close()

    # --- Assert: never-silent invariant.
    edge_present = _count_has_subsession_into(neo4j_container, _CHILD) > 0

    assert raised is not None or edge_present, (
        "SILENT DROP: a HAS_SUBSESSION edge whose parent (src) endpoint was "
        "absent at flush time produced NO exception AND left NO edge in the "
        "graph. The edge is lost forever with no error, log, or warning — a "
        "'never-silent' violation. Expected the flush to either fail loud or "
        "guarantee the edge (merge the endpoint)."
    )


async def test_edge_with_absent_dst_is_never_silent(
    neo4j_container: dict[str, Any],
) -> None:
    """Symmetric case: ``dst`` absent must also fail loud (generic writer).

    The edge writer is generic across edge types; a different caller may race on
    the ``dst`` endpoint instead of ``src``.  Both must be never-silent.
    """
    store = Neo4jGraphStore(
        uri=neo4j_container["bolt_url"],
        auth=(neo4j_container["user"], neo4j_container["password"]),
        workspace=_WS,
    )
    try:
        await _commit_node(store, "src-present")
        await store.upsert_edge("src-present", "dst-absent", {"type": "HAS_SUBSESSION"})
        raised: Exception | None = None
        try:
            await store.flush()
        except Exception as exc:  # noqa: BLE001
            raised = exc
    finally:
        await store.close()

    edge_present = (
        _count_edge(neo4j_container, "HAS_SUBSESSION", "src-present", "dst-absent") > 0
    )
    assert raised is not None or edge_present, (
        "SILENT DROP: edge with absent dst produced no error and no edge."
    )


async def test_edge_with_both_endpoints_absent_is_never_silent(
    neo4j_container: dict[str, Any],
) -> None:
    """Both endpoints absent must fail loud, never silently drop."""
    store = Neo4jGraphStore(
        uri=neo4j_container["bolt_url"],
        auth=(neo4j_container["user"], neo4j_container["password"]),
        workspace=_WS,
    )
    try:
        await store.upsert_edge("ghost-src", "ghost-dst", {"type": "HAS_SUBSESSION"})
        raised: Exception | None = None
        try:
            await store.flush()
        except Exception as exc:  # noqa: BLE001
            raised = exc
    finally:
        await store.close()

    edge_present = (
        _count_edge(neo4j_container, "HAS_SUBSESSION", "ghost-src", "ghost-dst") > 0
    )
    assert raised is not None or edge_present, (
        "SILENT DROP: edge with both endpoints absent produced no error and no edge."
    )


async def test_edge_with_both_endpoints_present_writes_without_raising(
    neo4j_container: dict[str, Any],
) -> None:
    """Happy path: when BOTH endpoints exist, the edge is written and NO error
    is raised — the fail-loud net must not break the normal write path.
    """
    store = Neo4jGraphStore(
        uri=neo4j_container["bolt_url"],
        auth=(neo4j_container["user"], neo4j_container["password"]),
        workspace=_WS,
    )
    try:
        await _commit_node(store, "parent-present")
        await _commit_node(store, "child-present")
        await store.upsert_edge(
            "parent-present",
            "child-present",
            {"type": "HAS_SUBSESSION", "sst_semantic": "LEADS_TO"},
        )
        raised: Exception | None = None
        try:
            await store.flush()
        except Exception as exc:  # noqa: BLE001
            raised = exc
    finally:
        await store.close()

    assert raised is None, f"happy-path edge write raised unexpectedly: {raised!r}"
    assert (
        _count_edge(
            neo4j_container, "HAS_SUBSESSION", "parent-present", "child-present"
        )
        == 1
    ), "happy-path edge was not written exactly once"
