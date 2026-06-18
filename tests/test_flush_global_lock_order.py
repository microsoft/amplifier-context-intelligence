"""Global lock-order invariant tests for the chunked Neo4j flush (issue #15 regression).

Background
----------
The deadlock fixed by Layer 1 is a lock-order inversion in the chunked flush.
``_flush_body`` splits each buffer into row/byte-bounded chunks and commits each
chunk as an independent Neo4j transaction. ``_write_batch`` sorts rows WITHIN a
single chunk, but if the FULL buffer is not sorted before chunking, the GLOBAL
order across chunks is whatever insertion order the handlers produced. Two
concurrent drainers that insert the same hot nodes/edges in different orders then
acquire Neo4j locks in inverted order across their chunk transactions, deadlocking.

These tests assert the GLOBAL invariant — that the concatenation of writes across
ALL chunks of a single flush is globally ordered — which is the property that
guarantees every drainer acquires locks in one consistent order.

They drive the real ``Neo4jGraphStore.flush()`` with a mocked driver, capturing the
exact chunk payloads handed to ``execute_write`` (``_write_batch``'s arguments):
    args = (node_chunk, edge_chunk, patch_chunk, workspace)
so the assertions are made against what the flush coordinator actually sends to
Neo4j, not against a re-implementation of the chunking.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from context_intelligence_server.neo4j_store import Neo4jGraphStore


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_store_chunked(rows: int, byts: int) -> Neo4jGraphStore:
    """Create a Neo4jGraphStore with explicit chunk-size knobs and a mocked driver."""
    with patch(
        "context_intelligence_server.neo4j_store.AsyncGraphDatabase"
    ) as mock_adb:
        mock_driver = AsyncMock()
        mock_adb.driver.return_value = mock_driver
        store = Neo4jGraphStore(
            uri="bolt://localhost:7687",
            auth=("neo4j", "password"),
            workspace="test",
            flush_chunk_rows=rows,
            flush_chunk_bytes=byts,
        )
    return store


async def _capture_chunks(
    store: Neo4jGraphStore,
) -> tuple[list[dict], list[dict]]:
    """Run store.flush() against a capturing driver.

    Returns (node_chunks, edge_chunks) — the args[0] (node) and args[1] (edge)
    payloads of every execute_write call, in call order, filtered to the
    non-empty ones (node-phase calls carry nodes; edge-phase calls carry edges).
    """
    node_chunks: list[dict] = []
    edge_chunks: list[dict] = []

    async def _capture(fn, *args, **kwargs):
        # fn=_write_batch; args = (node_chunk, edge_chunk, patch_chunk, workspace)
        nodes: dict = args[0]
        edges: dict = args[1]
        if nodes:
            node_chunks.append(nodes)
        if edges:
            edge_chunks.append(edges)

    fake_session = AsyncMock()
    fake_session.__aenter__ = AsyncMock(return_value=fake_session)
    fake_session.__aexit__ = AsyncMock(return_value=False)
    fake_session.execute_write = AsyncMock(side_effect=_capture)
    store._driver.session = MagicMock(return_value=fake_session)

    await store.flush()
    return node_chunks, edge_chunks


# ---------------------------------------------------------------------------
# (a) NODE global lock-order invariant
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_node_ids_globally_non_decreasing_across_all_chunks() -> None:
    """The node_ids across ALL flush chunks must be globally non-decreasing.

    Reproduces the #15 regression scenario: node_ids are buffered in an order
    that is NOT sorted, and the chunk bound (rows=2) forces multiple chunks.
    Without sorting the full snapshot before chunking, chunk boundaries follow
    insertion order, so the concatenation of node_ids across chunks is not
    globally sorted — meaning two drainers with different insertion orders
    acquire node locks in inverted order.
    """
    store = _make_store_chunked(rows=2, byts=10_000_000)
    store._schema_initialized = True

    # Insertion order is deliberately UNSORTED. Mirrors the real hot nodes:
    # a root session, a shared concept node, child sessions, event nodes.
    store._node_buffer = {
        "root-session": {"labels": ["Session"], "v": 1},
        "concept-orch": {"labels": ["Orchestrator"], "v": 2},
        "child-1": {"labels": ["Session"], "v": 3},
        "event-9": {"labels": ["Event"], "v": 4},
        "event-1": {"labels": ["Event"], "v": 5},
    }

    node_chunks, _ = await _capture_chunks(store)

    # Concatenate node_ids in the exact order they are handed to execute_write.
    global_node_order = [nid for chunk in node_chunks for nid in chunk.keys()]

    assert global_node_order == sorted(global_node_order), (
        "Node locks are acquired out of global order across chunks: "
        f"{global_node_order!r} is not globally non-decreasing. "
        "The full node snapshot must be sorted by node_id BEFORE chunking."
    )


# ---------------------------------------------------------------------------
# (b) EDGE global lock-order invariant
# ---------------------------------------------------------------------------


def _edge_rows_in_order(edge_chunks: list[dict]) -> list[tuple[str, str, str]]:
    """Flatten edge chunks into (edge_type, src_id, dst_id) tuples in chunk order."""
    out: list[tuple[str, str, str]] = []
    for chunk in edge_chunks:
        for (src_id, dst_id), data in chunk.items():
            edge_type = data.get("type", "RELATED")
            out.append((edge_type, src_id, dst_id))
    return out


@pytest.mark.asyncio
async def test_edge_rows_globally_ordered_by_type_src_dst_across_all_chunks() -> None:
    """Edge rows across ALL flush chunks must be globally ordered by (type, src, dst).

    Reproduces the production deadlock cycle directly: a child drainer buffers
    HAS_ATTRIBUTE (to the shared concept node) and HAS_SUBSESSION (to the root
    session) while a root drainer buffers HAS_EVENT then HAS_ATTRIBUTE. The
    handlers buffer edge types in NON-alphabetical insertion order
    (HAS_EVENT, HAS_ATTRIBUTE, HAS_SUBSESSION). With a chunk bound (rows=2) the
    edges span multiple chunks; unless the full edge snapshot is sorted by
    (type, src, dst) before chunking, the global lock order inverts between
    drainers — the confirmed cause of NODE(concept)/NODE(root) deadlocks.
    """
    store = _make_store_chunked(rows=2, byts=10_000_000)
    store._schema_initialized = True

    # Insertion order = handler order, deliberately NOT sorted by edge type.
    store._edge_buffer = {
        ("root-session", "event-1"): {"type": "HAS_EVENT", "occurred_at": "t1"},
        ("root-session", "event-9"): {"type": "HAS_EVENT", "occurred_at": "t2"},
        ("child-1", "concept-orch"): {"type": "HAS_ATTRIBUTE"},
        ("root-session", "child-1"): {"type": "HAS_SUBSESSION"},
    }

    _, edge_chunks = await _capture_chunks(store)

    global_edge_order = _edge_rows_in_order(edge_chunks)

    assert global_edge_order == sorted(global_edge_order), (
        "Edge locks are acquired out of global order across chunks: "
        f"{global_edge_order!r} is not globally non-decreasing by (type, src, dst). "
        "The full edge snapshot must be sorted by (type, src, dst) BEFORE chunking."
    )


@pytest.mark.asyncio
async def test_edge_type_groups_iterated_in_global_sorted_order() -> None:
    """Edge-type groups must appear in a single deterministic global order.

    Weaker, targeted companion to the row-level test: the SEQUENCE of edge types
    as they appear across all chunks must be non-decreasing, i.e. every
    HAS_ATTRIBUTE edge is locked before every HAS_EVENT edge before every
    HAS_SUBSESSION edge — never interleaved or inverted between drainers. This
    is the property that makes the cross-session lock order consistent.
    """
    store = _make_store_chunked(rows=2, byts=10_000_000)
    store._schema_initialized = True

    store._edge_buffer = {
        ("root-session", "event-1"): {"type": "HAS_EVENT"},
        ("child-1", "concept-orch"): {"type": "HAS_ATTRIBUTE"},
        ("root-session", "child-1"): {"type": "HAS_SUBSESSION"},
        ("root-session", "event-9"): {"type": "HAS_EVENT"},
        ("child-2", "concept-orch"): {"type": "HAS_ATTRIBUTE"},
    }

    _, edge_chunks = await _capture_chunks(store)

    type_sequence = [t for (t, _src, _dst) in _edge_rows_in_order(edge_chunks)]

    assert type_sequence == sorted(type_sequence), (
        "Edge types are not iterated in a single global sorted order across "
        f"chunks: {type_sequence!r}. Each edge type must form one contiguous, "
        "globally-ordered group so all drainers lock types in the same order."
    )


# ---------------------------------------------------------------------------
# Canonical node-lock pre-acquisition (eliminates the residual root<->concept
# topological deadlock that edge/node sorting alone cannot close)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_write_batch_prelocks_all_endpoints_sorted_before_edge_merge() -> None:
    """_write_batch must pre-lock ALL distinct edge endpoint node_ids in globally
    sorted order BEFORE issuing any edge MERGE.

    Even with the full node and edge snapshots sorted before chunking, the
    residual deadlock survives because the SAME node pair (the root Session and
    the shared concept node) occupies different src/dst roles across different
    edge types in two concurrent drainers:

        root drainer:  HAS_ATTRIBUTE (root -> concept)  => locks root, then concept
        child drainer: HAS_ATTRIBUTE (child -> concept) => locks concept ...
                       HAS_SUBSESSION (root -> child)   => ... then root

    => inverted lock order on (root, concept). Sorting edges by (type, src, dst)
    cannot fix this because the inversion is topological, not orderable by edge
    key. The fix: at the START of the edge phase, take an exclusive write-lock on
    every distinct endpoint node_id in globally-sorted node_id order, so every
    drainer acquires the shared nodes' write locks in the SAME canonical order
    before touching any relationship.

    Implementation shape (Eager-free): the pre-lock is issued as ONE
    single-row ``MATCH (n {node_id: $id, ...}) SET n.node_id = n.node_id`` call
    PER endpoint, in sorted order. A single-row MATCH+SET on node_id does NOT
    plan an ``Eager`` operator (verified via EXPLAIN), so it adds no buffering
    memory pressure; Python's sequential ``await`` guarantees the locks are
    acquired in the sorted order they are issued. Each pre-lock call carries an
    ``id`` (singular) kwarg, distinguishing it from node/edge MERGE (``rows``)
    and label/patch (``node_id``) calls.

    This asserts the pre-lock covers ALL endpoints, in sorted order, with EVERY
    pre-lock call preceding EVERY edge MERGE. FAILS on the prior UNWIND ``ids``
    shape and on pre-pre-lock code.
    """
    from context_intelligence_server.neo4j_store import _write_batch

    # Endpoints deliberately span an UNSORTED set across multiple edge types.
    # 'concept-z' (shared concept) and 'root-a' (root session) are the two hot
    # supernodes; a child-style edge tx references both via different edge types.
    edge_snapshot = {
        ("child-m", "concept-z"): {"type": "HAS_ATTRIBUTE"},
        ("root-a", "child-m"): {"type": "HAS_SUBSESSION"},
        ("root-a", "event-9"): {"type": "HAS_EVENT"},
    }

    tx = AsyncMock()
    await _write_batch(tx, {}, edge_snapshot, [], "test")

    calls = tx.run.call_args_list

    prelock_indices: list[int] = []
    prelock_ids: list[str] = []
    merge_indices: list[int] = []
    for i, c in enumerate(calls):
        query = c.args[0] if c.args else ""
        # Each pre-lock call is the only kind carrying a singular 'id' kwarg.
        if "id" in c.kwargs:
            prelock_indices.append(i)
            prelock_ids.append(c.kwargs["id"])
        # Edge MERGE calls carry a 'rows' kwarg and the relationship arrow.
        if "rows" in c.kwargs and "]->(dst)" in query:
            merge_indices.append(i)

    assert prelock_indices, (
        "Expected _write_batch to issue a canonical node-lock pre-acquisition "
        "(per-endpoint MATCH ... SET n.node_id = n.node_id) before any edge "
        "MERGE; no tx.run call with a singular 'id' kwarg was found."
    )

    all_endpoints = sorted({nid for (s, d) in edge_snapshot for nid in (s, d)})
    assert prelock_ids == all_endpoints, (
        "Pre-lock must lock ALL distinct endpoint node_ids in globally sorted "
        f"order. Expected {all_endpoints!r}, got {prelock_ids!r}."
    )

    assert merge_indices, "Expected at least one edge MERGE call in the edge phase"
    assert max(prelock_indices) < min(merge_indices), (
        f"EVERY canonical pre-lock call (last at idx {max(prelock_indices)}) must "
        f"precede EVERY edge MERGE (first at idx {min(merge_indices)}); otherwise "
        "a relationship lock could be taken before the endpoint pre-lock and "
        "reintroduce the inversion."
    )


@pytest.mark.asyncio
async def test_write_batch_no_prelock_when_no_edges() -> None:
    """The pre-lock must be a no-op for node-only / patch-only _write_batch calls.

    Phase 1 (nodes) and Phase 2 (label patches) call _write_batch with an empty
    edge_snapshot. The pre-lock is guarded by ``if edge_snapshot`` so it must not
    issue any 'ids' UNWIND for those phases (no wasted scan, no spurious locks).
    """
    from context_intelligence_server.neo4j_store import _write_batch

    node_snapshot = {
        "n-b": {"labels": ["Event"], "v": 1},
        "n-a": {"labels": ["Event"], "v": 2},
    }

    tx = AsyncMock()
    await _write_batch(tx, node_snapshot, {}, [], "test")

    for c in tx.run.call_args_list:
        assert "ids" not in c.kwargs, (
            "Pre-lock must not run for an edge-free _write_batch call; found a "
            f"call with 'ids' kwarg: {c.kwargs!r}"
        )
