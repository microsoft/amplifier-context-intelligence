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
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
from neo4j import AsyncGraphDatabase

import context_intelligence_server.neo4j_store as _cis_store_mod
from context_intelligence_server.neo4j_store import (
    Neo4jGraphStore,
    ensure_neo4j_schema,
)
from context_intelligence_server.queue_manager import QueueManager
from context_intelligence_server.registry import SessionRegistry, SessionWorker
from context_intelligence_server.services import HookStateService
from context_intelligence_server.utils import make_node_id

pytestmark = pytest.mark.neo4j


def _line(event: str, workspace: str, data: dict[str, Any]) -> bytes:
    """Encode one EventRequest JSON line exactly as POST /events appends it."""
    return json.dumps({"event": event, "workspace": workspace, "data": data}).encode(
        "utf-8"
    )


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


# ---------------------------------------------------------------------------
# Phase B2 durable-pipeline acceptance tests
# ---------------------------------------------------------------------------
#
# These two tests drive the DURABLE drain pipeline end to end (on-disk
# QueueManager log -> registry.drain_worker -> semaphore-gated _flush_barrier ->
# real Neo4j) rather than the direct store.flush() path exercised above.
#
# POISON-CHOICE EVIDENCE (recorded from the mandatory pre-step probe; the probe
# itself is throwaway and NOT committed, per the task spec):
#
#   * Store level: ``store.upsert_node("x", {"ci_poison_arr": [1, "two"]})``
#     followed by ``flush()`` raises ``neo4j.exceptions.CypherTypeError`` with
#     code ``Neo.ClientError.Statement.TypeError`` (heterogeneous arrays must be
#     homogeneous). It is NOT a ``TransientError``, so ``execute_write`` does NOT
#     retry it -> the flush raises -> the line is dead-lettered. PROVEN.
#
#   * Pipeline level: an ARBITRARY ``data`` key (e.g. ``ci_poison_arr``) does NOT
#     surface as a Neo4j node property, because ``DefaultHandler`` serialises the
#     whole event ``data`` dict via ``json.dumps`` into a single string property.
#     A ``user:prompt`` carrying ``ci_poison_arr=[1,"two"]`` therefore flushes
#     CLEANLY (no failure, no dead-letter). PROVEN by probe.
#
#   * The field the pipeline DOES surface as a top-level node property is
#     ``parent_id`` (lifted by ``UniversalLifter`` for every event). With
#     ``parent_id=[1,"two"]`` the heterogeneous array reaches Neo4j unchanged
#     (``_sanitize_properties`` keeps all-primitive lists as-is, neo4j_store.py
#     :846-847) and the flush raises the SAME non-transient ``CypherTypeError``.
#     PROVEN by probe.
#
# Faithful translation: the poison line below carries the heterogeneous primitive
# array on ``parent_id`` (the active, flush-failing poison the durable pipeline
# actually surfaces) AND on ``ci_poison_arr`` (the spec-named marker key, present
# in the raw appended line so the dead-letter payload assertion is truthful).
# The MECHANISM is exactly the one the spec describes: an all-primitive
# heterogeneous array reaching Neo4j and being rejected with a non-transient
# Neo.ClientError.Statement.TypeError that execute_write does not retry.


async def test_durable_drain_multi_writer_zero_loss(
    neo4j_container: dict[str, Any],
    tmp_path: Path,
) -> None:
    """N concurrent durable drainers, a shared hot RootSession, zero event loss.

    Three independent sessions each drain through the registry's
    semaphore-gated ``_flush_barrier`` (global cap = 2) into the SAME Neo4j
    instance. Every worker also MERGEs one shared ``RootSession`` node on its
    first flush — the cross-writer hot-node contention that, under the old raw
    ``begin_transaction()/commit()`` write path, produced
    ``Neo.TransientError.Transaction.DeadlockDetected`` and silently dropped
    events. The managed-transaction (``execute_write``) write path auto-retries
    those transient deadlocks, so conservation holds: every appended
    ``user:prompt`` Event node is persisted.

    A ``DeadlockDetected`` escaping a drainer, or a count mismatch, is a real
    regression — the assertions must NOT be weakened to hide it.
    """
    auth = (neo4j_container["user"], neo4j_container["password"])
    bolt = neo4j_container["bolt_url"]
    ws = "test"
    run = uuid.uuid4().hex[:8]
    root_id = f"durable-root-{run}"

    # Schema (uniqueness constraint) must be active before concurrent MERGE.
    driver = AsyncGraphDatabase.driver(bolt, auth=auth)
    try:
        await ensure_neo4j_schema(driver)
    finally:
        await driver.close()

    reg = SessionRegistry()
    reg._queue_manager = QueueManager(queues_dir=tmp_path / "queues")
    reg._write_semaphore = asyncio.Semaphore(2)
    reg._max_delivery_attempts = 5
    qm = reg._queue_manager

    n_sessions = 3
    events_per = 10
    workers: list[SessionWorker] = []
    stores: list[Neo4jGraphStore] = []
    expected_ids: set[str] = set()
    tick = 0

    for s in range(n_sessions):
        sid = f"durable-sess-{run}-{s}"
        store = Neo4jGraphStore(uri=bolt, auth=auth, workspace=ws)
        stores.append(store)
        services = HookStateService(workspace=ws, graph_store=store)
        worker = SessionWorker(session_id=sid, workspace=ws, services=services)
        reg._register_for_test(worker)
        workers.append(worker)

        # Shared hot RootSession node: every worker MERGEs the SAME (node_id,
        # workspace) Session node on its first flush -> real cross-writer
        # contention (the documented deadlock source).
        await store.upsert_node(
            root_id,
            {"labels": ["Session", "RootSession"], "last_updated": _ts(s)},
        )

        for _ in range(events_per):
            tick += 1
            ts = _ts(tick)
            expected_ids.add(make_node_id(sid, "user:prompt", ts))
            await qm.append(
                sid, _line("user:prompt", ws, {"session_id": sid, "timestamp": ts})
            )
        tick += 1
        await qm.append(
            sid,
            _line("session:end", ws, {"session_id": sid, "timestamp": _ts(tick)}),
        )

    # Run all drainers concurrently. Each session's session:end finalizes its
    # drainer, so the gather completes once every session is fully drained.
    tasks = [
        asyncio.create_task(reg.drain_worker(w, flush_timeout=5.0)) for w in workers
    ]
    await asyncio.wait_for(asyncio.gather(*tasks), timeout=60)

    # Conservation: every accepted user:prompt :Event node persisted (zero loss).
    verify = Neo4jGraphStore(uri=bolt, auth=auth, workspace=ws)
    try:
        rows = await verify.execute_query(
            "MATCH (n:Event) WHERE n.node_id IN $ids AND n.workspace = $ws "
            "RETURN count(n) AS c",
            {"ids": list(expected_ids), "ws": ws},
            workspace="*",
        )
        assert rows[0]["c"] == len(expected_ids), (
            f"event loss under durable multi-writer drain: expected "
            f"{len(expected_ids)} :Event nodes, found {rows[0]['c']}"
        )
    finally:
        await verify.close()
        for store in stores:
            await store.close()  # idempotent — finalize already closed each store


async def test_durable_poison_isolation_no_contamination(
    neo4j_container: dict[str, Any],
    tmp_path: Path,
) -> None:
    """COE-blocker (decision #13): a poison line is dead-lettered without
    contaminating its good neighbours.

    A batch of ``[good1, POISON, good2, session:end]`` is drained. The POISON
    line's flush fails non-transiently (heterogeneous primitive array reaches
    Neo4j and is rejected with ``Neo.ClientError.Statement.TypeError`` —
    ``execute_write`` does not retry it). After the batch exhausts its retry
    budget the drainer isolates it ONE LINE AT A TIME: the poison line is
    dead-lettered AND its write residue is dropped via ``discard_buffer`` so it
    cannot bleed into the next good line's flush.

    Without the ``discard_buffer`` calls in ``_handle_exhausted_batch`` the
    poison's resident node (``parent_id=[1,"two"]``) would re-enter good2's
    flush and cascade-fail it — so the "good lines persist" assertion below is
    the regression guard. It must NOT be weakened.
    """
    auth = (neo4j_container["user"], neo4j_container["password"])
    bolt = neo4j_container["bolt_url"]
    ws = "test"
    run = uuid.uuid4().hex[:8]
    sid = f"poison-sess-{run}"

    driver = AsyncGraphDatabase.driver(bolt, auth=auth)
    try:
        await ensure_neo4j_schema(driver)
    finally:
        await driver.close()

    reg = SessionRegistry()
    reg._queue_manager = QueueManager(queues_dir=tmp_path / "queues")
    reg._write_semaphore = asyncio.Semaphore(2)
    reg._max_delivery_attempts = 2
    qm = reg._queue_manager

    store = Neo4jGraphStore(uri=bolt, auth=auth, workspace=ws)
    services = HookStateService(workspace=ws, graph_store=store)
    worker = SessionWorker(session_id=sid, workspace=ws, services=services)
    reg._register_for_test(worker)

    await qm.append(
        sid,
        _line(
            "user:prompt",
            ws,
            {"session_id": sid, "timestamp": _ts(1), "prompt": "good1"},
        ),
    )
    # POISON: heterogeneous primitive array. ``parent_id`` is lifted to a
    # top-level node property by UniversalLifter, so [1,"two"] reaches Neo4j
    # unchanged and is rejected non-transitively. ``ci_poison_arr`` is the
    # spec-named marker; it rides in the same line (so the dead-letter payload
    # contains it) but is NOT itself surfaced as a Neo4j property (DefaultHandler
    # json.dumps-wraps arbitrary data keys). See the EVIDENCE block above.
    await qm.append(
        sid,
        _line(
            "user:prompt",
            ws,
            {
                "session_id": sid,
                "timestamp": _ts(2),
                "parent_id": [1, "two"],
                "ci_poison_arr": [1, "two"],
            },
        ),
    )
    await qm.append(
        sid,
        _line(
            "user:prompt",
            ws,
            {"session_id": sid, "timestamp": _ts(3), "prompt": "good2"},
        ),
    )
    await qm.append(
        sid, _line("session:end", ws, {"session_id": sid, "timestamp": _ts(4)})
    )

    task = asyncio.create_task(reg.drain_worker(worker, flush_timeout=5.0))
    try:
        # Drain to EOF: poll until the committed offset reaches end-of-log.
        for _ in range(600):  # bounded (~30s) well under the 60s ceiling
            await asyncio.sleep(0.05)
            if not (await qm.read_batch(sid, 10)).lines:
                break
        else:
            pytest.fail("poison batch did not drain to EOF within the poll window")
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    # Exactly one dead-letter, and it is the poison line (payload carries marker).
    deads = await qm.read_dead_letters(sid)
    assert len(deads) == 1, f"expected exactly 1 dead-letter, got {len(deads)}"
    assert "ci_poison_arr" in (deads[0].get("payload") or ""), (
        f"dead-lettered line is not the poison line: {deads[0]!r}"
    )

    # Offset advanced past every line (the whole batch is accounted for).
    assert (await qm.read_batch(sid, 10)).lines == []

    # No contamination: the good lines persisted (good1, good2, session:end are
    # all :Event nodes). The poison's Event node is NOT persisted. Without the
    # discard_buffer calls good2 would also fail and this would be < 2.
    verify = Neo4jGraphStore(uri=bolt, auth=auth, workspace=ws)
    try:
        rows = await verify.execute_query(
            "MATCH (n:Event) WHERE n.session_id = $sid AND n.workspace = $ws "
            "RETURN count(n) AS c",
            {"sid": sid, "ws": ws},
            workspace="*",
        )
        assert rows[0]["c"] >= 2, (
            f"contamination: expected >= 2 good :Event nodes for {sid}, "
            f"found {rows[0]['c']}"
        )
    finally:
        await verify.close()
        await store.close()


# ---------------------------------------------------------------------------
# Phase C chunked-flush tests
# ---------------------------------------------------------------------------


async def test_chunked_flush_writes_all_in_bounded_chunks(
    neo4j_container: dict[str, Any],
) -> None:
    """Buffer 500 nodes + 200 edges, flush_chunk_size=50: all items written to
    Neo4j, no single _write_batch call exceeds 50 items, expected call count.

    Correctness check: every node and edge is present after flush.
    Chunk-bound check: each execute_write call received ≤ 50 items per category
    (verified by wrapping _write_batch to record per-call counts while still
    forwarding to the real implementation).
    """
    auth = (neo4j_container["user"], neo4j_container["password"])
    bolt = neo4j_container["bolt_url"]
    run = uuid.uuid4().hex[:8]
    ws = f"chunked-{run}"
    chunk_size = 50

    store = Neo4jGraphStore(
        uri=bolt, auth=auth, workspace=ws, flush_chunk_size=chunk_size
    )

    # 500 nodes
    node_ids = [f"node-{run}-{i}" for i in range(500)]
    for nid in node_ids:
        await store.upsert_node(nid, {"labels": ["TestNode"], "run": run})

    # 200 edges: node-i → node-(i+1) for i in 0..199
    for i in range(200):
        await store.upsert_edge(node_ids[i], node_ids[i + 1], {"type": "TEST_EDGE"})

    # Wrap _write_batch to record per-call item counts while still writing.
    call_counts: list[tuple[int, int, int]] = []  # (n_nodes, n_edges, n_patches)
    original_write_batch = _cis_store_mod._write_batch

    async def recording_write_batch(
        tx: Any,
        nodes: dict[str, Any],
        edges: dict[Any, Any],
        patches: list[Any],
        workspace: str,
    ) -> None:
        call_counts.append((len(nodes), len(edges), len(patches)))
        await original_write_batch(tx, nodes, edges, patches, workspace)

    _cis_store_mod._write_batch = recording_write_batch  # type: ignore[assignment]
    try:
        await store.flush()
    finally:
        _cis_store_mod._write_batch = original_write_batch

    # (a) Correctness: all 500 nodes and 200 edges present in Neo4j
    verify = Neo4jGraphStore(uri=bolt, auth=auth, workspace=ws)
    try:
        node_rows = await verify.execute_query(
            "MATCH (n:TestNode) WHERE n.run = $run AND n.workspace = $ws "
            "RETURN count(n) AS c",
            {"run": run, "ws": ws},
            workspace="*",
        )
        assert node_rows[0]["c"] == 500, (
            f"node loss: expected 500, found {node_rows[0]['c']}"
        )
        edge_rows = await verify.execute_query(
            "MATCH ()-[r:TEST_EDGE]->() WHERE r.workspace = $ws RETURN count(r) AS c",
            {"ws": ws},
            workspace="*",
        )
        assert edge_rows[0]["c"] == 200, (
            f"edge loss: expected 200, found {edge_rows[0]['c']}"
        )
    finally:
        await verify.close()
        await store.close()

    # (b) No call exceeded chunk_size items (categories are mutually exclusive per call)
    for n_nodes, n_edges, n_patches in call_counts:
        total = n_nodes + n_edges + n_patches
        assert total <= chunk_size, (
            f"chunk overflow: call carried {n_nodes} nodes, {n_edges} edges, "
            f"{n_patches} patches (total {total} > {chunk_size})"
        )

    # Expected: 500/50=10 node calls + 200/50=4 edge calls + 0 patch calls = 14
    assert len(call_counts) == 14, (
        f"expected 14 chunk calls, got {len(call_counts)}: {call_counts}"
    )


async def test_chunked_flush_partial_failure_restores_only_remainder(
    neo4j_container: dict[str, Any],
) -> None:
    """3rd execute_write call raises; first 2 chunks (all 100 nodes) are durable;
    buffer holds only the un-written remainder (all 100 edges, no nodes); second
    flush() drains the remainder cleanly with all 100 edges in Neo4j.

    This directly tests the grow-spiral prevention guarantee: the restore set
    shrinks with every committed chunk so the buffer can never regrow from
    partial-failure restoration.
    """
    auth = (neo4j_container["user"], neo4j_container["password"])
    bolt = neo4j_container["bolt_url"]
    run = uuid.uuid4().hex[:8]
    ws = f"partial-{run}"
    chunk_size = 50

    store = Neo4jGraphStore(
        uri=bolt, auth=auth, workspace=ws, flush_chunk_size=chunk_size
    )

    # 100 nodes (2 chunks of 50)
    node_ids = [f"node-{run}-{i}" for i in range(100)]
    for nid in node_ids:
        await store.upsert_node(nid, {"labels": ["TestNode"], "run": run})

    # 100 edges (2 chunks of 50): node-i → node-(i+1) for i in 0..98, plus
    # node-0 → node-99 to reach exactly 100
    for i in range(99):
        await store.upsert_edge(node_ids[i], node_ids[i + 1], {"type": "CHAIN"})
    await store.upsert_edge(node_ids[0], node_ids[99], {"type": "CHAIN"})

    # Inject a failure on the 3rd execute_write call (first edge chunk).
    original_write_batch = _cis_store_mod._write_batch
    call_count = 0

    async def failing_write_batch(
        tx: Any,
        nodes: dict[str, Any],
        edges: dict[Any, Any],
        patches: list[Any],
        workspace: str,
    ) -> None:
        nonlocal call_count
        call_count += 1
        if call_count == 3:
            raise RuntimeError("injected failure on 3rd chunk")
        await original_write_batch(tx, nodes, edges, patches, workspace)

    _cis_store_mod._write_batch = failing_write_batch  # type: ignore[assignment]
    try:
        with pytest.raises(Exception, match="injected failure"):
            await store.flush()
    finally:
        _cis_store_mod._write_batch = original_write_batch

    # First 2 chunks (all 100 nodes) are durably present in Neo4j.
    verify1 = Neo4jGraphStore(uri=bolt, auth=auth, workspace=ws)
    try:
        node_rows = await verify1.execute_query(
            "MATCH (n:TestNode) WHERE n.run = $run AND n.workspace = $ws "
            "RETURN count(n) AS c",
            {"run": run, "ws": ws},
            workspace="*",
        )
        assert node_rows[0]["c"] == 100, (
            f"expected 100 durable nodes after partial failure, "
            f"found {node_rows[0]['c']}"
        )
    finally:
        await verify1.close()

    # Buffer holds ONLY the un-written remainder: all 100 edges, no nodes.
    assert len(store._node_buffer) == 0, (
        f"node buffer should be empty (all committed), "
        f"found {len(store._node_buffer)} items"
    )
    assert len(store._edge_buffer) == 100, (
        f"edge buffer should hold 100 un-committed edges, "
        f"found {len(store._edge_buffer)}"
    )

    # Second flush drains the remainder; all 100 edges now in Neo4j.
    await store.flush()

    verify2 = Neo4jGraphStore(uri=bolt, auth=auth, workspace=ws)
    try:
        edge_rows = await verify2.execute_query(
            "MATCH ()-[r:CHAIN]->() WHERE r.workspace = $ws RETURN count(r) AS c",
            {"ws": ws},
            workspace="*",
        )
        assert edge_rows[0]["c"] == 100, (
            f"expected 100 edges after second flush, found {edge_rows[0]['c']}"
        )
    finally:
        await verify2.close()
        await store.close()
