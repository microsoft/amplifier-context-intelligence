"""Lock-ordering regression tests for _flush_body.

Root cause (confirmed live via SHOW TRANSACTIONS, 2026-06-18):
  PR #15 split the single globally-sorted flush transaction into per-chunk
  execute_write calls, which broke the cross-transaction lock ordering that
  prevented Neo4j lock cycles.  _chunk_dict iterates the snapshot in
  dict-insertion order; _write_batch only sorts WITHIN each chunk.  Two
  concurrent flushes whose dict-insertion orders differ can acquire the same
  node locks in conflicting sequences across chunks -> circular wait-for cycle
  -> Neo4j DeadlockDetected or (with db.lock.acquisition.timeout=0) a silent
  infinite stall.  With write_semaphore permits exhausted by stalled coroutines
  the entire drain pipeline freezes (confirmed: 38->168 bolt connections,
  zero drain errors, all offsets frozen).

Fix (two layers, defence-in-depth):
  A) Global lock ordering: sort node_snapshot / edge_snapshot by key BEFORE
     calling _chunk_dict so every flush follows the same monotonic key sequence.
     A monotonic lock-acquisition order across all concurrent transactions rules
     out any circular wait-for cycle.
  B) Fail loud: a finite neo4j_lock_timeout (via unit_of_work(timeout=) on each
     execute_write call) makes a blocked flush raise Neo4jError instead of
     parking forever, so the write_semaphore permit is always released and the
     drain pipeline can make progress.

RED proofs (deterministic, no timing races required):
  test_node_chunk_sequence_is_globally_sorted -- without Layer A, _chunk_dict
      iterates dict-insertion order, so reverse-inserted nodes produce reverse
      chunk order; assertion fails for the right reason.
  test_edge_chunk_sequence_is_globally_sorted -- same, for (src_id, dst_id) keys.
  test_blocked_flush_fails_loud_with_finite_timeout -- explicit lock-holder tx
      makes the contested flush park indefinitely without Layer B; with Layer B
      it raises Neo4jError within the configured timeout.

GREEN integration test:
  test_concurrent_overlapping_flush_no_stall -- eight stores sharing a hot
      Session node, all flushed concurrently; with both fixes all stores
      complete cleanly and all Event nodes are present.

Run:
    uv run pytest tests/neo4j/test_flush_lock_ordering.py -v -m neo4j
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

import pytest
from neo4j import AsyncGraphDatabase
from neo4j.exceptions import Neo4jError

import context_intelligence_server.neo4j_store as _cis_store_mod
from context_intelligence_server.neo4j_store import (
    Neo4jGraphStore,
    ensure_neo4j_schema,
)

pytestmark = pytest.mark.neo4j


# ---------------------------------------------------------------------------
# Layer A: global sort order tests (deterministic RED/GREEN)
# Tests 1+2 use the plain Neo4jGraphStore constructor (no neo4j_lock_timeout)
# so they run correctly in RED state before the Layer B constructor change.
# ---------------------------------------------------------------------------


@pytest.mark.timeout(30)
async def test_node_chunk_sequence_is_globally_sorted(
    neo4j_container: dict[str, Any],
) -> None:
    """Node chunks must arrive at _write_batch in globally sorted key order.

    Setup: two nodes are upserted in REVERSE alphabetical order (z before a).
    flush_chunk_rows=1 forces one execute_write per node, so _chunk_dict's
    iteration order determines which node each transaction receives.

    RED (no Layer A fix): _chunk_dict iterates node_snapshot in dict-insertion
    order -> chunk 1 gets {z-id}, chunk 2 gets {a-id}.  Recorded sequence
    [z-id, a-id] is NOT sorted -> assertion fails with a clear message.

    GREEN (Layer A applied): _flush_body sorts node_snapshot by key before
    calling _chunk_dict -> chunk 1 gets {a-id}, chunk 2 gets {z-id}.  Sequence
    [a-id, z-id] is sorted -> assertion passes.
    """
    run = uuid.uuid4().hex[:8]
    ws = f"sort-nodes-{run}"
    bolt = neo4j_container["bolt_url"]
    auth = (neo4j_container["user"], neo4j_container["password"])
    z_id = f"node-z-{run}"
    a_id = f"node-a-{run}"

    store = Neo4jGraphStore(
        uri=bolt,
        auth=auth,
        workspace=ws,
        flush_chunk_rows=1,
        flush_chunk_bytes=10_000_000,
    )

    # Insert in REVERSE alphabetical order: z before a.  Without Layer A this
    # dict-insertion order propagates directly to the chunk sequence.
    await store.upsert_node(z_id, {"labels": ["TestNode"], "val": "z"})
    await store.upsert_node(a_id, {"labels": ["TestNode"], "val": "a"})

    chunk_first_keys: list[str] = []
    original = _cis_store_mod._write_batch

    async def recording_write_batch(
        tx: Any,
        nodes: dict[str, Any],
        edges: dict[Any, Any],
        patches: list[Any],
        workspace: str,
    ) -> None:
        if nodes:
            chunk_first_keys.append(next(iter(nodes)))
        await original(tx, nodes, edges, patches, workspace)

    _cis_store_mod._write_batch = recording_write_batch  # type: ignore[assignment]
    try:
        await store.flush()
    finally:
        _cis_store_mod._write_batch = original
        await store._driver.close()

    # Verify: chunk key sequence is globally monotonically sorted.
    # RED:   ['node-z-...', 'node-a-...'] is NOT sorted -> FAILS here.
    # GREEN: ['node-a-...', 'node-z-...'] IS sorted    -> PASSES.
    assert chunk_first_keys == sorted(chunk_first_keys), (
        f"Node chunks are NOT in globally sorted key order.\n"
        f"  chunk sequence : {chunk_first_keys!r}\n"
        f"  expected order : {sorted(chunk_first_keys)!r}\n"
        f"Fix: sort node_snapshot by key in _flush_body before _chunk_dict.\n"
        f"Without this fix, concurrent flushes with different dict-insertion\n"
        f"orders can acquire the same node locks in conflicting sequences\n"
        f"across chunks, creating circular wait-for cycles in Neo4j."
    )


@pytest.mark.timeout(30)
async def test_edge_chunk_sequence_is_globally_sorted(
    neo4j_container: dict[str, Any],
) -> None:
    """Edge chunks must arrive at _write_batch in globally sorted (src, dst) order.

    Setup: edges are upserted in REVERSE (src_id, dst_id) order (z->z before a->a).
    flush_chunk_rows=1 forces one execute_write per edge.

    RED: dict-insertion order -> edge chunk sequence [('z-src-...','z-dst-...'),
    ('a-src-...','a-dst-...')] is NOT sorted -> assertion fails.

    GREEN: sorted before chunking -> sequence [('a-src-...','a-dst-...'),
    ('z-src-...','z-dst-...')] is sorted -> assertion passes.
    """
    run = uuid.uuid4().hex[:8]
    ws = f"sort-edges-{run}"
    bolt = neo4j_container["bolt_url"]
    auth = (neo4j_container["user"], neo4j_container["password"])

    z_src, z_dst = f"z-src-{run}", f"z-dst-{run}"
    a_src, a_dst = f"a-src-{run}", f"a-dst-{run}"

    store = Neo4jGraphStore(
        uri=bolt,
        auth=auth,
        workspace=ws,
        flush_chunk_rows=1,
        flush_chunk_bytes=10_000_000,
    )

    # Flush the four endpoint nodes first (separate flush cycle so the edge
    # flush cycle contains only edges, keeping the recording unambiguous).
    for nid in (z_src, z_dst, a_src, a_dst):
        await store.upsert_node(nid, {"labels": ["TestNode"]})
    await store.flush()

    # Buffer edges in REVERSE alphabetical (src, dst) order: z-edge before a-edge.
    await store.upsert_edge(z_src, z_dst, {"type": "TEST"})
    await store.upsert_edge(a_src, a_dst, {"type": "TEST"})

    chunk_first_edge_keys: list[tuple[str, str]] = []
    original = _cis_store_mod._write_batch

    async def recording_write_batch(
        tx: Any,
        nodes: dict[str, Any],
        edges: dict[Any, Any],
        patches: list[Any],
        workspace: str,
    ) -> None:
        if edges:
            chunk_first_edge_keys.append(next(iter(edges)))
        await original(tx, nodes, edges, patches, workspace)

    _cis_store_mod._write_batch = recording_write_batch  # type: ignore[assignment]
    try:
        await store.flush()
    finally:
        _cis_store_mod._write_batch = original
        await store._driver.close()

    # RED:   [('z-src-...','z-dst-...'), ('a-src-...','a-dst-...')] not sorted.
    # GREEN: [('a-src-...','a-dst-...'), ('z-src-...','z-dst-...')] sorted.
    assert chunk_first_edge_keys == sorted(chunk_first_edge_keys), (
        f"Edge chunks are NOT in globally sorted (src_id, dst_id) key order.\n"
        f"  chunk sequence : {chunk_first_edge_keys!r}\n"
        f"  expected order : {sorted(chunk_first_edge_keys)!r}\n"
        f"Fix: sort edge_snapshot by key in _flush_body before _chunk_dict."
    )


# ---------------------------------------------------------------------------
# Layer B: fail-loud test (deterministic via explicit lock holder)
# ---------------------------------------------------------------------------


@pytest.mark.timeout(30)
async def test_blocked_flush_fails_loud_with_finite_timeout(
    neo4j_container: dict[str, Any],
) -> None:
    """A flush blocked on a held write lock raises Neo4jError (fail loud) instead
    of parking indefinitely, when neo4j_lock_timeout is configured (Layer B).

    Mechanism:
      1. ensure_neo4j_schema creates the Session uniqueness constraint so that
         MERGE (n:Session {node_id, workspace}) is atomic and contention-safe.
      2. lock_driver opens begin_transaction and MERGEs the contested Session
         node, holding the write lock on the constraint index entry WITHOUT
         committing.  The result is consumed to guarantee server-side execution.
      3. store buffers the same Session node.
         GREEN: store._neo4j_lock_timeout = 5.0 is injected; _flush_body reads
                it and wraps _write_batch with unit_of_work(timeout=5.0).
                _write_batch tries MERGE (n:Session ...) -> blocked by lock_tx
                -> server aborts after 5s -> Neo4jError raised within ~8s.
         RED:   _flush_body ignores self._neo4j_lock_timeout -> no timeout ->
                flush parks indefinitely -> asyncio.wait_for raises TimeoutError
                at 12s -> test FAILS.
      4. max_transaction_retry_time=2.0 on store._driver keeps retry loops short.

    The lock-holder tx is always rolled back in the finally block.
    """
    run = uuid.uuid4().hex[:8]
    ws = f"lock-test-{run}"
    bolt = neo4j_container["bolt_url"]
    auth = (neo4j_container["user"], neo4j_container["password"])
    contested_id = f"contested-session-{run}"

    # Ensure the Session uniqueness constraint exists so MERGE (n:Session ...)
    # contention uses the constraint lock (without it, duplicate nodes are
    # silently created and there is no lock conflict).
    schema_driver = AsyncGraphDatabase.driver(bolt, auth=auth)
    try:
        await ensure_neo4j_schema(schema_driver)
    finally:
        await schema_driver.close()

    # Open a background transaction to hold the write lock on the Session node.
    lock_driver = AsyncGraphDatabase.driver(bolt, auth=auth)
    lock_session = lock_driver.session()
    await lock_session.__aenter__()
    lock_tx = await lock_session.begin_transaction()
    result = await lock_tx.run(
        "MERGE (n:Session {node_id: $nid, workspace: $ws}) RETURN n.node_id",
        nid=contested_id,
        ws=ws,
    )
    await result.consume()  # consume ensures server-side execution and lock hold
    # lock_tx now holds the Session uniqueness constraint write lock; NOT committed.

    store = Neo4jGraphStore(
        uri=bolt,
        auth=auth,
        workspace=ws,
        flush_chunk_rows=1,
        flush_chunk_bytes=10_000_000,
    )
    # Swap to a short-retry driver so timeouts resolve within ~2s.
    await store._driver.close()
    store._driver = AsyncGraphDatabase.driver(
        bolt, auth=auth, max_transaction_retry_time=2.0
    )
    # Inject the transaction timeout via direct attribute assignment.
    # GREEN: _flush_body reads self._neo4j_lock_timeout and applies it via
    #        unit_of_work(timeout=5.0) on every execute_write call.
    # RED:   _flush_body ignores this attribute -> no timeout -> park.
    store._neo4j_lock_timeout = 5.0  # type: ignore[attr-defined]

    # Store a Session node — _write_batch processes it via the session_rows path
    # which uses MERGE (n:Session {node_id: ..., workspace: ...}), the same
    # Cypher that the lock_tx holds the constraint lock for.
    await store.upsert_node(
        contested_id,
        {"labels": ["Session", "RootSession"], "name": "store-write"},
    )

    try:
        # GREEN: raises Neo4jError within ~8s (5s tx timeout + up to 2s retry).
        # RED:   parks; asyncio.wait_for raises TimeoutError at 12s; test FAILS.
        with pytest.raises(Neo4jError):
            await asyncio.wait_for(store.flush(), timeout=12.0)
    except asyncio.TimeoutError:
        pytest.fail(
            "store.flush() did not raise Neo4jError within 12 seconds -- it parked "
            "indefinitely.  Layer B is not active: _flush_body must read "
            "self._neo4j_lock_timeout and pass it via unit_of_work(timeout=...) "
            "to every execute_write call."
        )
    finally:
        try:
            await lock_tx.rollback()
        except Exception:  # noqa: BLE001
            pass
        try:
            await lock_session.__aexit__(None, None, None)
        except Exception:  # noqa: BLE001
            pass
        try:
            await lock_driver.close()
        except Exception:  # noqa: BLE001
            pass
        try:
            await store._driver.close()
        except Exception:  # noqa: BLE001
            pass


# ---------------------------------------------------------------------------
# Integration: both layers together (GREEN validation)
# ---------------------------------------------------------------------------


@pytest.mark.timeout(120)
async def test_concurrent_overlapping_flush_no_stall(
    neo4j_container: dict[str, Any],
) -> None:
    """Eight stores sharing a hot Session node flush concurrently; no stall.

    Regression guard for the v4.0.1 drain-stall.  With both Layer A (sorted
    chunks) and Layer B (finite timeout), all flushes complete and all Event
    nodes are present.

    Configuration that stresses the lock ordering:
      - flush_chunk_rows=2: small chunks -> many concurrent transactions.
      - alternating stores insert the shared Session node in opposite
        dict-insertion order relative to their Event nodes, maximising
        the lock-order divergence that unsorted chunking would expose.
      - max_transaction_retry_time=2.0: transient deadlocks resolve fast.

    Note: tests 1+2 (chunk sort order) are the deterministic RED proofs for
    Layer A.  This test validates end-to-end integration with both fixes live.
    """
    run = uuid.uuid4().hex[:8]
    ws = f"concurrent-{run}"
    bolt = neo4j_container["bolt_url"]
    auth = (neo4j_container["user"], neo4j_container["password"])
    n_stores = 8
    events_per_store = 10

    schema_driver = AsyncGraphDatabase.driver(bolt, auth=auth)
    try:
        await ensure_neo4j_schema(schema_driver)
    finally:
        await schema_driver.close()

    shared_session_id = f"shared-session-{run}"
    stores: list[Neo4jGraphStore] = []
    expected_event_ids: set[str] = set()

    for i in range(n_stores):
        store = Neo4jGraphStore(
            uri=bolt,
            auth=auth,
            workspace=ws,
            flush_chunk_rows=2,
            flush_chunk_bytes=10_000_000,
            neo4j_lock_timeout=10.0,  # Layer B: fail loud if blocked > 10s
        )
        # Short-retry driver so transient deadlocks resolve quickly.
        await store._driver.close()
        store._driver = AsyncGraphDatabase.driver(
            bolt, auth=auth, max_transaction_retry_time=2.0
        )
        stores.append(store)

        if i % 2 == 0:
            # Even stores: shared Session FIRST, then Events
            await store.upsert_node(
                shared_session_id,
                {"labels": ["Session", "RootSession"], "name": f"store-{i}"},
            )
            for j in range(events_per_store):
                ev_id = f"event-{run}-{i}-{j}"
                expected_event_ids.add(ev_id)
                await store.upsert_node(ev_id, {"labels": ["Event"], "owner": i})
                await store.upsert_edge(shared_session_id, ev_id, {"type": "HAS_EVENT"})
        else:
            # Odd stores: Events FIRST, then shared Session (reversed insertion)
            for j in range(events_per_store):
                ev_id = f"event-{run}-{i}-{j}"
                expected_event_ids.add(ev_id)
                await store.upsert_node(ev_id, {"labels": ["Event"], "owner": i})
                await store.upsert_edge(shared_session_id, ev_id, {"type": "HAS_EVENT"})
            await store.upsert_node(
                shared_session_id,
                {"labels": ["Session", "RootSession"], "name": f"store-{i}"},
            )

    try:
        # Flush all stores concurrently.  Both Layer A and B must be active.
        results = await asyncio.wait_for(
            asyncio.gather(*(s.flush() for s in stores), return_exceptions=True),
            timeout=90.0,
        )
        failures = [r for r in results if isinstance(r, BaseException)]
        assert not failures, (
            f"Concurrent flush raised exceptions (Layer A should prevent "
            f"circular lock waits; Layer B should surface remaining stalls):\n"
            f"  {failures!r}"
        )

        # Conservation: every expected Event node is present in Neo4j.
        verify = Neo4jGraphStore(uri=bolt, auth=auth, workspace=ws)
        try:
            rows = await verify.execute_query(
                "MATCH (n:Event) WHERE n.node_id IN $ids AND n.workspace = $ws "
                "RETURN count(n) AS c",
                {"ids": list(expected_event_ids), "ws": ws},
                workspace="*",
            )
            assert rows[0]["c"] == len(expected_event_ids), (
                f"Event loss: expected {len(expected_event_ids)} :Event nodes, "
                f"found {rows[0]['c']}"
            )
        finally:
            await verify._driver.close()
    finally:
        for s in stores:
            try:
                await s._driver.close()
            except Exception:  # noqa: BLE001
                pass
