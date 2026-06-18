"""OOM regression tests for Neo4j memory-cap behaviour.

Proves that the 2 MiB per-transaction cap (NEO4J_db_memory_transaction_max=2m)
is correctly enforced by the capped container fixture.  The test suite has two
layers:

  calibration guard  — a tiny single-node write SUCCEEDS under the cap,
                        establishing that a later "stall" provably means OOM,
                        not a broken fixture.

All tests in this module require the ``docker`` package and are marked
``neo4j`` so they opt-out of the default run.

Run explicitly:
    uv run pytest tests/neo4j/test_oom_regression.py -v -m neo4j
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import pytest
from neo4j import AsyncGraphDatabase
from neo4j.exceptions import TransientError

from context_intelligence_server.neo4j_store import Neo4jGraphStore

pytestmark = pytest.mark.neo4j


# ---------------------------------------------------------------------------
# Shared event-line encoder (mirrors test_event_pipeline._line exactly)
# ---------------------------------------------------------------------------


def _line(event: str, workspace: str, data: dict[str, Any]) -> bytes:
    """Encode an appended event line exactly as POST /events stores it."""
    return json.dumps({"event": event, "workspace": workspace, "data": data}).encode(
        "utf-8"
    )

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

_OOM_CODE = "Neo.TransientError.General.MemoryPoolOutOfMemoryError"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _low_retry_store(
    container: dict[str, Any],
    *,
    rows: int,
    byts: int,
) -> Neo4jGraphStore:
    """Create a Neo4jGraphStore with a 2s max_transaction_retry_time.

    Constructs the store against the capped container, then:
      1. Closes the original 30s-retry driver (no async driver leak).
      2. Replaces store._driver with a new driver with max_transaction_retry_time=2.0.

    Args:
        container:  Connection info dict from neo4j_container_capped.
        rows:       flush_chunk_rows for the store.
        byts:       flush_chunk_bytes for the store.

    Returns:
        A Neo4jGraphStore ready for use against the capped container.
    """
    store = Neo4jGraphStore(
        uri=container["bolt_url"],
        auth=(container["user"], container["password"]),
        workspace="oom-test",
        flush_chunk_rows=rows,
        flush_chunk_bytes=byts,
    )
    # Close the default 30s-retry driver before swapping to avoid an async
    # driver leak (open handles left behind).
    await store._driver.close()
    store._driver = AsyncGraphDatabase.driver(
        container["bolt_url"],
        auth=(container["user"], container["password"]),
        max_transaction_retry_time=2.0,
    )
    return store


async def _buffer_fat_nodes(
    store: Neo4jGraphStore,
    *,
    n: int,
    blob_bytes: int,
    prefix: str,
) -> None:
    """Buffer *n* single-phase node rows, each carrying a ~blob_bytes blob.

    Each node gets a UNIQUE prefix-scoped node_id so test runs don't collide.
    Only nodes are buffered (no edges, no label patches — single phase).

    Args:
        store:      The Neo4jGraphStore to buffer into.
        n:          Number of nodes to buffer.
        blob_bytes: Approximate size of the ``blob`` property in bytes.
        prefix:     Namespace prefix for node_ids (e.g. ``"calib"``, ``"big"``).
    """
    blob = "x" * blob_bytes
    for i in range(n):
        node_id = f"{prefix}-{i}"
        await store.upsert_node(node_id, {"blob": blob})


async def _purge_prefix(store: Neo4jGraphStore, prefix: str) -> None:
    """DETACH DELETE all nodes whose node_id starts with *prefix*.

    Runs at the top of each OOM test so tests are order-independent.
    """
    await store.execute_query(
        "MATCH (n) WHERE n.node_id STARTS WITH $prefix DETACH DELETE n",
        {"prefix": prefix},
        workspace="*",
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_calibration_guard_tiny_write_succeeds(
    neo4j_container_capped: dict[str, Any],
) -> None:
    """A tiny single-node write SUCCEEDS under the 2 MiB cap.

    This calibration guard proves the fixture is functional: if the 2 MiB cap
    were so tight that even a minimal write failed, every subsequent OOM test
    would be meaningless (it could be a broken fixture, not real OOM).

    Pass condition: flush does NOT raise, and exactly 1 node is persisted.
    """
    store = await _low_retry_store(
        neo4j_container_capped,
        rows=500,
        byts=2_000_000,
    )
    try:
        # Buffer one tiny node — well under 2 MiB.
        await _buffer_fat_nodes(store, n=1, blob_bytes=64, prefix="calib")

        # Flush must NOT raise under the 2 MiB cap.
        await store.flush()

        # Exactly one node was persisted.
        records = await store.execute_query(
            "MATCH (n) WHERE n.node_id STARTS WITH $prefix RETURN count(n) AS cnt",
            {"prefix": "calib"},
            workspace="*",
        )
        count = records[0]["cnt"]
        assert count == 1, (
            f"Expected exactly 1 node after calibration flush, got {count}"
        )
    finally:
        await store._driver.close()


async def test_unbounded_single_phase_flush_ooms(
    neo4j_container_capped: dict[str, Any],
) -> None:
    """Enormous chunk bounds ⇒ single-phase flush ⇒ OOM on the 2 MiB cap.

    Calibration: enormous flush_chunk_rows / flush_chunk_bytes (10 M rows,
    10 GB bytes) means all 400 fat nodes (each ~20 KB blob ⇒ ~8 MB total)
    are sent in one transaction, which is 4× over the 2 MiB per-transaction
    cap ⇒ Neo4j raises MemoryPoolOutOfMemoryError.

    Assertions:
    - flush() raises TransientError with code == _OOM_CODE (positively
      identifies the cause, not just "something failed").
    - MATCH count under prefix == 0 after the failed flush (the failed
      transaction committed nothing; _flush_body restored the buffer).
    """
    store = await _low_retry_store(
        neo4j_container_capped,
        rows=10_000_000,
        byts=10_000_000_000,
    )
    try:
        prefix = "oom-unbounded"
        await _purge_prefix(store, prefix)
        await _buffer_fat_nodes(store, n=400, blob_bytes=20_000, prefix=prefix)

        with pytest.raises(TransientError) as exc_info:
            await store.flush()
        assert exc_info.value.code == _OOM_CODE, (
            f"Expected OOM code {_OOM_CODE!r}, got {exc_info.value.code!r}"
        )

        # Nothing was committed — the failed transaction is fully rolled back.
        records = await store.execute_query(
            "MATCH (n) WHERE n.node_id STARTS WITH $prefix RETURN count(n) AS cnt",
            {"prefix": prefix},
            workspace="*",
        )
        count = records[0]["cnt"]
        assert count == 0, (
            f"Expected 0 nodes after OOM flush (nothing should commit), got {count}"
        )
    finally:
        await store._driver.close()


async def test_chunked_flush_drains_same_single_phase_buffer(
    neo4j_container_capped: dict[str, Any],
) -> None:
    """Small chunk bounds ⇒ chunked flush ⇒ all 400 nodes drained (RED).

    This is the genuine RED test: the same 400 fat nodes that OOM the capped
    container in a single transaction MUST drain successfully when
    flush_chunk_rows=50 / flush_chunk_bytes=262_144 (256 KB) keeps each
    chunk well under the 2 MiB cap.

    RED state: against the current unchunked _flush_body the store ignores
    flush_chunk_rows / flush_chunk_bytes and sends all 400 nodes in one
    transaction ⇒ TransientError / MemoryPoolOutOfMemoryError.

    GREEN state (after the fix): flush() must NOT raise; _node_buffer must
    be empty; MATCH count under prefix must equal 400.

    Calibration note: each chunk is ~50 × 20 KB = ~1 MB ⇒ 4-8× UNDER the
    2 MiB cap. Production defaults (100 rows / 4 MB) are deliberately NOT
    used because a 4 MB chunk would itself exceed a 2 MiB cap.
    """
    store = await _low_retry_store(
        neo4j_container_capped,
        rows=50,
        byts=262_144,
    )
    try:
        prefix = "oom-drain"
        await _purge_prefix(store, prefix)
        await _buffer_fat_nodes(store, n=400, blob_bytes=20_000, prefix=prefix)

        # Must NOT raise once chunking is implemented (RED: raises OOM now).
        await store.flush()

        assert store._node_buffer == {}, (
            "_node_buffer must be empty after a successful chunked flush"
        )

        records = await store.execute_query(
            "MATCH (n) WHERE n.node_id STARTS WITH $prefix RETURN count(n) AS cnt",
            {"prefix": prefix},
            workspace="*",
        )
        count = records[0]["cnt"]
        assert count == 400, f"Expected 400 nodes after chunked flush, got {count}"
    finally:
        await store._driver.close()


# ---------------------------------------------------------------------------
# Finalization-path freeze → restart → drain arc (#278)
# ---------------------------------------------------------------------------


async def test_finalization_path_freezes_then_restart_then_drains(
    neo4j_container_capped: dict[str, Any],
    caplog: pytest.LogCaptureFixture,
    tmp_path: Path,
) -> None:
    """Three-leg arc proving the #278 finalization-path freeze bug.

    The full #278 symptom (offsets frozen across restarts) lives on the
    _finalize_session path: a flush failure logs finalize_tail_flush_failed
    and returns without committing, leaving the worker alive and the tail
    uncommitted.  The drain path self-heals via _handle_exhausted_batch line
    isolation; the finalization path does not.

    Seed: 100 fat tool:pre events followed by session:end.
    Each tool:pre creates:
      - A ToolCall node (node_id = 'f-{i}') with tool_input ~40 KB.
      - A fat Event node (~40 KB 'data' field via DefaultHandler).
    Total buffered nodes: ~201 nodes x 40 KB ~ 8 MB >> 2 MiB cap.

    Leg 1 OLD-FREEZE  (rows=10_000_000, byts=10_000_000_000):
      _finalize_session OOMs flushing the 100 fat nodes in a single
      transaction.  Asserts: finalize_tail_flush_failed + OOM code in caplog,
      worker stays registered, offset frozen, 0 f-* nodes committed.

    Leg 2 RESTART still old:
      Fresh registry/worker, same enormous bounds.  Asserts: offset still
      frozen, 0 committed.

    Leg 3 RESTART FIXED (rows=50, byts=262_144):
      flush_chunk_rows=50 / flush_chunk_bytes=262_144 keeps each chunk ~250 KB,
      well under the 2 MiB cap.  Asserts: offset advances to tail_end, worker
      deregistered, 100 f-* nodes committed.
    """
    from unittest.mock import AsyncMock

    from context_intelligence_server.pipeline import setup_handlers
    from context_intelligence_server.queue_manager import QueueManager
    from context_intelligence_server.registry import SessionRegistry, SessionWorker
    from context_intelligence_server.services import HookStateService

    sid = "fin-test-session"
    # Single timestamp shared by all events; each tool:pre uses a unique
    # tool_call_id as the make_node_id disambiguator so all 100 Event nodes
    # have distinct IDs.
    _TS = "2024-01-01T00:00:00+00:00"

    # Shared durable queue directory — same path that safe_settings (autouse
    # fixture) patches into get_settings().queues_path for every new
    # SessionRegistry() instance constructed in this test.
    queues_dir = tmp_path / "queues"
    qm = QueueManager(queues_dir=queues_dir)

    async def _make(
        rows: int, byts: int
    ) -> tuple[SessionRegistry, SessionWorker, Any, Any]:
        """Build (reg, worker, store, handlers) for one leg."""
        store = await _low_retry_store(neo4j_container_capped, rows=rows, byts=byts)
        services = HookStateService(workspace="oom-test", graph_store=store)
        worker = SessionWorker(session_id=sid, workspace="oom-test", services=services)
        reg = SessionRegistry()
        reg._register_for_test(worker)
        reg._max_delivery_attempts = 1
        handlers = setup_handlers(services)
        return reg, worker, store, handlers

    async def _committed_offset() -> int:
        """Return the current committed byte offset for sid."""
        batch = await qm.read_batch(sid, 1)
        return batch.start_offset

    # -----------------------------------------------------------------------
    # Seed queue once: 100 fat tool:pre events + session:end
    # -----------------------------------------------------------------------
    # tool_call_id = 'f-{i}' -> ToolCallHandler creates ToolCall node with
    #   node_id = 'f-{i}' and tool_input = 40 KB (fat node property).
    # DefaultHandler embeds the full event data as JSON in the Event node
    #   (~40 KB 'data' field), making each Event node also fat.
    # Phase 1 of flush (node tx) holds ~201 nodes x 40 KB ~ 8 MB >> 2 MiB cap.
    for i in range(100):
        await qm.append(
            sid,
            _line(
                "tool:pre",
                "oom-test",
                {
                    "tool_call_id": f"f-{i}",
                    "tool_input": "x" * 40_000,
                    "session_id": sid,
                    "timestamp": _TS,
                },
            ),
        )
    await qm.append(
        sid,
        _line(
            "session:end",
            "oom-test",
            {"session_id": sid, "timestamp": _TS},
        ),
    )

    # -----------------------------------------------------------------------
    # Leg 1: OLD-FREEZE — enormous flush bounds -> single-transaction OOM
    # -----------------------------------------------------------------------
    reg, worker, store, handlers = await _make(rows=10_000_000, byts=10_000_000_000)
    offset_before = await _committed_offset()  # 0 — nothing committed yet

    with caplog.at_level(logging.ERROR, logger="context_intelligence_server"):
        await reg._finalize_session(worker, handlers)

    # OOM cause must be positively asserted — not weakened to a proxy.
    assert "finalize_tail_flush_failed" in caplog.text, (
        "Expected 'finalize_tail_flush_failed' in registry log"
    )
    assert _OOM_CODE in caplog.text, (
        f"Expected OOM code {_OOM_CODE!r} in caplog — verify that "
        "_finalize_session uses logger.exception (carrying the traceback), "
        "not plain logger.error without exc_info"
    )
    # Worker must remain registered — _finalize_session returned without cleanup.
    assert reg._workers.get(sid) is not None, (
        "Worker must still be registered after OOM freeze"
    )
    # Committed offset must NOT have advanced — the tail is uncommitted.
    assert await _committed_offset() == offset_before, (
        "Committed offset must be frozen (OOM leaves tail uncommitted)"
    )
    # Zero f-* nodes were written to Neo4j (the OOM'd transaction rolled back).
    records = await store.execute_query(
        "MATCH (n) WHERE n.node_id STARTS WITH $prefix RETURN count(n) AS cnt",
        {"prefix": "f-"},
        workspace="*",
    )
    assert records[0]["cnt"] == 0, "No f-* nodes should be committed after OOM"
    await store._driver.close()

    # -----------------------------------------------------------------------
    # Leg 2: RESTART still old — proves freeze survives a fresh registry start
    # -----------------------------------------------------------------------
    caplog.clear()
    reg2, worker2, store2, handlers2 = await _make(rows=10_000_000, byts=10_000_000_000)

    with caplog.at_level(logging.ERROR, logger="context_intelligence_server"):
        await reg2._finalize_session(worker2, handlers2)

    assert await _committed_offset() == offset_before, (
        "Offset must still be frozen after second old-bounds restart"
    )
    records2 = await store2.execute_query(
        "MATCH (n) WHERE n.node_id STARTS WITH $prefix RETURN count(n) AS cnt",
        {"prefix": "f-"},
        workspace="*",
    )
    assert records2[0]["cnt"] == 0, "No f-* nodes after second OOM attempt"
    await store2._driver.close()

    # -----------------------------------------------------------------------
    # Leg 3: RESTART FIXED — chunked flush drains in multiple small tx
    # -----------------------------------------------------------------------
    caplog.clear()
    reg3, worker3, store3, handlers3 = await _make(rows=50, byts=262_144)

    # Learn tail_end: byte offset after the very last line in the queue.
    all_batch = await qm.read_batch(sid, 1_000)
    tail_end = all_batch.end_offset

    # Patch delete_drained to a no-op so the .offset file is preserved
    # after successful finalization — needed for the offset assertion below.
    reg3.queue_manager.delete_drained = AsyncMock()  # type: ignore[method-assign]

    await reg3._finalize_session(worker3, handlers3)

    # Offset must have advanced all the way to the end of the queue data.
    final_offset = await _committed_offset()
    assert final_offset == tail_end, (
        f"Final committed offset {final_offset} must equal tail_end {tail_end}"
    )
    # Worker must be deregistered on successful finalization.
    assert reg3._workers.get(sid) is None, (
        "Worker must be deregistered after successful _finalize_session"
    )

    # _finalize_session calls _safe_close(worker3) which closes store3's
    # driver.  Use a fresh connection to count the committed f-* nodes.
    check_store = await _low_retry_store(
        neo4j_container_capped, rows=500, byts=2_000_000
    )
    try:
        records3 = await check_store.execute_query(
            "MATCH (n) WHERE n.node_id STARTS WITH $prefix RETURN count(n) AS cnt",
            {"prefix": "f-"},
            workspace="*",
        )
        assert records3[0]["cnt"] == 100, (
            f"Expected 100 f-* ToolCall nodes after fixed-bounds drain, "
            f"got {records3[0]['cnt']}"
        )
    finally:
        await check_store._driver.close()
