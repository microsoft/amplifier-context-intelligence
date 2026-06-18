"""Live E2E test: a real finalization-path orphan surfaces on /status.

Proves the #278 signal works on a GENUINE orphan driven through the real
start_drain → drain_worker → _finalize_session path, so worker.task actually
exists and transitions to done().

Seed shape (line counts are LOAD-BEARING, do NOT reorder):
  Lines   1-99:  tiny tool:pre  (tool_input 'x'*16,     key space small-{i})
  Line   100:    session:end    (terminal; completes the exactly-100-line first batch)
  Lines 101-200: fat  tool:pre  (tool_input 'x'*40_000, key space f-{i}, ~8 MB total)

WHY the shape:
  The pre-terminal block MUST be exactly 100 lines so the drainer's first
  read_batch(max_items=_DRAIN_MAX_BATCH=100) returns only those 100 lines,
  commits cleanly (tiny flush << 2 MiB cap), sets saw_terminal → calls
  _finalize_session.  The finalization tail (100 fat lines) is flushed in ONE
  transaction (enormous flush bounds: rows=10_000_000, byts=10_000_000_000) →
  OOM.  _finalize_session does NOT retry or line-isolate; one OOM →
  finalize_tail_flush_failed log + early return → orphan (registered worker,
  completed task).

  If the pre-terminal block were NOT exactly 100 lines, the first batch would
  contain fat lines and the main loop's _handle_exhausted_batch line-isolation
  would self-heal → no orphan.

Requires Docker and the docker Python package.  Skip-if-absent via the
neo4j_container_capped fixture in tests/neo4j/conftest.py.

Run explicitly:
    cd amplifier-context-intelligence
    uv run pytest tests/neo4j/test_orphan_visibility.py -v -m neo4j
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Any

import pytest
from neo4j import AsyncGraphDatabase

from context_intelligence_server.dashboard import build_status_response
from context_intelligence_server.neo4j_store import Neo4jGraphStore
from context_intelligence_server.queue_manager import QueueManager
from context_intelligence_server.registry import SessionRegistry, SessionWorker
from context_intelligence_server.services import HookStateService

pytestmark = pytest.mark.neo4j


# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

_OOM_CODE = "Neo.TransientError.General.MemoryPoolOutOfMemoryError"


# ---------------------------------------------------------------------------
# Helpers (mirrors test_oom_regression to keep this file self-contained)
# ---------------------------------------------------------------------------


def _line(event: str, workspace: str, data: dict[str, Any]) -> bytes:
    """Encode an appended event line exactly as POST /events stores it."""
    return json.dumps({"event": event, "workspace": workspace, "data": data}).encode(
        "utf-8"
    )


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
        workspace="orphan-test",
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


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------


async def test_finalization_orphan_surfaces_on_status(
    neo4j_container_capped: dict[str, Any],
    caplog: pytest.LogCaptureFixture,
    tmp_path: Path,
) -> None:
    """Real finalization-path orphan drives through start_drain and surfaces on /status.

    The reproduction is the empirically-verified spike recipe (deterministic,
    ~29s against neo4j:5.26.22-community).  All assertions prove a GENUINE
    orphan — not a synthetic stub — from the real drain path.

    Post-state assertions:
      - worker.task.done() is True
      - sid still in registry._workers (not deregistered)
      - 'finalize_tail_flush_failed' in caplog.text
      - _OOM_CODE in caplog.text
      - committed offset frozen at the pre-terminal boundary (== boundary, != tail_end)
      - 0 f-* nodes committed (query via a FRESH check_store)
      - worker in registry.orphaned_sessions()
      - build_status_response reports orphaned_sessions >= 1 and
        the session's per-session dict has orphaned: True
    """
    sid = "orphan-test-session"
    _TS = "2024-01-01T00:00:00+00:00"

    # The autouse safe_settings fixture (tests/conftest.py) patches
    # context_intelligence_server.registry.get_settings() to return a proxy
    # with queues_path = str(tmp_path / "queues").  Using the same path here
    # guarantees this QueueManager and the registry's queue_manager point at
    # the same on-disk queue.
    queues_dir = tmp_path / "queues"
    qm = QueueManager(queues_dir=queues_dir)

    # -----------------------------------------------------------------------
    # Seed the durable queue in exact order (line counts are load-bearing).
    # -----------------------------------------------------------------------

    # Block 1: lines 1-99 — tiny tool:pre (tool_input 'x'*16, key space small-{i})
    for i in range(99):
        await qm.append(
            sid,
            _line(
                "tool:pre",
                "orphan-test",
                {
                    "tool_call_id": f"small-{i}",
                    "tool_input": "x" * 16,
                    "session_id": sid,
                    "timestamp": _TS,
                },
            ),
        )

    # Line 100: session:end (terminal; completes the exactly-100-line first batch
    # matching read_batch max_items=_DRAIN_MAX_BATCH=100)
    await qm.append(
        sid,
        _line(
            "session:end",
            "orphan-test",
            {"session_id": sid, "timestamp": _TS},
        ),
    )

    # Block 2: lines 101-200 — fat tool:pre (~8 MB total >> 2 MiB per-tx cap)
    for i in range(100):
        await qm.append(
            sid,
            _line(
                "tool:pre",
                "orphan-test",
                {
                    "tool_call_id": f"f-{i}",
                    "tool_input": "x" * 40_000,
                    "session_id": sid,
                    "timestamp": _TS,
                },
            ),
        )

    # -----------------------------------------------------------------------
    # Capture boundary and tail_end BEFORE starting the drain.
    # read_batch is non-destructive (reads from the committed offset without
    # modifying it); the committed offset is 0 so both reads start at 0.
    # -----------------------------------------------------------------------
    first_100 = await qm.read_batch(sid, 100)
    boundary = first_100.end_offset  # byte position AFTER line 100 (session:end)
    all_200 = await qm.read_batch(sid, 200)
    tail_end = all_200.end_offset  # byte position AFTER line 200 (last fat event)
    assert boundary > 0, "boundary must be positive — queue was seeded"
    assert tail_end > boundary, (
        "tail_end must be > boundary — fat block follows the pre-terminal block"
    )

    # -----------------------------------------------------------------------
    # Build worker with enormous flush bounds (single-transaction OOM on tail).
    # flush_chunk_rows=10_000_000 and flush_chunk_bytes=10_000_000_000 ensure
    # all 100 fat lines go in ONE transaction (~8 MB >> 2 MiB cap).
    # -----------------------------------------------------------------------
    store = await _low_retry_store(
        neo4j_container_capped,
        rows=10_000_000,
        byts=10_000_000_000,
    )
    services = HookStateService(workspace="orphan-test", graph_store=store)
    worker = SessionWorker(session_id=sid, workspace="orphan-test", services=services)

    # -----------------------------------------------------------------------
    # Register and start the drain through the REAL path.
    # Do NOT use get_or_create — it reads from get_settings() → production.
    # -----------------------------------------------------------------------
    registry = SessionRegistry()
    registry._register_for_test(worker)

    with caplog.at_level(logging.ERROR, logger="context_intelligence_server"):
        registry.start_drain(worker)
        # start_drain always sets worker.task; assert here so the type-checker
        # knows it is not None before passing to asyncio.shield.
        assert worker.task is not None, "start_drain must create worker.task"
        # asyncio.shield() prevents the timeout from cancelling worker.task.
        # The task MUST complete on its own (OOM → return); do NOT cancel it.
        await asyncio.wait_for(asyncio.shield(worker.task), timeout=90.0)

    # -----------------------------------------------------------------------
    # Assert orphan post-state (assertions are the proof — do NOT weaken them)
    # -----------------------------------------------------------------------

    # 1. Drain task completed (not stuck, not cancelled).
    assert worker.task is not None and worker.task.done(), (
        "worker.task must be done after the drain exits via OOM early-return"
    )

    # 2. Worker is still registered (not deregistered — _finalize_session
    #    returned early without calling _safe_close / _deregister).
    assert sid in registry._workers, (
        "worker must still be registered in registry._workers after OOM orphan "
        "(finalize returned early without deregistering)"
    )

    # 3. finalize_tail_flush_failed logged — proves the OOM was on the
    #    finalization path, not the main drain loop.
    assert "finalize_tail_flush_failed" in caplog.text, (
        "Expected 'finalize_tail_flush_failed' in registry log after finalization OOM"
    )

    # 4. OOM error code in log — positively identifies the cause.
    assert _OOM_CODE in caplog.text, (
        f"Expected OOM code {_OOM_CODE!r} in caplog — verify that "
        "_finalize_session uses logger.exception (carries the traceback), "
        "not plain logger.error without exc_info"
    )

    # 5. Committed offset frozen at the pre-terminal boundary, NOT at tail_end.
    #    The drain committed the first batch (lines 1-100) but _finalize_session
    #    returned early without committing the tail (lines 101-200).
    post_drain_batch = await qm.read_batch(sid, 1)
    committed_offset = post_drain_batch.start_offset
    assert committed_offset == boundary, (
        f"Committed offset {committed_offset} must equal boundary {boundary} "
        "(drain committed first batch, OOM froze the tail)"
    )
    assert committed_offset != tail_end, (
        f"Committed offset {committed_offset} must NOT equal tail_end {tail_end} "
        "(OOM tail is uncommitted)"
    )

    # 6. Zero f-* nodes committed.
    #    The worker's driver is in a post-OOM state; use a FRESH store for the query
    #    (the worker driver is not in a condition to serve reliable queries).
    check_store = await _low_retry_store(
        neo4j_container_capped, rows=500, byts=2_000_000
    )
    try:
        records = await check_store.execute_query(
            "MATCH (n) WHERE n.node_id STARTS WITH $prefix RETURN count(n) AS cnt",
            {"prefix": "f-"},
            workspace="*",
        )
        assert records[0]["cnt"] == 0, (
            f"Expected 0 f-* nodes after OOM finalization, got {records[0]['cnt']} "
            "(the OOM transaction must have rolled back completely)"
        )
    finally:
        await check_store._driver.close()

    # 7. Worker appears in registry.orphaned_sessions().
    orphans = registry.orphaned_sessions()
    assert worker in orphans, (
        "worker must appear in registry.orphaned_sessions() "
        "(task done AND still registered = the orphan signal)"
    )

    # 8. build_status_response reports orphaned_sessions >= 1 AND the
    #    per-session dict for sid has orphaned: True.
    status = build_status_response(registry, time.time())
    assert status["orphaned_sessions"] >= 1, (
        f"build_status_response must report orphaned_sessions >= 1, "
        f"got {status['orphaned_sessions']}"
    )
    session_dicts = {s["session_id"]: s for s in status["sessions"]}
    assert sid in session_dicts, (
        f"Session {sid!r} must appear in status['sessions'] — "
        "check dashboard_inactive_timeout filter"
    )
    assert session_dicts[sid]["orphaned"] is True, (
        f"status['sessions'] entry for {sid!r} must have orphaned: True, "
        f"got: {session_dicts[sid]}"
    )

    # -----------------------------------------------------------------------
    # Teardown: close the worker's still-open store driver.
    # _finalize_session returned early without calling _safe_close (which
    # would have closed the driver via worker.services.graph.close()).
    # Leaving it open causes an unclosed-resource warning in the test suite.
    # -----------------------------------------------------------------------
    await store._driver.close()
