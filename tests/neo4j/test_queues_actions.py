"""Tier 3 — Neo4j end-to-end pin for the dead-letter REPLAY -> drainer -> graph path.

This is the only place that proves the replay path is REAL (not stubbed): a
seeded dead-letter is replayed back onto the durable log, a real
``SessionRegistry`` drainer reads it, dispatches it through the pipeline, and
flushes it to a LIVE Neo4j container. After the round trip we assert three
honest end states:

1. the dead-letter file is purged (``read_dead_letters == []``),
2. the replayed ``session:start`` wrote a real ``:Session`` node to Neo4j, and
3. conservation stays honest — after seeding the crash-recovery baseline the
   pipeline metrics show ``dead_letter_total == 0``, ``residual == 0`` and
   ``degraded is False``.

Heavier and slower than the unit/contract tests: the whole file is skipped via
the ``neo4j`` marker when no Docker/Neo4j container is available.

Run explicitly:
    uv run pytest tests/neo4j/test_queues_actions.py -v -m neo4j
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from neo4j import AsyncGraphDatabase

from context_intelligence_server.neo4j_store import (
    Neo4jGraphStore,
    ensure_neo4j_schema,
)
from context_intelligence_server.queue_manager import QueueManager
from context_intelligence_server.registry import SessionRegistry, SessionWorker
from context_intelligence_server.services import HookStateService

pytestmark = pytest.mark.neo4j

SESSION_ID = "replay-s"
WORKSPACE = "test"


async def _drain_until(
    predicate: Callable[[], bool],
    *,
    timeout: float = 30.0,
    interval: float = 0.05,
) -> bool:
    """Poll *predicate* until it is truthy or *timeout* elapses.

    Returns the final value of the predicate (True if it became truthy in time,
    False on timeout). Used to drive the real background drainer without
    reaching into its internals — we only observe the registry's live counters.
    """
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while True:
        if predicate():
            return True
        if loop.time() >= deadline:
            return False
        await asyncio.sleep(interval)


async def test_replay_rewrites_through_real_drainer(
    neo4j_container: dict[str, Any],
    tmp_path: Path,
) -> None:
    """Replay re-writes a :Session through the REAL drainer; residual stays honest.

    A well-formed ``session:start`` EventRequest is pre-seeded as a dead-letter,
    then replayed (re-appended to the durable log + purged) exactly as the
    ``/queues/dead-letter/{key}/replay`` route does. A real ``SessionWorker``
    wired to the live container drains the replayed line and flushes it to Neo4j.

    The assertions prove the path is REAL end to end and must NOT be weakened:
    the dead-letter file is purged, a real ``:Session`` node lands in Neo4j, the
    drainer advanced ``written_total``, and after seeding the crash-recovery
    baseline the conservation view shows ``residual == 0`` / ``degraded False``.
    """
    auth = (neo4j_container["user"], neo4j_container["password"])
    bolt = neo4j_container["bolt_url"]

    # Schema (uniqueness constraints) active before any MERGE. Idempotent.
    driver = AsyncGraphDatabase.driver(bolt, auth=auth)
    try:
        await ensure_neo4j_schema(driver)
    finally:
        await driver.close()

    # Fresh registry pointed at a tmp queues dir (no settings dependency: the
    # worker is pre-registered, so get_or_create never builds a settings-derived
    # store).
    reg = SessionRegistry()
    reg._queue_manager = QueueManager(queues_dir=tmp_path / "queues")
    reg._write_semaphore = asyncio.Semaphore(1)
    reg._max_delivery_attempts = 3
    qm = reg._queue_manager

    # Pre-seed a well-formed EventRequest body as a dead-letter for SESSION_ID.
    body = json.dumps(
        {
            "event": "session:start",
            "workspace": WORKSPACE,
            "data": {
                "session_id": SESSION_ID,
                "timestamp": "2026-06-12T12:00:00+00:00",
            },
        }
    ).encode("utf-8")
    await qm.dead_letter(SESSION_ID, body + b"\n", "seeded for replay pin")

    # Build a REAL SessionWorker wired to the live container, then register +
    # start its drainer so get_or_create returns THIS worker during replay.
    store = Neo4jGraphStore(uri=bolt, auth=auth, workspace=WORKSPACE)
    services = HookStateService(workspace=WORKSPACE, graph_store=store)
    worker = SessionWorker(
        session_id=SESSION_ID, workspace=WORKSPACE, services=services
    )
    reg._register_for_test(worker)
    reg.start_drain(worker)

    try:
        # --- Replay (mirrors routers/queues.py::replay_dead_letters) ----------
        records = await qm.read_dead_letters(SESSION_ID)
        assert records, "pre-seeded dead-letter must exist before replay"
        for record in records:
            raw = record["payload"].encode("utf-8")
            obj = json.loads(raw)
            reg.get_or_create(SESSION_ID, obj.get("workspace", ""))
            await qm.append(SESSION_ID, raw)
        await qm.purge_dead_letters(SESSION_ID)
        reg.record_replayed(len(records))

        # Drive the real drainer until it persists the replayed line.
        wrote = await _drain_until(
            lambda: reg.pipeline_counters()["written_total"] >= 1, timeout=30.0
        )
        assert wrote, "drainer did not persist the replayed line within the window"
    finally:
        if worker.task is not None:
            worker.task.cancel()
            try:
                await worker.task
            except asyncio.CancelledError:
                pass

    # 1. Dead-letter file purged.
    assert await qm.read_dead_letters(SESSION_ID) == []

    # 2. The replayed session:start wrote a REAL :Session node to Neo4j.
    verify_driver = AsyncGraphDatabase.driver(bolt, auth=auth)
    try:
        async with verify_driver.session() as session:
            result = await session.run(
                "MATCH (n:Session {session_id: $sid, workspace: $ws}) "
                "RETURN count(n) AS c",
                {"sid": SESSION_ID, "ws": WORKSPACE},
            )
            row = await result.single()
        assert row is not None and row["c"] >= 1, (
            f"expected >=1 :Session node for {SESSION_ID}, found "
            f"{None if row is None else row['c']}"
        )
    finally:
        await verify_driver.close()

    # 3. Conservation honest: seed the crash-recovery baseline so the original
    #    ingest of the replayed line is accounted for (accepted=1), then the
    #    drainer's write balances it. residual == 0, degraded False, no dead.
    reg.seed_counters(accepted=1, written=0)
    metrics = await reg.pipeline_metrics()
    assert metrics["dead_letter_total"] == 0, metrics
    assert metrics["residual"] == 0, metrics
    assert metrics["degraded"] is False, metrics
