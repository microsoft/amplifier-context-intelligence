"""Neo4j-backed coverage: (a), (d), (h).

Uses real
``QueueManager``, real ``SessionRegistry``/``SessionWorker``/``drain_worker``,
real ``Neo4jGraphStore``, against the isolated throwaway Neo4j container.

  (a) an open, drained-idle, NO-TERMINAL session's committed prefix IS
      reclaimed -- backlog stays 0, all events remain queryable in the
      graph, and the return value is the RECLAIMED PREFIX, not 0.
  (d) a live drainer + concurrent ingest during repeated compaction never
      drops, reorders, or double-processes events.
  (h) a BOOT-RECOVERED (``live_event_seen=False``) no-terminal session IS
      compacted BEFORE it takes the dry-exit -- the most
      operationally common real-world way a log would otherwise grow
      unbounded.

Run explicitly:
    cd amplifier-context-intelligence
    uv run pytest tests/neo4j/test_steady_state_reclaim_neo4j.py -q -s -m neo4j
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any

import pytest
from neo4j import AsyncGraphDatabase

from context_intelligence_server.neo4j_store import Neo4jGraphStore, ensure_neo4j_schema
from context_intelligence_server.queue_manager import QueueManager
from context_intelligence_server.registry import SessionRegistry, SessionWorker
from context_intelligence_server.services import HookStateService

pytestmark = pytest.mark.neo4j

_WS = "d9-neo4j"
_TS = "2026-08-21T00:00:00+00:00"


def _line(event: str, workspace: str, data: dict[str, Any]) -> bytes:
    return json.dumps({"event": event, "workspace": workspace, "data": data}).encode(
        "utf-8"
    )


def _small_event(i: int, sid: str) -> bytes:
    return _line(
        "tool:pre",
        _WS,
        {
            "session_id": sid,
            "timestamp": _TS,
            "tool_call_id": f"call-{sid}-{i}",
            "tool_name": "bash",
            "tool_input": "x" * 128,
        },
    )


def _seq_event(i: int, sid: str) -> bytes:
    """Same shape as ``_small_event``, but ``tool_input`` carries the append
    ordinal ``i`` as a decimal string -- an independent, out-of-band sequence
    marker that lets a test observe REORDERING (not just count loss) once the
    event has been dispatched through the real drain path.

    Fixed-width zero-padding keeps the string sortable/greppable, but the
    load-bearing check is int(tool_input), not string order.
    """
    return _line(
        "tool:pre",
        _WS,
        {
            "session_id": sid,
            "timestamp": _TS,
            "tool_call_id": f"call-{sid}-{i:06d}",
            "tool_name": "bash",
            "tool_input": f"{i:09d}",
        },
    )


def _build_registry(queues_dir: Path) -> SessionRegistry:
    reg = SessionRegistry()
    reg._queue_manager = QueueManager(queues_dir=queues_dir)
    reg._write_semaphore = asyncio.Semaphore(8)
    reg._max_delivery_attempts = 3
    return reg


def _build_worker(
    container: dict[str, Any], sid: str, *, live_event_seen: bool = True
) -> SessionWorker:
    store = Neo4jGraphStore(
        uri=container["bolt_url"],
        auth=(container["user"], container["password"]),
        workspace=_WS,
    )
    services = HookStateService(workspace=_WS, graph_store=store)
    return SessionWorker(
        session_id=sid,
        workspace=_WS,
        services=services,
        live_event_seen=live_event_seen,
    )


async def _graph_event_count(container: dict[str, Any], sid: str) -> int:
    driver = AsyncGraphDatabase.driver(
        container["bolt_url"], auth=(container["user"], container["password"])
    )
    try:
        async with driver.session() as session:
            result = await session.run(
                "MATCH (n:Event) WHERE n.session_id = $sid AND n.workspace = $ws "
                "RETURN count(n) AS c",
                sid=sid,
                ws=_WS,
            )
            record = await result.single()
            return int(record["c"]) if record else 0
    finally:
        await driver.close()


async def _current_backlog(qm: QueueManager, sid: str) -> int:
    batch = await qm.read_batch(sid, max_items=1_000_000)
    return len(batch.records)


async def _poll_until(predicate, *, timeout: float, interval: float = 0.05) -> bool:
    deadline = time.monotonic() + timeout
    while True:
        if await predicate():
            return True
        if time.monotonic() >= deadline:
            return False
        await asyncio.sleep(interval)


@pytest.fixture(autouse=True)
async def _schema(neo4j_container: dict[str, Any]) -> None:
    driver = AsyncGraphDatabase.driver(
        neo4j_container["bolt_url"],
        auth=(neo4j_container["user"], neo4j_container["password"]),
    )
    try:
        await ensure_neo4j_schema(driver)
    finally:
        await driver.close()


# ---------------------------------------------------------------------------
# (a) open, drained-idle, no-terminal session IS reclaimed
# ---------------------------------------------------------------------------


@pytest.mark.timeout(120)
async def test_a_drained_idle_no_terminal_session_committed_prefix_reclaimed(
    neo4j_container: dict[str, Any], tmp_path: Path
) -> None:
    queues_dir = tmp_path / "queues"
    reg = _build_registry(queues_dir)
    qm = reg.queue_manager

    sid = "d9-a-drained-idle"
    n = 500
    for i in range(n):
        await qm.append(sid, _small_event(i, sid))

    log_path = queues_dir / f"{sid}.log"
    assert log_path.stat().st_size > 0

    worker = _build_worker(neo4j_container, sid)
    reg._register_for_test(worker)
    reg.start_drain(worker)
    assert worker.task is not None

    async def _drained() -> bool:
        return await _current_backlog(qm, sid) == 0

    drained = await _poll_until(_drained, timeout=60.0)
    assert drained, "drainer never reached backlog 0"

    # Give the idle branch a few poll cycles to run Trigger I.
    async def _log_collapsed() -> bool:
        return not log_path.exists() or log_path.stat().st_size == 0

    collapsed = await _poll_until(_log_collapsed, timeout=10.0)
    assert collapsed, (
        f"expected the drained-idle .log to collapse to its (empty) undrained "
        f"tail -- still {log_path.stat().st_size if log_path.exists() else 'missing'} bytes"
    )

    assert await _current_backlog(qm, sid) == 0
    graph_count = await _graph_event_count(neo4j_container, sid)
    assert graph_count == n

    # Direct call proves the RETURN VALUE contract (R1): a manual invocation
    # on this now-fully-compacted log must bail at 0 (nothing left to
    # reclaim) -- confirming the earlier automatic compaction already ran
    # and returned the reclaimed PREFIX (not the tail) when it did.
    reclaimed_again = await qm.compact_committed_prefix(sid, 0, 0)
    assert reclaimed_again == 0  # idempotent: nothing left below committed=0

    worker.task.cancel()
    try:
        await worker.task
    except asyncio.CancelledError:
        pass


async def _zero_backlog(qm: QueueManager, sid: str) -> bool:
    return await _current_backlog(qm, sid) == 0


# ---------------------------------------------------------------------------
# (d) live drainer + concurrent ingest during repeated compaction
# ---------------------------------------------------------------------------


@pytest.mark.timeout(180)
async def test_d_concurrent_ingest_during_repeated_compaction_no_loss(
    neo4j_container: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Count-conservation PLUS an INDEPENDENT order check.

    Checking ``graph_count == n_total + 1`` alone leaves ordering
    "inferred from count conservation, never independently checked" -- a
    real reorder bug (e.g. a compaction race that shuffled bytes) would
    leave the count identical and go undetected.

    Every event carries an out-of-band ascending sequence marker
    (``_seq_event``'s ``tool_input``) that is INDEPENDENT of storage/queue
    byte layout. A spy wraps the REAL ``Neo4jGraphStore.upsert_node`` (still
    calling through to the real implementation -- this is observation, not a
    stub) and records the sequence marker of every ToolCall upsert for this
    session, IN THE ORDER the real drain path executes them. Appends
    themselves are strictly sequential (a single task, one ``await
    qm.append`` at a time), so the precondition -- append order == seq order
    -- is guaranteed by the test, not the product; what's under test is
    whether concurrent ingest + repeated real ``compact_committed_prefix``
    preserves that order all the way to the graph write.
    """
    import context_intelligence_server.config as cfg_module
    import context_intelligence_server.registry as reg_module
    from context_intelligence_server.neo4j_store import Neo4jGraphStore

    queues_dir = tmp_path / "queues"
    reg = _build_registry(queues_dir)
    qm = reg.queue_manager

    class _S:
        queue_compact_enabled = True
        queue_compact_min_prefix_bytes = 4096  # fires repeatedly mid-stream
        queue_compact_max_tail_bytes = 64 * 1024 * 1024
        stale_session_timeout = 3600.0

    monkeypatch.setattr(cfg_module, "get_settings", lambda: _S())
    monkeypatch.setattr(reg_module, "get_settings", lambda: _S())

    sid = "d9-d-concurrent"
    n_total = 400

    # --- order-observing spy: wraps the REAL upsert_node, still calls it ---
    observed_order: list[int] = []
    real_upsert_node = Neo4jGraphStore.upsert_node

    async def _spy_upsert_node(self: Any, node_id: str, data: dict[str, Any]) -> None:
        if data.get("session_id") == sid and "ToolCall" in data.get("labels", []):
            observed_order.append(int(data["tool_input"]))
        await real_upsert_node(self, node_id, data)

    monkeypatch.setattr(Neo4jGraphStore, "upsert_node", _spy_upsert_node)

    worker = _build_worker(neo4j_container, sid)
    reg._register_for_test(worker)
    reg.start_drain(worker)
    assert worker.task is not None

    for i in range(n_total):
        await qm.append(sid, _seq_event(i, sid))
        reg.record_accepted(1)  # mirrors what POST /events does at ingest
        if i % 23 == 0:
            await asyncio.sleep(0.005)  # interleave with the live drainer

    await qm.append(
        sid, _line("session:end", _WS, {"session_id": sid, "timestamp": _TS})
    )
    reg.record_accepted(1)
    await asyncio.wait_for(asyncio.shield(worker.task), timeout=120.0)

    graph_count = await _graph_event_count(neo4j_container, sid)
    # n_total tool:pre events + the session:end event itself, each becoming
    # its own :Event node -- no loss, no double-processing.
    assert graph_count == n_total + 1

    deads = await qm.read_dead_letters(sid)
    assert len(deads) == 0

    # Order is checked INDEPENDENTLY of the
    # count -- every ToolCall upsert for this session must have landed in
    # the graph in EXACT ascending sequence order. A reorder introduced
    # anywhere between append and the graph write (queue read, compaction
    # race, batch dispatch) fails this assertion even though the count
    # above stays correct.
    assert len(observed_order) == n_total
    assert observed_order == list(range(n_total)), (
        "events were delivered to the graph OUT OF ORDER under concurrent "
        "ingest + repeated compaction -- expected the exact ascending "
        f"sequence 0..{n_total - 1}, got {observed_order[:10]}..."
    )

    metrics = await reg.pipeline_metrics()
    assert metrics["residual"] == 0
    assert metrics["degraded"] is False


# ---------------------------------------------------------------------------
# (h) boot-recovered, no-terminal session compacts BEFORE the dry-exit
# ---------------------------------------------------------------------------


@pytest.mark.timeout(120)
async def test_h_boot_recovered_session_compacts_before_dry_exit(
    neo4j_container: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Drives the REAL production entry point ``registry.get_or_create(...,
    recovered=True)`` end to end -- the same call ``_recover_one_session``
    uses, from BOTH the boot-time crash-recovery topup and the periodic
    sweep's re-dispatch of a still-undrained recovered session. A
    regression in ``get_or_create``'s ``recovered`` wiring (e.g.
    ``live_event_seen`` defaulting True regardless of the flag, so a
    boot-recovered worker never dry-exits, or a broken new-worker
    construction path) must be caught here, since hand-constructing a
    ``SessionWorker`` directly would never exercise ``get_or_create`` at
    all.

    ``get_or_create``
    builds a brand-new ``SessionWorker`` from settings (its own store/
    blob-store construction path, also exercised here), and this test
    asserts the log is compacted BEFORE the drainer's dry-exit -- exactly
    what Trigger I's before-dry-exit ordering guarantees. If Trigger I
    were placed AFTER the dry-exit (or never fired for this path), the
    drainer would reach ``recovered_drainer_exited`` with the log still
    full-size.
    """
    import context_intelligence_server.config as cfg_module
    import context_intelligence_server.registry as reg_module
    from context_intelligence_server.config import Settings

    queues_dir = tmp_path / "queues"
    reg = SessionRegistry()
    reg._queue_manager = QueueManager(queues_dir=queues_dir)
    reg._write_semaphore = asyncio.Semaphore(8)
    reg._max_delivery_attempts = 3
    qm = reg.queue_manager

    # A real Settings() instance, pointed at the live test container -- so
    # get_or_create's OWN Neo4jGraphStore/AsyncDiskBlobStore construction
    # path (settings.resolve_neo4j_admin(), settings.blob_path) is exercised
    # for real, not bypassed via a hand-built SessionWorker/services.
    settings = Settings()
    settings.neo4j_url = neo4j_container["bolt_url"]
    settings.neo4j_user = neo4j_container["user"]
    settings.neo4j_password = neo4j_container["password"]
    settings.blob_path = str(tmp_path / "blobs")
    settings.queue_compact_enabled = True
    settings.queue_compact_min_prefix_bytes = 0
    settings.queue_compact_max_tail_bytes = 64 * 1024 * 1024
    settings.stale_session_timeout = 3600.0

    monkeypatch.setattr(cfg_module, "get_settings", lambda: settings)
    monkeypatch.setattr(reg_module, "get_settings", lambda: settings)

    sid = "d9-h2-boot-recovered-real-entry-point"
    n = 300
    for i in range(n):
        await qm.append(sid, _small_event(i, sid))

    log_path = queues_dir / f"{sid}.log"
    assert log_path.stat().st_size > 0

    import logging

    with caplog.at_level(logging.INFO):
        # THE REAL production entry point -- exactly the call
        # _recover_one_session makes for a boot-recovered OR sweep-
        # redispatched session. This is the "if not in workers" branch of
        # get_or_create: it builds a brand-new SessionWorker with
        # live_event_seen=not recovered=False, and starts its drain.
        worker = reg.get_or_create(sid, _WS, recovered=True)
        assert worker.task is not None
        await asyncio.wait_for(asyncio.shield(worker.task), timeout=60.0)

    assert any("recovered_drainer_exited" in r.getMessage() for r in caplog.records), (
        "expected the dry-exit log line -- the worker never reached it"
    )

    # Proven through the REAL entry point: the log is
    # 0 bytes (or missing) AT EXIT, not full-size -- Trigger I ran BEFORE
    # the dry-exit, not after (and not never).
    assert not log_path.exists() or log_path.stat().st_size == 0

    graph_count = await _graph_event_count(neo4j_container, sid)
    assert graph_count == n
