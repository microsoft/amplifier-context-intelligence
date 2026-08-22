"""Neo4j-backed tests for steady-state queue-log reclaim (compaction).

Uses the real QueueManager, SessionRegistry/SessionWorker/drain_worker, and
Neo4jGraphStore against an isolated throwaway container. Covers: a drained-idle
no-terminal session's committed prefix is reclaimed with all events preserved;
concurrent ingest during repeated compaction never drops or reorders events;
and a boot-recovered session is compacted before it dry-exits.

    uv run pytest tests/neo4j/test_steady_state_reclaim_neo4j.py -q -m neo4j
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

_WS = "reclaim"
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
    # tool_input carries the append ordinal so a test can detect reordering,
    # not just count loss. The check is int(tool_input), not string order.
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


@pytest.mark.timeout(120)
async def test_drained_idle_no_terminal_session_prefix_reclaimed(
    neo4j_container: dict[str, Any], tmp_path: Path
) -> None:
    queues_dir = tmp_path / "queues"
    reg = _build_registry(queues_dir)
    qm = reg.queue_manager

    sid = "drained-idle"
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

    assert await _poll_until(_drained, timeout=60.0), "drainer never reached backlog 0"

    async def _log_collapsed() -> bool:
        return not log_path.exists() or log_path.stat().st_size == 0

    assert await _poll_until(_log_collapsed, timeout=10.0), (
        "drained-idle .log did not collapse to its empty undrained tail"
    )

    assert await _current_backlog(qm, sid) == 0
    assert await _graph_event_count(neo4j_container, sid) == n

    # A manual invocation on the now-fully-compacted log reclaims nothing.
    assert await qm.compact_committed_prefix(sid, 0) == 0

    worker.task.cancel()
    try:
        await worker.task
    except asyncio.CancelledError:
        pass


@pytest.mark.timeout(180)
async def test_concurrent_ingest_during_repeated_compaction_no_loss(
    neo4j_container: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Verifies count conservation AND ordering: a spy wraps the real
    # upsert_node (still calling through) to record the delivery order of each
    # ToolCall's sequence marker, so a reorder that preserves the count is
    # still caught.
    import context_intelligence_server.config as cfg_module
    import context_intelligence_server.registry as reg_module
    from context_intelligence_server.neo4j_store import Neo4jGraphStore

    queues_dir = tmp_path / "queues"
    reg = _build_registry(queues_dir)
    qm = reg.queue_manager

    class _S:
        queue_compact_enabled = True
        queue_compact_min_prefix_bytes = 4096  # fires repeatedly mid-stream
        stale_session_timeout = 3600.0

    monkeypatch.setattr(cfg_module, "get_settings", lambda: _S())
    monkeypatch.setattr(reg_module, "get_settings", lambda: _S())

    sid = "concurrent"
    n_total = 400

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
        reg.record_accepted(1)
        if i % 23 == 0:
            await asyncio.sleep(0.005)  # interleave with the live drainer

    await qm.append(
        sid, _line("session:end", _WS, {"session_id": sid, "timestamp": _TS})
    )
    reg.record_accepted(1)
    await asyncio.wait_for(asyncio.shield(worker.task), timeout=120.0)

    # n_total tool:pre events + the session:end event, each its own :Event node.
    assert await _graph_event_count(neo4j_container, sid) == n_total + 1
    assert len(await qm.read_dead_letters(sid)) == 0

    assert len(observed_order) == n_total
    assert observed_order == list(range(n_total)), (
        f"events delivered out of order: {observed_order[:10]}..."
    )

    metrics = await reg.pipeline_metrics()
    assert metrics["residual"] == 0
    assert metrics["degraded"] is False


@pytest.mark.timeout(120)
async def test_boot_recovered_session_compacts_before_dry_exit(
    neo4j_container: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Drives the real get_or_create(..., recovered=True) entry point that
    # crash-recovery and the sweep use, and asserts the log is compacted
    # before the drainer's dry-exit (not after, not never).
    import logging

    import context_intelligence_server.config as cfg_module
    import context_intelligence_server.registry as reg_module
    from context_intelligence_server.config import Settings

    queues_dir = tmp_path / "queues"
    reg = SessionRegistry()
    reg._queue_manager = QueueManager(queues_dir=queues_dir)
    reg._write_semaphore = asyncio.Semaphore(8)
    reg._max_delivery_attempts = 3
    qm = reg.queue_manager

    # Real Settings pointed at the test container, so get_or_create's own
    # store/blob-store construction path runs for real.
    settings = Settings()
    settings.neo4j_url = neo4j_container["bolt_url"]
    settings.neo4j_user = neo4j_container["user"]
    settings.neo4j_password = neo4j_container["password"]
    settings.blob_path = str(tmp_path / "blobs")
    settings.queue_compact_enabled = True
    settings.queue_compact_min_prefix_bytes = 0
    settings.stale_session_timeout = 3600.0

    monkeypatch.setattr(cfg_module, "get_settings", lambda: settings)
    monkeypatch.setattr(reg_module, "get_settings", lambda: settings)

    sid = "boot-recovered"
    n = 300
    for i in range(n):
        await qm.append(sid, _small_event(i, sid))

    log_path = queues_dir / f"{sid}.log"
    assert log_path.stat().st_size > 0

    with caplog.at_level(logging.INFO):
        worker = reg.get_or_create(sid, _WS, recovered=True)
        assert worker.task is not None
        await asyncio.wait_for(asyncio.shield(worker.task), timeout=60.0)

    assert any("recovered_drainer_exited" in r.getMessage() for r in caplog.records), (
        "expected the dry-exit log line; the worker never reached it"
    )
    assert not log_path.exists() or log_path.stat().st_size == 0
    assert await _graph_event_count(neo4j_container, sid) == n
