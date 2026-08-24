"""Neo4j-backed tests for single-terminal-label behaviour on session:end.

Covers that _handle_end does not flush on its own, that no flush ever runs
outside the drainer's write_semaphore, and that terminal data is durably
flushed before the queue log is deleted.

    uv run pytest tests/neo4j/test_handler_flush_concurrency.py -q -m neo4j
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
from neo4j import AsyncGraphDatabase

from context_intelligence_server.handlers.data_layer_2.session import SessionHandler
from context_intelligence_server.neo4j_store import Neo4jGraphStore, ensure_neo4j_schema
from context_intelligence_server.pipeline import process_event, setup_handlers
from context_intelligence_server.queue_manager import FileSystemQueueManager, QueueManager
from context_intelligence_server.registry import SessionRegistry, SessionWorker
from context_intelligence_server.services import HookStateService

pytestmark = pytest.mark.neo4j

_WS = "handler-flush"
_TS = "2026-01-01T10:00:00+00:00"
_TS2 = "2026-01-01T10:05:00+00:00"
_TS3 = "2026-01-01T10:10:00+00:00"


async def _neo4j_labels(services: Any, node_id: str) -> list[str]:
    rows = await services.graph.execute_query(
        "MATCH (n) WHERE n.node_id = $id AND n.workspace = $workspace "
        "RETURN labels(n) AS lbls",
        {"id": node_id, "workspace": services.graph.workspace},
        workspace="*",
    )
    return list(rows[0]["lbls"]) if rows else []


async def _neo4j_labels_and_props_via_container(
    container: dict[str, Any], workspace: str, node_id: str
) -> tuple[list[str], dict[str, Any]]:
    # Fresh driver: after finalize, the worker's own store/driver is closed.
    driver = AsyncGraphDatabase.driver(
        container["bolt_url"], auth=(container["user"], container["password"])
    )
    try:
        async with driver.session() as session:
            result = await session.run(
                "MATCH (n) WHERE n.node_id = $id AND n.workspace = $ws "
                "RETURN labels(n) AS lbls, properties(n) AS props",
                id=node_id,
                ws=workspace,
            )
            record = await result.single()
            if record is None:
                return [], {}
            return list(record["lbls"]), dict(record["props"])
    finally:
        await driver.close()


def _terminals(labels: list[str]) -> list[str]:
    return [
        label
        for label in labels
        if label in ("RootSession", "SubSession", "ForkedSession", "IncompleteSession")
    ]


def _line(event: str, workspace: str, data: dict[str, Any]) -> bytes:
    import json

    return json.dumps({"event": event, "workspace": workspace, "data": data}).encode(
        "utf-8"
    )


def _build_registry(queues_dir: Path, *, write_concurrency: int = 8) -> SessionRegistry:
    reg = SessionRegistry()
    reg._queue_manager = FileSystemQueueManager(queues_dir=queues_dir)
    reg._write_semaphore = asyncio.Semaphore(write_concurrency)
    reg._max_delivery_attempts = 3
    return reg


def _build_worker(container: dict[str, Any], sid: str) -> SessionWorker:
    store = Neo4jGraphStore(
        uri=container["bolt_url"],
        auth=(container["user"], container["password"]),
        workspace=_WS,
    )
    services = HookStateService(workspace=_WS, graph_store=store)
    return SessionWorker(session_id=sid, workspace=_WS, services=services)


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


class TestSingleTerminalLabel:
    async def test_same_batch_end_then_end_via_pipeline(
        self, neo4j_services: Any
    ) -> None:
        # Two session:end for the same session in one batch (no flush between)
        # must still leave exactly one terminal label.
        services = neo4j_services
        handlers = setup_handlers(services)
        worker = SessionWorker(session_id="worker", workspace=_WS, services=services)
        child_id = "child"

        await process_event(
            worker,
            "session:start",
            {"session_id": child_id, "parent_id": "parent", "timestamp": _TS},
            handlers,
        )
        await services.graph.flush()

        lbls_after_start = await _neo4j_labels(services, child_id)
        assert "SubSession" in lbls_after_start, lbls_after_start

        await process_event(
            worker, "session:end", {"session_id": child_id, "timestamp": _TS2}, handlers
        )
        await process_event(
            worker, "session:end", {"session_id": child_id, "timestamp": _TS3}, handlers
        )
        await services.graph.flush()

        terminals = _terminals(await _neo4j_labels(services, child_id))
        assert terminals == ["SubSession"], terminals

    async def test_same_batch_end_then_end_direct_handler(
        self, neo4j_services: Any
    ) -> None:
        # Same property via a direct handler call (no pipeline touch_session):
        # isolates the label-seed guard in _handle_end.
        services = neo4j_services
        handler = SessionHandler(services)
        child_id = "iso-child"

        await handler(
            "session:start",
            {"session_id": child_id, "parent_id": "iso-parent", "timestamp": _TS},
        )
        await services.graph.flush()

        lbls_after_start = await _neo4j_labels(services, child_id)
        assert "SubSession" in lbls_after_start, lbls_after_start

        await handler("session:end", {"session_id": child_id, "timestamp": _TS2})
        await handler("session:end", {"session_id": child_id, "timestamp": _TS3})
        await services.graph.flush()

        terminals = _terminals(await _neo4j_labels(services, child_id))
        assert terminals == ["SubSession"], terminals


@pytest.mark.timeout(60)
async def test_no_flush_outside_write_semaphore(
    neo4j_container: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    reg = _build_registry(tmp_path / "queues", write_concurrency=1)
    qm = reg.queue_manager
    sid = "fence"
    await qm.append(
        sid, _line("session:end", _WS, {"session_id": sid, "timestamp": _TS})
    )

    worker = _build_worker(neo4j_container, sid)
    reg._register_for_test(worker)

    observations: list[bool] = []
    real_flush = worker.services.graph.flush

    async def _spy_flush() -> None:
        # The teardown close()->flush() is un-gated by construction; only the
        # flushes during live drain must hold the semaphore.
        if not worker.store_closed:
            observations.append(reg.write_semaphore.locked())
        await real_flush()

    monkeypatch.setattr(worker.services.graph, "flush", _spy_flush)

    reg.start_drain(worker)
    assert worker.task is not None
    await asyncio.wait_for(worker.task, timeout=30.0)

    assert observations, "no flush was recorded, so the test proves nothing"
    assert all(observations), f"a flush ran with the semaphore unlocked: {observations}"


@pytest.mark.timeout(60)
async def test_terminal_data_durably_flushed_before_log_deleted(
    neo4j_container: dict[str, Any], tmp_path: Path
) -> None:
    reg = _build_registry(tmp_path / "queues", write_concurrency=8)
    qm = reg.queue_manager
    sid = "durability"
    await qm.append(
        sid,
        _line(
            "session:start",
            _WS,
            {"session_id": sid, "parent_id": "parent", "timestamp": _TS},
        ),
    )
    await qm.append(
        sid, _line("session:end", _WS, {"session_id": sid, "timestamp": _TS2})
    )

    log_path = tmp_path / "queues" / f"{sid}.log"
    offset_path = tmp_path / "queues" / f"{sid}.offset"
    assert log_path.stat().st_size > 0

    worker = _build_worker(neo4j_container, sid)
    reg._register_for_test(worker)
    reg.start_drain(worker)
    assert worker.task is not None
    await asyncio.wait_for(worker.task, timeout=30.0)

    # log + offset gone => finalize completed, which only deletes after the
    # tail was drained and flushed.
    assert not log_path.exists(), "log not deleted: finalize did not complete"
    assert not offset_path.exists(), "offset not deleted: finalize did not complete"

    final_labels, props = await _neo4j_labels_and_props_via_container(
        neo4j_container, _WS, sid
    )
    assert "SubSession" in final_labels, final_labels
    assert props.get("status") == "completed", props

    # ended_at returns as neo4j.time.DateTime from a raw session.run.
    ended_at = props.get("ended_at")
    _to_native = getattr(ended_at, "to_native", None)
    ended_at_native: Any = _to_native() if callable(_to_native) else ended_at
    assert ended_at_native == datetime.fromisoformat(_TS2), props
