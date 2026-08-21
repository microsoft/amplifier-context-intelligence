"""D-DUW Neo4j-backed coverage: un-semaphored
``graph.flush()`` in ``SessionHandler._handle_end``.

Change 1: delete the 3-line terminal flush at the end of ``_handle_end``
(``session.py:361-363``) -- every byte it forced out is already flushed by
the drainer's unconditional, semaphore-gated ``_flush_barrier``.

Change 2: seed the buffered end-node's labels with the type labels that were
just READ (``end_node_data["labels"] = ["Session", "SST_EVENT", *labels]``)
so the buffer can never SHED a persisted terminal type once the handler no
longer clears the buffer via its own flush.

BOTH changes are REQUIRED (headline correction): the
dual-terminal-label defect (a same-batch ``session:end`` followed by another
lifecycle event for the same session yields BOTH a real terminal label AND
``IncompleteSession``) is measurably LIVE on the current tree, caused by
``pipeline.process_event``'s step 6 (``touch_session``) RE-POPULATING the
node buffer with a type-less entry after ANY flush (the handler's own, or a
test-driven one) empties it. Neither change alone fixes it.

Run explicitly:
    cd amplifier-context-intelligence
    uv run pytest tests/neo4j/test_handler_flush_concurrency.py -q -s -m neo4j
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
from context_intelligence_server.queue_manager import QueueManager
from context_intelligence_server.registry import SessionRegistry, SessionWorker
from context_intelligence_server.services import HookStateService

pytestmark = pytest.mark.neo4j

_WS = "duw-neo4j"
_TS = "2026-01-01T10:00:00+00:00"
_TS2 = "2026-01-01T10:05:00+00:00"
_TS3 = "2026-01-01T10:10:00+00:00"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _neo4j_labels(services: Any, node_id: str) -> list[str]:
    """Return labels from Neo4j directly (bypasses buffer)."""
    rows = await services.graph.execute_query(
        "MATCH (n) WHERE n.node_id = $id AND n.workspace = $workspace "
        "RETURN labels(n) AS lbls",
        {"id": node_id, "workspace": services.graph.workspace},
        workspace="*",
    )
    return list(rows[0]["lbls"]) if rows else []


async def _neo4j_props(services: Any, node_id: str) -> dict[str, Any]:
    """Return properties from Neo4j directly (bypasses buffer)."""
    rows = await services.graph.execute_query(
        "MATCH (n) WHERE n.node_id = $id AND n.workspace = $workspace "
        "RETURN properties(n) AS props",
        {"id": node_id, "workspace": services.graph.workspace},
        workspace="*",
    )
    return dict(rows[0]["props"]) if rows else {}


async def _neo4j_labels_and_props_via_container(
    container: dict[str, Any], workspace: str, node_id: str
) -> tuple[list[str], dict[str, Any]]:
    """Query Neo4j via a FRESH driver connection to the container.

    Used post-finalization, when the worker's own store/driver has already
    been closed by ``_safe_close`` -- reusing the closed store's driver
    raises ``DriverError: Driver closed``.
    """
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
    reg._queue_manager = QueueManager(queues_dir=queues_dir)
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


# ---------------------------------------------------------------------------
# T-e (headline, LIVE-defect) -- via the REAL pipeline process_event
# ---------------------------------------------------------------------------
#
# CG-4 (R5): "Change 1 + Change 2 together ELIMINATE a
# dual-terminal-label defect that is live on the current tree." This is the
# adverse-state test for that claim.


class TestDuwHeadlineLiveDefect:
    """Drives the full pipeline (process_event, including touch_session step
    6) for a same-batch session:end -> session:end pair, no intervening
    flush. This is what actually happens inside one drain-loop dispatch of
    ``_process_batch`` across two records for the same session.

    MUST measure: RED on the current (unmodified) tree, RED with Change 1
    alone, RED with Change 2 alone, GREEN only with BOTH.
    """

    async def test_e_same_batch_end_then_end_yields_single_terminal_label(
        self, neo4j_services: Any
    ) -> None:
        services = neo4j_services
        handlers = setup_handlers(services)
        worker = SessionWorker(
            session_id="duw-e-worker", workspace=_WS, services=services
        )
        parent_id = "duw-e-parent"
        child_id = "duw-e-child"

        # session:start(child, parent=P) -- sets SubSession.
        await process_event(
            worker,
            "session:start",
            {
                "session_id": child_id,
                "parent_id": parent_id,
                "timestamp": _TS,
            },
            handlers,
        )

        # flush -- as the drainer does between batches.
        await services.graph.flush()

        lbls_after_start = await _neo4j_labels(services, child_id)
        assert "SubSession" in lbls_after_start, (
            f"precondition: SubSession expected after start+flush; got {lbls_after_start}"
        )

        # session:end(child) -- first terminal event, SAME BATCH as the next.
        await process_event(
            worker,
            "session:end",
            {"session_id": child_id, "timestamp": _TS2},
            handlers,
        )

        # session:end(child) -- SECOND, later timestamp, NO intervening
        # flush before it (this is the same-batch condition: within one
        # _process_batch dispatch, no flush happens between records).
        await process_event(
            worker,
            "session:end",
            {"session_id": child_id, "timestamp": _TS3},
            handlers,
        )

        # Now flush (as the drainer's _flush_barrier eventually does) and
        # read the REAL persisted result.
        await services.graph.flush()

        final_labels = await _neo4j_labels(services, child_id)
        terminals = _terminals(final_labels)

        assert terminals == ["SubSession"], (
            f"dual-terminal-label defect: expected exactly one terminal label "
            f"SubSession, got {terminals} in {final_labels}"
        )


# ---------------------------------------------------------------------------
# T-e-isolator -- direct handler call, isolates whether Change 2 is
# load-bearing (bypasses touch_session entirely: proves the guard, not the
# live pipeline defect).
# ---------------------------------------------------------------------------


class TestDuwGuardIsolator:
    """Calls SessionHandler directly (no process_event/touch_session).

    Per R4: GREEN today (the handler's own flush clears the
    buffer so the second end's get_node falls through to Neo4j), RED with
    Change 1 alone (no self-flush AND no label-seed -- the buffer entry left
    behind by the first end() sheds the type), GREEN with BOTH.
    """

    async def test_e_isolator_direct_handler_no_intervening_flush(
        self, neo4j_services: Any
    ) -> None:
        services = neo4j_services
        handler = SessionHandler(services)
        parent_id = "duw-e-iso-parent"
        child_id = "duw-e-iso-child"

        await handler(
            "session:start",
            {
                "session_id": child_id,
                "parent_id": parent_id,
                "timestamp": _TS,
            },
        )
        await services.graph.flush()

        lbls_after_start = await _neo4j_labels(services, child_id)
        assert "SubSession" in lbls_after_start, (
            f"precondition: SubSession expected after start+flush; got {lbls_after_start}"
        )

        # First session:end -- direct handler call, no touch_session.
        await handler(
            "session:end",
            {"session_id": child_id, "timestamp": _TS2},
        )

        # Second session:end -- same batch, NO intervening flush.
        await handler(
            "session:end",
            {"session_id": child_id, "timestamp": _TS3},
        )

        await services.graph.flush()

        final_labels = await _neo4j_labels(services, child_id)
        terminals = _terminals(final_labels)

        assert terminals == ["SubSession"], (
            f"guard not load-bearing: expected exactly one terminal label "
            f"SubSession, got {terminals} in {final_labels}"
        )


# ---------------------------------------------------------------------------
# T-a (semaphore fence, corrected per R2) -- no flush occurs outside
# write_semaphore, EXCLUDING the teardown close() flush.
# ---------------------------------------------------------------------------
#
# CG-1 adverse-state test.


@pytest.mark.timeout(60)
async def test_a_no_flush_outside_write_semaphore(
    neo4j_container: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    queues_dir = tmp_path / "queues"
    reg = _build_registry(queues_dir, write_concurrency=1)
    qm = reg.queue_manager

    sid = "duw-a-fence"
    await qm.append(
        sid, _line("session:end", _WS, {"session_id": sid, "timestamp": _TS})
    )

    worker = _build_worker(neo4j_container, sid)
    reg._register_for_test(worker)

    observations: list[bool] = []
    real_flush = worker.services.graph.flush

    async def _spy_flush() -> None:
        # R2: stop recording once the worker's store has been
        # marked closed -- the teardown close()->flush() is a last-resort
        # net, un-gated by construction, and is NOT what this fence is
        # about (an un-gated flush DURING live drain is the bug).
        if not worker.store_closed:
            observations.append(reg.write_semaphore.locked())
        await real_flush()

    monkeypatch.setattr(worker.services.graph, "flush", _spy_flush)

    reg.start_drain(worker)
    assert worker.task is not None
    await asyncio.wait_for(worker.task, timeout=30.0)

    assert len(observations) >= 1, (
        "non-vacuity: the spy recorded no flush at all -- the test never "
        "reached a flush, so it cannot honestly prove anything"
    )
    assert all(observations), (
        f"a flush occurred with write_semaphore NOT locked (un-gated flush "
        f"outside the drainer's barrier): {observations}"
    )


# ---------------------------------------------------------------------------
# T-d (durability regression) -- terminal graph data is still durably
# flushed by the drainer under the semaphore, and the log is only deleted
# AFTER that flush succeeded. Must pass BOTH before and after Change 1.
# ---------------------------------------------------------------------------
#
# CG-2 adverse-state test.


@pytest.mark.timeout(60)
async def test_d_terminal_data_durably_flushed_before_log_deleted(
    neo4j_container: dict[str, Any], tmp_path: Path
) -> None:
    queues_dir = tmp_path / "queues"
    reg = _build_registry(queues_dir, write_concurrency=8)
    qm = reg.queue_manager

    sid = "duw-d-durability"
    parent_id = "duw-d-parent"
    await qm.append(
        sid,
        _line(
            "session:start",
            _WS,
            {"session_id": sid, "parent_id": parent_id, "timestamp": _TS},
        ),
    )
    await qm.append(
        sid,
        _line("session:end", _WS, {"session_id": sid, "timestamp": _TS2}),
    )

    log_path = queues_dir / f"{sid}.log"
    offset_path = queues_dir / f"{sid}.offset"
    assert log_path.stat().st_size > 0

    worker = _build_worker(neo4j_container, sid)
    reg._register_for_test(worker)
    reg.start_drain(worker)
    assert worker.task is not None
    await asyncio.wait_for(worker.task, timeout=30.0)

    # The queue log + offset were deleted -- proof finalize completed, which
    # only happens after delete_drained succeeds, which only happens after
    # the tail was drained-and-flushed (registry.py _finalize_session).
    assert not log_path.exists(), "log was not deleted -- finalize did not complete"
    assert not offset_path.exists(), (
        "offset was not deleted -- finalize did not complete"
    )

    # And the graph data genuinely reached the store: ended_at/status/label.
    # NOTE: the worker's own store/driver is already CLOSED by _safe_close at
    # this point (finalize completed) -- query via a fresh driver connection.
    final_labels, props = await _neo4j_labels_and_props_via_container(
        neo4j_container, _WS, sid
    )

    assert "SubSession" in final_labels, (
        f"terminal label missing from Neo4j after drain: {final_labels}"
    )
    assert props.get("status") == "completed", f"status missing/wrong: {props}"
    # ended_at comes back as a neo4j.time.DateTime (raw session.run, no
    # _normalize_temporal) -- convert via .to_native() before comparing,
    # mirroring neo4j_store._normalize_temporal's own getattr pattern.
    ended_at = props.get("ended_at")
    _to_native = getattr(ended_at, "to_native", None)
    ended_at_native: Any = _to_native() if callable(_to_native) else ended_at
    expected_ended_at = datetime.fromisoformat(_TS2)
    assert ended_at_native == expected_ended_at, f"ended_at missing/wrong: {props}"
