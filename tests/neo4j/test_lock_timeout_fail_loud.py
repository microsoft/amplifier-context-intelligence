"""Fail-loud guarantee: a finite Neo4j lock-acquisition timeout makes a stuck
flush raise LOUDLY within a bound instead of blocking forever silently.

Background
----------
The v4.0.1 drain-stall incident was a SILENT infinite block: with Neo4j's default
``db.lock.acquisition.timeout=0`` a flush that could not acquire a node's write lock
waited forever, holding its write slot and never surfacing an error. Setting a
finite timeout converts that hang into a ``TransientError`` which the managed-tx
retry + drain loop surface LOUDLY (ERROR log, dead-letter, ``/status`` degraded),
and bounds starvation because a blocked flush releases its slot within the window.

These tests inject the exact fault: a side Neo4j session holds an EXCLUSIVE write
lock on a node, then the store is driven to flush a write to that same node. They
assert the decisive property: **bounded, loud failure — never an infinite silent
wait.**

They run against ``neo4j_container_lock_timeout`` (a real Neo4j 5.26 container with
``db.lock.acquisition.timeout=2s``) and use a low ``max_transaction_retry_time`` so
the bounded-failure path resolves in seconds. The shipped default is 30s
(docker-compose.yml); 2s here only shortens the same behaviour for test speed.
"""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import Any, Generator
from unittest.mock import patch

import pytest
from neo4j import AsyncGraphDatabase, GraphDatabase

from context_intelligence_server.neo4j_store import Neo4jGraphStore
from context_intelligence_server.registry import SessionRegistry, SessionWorker
from context_intelligence_server.services import HookStateService
from context_intelligence_server.queue_manager import QueueManager
from context_intelligence_server.dashboard import build_status_response

_WORKSPACE = "lock-test"
_LOCKED_NODE = "locked-node"
_FREE_NODE = "free-node"

# Bounded-failure ceiling. The lock timeout is 2s and the driver retry budget is
# ~2s, so the real flush failure surfaces in single-digit seconds. We assert the
# whole drive completes well under this ceiling — the point is it is FINITE, not
# the v4.0.1 infinite hang. A `wait_for` cap at this value turns any infinite
# block into a TimeoutError we can detect.
_BOUND_SECONDS = 30.0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _precreate_node(container: dict[str, Any], node_id: str) -> None:
    """Commit a Session node so a side session can take a write-lock on it."""
    driver = GraphDatabase.driver(
        container["bolt_url"], auth=(container["user"], container["password"])
    )
    try:
        with driver.session() as session:
            session.run(
                "MERGE (n:Session {node_id: $id, workspace: $ws}) "
                "SET n.precreated = true",
                id=node_id,
                ws=_WORKSPACE,
            ).consume()
    finally:
        driver.close()


def _cleanup_graph(container: dict[str, Any]) -> None:
    driver = GraphDatabase.driver(
        container["bolt_url"], auth=(container["user"], container["password"])
    )
    try:
        with driver.session() as session:
            session.run(
                "MATCH (n {workspace: $ws}) DETACH DELETE n", ws=_WORKSPACE
            ).consume()
    finally:
        driver.close()


class _LockHolder:
    """Hold an EXCLUSIVE write-lock on a node via an open, uncommitted tx.

    Entering runs ``MATCH (n) SET n.holder = ...`` (acquires the node write-lock)
    and leaves the transaction OPEN. Exiting rolls it back, releasing the lock.
    A separate, synchronous driver/connection so it is independent of the async
    store under test.
    """

    def __init__(self, container: dict[str, Any], node_id: str) -> None:
        self._container = container
        self._node_id = node_id
        self._driver: Any = None
        self._session: Any = None
        self._tx: Any = None

    def __enter__(self) -> "_LockHolder":
        self._driver = GraphDatabase.driver(
            self._container["bolt_url"],
            auth=(self._container["user"], self._container["password"]),
        )
        self._session = self._driver.session()
        self._tx = self._session.begin_transaction()
        # SET takes the node's EXCLUSIVE write-lock; tx stays open => lock held.
        self._tx.run(
            "MATCH (n {node_id: $id, workspace: $ws}) SET n.holder = timestamp()",
            id=self._node_id,
            ws=_WORKSPACE,
        ).consume()
        return self

    def __exit__(self, *exc: object) -> None:
        try:
            if self._tx is not None:
                self._tx.rollback()
        finally:
            if self._session is not None:
                self._session.close()
            if self._driver is not None:
                self._driver.close()


async def _low_retry_store(
    container: dict[str, Any], *, retry: float = 2.0
) -> Neo4jGraphStore:
    """Store against the lock-timeout container with a low managed-tx retry budget."""
    store = Neo4jGraphStore(
        uri=container["bolt_url"],
        auth=(container["user"], container["password"]),
        workspace=_WORKSPACE,
    )
    # Swap the default 30s-retry driver for a short one so the bounded-failure
    # path resolves in seconds (mirrors the orphan-test helper pattern).
    await store._driver.close()
    store._driver = AsyncGraphDatabase.driver(
        container["bolt_url"],
        auth=(container["user"], container["password"]),
        max_transaction_retry_time=retry,
    )
    store._schema_initialized = True  # skip schema bootstrap; node already exists
    return store


@pytest.fixture
def _clean_lock_graph(
    neo4j_container_lock_timeout: dict[str, Any],
) -> Generator[dict[str, Any], None, None]:
    """Yield the lock-timeout container info; wipe the test workspace after."""
    yield neo4j_container_lock_timeout
    _cleanup_graph(neo4j_container_lock_timeout)


# ---------------------------------------------------------------------------
# Gate 4 — NEVER SILENT: bounded, loud failure on lock-acquisition timeout
# ---------------------------------------------------------------------------


@pytest.mark.timeout(120)
async def test_lock_acquisition_timeout_flush_fails_loud_and_bounded(
    _clean_lock_graph: dict[str, Any],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A flush blocked on a held node-lock RAISES within a bound and logs ERROR.

    Decisive assertions:
      (a) bounded: the flush raises (does NOT hang) — wrapping it in
          asyncio.wait_for(_BOUND) yields the flush's OWN exception, never an
          asyncio.TimeoutError (which would mean an infinite silent block).
      (b) loud: ``flush_chunk_failed`` is logged at ERROR by neo4j_store.
    """
    container = _clean_lock_graph
    _precreate_node(container, _LOCKED_NODE)

    store = await _low_retry_store(container, retry=2.0)
    # Buffer a write to the locked node so the flush MUST acquire its write-lock.
    await store.upsert_node(
        _LOCKED_NODE, {"labels": ["Session"], "touched_by_flush": True}
    )

    raised: BaseException | None = None
    elapsed = 0.0
    with _LockHolder(container, _LOCKED_NODE):
        with caplog.at_level(logging.ERROR):
            t0 = time.monotonic()
            try:
                await asyncio.wait_for(store.flush(), timeout=_BOUND_SECONDS)
            except BaseException as exc:  # noqa: BLE001 - we classify it below
                raised = exc
            elapsed = time.monotonic() - t0

    await store.close()

    # (a) Something was raised, and it was NOT the wait_for timeout (= no hang).
    assert raised is not None, (
        "flush() returned without error while a node-lock was held — the write "
        "could not have succeeded; expected a bounded TransientError failure"
    )
    assert not isinstance(raised, asyncio.TimeoutError), (
        f"flush() HUNG past the {_BOUND_SECONDS}s bound (asyncio.TimeoutError) — "
        "this is the v4.0.1 silent infinite block, NOT a bounded loud failure"
    )
    # It raised well within the ceiling (bounded). Record the measured bound.
    assert elapsed < _BOUND_SECONDS, f"flush took {elapsed:.1f}s (>= bound)"

    # (b) The failure was LOUD: neo4j_store logged flush_chunk_failed at ERROR.
    error_msgs = [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]
    assert any("flush_chunk_failed" in m for m in error_msgs), (
        f"expected an ERROR 'flush_chunk_failed' log; got ERROR records: {error_msgs}"
    )
    # Surface the measured bound for the report.
    logging.getLogger(__name__).info(
        "MEASURED_BOUND flush raised in %.2fs (lock_timeout=2s, retry=2s)", elapsed
    )


@pytest.mark.timeout(120)
async def test_lock_timeout_dead_letters_and_surfaces_on_status(
    _clean_lock_graph: dict[str, Any],
    caplog: pytest.LogCaptureFixture,
    tmp_path: Path,
) -> None:
    """End-to-end: a lock-blocked flush dead-letters and surfaces on /status.

    Drives the REAL drain loop. With the node-lock held the drainer's flush
    fails every attempt, exhausts its delivery budget, dead-letters the line,
    and the failure is visible at ERROR and on the /status metrics block.

    Decisive assertions (within a bound — never an infinite hang):
      - the line is dead-lettered WITHIN the bound (dead_letter_total >= 1)
      - ``drain_batch_exhausted`` logged at ERROR
      - /status reports degraded True
      - the worker made no successful flush (last_successful_flush did not advance)

    Note: ``session:start`` is not a terminal event, so the drainer keeps
    idle-polling after dead-lettering rather than returning. We therefore poll
    for the dead-letter to surface (bounded) instead of waiting for task
    completion, then cancel the drainer.
    """
    container = _clean_lock_graph
    _precreate_node(container, _LOCKED_NODE)

    queues_dir = tmp_path / "queues"

    class _Proxy:
        queues_path = str(queues_dir)
        write_concurrency = 4
        max_delivery_attempts = 2  # short budget => fast exhaustion (ERROR at attempt 2)
        stale_session_timeout = 432000.0

    with patch(
        "context_intelligence_server.registry.get_settings", return_value=_Proxy()
    ):
        registry = SessionRegistry()
        qm = QueueManager(queues_dir=queues_dir)
        # Wire the registry to this QueueManager (same on-disk queue).
        registry._queue_manager = qm
        registry._write_semaphore = asyncio.Semaphore(4)
        registry._max_delivery_attempts = 2

        # Seed ONE event whose flush writes the locked node (session_id == node id
        # => ensure_session_node MERGEs that Session node => blocks on the lock).
        import json

        line = json.dumps(
            {
                "event": "session:start",
                "workspace": _WORKSPACE,
                "data": {
                    "session_id": _LOCKED_NODE,
                    "timestamp": "2024-01-01T00:00:00+00:00",
                },
            }
        ).encode()
        await qm.append(_LOCKED_NODE, line)

        store = await _low_retry_store(container, retry=2.0)
        services = HookStateService(workspace=_WORKSPACE, graph_store=store)
        worker = SessionWorker(
            session_id=_LOCKED_NODE, workspace=_WORKSPACE, services=services
        )
        flush_marker_before = worker.last_successful_flush
        registry._register_for_test(worker)

        dead_at: float | None = None
        with _LockHolder(container, _LOCKED_NODE):
            with caplog.at_level(logging.ERROR, logger="context_intelligence_server"):
                registry.start_drain(worker)
                assert worker.task is not None
                # Poll (bounded) for the dead-letter to surface — never wait forever.
                t0 = time.monotonic()
                deadline = t0 + _BOUND_SECONDS
                while time.monotonic() < deadline:
                    metrics = await registry.pipeline_metrics()
                    if metrics["dead_letter_total"] >= 1:
                        dead_at = time.monotonic()
                        break
                    await asyncio.sleep(0.25)

            # Stop the (non-terminal) drainer cleanly.
            if worker.task is not None and not worker.task.done():
                worker.task.cancel()
            await asyncio.gather(worker.task, return_exceptions=True)

        # (a) bounded: the line was dead-lettered within the ceiling, not hung.
        assert dead_at is not None, (
            f"lock-blocked line was NOT dead-lettered within {_BOUND_SECONDS}s — "
            "the failure did not surface in a bounded time"
        )

        # (b) loud: the drain loop logged drain_batch_exhausted at ERROR.
        assert "drain_batch_exhausted" in caplog.text, (
            "expected ERROR 'drain_batch_exhausted' after the lock-blocked flush "
            f"exhausted its delivery budget; caplog: {caplog.text[:500]}"
        )

        # (c) surfaces on /status. The /status endpoint composes
        # response["metrics"] = await registry.pipeline_metrics(), so the
        # dead-letter + degraded signal is exactly what an operator sees.
        metrics = await registry.pipeline_metrics()
        assert metrics["dead_letter_total"] >= 1, (
            f"expected the lock-blocked line to be dead-lettered; metrics={metrics}"
        )
        assert metrics["degraded"] is True, f"expected degraded=True; metrics={metrics}"

        # The worker never completed a successful flush (no silent ack).
        assert worker.last_successful_flush == flush_marker_before, (
            "last_successful_flush advanced despite every flush being lock-blocked"
        )

        # The per-session /status body surfaces this worker with its stale
        # (never-advanced) last_successful_flush — the offset-not-advancing signal.
        status = build_status_response(registry, time.time())
        session_ids = {s["session_id"] for s in status["sessions"]}
        assert _LOCKED_NODE in session_ids, (
            f"lock-blocked worker missing from /status sessions: {session_ids}"
        )
        locked_session = next(
            s for s in status["sessions"] if s["session_id"] == _LOCKED_NODE
        )
        assert locked_session["last_successful_flush"] == flush_marker_before, (
            "per-session /status last_successful_flush advanced despite lock-block"
        )

        await store.close()

        logging.getLogger(__name__).info(
            "MEASURED_BOUND dead-letter surfaced in %.2fs (lock_timeout=2s, retry=2s)",
            dead_at - t0,
        )


# ---------------------------------------------------------------------------
# Gate 2 — STARVATION BOUNDED: a lock-blocked drainer does not starve others
# ---------------------------------------------------------------------------


async def test_lock_contention_does_not_starve_other_drainers(
    _clean_lock_graph: dict[str, Any],
    tmp_path: Path,
) -> None:
    """Under write_concurrency=1, a lock-blocked drainer must not starve a free one.

    With only ONE global write slot, drainer A (writing the locked node) and
    drainer B (writing a free node) compete for it. If A's blocked flush held the
    slot forever (the db.lock.acquisition.timeout=0 incident), B would NEVER
    flush. With a finite lock timeout, A's flush releases the slot within the
    timeout window, so B commits within a bound. We assert B's free node reaches
    Neo4j within _BOUND — proving the lock timeout ALONE bounds starvation (no
    write-semaphore timeout needed).
    """
    container = _clean_lock_graph
    _precreate_node(container, _LOCKED_NODE)

    queues_dir = tmp_path / "queues"

    class _Proxy:
        queues_path = str(queues_dir)
        write_concurrency = 1  # single global write slot => contention is real
        max_delivery_attempts = 2
        stale_session_timeout = 432000.0

    import json

    def _event_line(session_id: str) -> bytes:
        return json.dumps(
            {
                "event": "session:start",
                "workspace": _WORKSPACE,
                "data": {
                    "session_id": session_id,
                    "timestamp": "2024-01-01T00:00:00+00:00",
                },
            }
        ).encode()

    with patch(
        "context_intelligence_server.registry.get_settings", return_value=_Proxy()
    ):
        registry = SessionRegistry()
        qm = QueueManager(queues_dir=queues_dir)
        registry._queue_manager = qm
        registry._write_semaphore = asyncio.Semaphore(1)  # the single slot
        registry._max_delivery_attempts = 2

        await qm.append(_LOCKED_NODE, _event_line(_LOCKED_NODE))
        await qm.append(_FREE_NODE, _event_line(_FREE_NODE))

        store_a = await _low_retry_store(container, retry=2.0)
        store_b = await _low_retry_store(container, retry=2.0)
        worker_a = SessionWorker(
            session_id=_LOCKED_NODE,
            workspace=_WORKSPACE,
            services=HookStateService(workspace=_WORKSPACE, graph_store=store_a),
        )
        worker_b = SessionWorker(
            session_id=_FREE_NODE,
            workspace=_WORKSPACE,
            services=HookStateService(workspace=_WORKSPACE, graph_store=store_b),
        )
        registry._register_for_test(worker_a)
        registry._register_for_test(worker_b)

        free_committed_at: float | None = None
        t0 = time.monotonic()
        with _LockHolder(container, _LOCKED_NODE):
            # Start BOTH drainers. A will block/retry on the locked node; B writes
            # the free node and must make progress despite sharing the 1 slot.
            registry.start_drain(worker_a)
            registry.start_drain(worker_b)

            # Poll for B's free node to land in Neo4j (proves B committed).
            check = GraphDatabase.driver(
                container["bolt_url"],
                auth=(container["user"], container["password"]),
            )
            try:
                deadline = t0 + _BOUND_SECONDS
                while time.monotonic() < deadline:
                    with check.session() as s:
                        rec = s.run(
                            "MATCH (n {node_id: $id, workspace: $ws}) RETURN count(n) AS c",
                            id=_FREE_NODE,
                            ws=_WORKSPACE,
                        ).single()
                    if rec is not None and rec["c"] >= 1:
                        free_committed_at = time.monotonic()
                        break
                    await asyncio.sleep(0.25)
            finally:
                check.close()

        # Stop drainers cleanly.
        for w in (worker_a, worker_b):
            if w.task is not None and not w.task.done():
                w.task.cancel()
        await asyncio.gather(
            *[w.task for w in (worker_a, worker_b) if w.task is not None],
            return_exceptions=True,
        )
        await store_a.close()
        await store_b.close()

    assert free_committed_at is not None, (
        f"free drainer B did NOT commit its node within {_BOUND_SECONDS}s while "
        "drainer A was lock-blocked — starvation was NOT bounded by the lock timeout"
    )
    bounded = free_committed_at - t0
    logging.getLogger(__name__).info(
        "MEASURED_BOUND free drainer committed in %.2fs under contention", bounded
    )
    assert bounded < _BOUND_SECONDS
