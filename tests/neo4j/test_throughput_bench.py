"""Throughput bench + disk-reclaim/dead-letter evidence.

This is a MEASUREMENT / SANITY BENCH, not a tight unit test. It drives the
REAL ingest -> append -> drain -> Neo4j path (real ``QueueManager``, real
``SessionRegistry``/``SessionWorker``/``drain_worker``, real
``Neo4jGraphStore``) against the isolated throwaway Neo4j container the other
``tests/neo4j/`` tests already use. No mocks of the unit under test.

Two things are produced, printed in a RESULTS block (run with ``-s``), and
asserted GENEROUSLY (a hard threshold that flakes on slow CI hardware is
worse than a printed number an engineer can read):

PART A -- single-drainer throughput:
    A1. Burst: push N events at ONE session_id as fast as possible, then run
        the real drain worker to completion. Measures sustained drain
        throughput (events/sec) and reports the headroom multiple against
        the real measured production peak-minute burst of ~16 events/sec
        (see the earlier production-throughput investigation: whole-system
        peak ~10 ev/s, peak-minute burst ~16 ev/s, busiest single stream a
        few ev/s).
    A2. Steady-state: feed ~20 events/sec for a few seconds concurrently with
        a live drainer, and prove the backlog clears shortly after the feed
        stops -- i.e. drain rate >= feed rate, not just "eventually catches
        up given unlimited time".

PART B -- steady-state reclaim evidence (disk reclaim + dead-letter retention):
    B1. Reclaim-on-finalize: a session driven to session:end and fully
        drained has its .log/.offset files REMOVED from disk afterward
        (delete_drained, called from _finalize_session). This already works
        today -- proven here with a before/after file listing.
    B2. Drained-but-unfinalized lingering (the core gap): a session that
        ingests + fully drains many events to the graph but NEVER sends
        session:end keeps its full .log on disk indefinitely (delete_drained
        only ever runs from _finalize_session). Measured and printed in
        bytes -- this is the "queue must shrink as data is ingested, not
        only at session-end" gap steady-state reclaim needs to close.
    B3. Dead-letter accumulation: poisoned lines get dead-lettered, and the
        .dead.jsonl file is NEVER touched by drain or finalize -- it
        persists on disk indefinitely even after the session that produced
        it is fully finalized and reclaimed.

Run explicitly:
    cd amplifier-context-intelligence
    uv run pytest tests/neo4j/test_throughput_bench.py -q -s -m neo4j
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

_WS = "xu2-bench"
_TS = "2026-08-21T00:00:00+00:00"

# Real-traffic reference point measured against the live team-shared
# production graph during an earlier production-throughput investigation:
# whole-system peak-minute burst was ~16 events/sec (~941 events/minute);
# the busiest single stream
# sustained only a few events/sec. This is the number the bench sizes headroom
# against.
_REAL_PEAK_EV_S = 16.0


# ---------------------------------------------------------------------------
# Helpers (mirror the wiring in test_orphan_visibility.py / test_queues_actions.py)
# ---------------------------------------------------------------------------


def _line(event: str, workspace: str, data: dict[str, Any]) -> bytes:
    """Encode an appended event line exactly as POST /events stores it."""
    return json.dumps({"event": event, "workspace": workspace, "data": data}).encode(
        "utf-8"
    )


def _small_event(i: int, sid: str) -> bytes:
    """One realistic small event (~1-4 KiB incl. envelope, real-traffic shape).

    Alternates ``content_block:start`` (tiny streaming telemetry -- ~50% of
    real event volume per the live corpus) and ``tool:pre`` (a slightly
    larger, still-small tool-call event). Both are handled by real
    data_layer_2 enrichers with no other setup required.
    """
    if i % 2 == 0:
        return _line(
            "content_block:start",
            _WS,
            {
                "session_id": sid,
                "timestamp": _TS,
                "block_index": i,
                "block_type": "text",
            },
        )
    return _line(
        "tool:pre",
        _WS,
        {
            "session_id": sid,
            "timestamp": _TS,
            "tool_call_id": f"call-{sid}-{i}",
            "tool_name": "bash",
            "tool_input": "x" * 512,
        },
    )


def _build_registry(queues_dir: Path) -> SessionRegistry:
    """A fresh SessionRegistry wired to a tmp queue dir, no settings dependency.

    Mirrors test_queues_actions.py: workers are pre-registered via
    ``_register_for_test``, so ``get_or_create`` (which reads
    ``get_settings()`` -> production Neo4j) is never called.
    """
    reg = SessionRegistry()
    reg._queue_manager = QueueManager(queues_dir=queues_dir)
    # Matches the production default (config.py: write_concurrency = 8) so
    # the throughput measurement reflects the real write-concurrency cap.
    reg._write_semaphore = asyncio.Semaphore(8)
    reg._max_delivery_attempts = 3
    return reg


def _build_worker(container: dict[str, Any], sid: str) -> SessionWorker:
    """A real SessionWorker wired to the isolated Neo4j test container."""
    store = Neo4jGraphStore(
        uri=container["bolt_url"],
        auth=(container["user"], container["password"]),
        workspace=_WS,
    )
    services = HookStateService(workspace=_WS, graph_store=store)
    return SessionWorker(session_id=sid, workspace=_WS, services=services)


def _file_snapshot(queues_dir: Path, sid: str) -> dict[str, dict[str, Any]]:
    """Existence + size of a session's .log/.offset/.dead.jsonl on disk."""
    out: dict[str, dict[str, Any]] = {}
    for suffix, key in (
        (".log", "log"),
        (".offset", "offset"),
        (".dead.jsonl", "dead"),
    ):
        p = queues_dir / f"{sid}{suffix}"
        out[key] = {
            "exists": p.exists(),
            "bytes": p.stat().st_size if p.exists() else 0,
        }
    return out


async def _line_count(path: Path) -> int:
    if not path.exists():
        return 0

    def _count() -> int:
        with open(path, "rb") as f:
            return sum(1 for _ in f)

    return await asyncio.to_thread(_count)


async def _current_backlog(qm: QueueManager, sid: str) -> int:
    """Complete, uncommitted lines still pending for ``sid``."""
    batch = await qm.read_batch(sid, max_items=1_000_000)
    return len(batch.records)


def _zero_backlog_check(qm: QueueManager, sid: str):
    """Build a fresh async zero-arg predicate: True once ``sid``'s backlog is 0.

    A NEW coroutine must be created on every poll iteration (a coroutine
    object can only be awaited once) -- this returns a callable that builds
    one on each call, rather than a single pre-built coroutine.
    """

    async def _check() -> bool:
        return await _current_backlog(qm, sid) == 0

    return _check


async def _poll_until(predicate, *, timeout: float, interval: float = 0.05) -> bool:
    """Poll an async zero-arg predicate CALLABLE until truthy or timeout."""
    deadline = time.monotonic() + timeout
    while True:
        if await predicate():
            return True
        if time.monotonic() >= deadline:
            return False
        await asyncio.sleep(interval)


# ---------------------------------------------------------------------------
# The bench
# ---------------------------------------------------------------------------


@pytest.mark.timeout(300)
async def test_xu2_throughput_and_d9_disk_reclaim_evidence(
    neo4j_container: dict[str, Any],
    tmp_path: Path,
) -> None:
    """Throughput + scoping evidence, all against the real drain path."""
    queues_dir = tmp_path / "queues"
    reg = _build_registry(queues_dir)
    qm = reg._queue_manager
    assert qm is not None

    # Schema once for the whole bench (idempotent constraints) so write
    # performance is representative of a real deployment, not a bare MERGE
    # against an unindexed graph.
    schema_driver = AsyncGraphDatabase.driver(
        neo4j_container["bolt_url"],
        auth=(neo4j_container["user"], neo4j_container["password"]),
    )
    try:
        await ensure_neo4j_schema(schema_driver)
    finally:
        await schema_driver.close()

    results: dict[str, Any] = {}

    # =====================================================================
    # PART A1 -- burst throughput: one session_id, N events, pushed into the
    # queue as fast as possible, then drained to completion by the REAL
    # drain worker.
    # =====================================================================
    sid_burst = "xu2-burst"
    n_burst = 2000

    for i in range(n_burst):
        await qm.append(sid_burst, _small_event(i, sid_burst))
    await qm.append(
        sid_burst,
        _line("session:end", _WS, {"session_id": sid_burst, "timestamp": _TS}),
    )

    burst_before = _file_snapshot(queues_dir, sid_burst)

    worker_burst = _build_worker(neo4j_container, sid_burst)
    reg._register_for_test(worker_burst)

    drain_start = time.monotonic()
    reg.start_drain(worker_burst)
    assert worker_burst.task is not None, "start_drain must create worker.task"
    await asyncio.wait_for(asyncio.shield(worker_burst.task), timeout=180.0)
    drain_elapsed = time.monotonic() - drain_start

    throughput = n_burst / drain_elapsed if drain_elapsed > 0 else float("inf")
    headroom = throughput / _REAL_PEAK_EV_S

    # EOF reached, no permanent backlog: delete_drained (called from
    # _finalize_session) REFUSES to remove a log with uncommitted bytes
    # (queue_manager.py delete_drained docstring) -- so a fully-reclaimed
    # log/offset pair IS the proof the committed offset reached EOF.
    burst_after = _file_snapshot(queues_dir, sid_burst)
    assert not burst_after["log"]["exists"], (
        "burst session .log must be reclaimed after finalize -- if this "
        "fails, the drainer did not reach EOF (permanent backlog)"
    )
    assert not burst_after["offset"]["exists"], (
        "burst session .offset must be reclaimed after finalize"
    )

    results["A1_burst"] = {
        "events": n_burst,
        "elapsed_s": round(drain_elapsed, 3),
        "events_per_sec": round(throughput, 1),
        "headroom_vs_real_peak_16ev_s": round(headroom, 1),
        "before": burst_before,
        "after": burst_after,
    }

    # Generous sanity floor only -- this is a measurement bench.
    assert throughput > 0

    # =====================================================================
    # PART A2 -- steady-state: feed ~20 events/sec for a few seconds
    # concurrently with a live drainer; prove the backlog clears shortly
    # after the feed stops (drain rate >= feed rate for the real-shaped
    # single-stream case, not a one-shot flood).
    # =====================================================================
    sid_steady = "xu2-steady"
    feed_rate = 20.0
    feed_seconds = 5.0
    feed_interval = 1.0 / feed_rate
    n_steady = int(feed_rate * feed_seconds)

    worker_steady = _build_worker(neo4j_container, sid_steady)
    reg._register_for_test(worker_steady)
    reg.start_drain(worker_steady)
    assert worker_steady.task is not None

    backlog_samples: list[int] = []
    stop_monitor = asyncio.Event()

    async def _monitor() -> None:
        while not stop_monitor.is_set():
            backlog_samples.append(await _current_backlog(qm, sid_steady))
            await asyncio.sleep(0.1)

    monitor_task = asyncio.create_task(_monitor())

    feed_start = time.monotonic()
    for i in range(n_steady):
        await qm.append(sid_steady, _small_event(i, sid_steady))
        await asyncio.sleep(feed_interval)
    feed_elapsed = time.monotonic() - feed_start

    drain_after_feed_start = time.monotonic()
    cleared = await _poll_until(
        _zero_backlog_check(qm, sid_steady), timeout=10.0, interval=0.05
    )
    drain_after_feed_s = time.monotonic() - drain_after_feed_start

    stop_monitor.set()
    await monitor_task
    max_backlog = max(backlog_samples) if backlog_samples else 0

    assert cleared, (
        "backlog never cleared within 10s of the feed stopping -- the "
        "single drainer is NOT keeping up with a ~20 ev/s steady stream"
    )

    # Finish the steady session (session:end -> finalize) so it too proves
    # reclaim-on-finalize, and so its worker task exits cleanly.
    await qm.append(
        sid_steady,
        _line("session:end", _WS, {"session_id": sid_steady, "timestamp": _TS}),
    )
    await asyncio.wait_for(asyncio.shield(worker_steady.task), timeout=60.0)

    results["A2_steady_state"] = {
        "feed_rate_ev_s": feed_rate,
        "feed_seconds": feed_seconds,
        "events_fed": n_steady,
        "feed_elapsed_s": round(feed_elapsed, 3),
        "max_backlog_observed": max_backlog,
        "backlog_cleared_after_feed_s": round(drain_after_feed_s, 3),
    }

    # Generous: backlog must never reach anywhere near the full feed volume
    # (that would mean the drainer never even started keeping pace).
    assert max_backlog < n_steady

    # =====================================================================
    # PART B1 -- reclaim-on-finalize (already proven structurally by A1's
    # burst_after assertions above; restated here as explicit evidence
    # with the full before/after listing).
    # =====================================================================
    results["B1_reclaim_on_finalize"] = {
        "session": sid_burst,
        "before_finalize": burst_before,
        "after_finalize": burst_after,
        "reclaimed": (not burst_after["log"]["exists"])
        and (not burst_after["offset"]["exists"]),
    }

    # =====================================================================
    # PART B2 -- drained-but-unfinalized lingering (the core gap):
    # ingest + fully drain MANY events, but never send session:end.
    # =====================================================================
    sid_linger = "xu2-linger-unfinalized"
    n_linger = 500

    for i in range(n_linger):
        await qm.append(sid_linger, _small_event(i, sid_linger))

    linger_before = _file_snapshot(queues_dir, sid_linger)

    worker_linger = _build_worker(neo4j_container, sid_linger)
    reg._register_for_test(worker_linger)
    reg.start_drain(worker_linger)
    assert worker_linger.task is not None

    fully_drained = await _poll_until(
        _zero_backlog_check(qm, sid_linger), timeout=60.0, interval=0.05
    )
    assert fully_drained, (
        "drainer never caught up to EOF for the unfinalized-linger session"
    )

    linger_after = _file_snapshot(queues_dir, sid_linger)
    linger_line_count = await _line_count(queues_dir / f"{sid_linger}.log")

    # THE EVIDENCE: every event is fully committed (backlog == 0, i.e.
    # every line is already in Neo4j), yet the .log is NOT reclaimed --
    # because delete_drained only ever runs from _finalize_session, which
    # only runs on session:end. This session never sent one.
    assert linger_after["log"]["exists"], (
        "expected the drained-but-unfinalized .log to still be on disk -- "
        "this non-removal IS the gap being measured"
    )

    # Clean up: cancel the still-running drainer (mirrors
    # test_queues_actions.py's teardown) -- this routes through
    # drain_worker's CancelledError handler, which calls _safe_close (closes
    # the Neo4j driver) and deregisters the worker.
    worker_linger.task.cancel()
    try:
        await worker_linger.task
    except asyncio.CancelledError:
        pass

    results["B2_drained_but_unfinalized_lingering"] = {
        "session": sid_linger,
        "events_ingested": n_linger,
        "events_confirmed_in_graph": n_linger,  # backlog reached 0
        "log_before_bytes": linger_before["log"]["bytes"],
        "log_after_bytes": linger_after["log"]["bytes"],
        "log_after_line_count": linger_line_count,
        "still_on_disk": linger_after["log"]["exists"],
    }

    # =====================================================================
    # PART B3 -- dead-letter accumulation: poisoned lines get dead-lettered
    # and the .dead.jsonl is NEVER reclaimed, even after the session that
    # produced it is fully finalized.
    # =====================================================================
    sid_dead = "xu2-dead-letter"
    n_dead_good = 20
    n_dead_poison = 5

    for i in range(n_dead_good):
        await qm.append(sid_dead, _small_event(i, sid_dead))
    for j in range(n_dead_poison):
        # Guaranteed-unparseable line (not JSON at all).
        await qm.append(sid_dead, f"not-json-poison-{j}".encode())

    worker_dead = _build_worker(neo4j_container, sid_dead)
    reg._register_for_test(worker_dead)
    reg.start_drain(worker_dead)
    assert worker_dead.task is not None

    # Wait for the poison batch to be isolated (retried to exhaustion, then
    # dead-lettered line by line) -- backlog reaches 0 once every line
    # (good + poison) has been committed past.
    resolved = await _poll_until(
        _zero_backlog_check(qm, sid_dead), timeout=60.0, interval=0.05
    )
    assert resolved, "dead-letter isolation never resolved the poisoned batch"

    dead_before_finalize = _file_snapshot(queues_dir, sid_dead)
    dead_records_before = await qm.read_dead_letters(sid_dead)

    # Now send session:end as a FRESH append -- its own batch, so it goes
    # through the normal terminal-detection path (not the exhausted-batch
    # isolation path, which does not check for TERMINAL_EVENTS).
    await qm.append(
        sid_dead, _line("session:end", _WS, {"session_id": sid_dead, "timestamp": _TS})
    )
    await asyncio.wait_for(asyncio.shield(worker_dead.task), timeout=60.0)

    dead_after_finalize = _file_snapshot(queues_dir, sid_dead)
    dead_line_count = await _line_count(queues_dir / f"{sid_dead}.dead.jsonl")

    # THE EVIDENCE: log/offset ARE reclaimed (finalize ran normally), but
    # the dead-letter file is KEPT -- forever, with no mechanism anywhere
    # that purges it automatically.
    assert not dead_after_finalize["log"]["exists"], (
        "dead-letter session .log must be reclaimed"
    )
    assert not dead_after_finalize["offset"]["exists"], (
        "dead-letter session .offset must be reclaimed"
    )
    assert dead_after_finalize["dead"]["exists"], (
        ".dead.jsonl must still be on disk after finalize -- dead-letters "
        "are never auto-reclaimed (this IS the gap being measured)"
    )
    assert len(dead_records_before) == n_dead_poison

    results["B3_dead_letter_accumulation"] = {
        "session": sid_dead,
        "good_events": n_dead_good,
        "poison_events": n_dead_poison,
        "dead_letter_records": len(dead_records_before),
        "dead_letter_line_count": dead_line_count,
        "dead_letter_bytes_before_finalize": dead_before_finalize["dead"]["bytes"],
        "dead_letter_bytes_after_finalize": dead_after_finalize["dead"]["bytes"],
        "log_reclaimed_after_finalize": not dead_after_finalize["log"]["exists"],
        "dead_letter_reclaimed_ever": False,
    }

    # =====================================================================
    # RESULTS
    # =====================================================================
    print("\n" + "=" * 78)
    print("THROUGHPUT + EVIDENCE BENCH -- RESULTS")
    print("=" * 78)

    a1 = results["A1_burst"]
    print("\n-- PART A1: burst throughput (single drainer, single session_id) --")
    print(f"  events fed:              {a1['events']}")
    print(f"  drain wall time:         {a1['elapsed_s']}s")
    print(f"  sustained throughput:    {a1['events_per_sec']} events/sec")
    print(f"  headroom vs real 16 ev/s peak: {a1['headroom_vs_real_peak_16ev_s']}x")
    print(
        f"  .log/.offset reclaimed after finalize: {results['B1_reclaim_on_finalize']['reclaimed']}"
    )

    a2 = results["A2_steady_state"]
    print(
        "\n-- PART A2: steady-state (~20 ev/s feed for 5s, concurrent live drainer) --"
    )
    print(f"  events fed:              {a2['events_fed']} over {a2['feed_elapsed_s']}s")
    print(f"  max backlog observed:    {a2['max_backlog_observed']} events")
    print(
        f"  backlog cleared within:  {a2['backlog_cleared_after_feed_s']}s of feed stopping"
    )

    b1 = results["B1_reclaim_on_finalize"]
    print("\n-- PART B1: reclaim-on-finalize proof --")
    print(f"  session: {b1['session']}")
    print(f"  before finalize: {b1['before_finalize']}")
    print(f"  after  finalize: {b1['after_finalize']}")
    print(f"  reclaimed: {b1['reclaimed']}")

    b2 = results["B2_drained_but_unfinalized_lingering"]
    print("\n-- PART B2: drained-but-unfinalized lingering (the gap) --")
    print(f"  session: {b2['session']}")
    print(
        f"  {b2['events_confirmed_in_graph']} events fully committed to Neo4j "
        f"(backlog == 0), yet {b2['session']}.log still holds "
        f"{b2['log_after_bytes']} bytes ({b2['log_after_line_count']} lines) on disk"
    )
    print(f"  still on disk: {b2['still_on_disk']}")

    b3 = results["B3_dead_letter_accumulation"]
    print("\n-- PART B3: dead-letter accumulation --")
    print(f"  session: {b3['session']}")
    print(
        f"  {b3['poison_events']} poisoned lines dead-lettered "
        f"({b3['dead_letter_records']} records, {b3['dead_letter_bytes_after_finalize']} bytes)"
    )
    print(
        f"  log/offset reclaimed after finalize: {b3['log_reclaimed_after_finalize']}"
    )
    print(
        f"  dead-letter file reclaimed at any point: {b3['dead_letter_reclaimed_ever']}"
    )

    print("\n" + "=" * 78 + "\n")
