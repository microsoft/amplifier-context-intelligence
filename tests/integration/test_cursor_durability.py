"""Integration -- durable handler cursor across a worker rebuild (I5b).

Exercises the REAL, unmocked handler pipeline (``setup_handlers`` +
``process_event``) against the default in-memory ``GraphState``, so **no
Neo4j is required** -- this file runs under the repo's no-Neo4j gate
(``uv run pytest tests/ -q``, AGENTS.md).

``tests/integration/test_crash_recovery.py`` patches
``context_intelligence_server.registry.process_event``; with dispatch
mocked, no handler ever touches ``DataLayer2State``/``DataLayer3State``, so
that suite is structurally incapable of catching cursor loss. This file
deliberately does **not** patch ``process_event`` -- the whole point is to
prove the cross-handler cursor (``execution_start_ts``, ``iteration_count``,
``pending_tool_block_ids``, ...) survives a worker rebuild.

Because ``GraphState`` is an in-memory, per-``HookStateService`` store (not a
shared backing store like Neo4j), a simulated worker rebuild uses a FRESH
``HookStateService`` -- exactly as it would in production, where the new
worker's in-process state starts empty and only the persisted ``.offset``
cursor bridges the gap. Assertions therefore check each worker's own graph
for the nodes/edges IT was responsible for creating, never a private
``GraphState._nodes`` dict -- always through the public
``await worker.services.graph.get_node(...)`` / ``get_edge(...)`` accessors.
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

from context_intelligence_server import registry as registry_module
from context_intelligence_server.pipeline import setup_handlers
from context_intelligence_server.queue_manager import QueueManager
from context_intelligence_server.registry import SessionRegistry, SessionWorker
from context_intelligence_server.services import HookStateService

WORKSPACE = "/ws"

# make_node_id (data_layer_1 default handler, invoked for EVERY event) parses
# "timestamp" as ISO-8601, so all event timestamps below must be valid ISO
# strings -- unlike orch_run_id/iteration_id, which embed the raw string
# verbatim and impose no format requirement of their own.
T0 = "2026-01-01T00:00:00+00:00"
T1 = "2026-01-01T00:00:01+00:00"
EXEC_START_TS = "2026-01-01T00:00:02+00:00"  # execution:start's timestamp
T3 = "2026-01-01T00:00:03+00:00"
T4 = "2026-01-01T00:00:04+00:00"
T5 = "2026-01-01T00:00:05+00:00"
T5A = "2026-01-01T00:00:06+00:00"
T5B = "2026-01-01T00:00:07+00:00"
T6 = "2026-01-01T00:00:08+00:00"
T7 = "2026-01-01T00:00:09+00:00"
T8 = "2026-01-01T00:00:10+00:00"


def _line(event: str, data: dict) -> bytes:
    return json.dumps({"event": event, "workspace": WORKSPACE, "data": data}).encode(
        "utf-8"
    )


def _first_triplet(sid: str) -> list[bytes]:
    """session:start -> prompt -> execution:start -> provider/llm triplet."""
    return [
        _line("session:start", {"session_id": sid, "timestamp": T0}),
        _line("prompt:submit", {"session_id": sid, "timestamp": T1, "prompt": "hi"}),
        _line("execution:start", {"session_id": sid, "timestamp": EXEC_START_TS}),
        _line("provider:request", {"session_id": sid, "timestamp": T3}),
        _line(
            "llm:request",
            {
                "session_id": sid,
                "timestamp": T4,
                "provider": "anthropic",
                "model": "claude",
            },
        ),
        _line(
            "llm:response",
            {
                "session_id": sid,
                "timestamp": T5,
                "usage": {"input_tokens": 10, "output_tokens": 5},
            },
        ),
    ]


def _second_triplet(sid: str) -> list[bytes]:
    """A second provider/llm triplet, driven AFTER the state-reset boundary."""
    return [
        _line("provider:request", {"session_id": sid, "timestamp": T6}),
        _line(
            "llm:request",
            {
                "session_id": sid,
                "timestamp": T7,
                "provider": "anthropic",
                "model": "claude",
            },
        ),
        _line(
            "llm:response",
            {
                "session_id": sid,
                "timestamp": T8,
                "usage": {"input_tokens": 3, "output_tokens": 2},
            },
        ),
    ]


def _worker(sid: str) -> SessionWorker:
    return SessionWorker(
        session_id=sid,
        workspace=WORKSPACE,
        services=HookStateService(workspace=WORKSPACE),
    )


async def _drain_to_idle(
    reg: SessionRegistry, worker: SessionWorker, timeout: float = 5.0
) -> None:
    """Start drain_worker and poll until the log is fully committed, then cancel.

    "Idle" (queue empty) is the natural point at which a real process could
    be interrupted between batches -- exactly the T1 (crash-restart) and T2
    (stale-reap) triggers described in the spec, both of which occur once a
    batch has already committed and the drainer is polling an empty log.
    """
    task = asyncio.create_task(reg.drain_worker(worker, flush_timeout=10.0))
    deadline = time.monotonic() + timeout
    reached_idle = False
    while time.monotonic() < deadline:
        await asyncio.sleep(0.01)
        if (await reg.queue_manager.read_batch(worker.session_id, 10)).lines == []:
            reached_idle = True
            break
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    if not reached_idle:
        raise AssertionError("drain did not reach idle within timeout")


# ---------------------------------------------------------------------------
# TC-1 -- T1: crash restart
# ---------------------------------------------------------------------------


async def test_tc1_crash_restart_preserves_cursor() -> None:
    sid = "cursor-tc1"
    reg1 = SessionRegistry()
    for raw in _first_triplet(sid):
        await reg1.queue_manager.append(sid, raw)

    w1 = _worker(sid)
    reg1._register_for_test(w1)
    await _drain_to_idle(reg1, w1)

    node1 = f"{sid}::orch_run::{EXEC_START_TS}::iteration::1"
    assert await w1.services.graph.get_node(node1) is not None
    assert await w1.services.graph.get_node(f"{sid}::iteration::1") is None
    assert w1.services.data_layer_2.iteration_count == 1

    # "Crash": w1/reg1 are simply discarded (never deregistered) -- a fresh
    # SessionRegistry over the SAME on-disk queues dir simulates a process
    # restart recovering from the persisted .offset cursor.
    reg2 = SessionRegistry()
    for raw in _second_triplet(sid):
        await reg2.queue_manager.append(sid, raw)

    w2 = _worker(sid)
    reg2._register_for_test(w2)
    await _drain_to_idle(reg2, w2)

    node2 = f"{sid}::orch_run::{EXEC_START_TS}::iteration::2"
    assert await w2.services.graph.get_node(node2) is not None
    assert await w2.services.graph.get_node(f"{sid}::iteration::2") is None
    # iteration_count CONTINUED at 2 -- no reset back to 1.
    assert w2.services.data_layer_2.iteration_count == 2
    assert w2.services.data_layer_2.execution_start_ts == EXEC_START_TS


# ---------------------------------------------------------------------------
# TC-2 -- T2: stale-worker reap
# ---------------------------------------------------------------------------


async def test_tc2_stale_reap_preserves_cursor() -> None:
    sid = "cursor-tc2"
    reg = SessionRegistry()
    for raw in _first_triplet(sid):
        await reg.queue_manager.append(sid, raw)

    w1 = _worker(sid)
    reg._register_for_test(w1)
    await _drain_to_idle(reg, w1)

    # Simulate the stale-reap path: _deregister pops the worker WITHOUT
    # touching .log/.offset (registry.py _deregister) -- the queue files
    # survive on disk exactly as they would after a real stale-session reap.
    reg._deregister(sid)

    for raw in _second_triplet(sid):
        await reg.queue_manager.append(sid, raw)

    w2 = _worker(sid)
    reg._register_for_test(w2)
    await _drain_to_idle(reg, w2)

    node2 = f"{sid}::orch_run::{EXEC_START_TS}::iteration::2"
    orch_run_id = f"{sid}::orch_run::{EXEC_START_TS}"
    assert await w2.services.graph.get_node(node2) is not None
    assert await w2.services.graph.get_node(f"{sid}::iteration::1") is None
    assert await w2.services.graph.get_node(f"{sid}::iteration::2") is None
    # E06 HAS_PART edge from the SAME orchestrator run -- proves
    # execution_start_ts (not just iteration_count) was carried through.
    edge = await w2.services.graph.get_edge(orch_run_id, node2)
    assert edge is not None
    assert edge["type"] == "HAS_PART"


# ---------------------------------------------------------------------------
# TC-3 -- sibling cursor state (pending_tool_block_ids / E09) survives
# ---------------------------------------------------------------------------


async def test_tc3_pending_tool_block_ids_survives_rebuild() -> None:
    sid = "cursor-tc3"
    reg1 = SessionRegistry()
    lines = _first_triplet(sid) + [
        _line(
            "content_block:start",
            {"session_id": sid, "timestamp": T5A, "block_index": 0},
        ),
        _line(
            "content_block:end",
            {
                "session_id": sid,
                "timestamp": T5B,
                "block_index": 0,
                "block": {"type": "tool_call", "id": "toolblock-1"},
            },
        ),
    ]
    for raw in lines:
        await reg1.queue_manager.append(sid, raw)

    w1 = _worker(sid)
    reg1._register_for_test(w1)
    await _drain_to_idle(reg1, w1)

    block_node_id = f"{sid}::block::1::0"
    assert w1.services.data_layer_2.pending_tool_block_ids == {
        "toolblock-1": block_node_id
    }

    # Rebuild: fresh registry + fresh HookStateService (new in-memory graph),
    # restoring ONLY from the persisted cursor.
    reg2 = SessionRegistry()
    await reg2.queue_manager.append(
        sid,
        _line(
            "tool:pre",
            {
                "session_id": sid,
                "timestamp": T6,
                "tool_call_id": "toolblock-1",
                "tool_name": "bash",
            },
        ),
    )
    w2 = _worker(sid)
    reg2._register_for_test(w2)
    await _drain_to_idle(reg2, w2)

    # E09: ContentBlock -[:CAUSED]-> ToolCall -- only possible if
    # pending_tool_block_ids was restored from the persisted cursor.
    edge = await w2.services.graph.get_edge(block_node_id, "toolblock-1")
    assert edge is not None
    assert edge["type"] == "CAUSED"


# ---------------------------------------------------------------------------
# TC-4 -- atomicity: offset and cursor always come from the SAME JSON write
# ---------------------------------------------------------------------------


async def test_tc4_offset_and_cursor_written_atomically() -> None:
    sid = "cursor-tc4"
    reg = SessionRegistry()
    for raw in _first_triplet(sid):
        await reg.queue_manager.append(sid, raw)

    w = _worker(sid)
    reg._register_for_test(w)
    await _drain_to_idle(reg, w)

    settings = registry_module.get_settings()
    offset_path = Path(settings.queues_path) / f"{sid}.offset"
    record = json.loads(offset_path.read_text(encoding="utf-8"))
    assert record["v"] == 1
    assert isinstance(record["offset"], int) and record["offset"] > 0
    assert record["cursor"]["dl2"]["iteration_count"] == 1
    assert record["cursor"]["dl2"]["execution_start_ts"] == EXEC_START_TS

    # The manager's own reader agrees exactly with the raw file -- there is
    # only ever ONE record, never a separate cursor file that could skew.
    committed, cursor = reg.queue_manager._read_offset_record(sid)
    assert committed == record["offset"]
    assert cursor == record["cursor"]


# ---------------------------------------------------------------------------
# TC-4b -- crash-before-commit replay idempotence (edge preservation)
# ---------------------------------------------------------------------------


async def test_tc4b_crash_before_commit_replay_is_idempotent() -> None:
    sid = "cursor-tc4b"
    reg = SessionRegistry()
    lines = _first_triplet(sid) + [
        _line(
            "content_block:start",
            {"session_id": sid, "timestamp": T5A, "block_index": 0},
        ),
        _line(
            "content_block:end",
            {
                "session_id": sid,
                "timestamp": T5B,
                "block_index": 0,
                "block": {"type": "tool_call", "id": "toolblock-2"},
            },
        ),
    ]
    for raw in lines:
        await reg.queue_manager.append(sid, raw)

    # Simulate a crash AFTER dispatch but BEFORE commit: drive the batch
    # through the exact same dispatch step drain_worker uses
    # (SessionRegistry._process_batch), then never call commit -- the
    # offset file never advances past 0.
    batch = await reg.queue_manager.read_batch(sid, max_items=100)
    w_crashed = _worker(sid)
    handlers = setup_handlers(w_crashed.services)
    await reg._process_batch(w_crashed, batch, handlers)

    node_id = f"{sid}::orch_run::{EXEC_START_TS}::iteration::1"
    assert await w_crashed.services.graph.get_node(node_id) is not None
    assert (await reg.queue_manager.read_batch(sid, 10)).lines != []  # uncommitted

    # Re-drain from scratch (fresh worker, offset still 0, cursor still
    # None) via the REAL drain loop -- this is the replay.
    w_replay = _worker(sid)
    reg._register_for_test(w_replay)
    await _drain_to_idle(reg, w_replay)

    replayed_node = await w_replay.services.graph.get_node(node_id)
    assert replayed_node is not None
    # Identical node_id, identical iteration_number -- no double-increment
    # from replaying a batch the crashed attempt already touched in memory.
    assert replayed_node["iteration_number"] == 1
    assert w_replay.services.data_layer_2.iteration_count == 1

    # E09 edge preserved across the crash-before-commit boundary too.
    block_node_id = f"{sid}::block::1::0"
    edge = await w_replay.services.graph.get_edge(block_node_id, "toolblock-2")
    assert edge is None or edge["type"] == "CAUSED"  # created on tool:pre, not here
    # No tool:pre in this script; the important, load-bearing assertion is
    # that pending_tool_block_ids itself round-tripped through the replay:
    assert w_replay.services.data_layer_2.pending_tool_block_ids == {
        "toolblock-2": block_node_id
    }


# ---------------------------------------------------------------------------
# TC-5 -- legacy bare-integer .offset compatibility
# ---------------------------------------------------------------------------


async def test_tc5_legacy_bare_int_offset_compat() -> None:
    sid = "cursor-tc5"
    reg = SessionRegistry()
    first = _line("session:start", {"session_id": sid, "timestamp": T0})
    second = _line(
        "prompt:submit", {"session_id": sid, "timestamp": T1, "prompt": "hi"}
    )
    await reg.queue_manager.append(sid, first)
    await reg.queue_manager.append(sid, second)

    # Pre-write a bare-integer offset (pre-upgrade shape) covering only the
    # first (newline-terminated) line.
    settings = registry_module.get_settings()
    offset_path = Path(settings.queues_path) / f"{sid}.offset"
    first_line_len = len(first) + 1  # +1 for the newline append() adds
    offset_path.write_text(str(first_line_len), encoding="utf-8")

    committed, cursor = reg.queue_manager._read_offset_record(sid)
    assert committed == first_line_len
    assert cursor is None

    w = _worker(sid)
    reg._register_for_test(w)
    await _drain_to_idle(reg, w)  # no exception; drains the remaining line

    # The next commit rewrites the file in the new JSON form.
    text = offset_path.read_text(encoding="utf-8").strip()
    assert text.startswith("{")
    record = json.loads(text)
    assert record["v"] == 1
    assert record["offset"] == first_line_len + len(second) + 1


# ---------------------------------------------------------------------------
# TC-6 -- cursor is JSON-round-trippable (regression guard for future fields)
# ---------------------------------------------------------------------------


def test_tc6_cursor_json_round_trip() -> None:
    baseline = HookStateService(workspace=WORKSPACE)
    snapshot = baseline.snapshot_cursor()

    # JSON-safety regression guard: every field must survive a JSON round-trip.
    round_tripped = json.loads(json.dumps(snapshot))
    assert round_tripped == snapshot

    mutated = HookStateService(workspace=WORKSPACE)
    mutated.data_layer_2.iteration_count = 99
    mutated.data_layer_2.execution_start_ts = "should-be-overwritten"
    mutated.restore_cursor(round_tripped)

    assert mutated.data_layer_2 == baseline.data_layer_2
    assert mutated.data_layer_3 == baseline.data_layer_3


# ---------------------------------------------------------------------------
# TC-8 -- recovery_reconcile_dead preserves the cursor (R2 guard)
# ---------------------------------------------------------------------------


async def test_tc8_recovery_reconcile_dead_preserves_cursor() -> None:
    # DO NOT skip or xfail this test (spec \u00a710.4): recovery_reconcile_dead is
    # the ONLY offset writer besides commit(). If it ever drops the cursor,
    # a startup that skips a dead-lettered line silently wipes cross-handler
    # state on the recovery path -- the exact bug this change fixes,
    # reintroduced through the one writer that isn't commit().
    sid = "cursor-tc8"
    settings = registry_module.get_settings()
    qm = QueueManager(queues_dir=Path(settings.queues_path))
    line = _line("provider:request", {"session_id": sid, "timestamp": T0})
    await qm.append(sid, line)

    cursor = {
        "dl2": {
            "execution_start_ts": "2026-01-01T00:00:09+00:00",
            "active_iteration_id": None,
            "pending_tool_block_ids": {},
            "last_prompt_id": None,
            "last_completed_orch_run_id": None,
            "iteration_count": 7,
        },
        "dl3": {"active_recipe_run_stack": [], "active_recipe_step_id": None},
    }
    # Commit a real cursor at offset 0 (line not yet consumed), then
    # dead-letter the pending line so recovery_reconcile_dead has something
    # to skip past -- mirroring the dead_letter-then-crash-before-commit window.
    await qm.commit(sid, 0, cursor)
    await qm.dead_letter(sid, line, "boom")

    skipped = await qm.recovery_reconcile_dead()
    assert skipped == 1

    new_offset, new_cursor = qm._read_offset_record(sid)
    assert new_offset == len(line) + 1  # +1 for the newline append() added
    assert new_cursor == cursor  # cursor carried through UNMODIFIED


# ---------------------------------------------------------------------------
# TC-9 -- delete_drained removes the cursor (no stale-restore on a recycled key)
# ---------------------------------------------------------------------------


async def test_tc9_delete_drained_removes_cursor() -> None:
    sid = "cursor-tc9"
    settings = registry_module.get_settings()
    qm = QueueManager(queues_dir=Path(settings.queues_path))
    await qm.commit(sid, 10, {"dl2": {"iteration_count": 3}, "dl3": {}})
    assert await qm.read_cursor(sid) is not None

    await qm.delete_drained(sid)
    assert await qm.read_cursor(sid) is None


# ---------------------------------------------------------------------------
# BLOCKER-1 -- phantom-cursor guard (R6xD1): a dead-lettered line's in-memory
# cursor mutation must not survive into the committed cursor snapshot.
# ---------------------------------------------------------------------------


async def test_blocker1_dead_lettered_line_does_not_leak_cursor_mutation() -> None:
    """A line that fails ALL flush retries is dead-lettered and its graph
    write is discarded (``_handle_exhausted_batch``) -- but ``IterationHandler``
    mutates ``active_iteration_id``/``iteration_count`` in memory BEFORE the
    graph write (``iteration.py`` ~86-100). Without a guard, the unconditional
    ``qm.commit(..., worker.services.snapshot_cursor())`` at the end of every
    iteration of the per-line loop persists that mutation anyway, so the
    durable cursor ends up pointing at a node that was NEVER written -- a
    phantom that survives a restart (R6, promoted to BLOCKER-1 by persisting
    the cursor at all -- see spec \u00a710.1).
    """
    sid = "cursor-blocker1"
    reg = SessionRegistry()
    w = _worker(sid)
    handlers = setup_handlers(w.services)

    # Clean baseline: session:start -> prompt -> execution:start, drained
    # normally so execution_start_ts is set while iteration_count /
    # active_iteration_id are still at their untouched defaults.
    setup_lines = [
        _line("session:start", {"session_id": sid, "timestamp": T0}),
        _line("prompt:submit", {"session_id": sid, "timestamp": T1, "prompt": "hi"}),
        _line("execution:start", {"session_id": sid, "timestamp": EXEC_START_TS}),
    ]
    for raw in setup_lines:
        await reg.queue_manager.append(sid, raw)
    reg._register_for_test(w)
    await _drain_to_idle(reg, w)

    pre_cursor = await reg.queue_manager.read_cursor(sid)
    assert pre_cursor is not None
    assert pre_cursor["dl2"]["iteration_count"] == 0
    assert pre_cursor["dl2"]["active_iteration_id"] is None

    # The poison line: provider:request mutates active_iteration_id /
    # iteration_count BEFORE its graph write (iteration.py _handle_provider_
    # request). Force that write to fail -- standing in for a flush that
    # exhausted every retry -- so the line is routed down the dead-letter/
    # discard path exactly as _handle_exhausted_batch runs it once
    # drain_worker's batch-level retry budget is spent.
    poison_raw = _line("provider:request", {"session_id": sid, "timestamp": T3})
    await reg.queue_manager.append(sid, poison_raw)
    batch = await reg.queue_manager.read_batch(sid, max_items=10)
    assert batch.lines == [poison_raw]

    # IterationHandler is an ENRICHER (pipeline step 5), dispatched AFTER the
    # DefaultHandler's own Event-node upsert_node call (step 4). Only fail the
    # write that creates the Iteration node itself (identified by its
    # ``labels``) -- otherwise the boom fires on the DefaultHandler's raw
    # Event-node write first and IterationHandler's mutation (which happens
    # BEFORE its own upsert_node call) never even runs, which would not
    # reproduce the phantom at all.
    phantom_iteration_id = f"{sid}::orch_run::{EXEC_START_TS}::iteration::1"
    original_upsert_node = w.services.graph.upsert_node

    async def _boom_upsert_node(node_id: str, data: dict) -> None:
        if "Iteration" in (data.get("labels") or []):
            raise RuntimeError("simulated write failure (flush retries exhausted)")
        await original_upsert_node(node_id, data)

    w.services.graph.upsert_node = _boom_upsert_node  # type: ignore[method-assign]

    await reg._handle_exhausted_batch(w, batch, handlers)

    # The write really never landed -- confirms this is the discard path,
    # not a passing line.
    assert await w.services.graph.get_node(phantom_iteration_id) is None
    dead_letters = await reg.queue_manager.read_dead_letters(sid)
    assert len(dead_letters) == 1

    # THE BUG (BLOCKER-1): the committed cursor must NOT carry the
    # dead-lettered line's mutation forward -- it must match the clean
    # pre-line snapshot, not point at the never-written node.
    persisted = await reg.queue_manager.read_cursor(sid)
    assert persisted is not None
    assert persisted["dl2"]["iteration_count"] == 0, (
        "phantom cursor: iteration_count advanced past a dead-lettered line"
    )
    assert persisted["dl2"]["active_iteration_id"] is None, (
        "phantom cursor: active_iteration_id points at a never-written node"
    )


async def test_blocker1_successful_line_after_poison_keeps_its_own_mutation() -> None:
    """Selectivity guard: only the DISCARDED line's mutation rolls back. A
    line that succeeds -- even immediately after a dead-lettered one in the
    same exhausted-batch pass -- must keep its own cursor mutation and its
    own graph write. Proves the fix rolls back per-line, not the whole batch.
    """
    sid = "cursor-blocker1b"
    reg = SessionRegistry()
    w = _worker(sid)
    handlers = setup_handlers(w.services)

    setup_lines = [
        _line("session:start", {"session_id": sid, "timestamp": T0}),
        _line("prompt:submit", {"session_id": sid, "timestamp": T1, "prompt": "hi"}),
        _line("execution:start", {"session_id": sid, "timestamp": EXEC_START_TS}),
    ]
    for raw in setup_lines:
        await reg.queue_manager.append(sid, raw)
    reg._register_for_test(w)
    await _drain_to_idle(reg, w)

    poison_raw = _line("provider:request", {"session_id": sid, "timestamp": T3})
    good_raw = _line("provider:request", {"session_id": sid, "timestamp": T4})
    await reg.queue_manager.append(sid, poison_raw)
    await reg.queue_manager.append(sid, good_raw)
    batch = await reg.queue_manager.read_batch(sid, max_items=10)
    assert batch.lines == [poison_raw, good_raw]

    # Fail only the FIRST Iteration-node creation call (the poisoned line's).
    # Identified by ``labels`` rather than call order or node_id, since the
    # good line's computed node_id depends on whether the guard rolled the
    # counter back (that dependency is exactly what this test proves).
    original_upsert_node = w.services.graph.upsert_node
    iteration_upserts = 0

    async def _flaky_upsert_node(node_id: str, data: dict) -> None:
        nonlocal iteration_upserts
        if "Iteration" in (data.get("labels") or []):
            iteration_upserts += 1
            if iteration_upserts == 1:
                raise RuntimeError("simulated write failure (flush retries exhausted)")
        await original_upsert_node(node_id, data)

    w.services.graph.upsert_node = _flaky_upsert_node  # type: ignore[method-assign]

    await reg._handle_exhausted_batch(w, batch, handlers)

    dead_letters = await reg.queue_manager.read_dead_letters(sid)
    assert len(dead_letters) == 1

    # The final committed cursor reflects ONLY the successful line's
    # mutation: the counter advanced exactly once (0 -> 1), not twice --
    # this only holds if the poisoned line's increment was rolled back
    # before the good line ran.
    persisted = await reg.queue_manager.read_cursor(sid)
    assert persisted is not None
    assert persisted["dl2"]["iteration_count"] == 1

    # The successful line's own node must exist and carry iteration_number 1
    # (not 2) -- proving its mutation was NOT disturbed by the earlier
    # line's rollback.
    good_iteration_id = persisted["dl2"]["active_iteration_id"]
    assert good_iteration_id is not None
    good_node = await w.services.graph.get_node(good_iteration_id)
    assert good_node is not None
    assert good_node["iteration_number"] == 1


# ---------------------------------------------------------------------------
# BLOCKER-2 -- iteration_scope completeness (spec §10.1): dead-letter ->
# enrichment ordering. The Iteration node is upsert_node'd from THREE sites
# (provider:request, llm:request, llm:response); iteration_scope must be
# stamped at all three so a dead-lettered provider:request followed by a
# surviving llm:request/llm:response can never leave the node with neither
# value.
# ---------------------------------------------------------------------------


async def test_blocker2_dead_lettered_provider_request_then_surviving_llm_events() -> (
    None
):
    """Drive a provider:request down the dead-letter path -- exactly as
    BLOCKER-1's own reproduction does (the Iteration-node write fails, is
    dead-lettered, and BLOCKER-1's phantom-cursor guard rolls
    ``active_iteration_id`` back to its pre-line value) -- then append a
    'surviving' llm:request/llm:response pair for the same run, as a client
    that is unaware the provider:request failed server-side would.

    The invariant under test: whatever the Iteration node ends up looking
    like -- present or genuinely absent -- it must NEVER be present without a
    valid ``iteration_scope``. That is the literal defect BLOCKER-2 exists to
    close: a node written by one of the three sites (llm:request/llm:response)
    that has properties but no scope at all, because only provider:request
    used to stamp it.

    SURPRISE (see task's evidence requirements): with BLOCKER-1's rollback
    already in place, ``active_iteration_id`` resets to ``None`` (this run's
    pre-line value, since this is the run's first iteration) once the
    provider:request line is dead-lettered. ``_handle_llm_request`` /
    ``_handle_llm_response`` both early-return when the cursor is ``None``
    (iteration.py:163-165, :198-200), so in THIS exact ordering no phantom
    node is created at all -- the node is legitimately absent, not
    present-but-scopeless. BLOCKER-1 and BLOCKER-2 therefore compose as
    defense-in-depth: BLOCKER-1 prevents the enrichers from writing to a
    phantom id in the first place; BLOCKER-2 additionally guarantees that IF
    they ever do write (e.g. a future change loosens BLOCKER-1's rollback, or
    active_iteration_id survives because a PRIOR iteration in the same run
    was the one dead-lettered instead of the first), the node still cannot
    end up scope-less.
    """
    sid = "cursor-blocker2"
    reg = SessionRegistry()
    w = _worker(sid)
    handlers = setup_handlers(w.services)

    setup_lines = [
        _line("session:start", {"session_id": sid, "timestamp": T0}),
        _line("prompt:submit", {"session_id": sid, "timestamp": T1, "prompt": "hi"}),
        _line("execution:start", {"session_id": sid, "timestamp": EXEC_START_TS}),
    ]
    for raw in setup_lines:
        await reg.queue_manager.append(sid, raw)
    reg._register_for_test(w)
    await _drain_to_idle(reg, w)

    # Dead-letter the provider:request: its own Iteration-node write fails,
    # standing in for flush retries exhausted (BLOCKER-1's exact setup).
    phantom_iteration_id = f"{sid}::orch_run::{EXEC_START_TS}::iteration::1"
    original_upsert_node = w.services.graph.upsert_node

    async def _boom_upsert_node(node_id: str, data: dict) -> None:
        if "Iteration" in (data.get("labels") or []):
            raise RuntimeError("simulated write failure (flush retries exhausted)")
        await original_upsert_node(node_id, data)

    w.services.graph.upsert_node = _boom_upsert_node  # type: ignore[method-assign]

    poison_raw = _line("provider:request", {"session_id": sid, "timestamp": T3})
    await reg.queue_manager.append(sid, poison_raw)
    batch = await reg.queue_manager.read_batch(sid, max_items=10)
    assert batch.lines == [poison_raw]
    await reg._handle_exhausted_batch(w, batch, handlers)

    dead_letters = await reg.queue_manager.read_dead_letters(sid)
    assert len(dead_letters) == 1
    # BLOCKER-1 confirmed: the cursor was rolled back, not left phantom.
    assert w.services.data_layer_2.active_iteration_id is None
    assert await w.services.graph.get_node(phantom_iteration_id) is None

    # Restore the real upsert_node -- a "surviving" llm:request/llm:response
    # pair, emitted by a client that doesn't know the provider:request failed
    # server-side, must be able to write normally.
    w.services.graph.upsert_node = original_upsert_node  # type: ignore[method-assign]

    for raw in (
        _line(
            "llm:request",
            {
                "session_id": sid,
                "timestamp": T4,
                "provider": "anthropic",
                "model": "claude",
            },
        ),
        _line(
            "llm:response",
            {
                "session_id": sid,
                "timestamp": T5,
                "usage": {"input_tokens": 1, "output_tokens": 1},
            },
        ),
    ):
        await reg.queue_manager.append(sid, raw)
    tail_batch = await reg.queue_manager.read_batch(sid, max_items=10)
    await reg._process_batch(w, tail_batch, handlers)
    await reg._flush_barrier(w)
    await reg.queue_manager.commit(
        sid, tail_batch.end_offset, w.services.snapshot_cursor()
    )

    # THE INVARIANT: never present-but-scopeless. In this repo's current code
    # (BLOCKER-1 active) the node is legitimately absent -- see the docstring
    # "SURPRISE" note above.
    node = await w.services.graph.get_node(phantom_iteration_id)
    assert node is None, (
        "with BLOCKER-1's rollback active, active_iteration_id is None so "
        "the surviving llm:* events must early-return and write nothing -- "
        f"if this now fails, BLOCKER-1's guard regressed. Got node: {node!r}"
    )
    if node is not None:  # pragma: no cover -- documents the invariant even
        # if BLOCKER-1's rollback semantics ever change so a write DOES land.
        assert node.get("iteration_scope") in ("run", "unscoped"), (
            f"Iteration node present without a valid iteration_scope: {node!r}"
        )


async def test_blocker2_llm_events_stamp_scope_when_cursor_survives() -> None:
    """Direct, ordering-independent proof of the completeness fix: even
    without going through the dead-letter machinery at all, if
    ``active_iteration_id`` ever points at a node that llm:request/
    llm:response are the FIRST to write (simulating any path -- present or
    future -- by which the cursor mutation outlives the node's own creation
    write), the enrichers stamp iteration_scope on their own, independent of
    provider:request. Uses ``IterationHandler`` directly (not the full
    registry/drain machinery) since this is a targeted unit-level proof of
    sites 2 and 3, not a durability/ordering scenario.
    """
    from context_intelligence_server.handlers.data_layer_2.iteration import (
        IterationHandler,
    )

    sid = "cursor-blocker2b"
    services = HookStateService(workspace=WORKSPACE)
    handler = IterationHandler(services)

    # No execution:start -- unscoped branch.
    services.data_layer_2.active_iteration_id = f"{sid}::iteration::99"
    await handler(
        "llm:request",
        {
            "session_id": sid,
            "timestamp": T4,
            "provider": "anthropic",
            "model": "claude",
        },
    )
    node = await services.graph.get_node(f"{sid}::iteration::99")
    assert node is not None
    assert node.get("iteration_scope") == "unscoped"

    # execution_start_ts now set -- run-scoped branch, llm:response site.
    services.data_layer_2.execution_start_ts = EXEC_START_TS
    run_scoped_id = f"{sid}::orch_run::{EXEC_START_TS}::iteration::7"
    services.data_layer_2.active_iteration_id = run_scoped_id
    await handler(
        "llm:response",
        {
            "session_id": sid,
            "timestamp": T5,
            "usage": {"input_tokens": 1, "output_tokens": 1},
        },
    )
    node2 = await services.graph.get_node(run_scoped_id)
    assert node2 is not None
    assert node2.get("iteration_scope") == "run"
