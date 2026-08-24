"""Regression guards for the durable-cursor hardening round.

Each test drives the real code path of a distinct, execution-proven defect:
cross-handler run-id desync, cursor loss on offset-deletion paths, the
commit/compaction lock race, a corrupt-offset crash-loop, snapshot aliasing,
respawn double-dead-letter, and the session-node lost on isolation.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, cast

from context_intelligence_server.handlers.data_layer_2.iteration import (
    IterationHandler,
)
from context_intelligence_server.handlers.data_layer_2.orchestrator_run import (
    OrchestratorRunHandler,
)
from context_intelligence_server.queue_manager import FileSystemQueueManager
from context_intelligence_server.registry import SessionRegistry, SessionWorker
from context_intelligence_server.services import HookStateService


def _line(event: str, data: dict[str, Any]) -> bytes:
    return json.dumps({"event": event, "workspace": "/ws", "data": data}).encode(
        "utf-8"
    )


# ---------------------------------------------------------------------------
# Fix 1: cross-handler run-id consistency (E06 HAS_PART survives a rebuild)
# ---------------------------------------------------------------------------


async def test_e06_has_part_edge_survives_worker_rebuild(
    services: HookStateService,
) -> None:
    """After a rebuild leaves a partial cursor (active_orch_run_id lost, but
    execution_start_ts + orch_run_seq preserved), the Iteration handler must
    re-derive the run's real id and still wire the E06 HAS_PART edge, instead
    of falling through to the run-less shape and dropping the edge."""
    orch = OrchestratorRunHandler(services)
    it = IterationHandler(services)
    ts = "2026-01-01T00:00:00Z"

    await orch("execution:start", {"session_id": "s1", "timestamp": ts})
    run_id = f"s1::orch_run::{ts}::1"

    # Simulate the rebuild: the run is still active (execution_start_ts + seq
    # survived in the cursor) but the full id cursor was not restored.
    services.data_layer_2.active_orch_run_id = None

    await it("provider:request", {"session_id": "s1", "timestamp": ts})

    iteration_id = f"{run_id}::iteration::1"
    edge = await services.graph.get_edge(run_id, iteration_id)
    assert edge is not None, (
        "E06 HAS_PART edge dropped: the Iteration handler did not re-derive the "
        "run id after a partial-cursor rebuild"
    )
    assert edge["type"] == "HAS_PART"


async def test_content_block_inherits_run_scope_after_rebuild(
    services: HookStateService,
) -> None:
    """A ContentBlock whose active_iteration_id cursor was lost must reconstruct
    the run-scoped iteration key, not the run-less ``::iteration::0`` fallback."""
    orch = OrchestratorRunHandler(services)
    it = IterationHandler(services)
    ts = "2026-01-01T00:00:00Z"

    await orch("execution:start", {"session_id": "s1", "timestamp": ts})
    await it("provider:request", {"session_id": "s1", "timestamp": ts})
    run_id = f"s1::orch_run::{ts}::1"
    expected_iteration_id = f"{run_id}::iteration::1"

    resolved = services.data_layer_2.resolve_active_iteration_id("s1")
    assert resolved == expected_iteration_id

    # Drop the iteration cursor (partial rebuild) -- resolution must reconstruct
    # the same run-scoped key from run id + iteration_count.
    services.data_layer_2.active_iteration_id = None
    reconstructed = services.data_layer_2.resolve_active_iteration_id("s1")
    assert reconstructed == expected_iteration_id


# ---------------------------------------------------------------------------
# Fix 2: cursor survives the offset-deletion paths
# ---------------------------------------------------------------------------


async def test_reset_offset_preserves_cursor(tmp_path) -> None:
    """A RESET_OFFSET boot-reclaim (bad offset, re-drainable log) must reset the
    committed offset to 0 while PRESERVING the committed cursor."""
    qm = FileSystemQueueManager(queues_dir=tmp_path)
    sid = "s-reset"
    cursor = {"dl2": {"orch_run_seq": 4, "active_orch_run_id": "s-reset::r"}, "dl3": {}}

    await qm.append(sid, b"one-real-line")
    # A valid record whose offset points PAST end-of-log -> classify RESET_OFFSET
    # while the cursor stays perfectly readable.
    log_size = (tmp_path / f"{sid}.log").stat().st_size
    await qm.commit(sid, log_size + 10_000, cursor)

    classification = await qm.classify_session(sid, head_is_resumable=lambda _b: True)
    assert classification.verdict.value == "reset_offset"
    reclaimed = await qm.reclaim(classification, is_owned=lambda: False)
    assert reclaimed is True

    assert qm._read_committed_offset(sid) == 0  # re-drains from the start
    assert await qm.read_cursor(sid) == cursor  # cursor preserved across the reset


async def test_delete_drained_is_terminal_and_clears_offset(tmp_path) -> None:
    """delete_drained is terminal cleanup: it removes BOTH the log and the
    offset even when the cursor is non-empty. A finalized session's cursor is
    intentionally dropped -- an orch_run_id is scoped by execution_start_ts, so
    a genuinely-new post-finalize run gets a distinct id regardless of the seq
    counter, and keeping the offset would leak a file per finalized session."""
    qm = FileSystemQueueManager(queues_dir=tmp_path)
    sid = "s-final"
    line = b"a\n"
    await qm.append(sid, line)
    await qm.commit(sid, len(line), {"dl2": {"orch_run_seq": 9}, "dl3": {}})

    assert await qm.delete_drained(sid) is True

    assert not (tmp_path / f"{sid}.log").exists()
    assert not (tmp_path / f"{sid}.offset").exists()
    assert await qm.read_cursor(sid) is None


# ---------------------------------------------------------------------------
# Fix 3: commit serializes under the same per-key lock as compaction
# ---------------------------------------------------------------------------


async def test_commit_holds_file_lock_during_write(tmp_path, monkeypatch) -> None:
    """commit() must write the offset record under the per-key file_lock -- the
    same lock compaction takes -- so a commit concurrent with a compaction can
    never be silently erased. Verified by observing the lock is held at the
    moment commit writes the record."""
    qm = FileSystemQueueManager(queues_dir=tmp_path)
    sid = "s-lock"
    await qm.append(sid, b"a")

    observed: dict[str, bool] = {}
    real_write = qm._write_offset_record

    def _spy_write(session_id, offset, cursor):
        guard = qm._guards.get(session_id)
        observed["guard_exists"] = guard is not None
        observed["locked"] = bool(guard is not None and guard.file_lock.locked())
        return real_write(session_id, offset, cursor)

    monkeypatch.setattr(qm, "_write_offset_record", _spy_write)
    await qm.commit(sid, 2, {"dl2": {}, "dl3": {}})

    assert observed.get("guard_exists") is True
    assert observed.get("locked") is True, (
        "commit wrote the offset record without holding the per-key file_lock -- "
        "a concurrent compaction could erase it"
    )


# ---------------------------------------------------------------------------
# Fix 4: a corrupt .offset quarantines the worker, never crash-loops it
# ---------------------------------------------------------------------------


class _InertGraph:
    workspace = "/ws"
    created_by: str | None = None

    async def flush(self) -> None:  # pragma: no cover - never reached
        return None

    def discard_buffer(self) -> None:  # pragma: no cover
        return None

    async def close(self) -> None:
        return None


def _make_worker(sid: str, graph: Any) -> SessionWorker:
    worker = SessionWorker(
        session_id=sid, workspace="/ws", services=HookStateService(workspace="/ws")
    )
    worker.services.graph = graph  # type: ignore[assignment]
    return worker


async def test_corrupt_offset_quarantines_worker_without_dying(caplog) -> None:
    reg = SessionRegistry()
    qm = cast(FileSystemQueueManager, reg.queue_manager)
    sid = "s-corrupt"
    await qm.append(sid, _line("e1", {"session_id": sid}))
    # A genuinely corrupt offset document on disk: read_batch reads the offset
    # first and raises ValueError before the guarded cursor read is reached.
    qm._offset_path(sid).write_text('{"v":1,"offset":"NOT-AN-INT"}', encoding="utf-8")

    worker = _make_worker(sid, _InertGraph())
    reg._register_for_test(worker)

    with caplog.at_level(logging.ERROR, logger="context_intelligence_server"):
        # Must RETURN (quarantine), never raise out of the drain loop.
        await reg.drain_worker(worker, flush_timeout=0.05)

    assert any("drain_worker_quarantined" in r.getMessage() for r in caplog.records), (
        "a corrupt .offset must quarantine the worker with a loud log"
    )
    assert not any("drain_worker_died" in r.getMessage() for r in caplog.records), (
        "a corrupt .offset must never crash-loop the worker"
    )
    assert sid not in reg._workers  # deregistered cleanly


# ---------------------------------------------------------------------------
# Fix 5: restore_cursor deep-copies mutable fields (no snapshot aliasing)
# ---------------------------------------------------------------------------


async def test_restore_cursor_does_not_alias_snapshot_mutables(
    services: HookStateService,
) -> None:
    """restore_cursor must deep-copy mutable fields so a later in-place mutation
    of the LIVE state cannot corrupt the snapshot replayed on the next retry."""
    snapshot = {
        "dl2": {"pending_tool_block_ids": {"b1": "n1"}},
        "dl3": {"active_recipe_run_stack": ["r1"]},
    }
    services.restore_cursor(snapshot)

    # A handler mutates the live containers in place on a later attempt.
    services.data_layer_2.pending_tool_block_ids["b2"] = "n2"
    services.data_layer_3.active_recipe_run_stack.append("r2")

    # The snapshot (the pre_batch_cursor baseline) must be untouched.
    assert snapshot["dl2"]["pending_tool_block_ids"] == {"b1": "n1"}
    assert snapshot["dl3"]["active_recipe_run_stack"] == ["r1"]

    # And a second restore reproduces the identical baseline, not the mutation.
    services.restore_cursor(snapshot)
    assert services.data_layer_2.pending_tool_block_ids == {"b1": "n1"}
    assert services.data_layer_3.active_recipe_run_stack == ["r1"]


# ---------------------------------------------------------------------------
# Fix 6: a respawn does not re-dead-letter an already-dead leading record
# ---------------------------------------------------------------------------


class _PoisonGraph:
    """Flush always succeeds; the poison is injected in the handler."""

    workspace = "/ws"
    created_by: str | None = None

    def __init__(self) -> None:
        self.flushed = 0

    async def flush(self) -> None:
        self.flushed += 1

    def discard_buffer(self) -> None:
        return None

    async def close(self) -> None:
        return None


async def test_respawn_does_not_double_dead_letter(monkeypatch) -> None:
    reg = SessionRegistry()
    qm = cast(FileSystemQueueManager, reg.queue_manager)
    reg._max_delivery_attempts = 1
    sid = "s-respawn"

    e1 = _line("e1", {"session_id": sid})  # poison
    e2 = _line("e2", {"session_id": sid})  # good
    await qm.append(sid, e1)
    await qm.append(sid, e2)

    # Reconstruct the exact post-crash state: e1 was dead-lettered but the
    # offset was never advanced past it (the crash landed in the
    # dead_letter -> commit gap). dead_letter() marks the session
    # dead-unreconciled in-memory, so the NEXT read_batch reconciles past e1
    # once (the internalized dirty-flag path, no registry-level call).
    await qm.dead_letter(sid, e1, "poison")
    assert qm._read_committed_offset(sid) == 0

    async def _handler(w, event, data, handlers):
        if event == "e1":
            raise RuntimeError("poison record")

    worker = _make_worker(sid, _PoisonGraph())
    reg._register_for_test(worker)

    with monkeypatch.context() as m:
        m.setattr(
            "context_intelligence_server.registry.process_event",
            _handler,
        )
        reg.start_drain(worker)
        for _ in range(200):
            await asyncio.sleep(0.01)
            if (await qm.read_batch(sid, 10)).lines == []:
                break
        task = worker.task
        if task is not None and not task.done():
            task.cancel()
        try:
            if task is not None:
                await task
        except asyncio.CancelledError:
            pass

    dead = await qm.read_dead_letters(sid)
    payloads = [d.get("payload") for d in dead]
    assert payloads.count(e1.decode("utf-8")) == 1, (
        f"e1 was dead-lettered {payloads.count(e1.decode('utf-8'))} times; a "
        "respawn must reconcile the already-dead leading record, not re-dead-letter it"
    )


# ---------------------------------------------------------------------------
# Fix 7: the Session node survives an exhausted-batch isolation
# ---------------------------------------------------------------------------


class _BufferedGraph:
    """Models Neo4jGraphStore's buffer/flush split: upsert_node BUFFERS, flush
    promotes the buffer to the store, discard_buffer DROPS the buffer, and
    get_node reads buffer-first. The in-memory GraphState has no such split, so
    only a faithful buffered fake can exercise the isolation buffer-discard."""

    workspace = "/ws"
    created_by: str | None = None

    def __init__(self) -> None:
        self._store: dict[str, dict[str, Any]] = {}
        self._buffer: dict[str, dict[str, Any]] = {}

    async def upsert_node(self, node_id: str, data: dict[str, Any]) -> None:
        node = self._buffer.setdefault(node_id, {})
        if "labels" in data:
            node["labels"] = sorted(set(node.get("labels", [])) | set(data["labels"]))
        for k, v in data.items():
            if k != "labels":
                node[k] = v

    async def get_node(self, node_id: str) -> dict[str, Any] | None:
        if node_id in self._buffer:
            return dict(self._buffer[node_id])
        if node_id in self._store:
            return dict(self._store[node_id])
        return None

    async def upsert_edge(self, *args: Any) -> None:
        return None

    async def flush(self) -> None:
        for nid, data in self._buffer.items():
            self._store.setdefault(nid, {}).update(data)
        self._buffer.clear()

    def discard_buffer(self) -> None:
        self._buffer.clear()

    async def close(self) -> None:
        return None


async def test_session_node_survives_isolation(monkeypatch) -> None:
    """When a failed batch is isolated line-by-line, the buffer discard must
    also invalidate the seen-session cache, so the re-dispatched session:start
    re-issues the Session node (status/started_at) instead of early-returning."""
    reg = SessionRegistry()
    sid = "s-node"
    ts = "2026-02-02T00:00:00Z"

    worker = _make_worker(sid, _BufferedGraph())
    reg._register_for_test(worker)

    async def _handler(w, event, data, handlers):
        if event == "session:start":
            await w.services.ensure_session_node(data["session_id"], data)
        elif event == "poison":
            raise RuntimeError("poison")

    # Prime the exact pre-isolation condition: the failed batch already
    # dispatched session:start, buffering the node and caching the id (the node
    # is NOT yet flushed to the store).
    await worker.services.ensure_session_node(sid, {"session_id": sid, "timestamp": ts})
    assert sid in worker.services._seen_sessions
    pre_batch_cursor = worker.services.snapshot_cursor()

    from context_intelligence_server.queue_manager.protocol import Batch, Record

    batch = Batch(
        session_id=sid,
        records=[
            Record(_line("session:start", {"session_id": sid, "timestamp": ts}), 0, 40),
            Record(_line("poison", {"session_id": sid}), 40, 80),
        ],
        start_offset=0,
        end_offset=80,
    )

    with monkeypatch.context() as m:
        m.setattr("context_intelligence_server.registry.process_event", _handler)
        await reg._handle_exhausted_batch(
            worker, batch, handlers=None, pre_batch_cursor=pre_batch_cursor
        )

    node = await worker.services.graph.get_node(sid)
    assert node is not None, "Session node was lost on isolation"
    assert node.get("status") == "running"
    assert node.get("started_at") == ts
