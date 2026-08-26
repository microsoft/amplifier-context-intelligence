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
import sys
import threading
import time
from pathlib import Path
from typing import Any, cast

import pytest
from context_intelligence_server.handlers.data_layer_2.iteration import (
    IterationHandler,
)
from context_intelligence_server.handlers.data_layer_2.orchestrator_run import (
    OrchestratorRunHandler,
)
from context_intelligence_server.queue_manager import FileSystemQueueManager
from context_intelligence_server.registry import (
    SessionRegistry,
    SessionWorker,
    _SessionQuarantined,
)
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


# ---------------------------------------------------------------------------
# Fix 8: every path that moves a session's .offset serialises on its file_lock
# ---------------------------------------------------------------------------


async def _commit_racing_reconcile(queues_dir: Path) -> tuple[int, dict | None]:
    """Run one commit concurrent with the read_batch dead-letter reconcile.

    Both move the same session's ``.offset``: the commit writes it, and the
    reconcile read-modify-writes it. Returns the record left on disk.
    """
    qm = FileSystemQueueManager(queues_dir=queues_dir)
    sid = "s-race"
    poison = b"poison-line"

    await qm.append(sid, poison)
    for i in range(20):
        await qm.append(sid, b'{"n":%d}' % i)
    # Arms the reconcile: the next read_batch walks the dead payloads and
    # advances the offset past the already-dead leading line.
    await qm.dead_letter(sid, poison, "boom")

    await asyncio.gather(
        qm.commit(sid, 999_999, {"dl2": {"orch_run_seq": 3}, "dl3": {}}),
        qm.read_batch(sid, max_items=10),
    )
    return qm._read_offset_record(sid)


async def test_concurrent_offset_writers_never_clobber_or_tear(tmp_path) -> None:
    """A commit racing the reconcile must survive intact.

    The reconcile reaches the offset through read_batch, on a different thread
    than the commit. With both writes funnelled through the one locked writer,
    the commit is never rolled back to the value the reconcile read before it
    and the record is never half-written. Repeated because a race that survives
    one interleaving proves nothing.
    """
    for attempt in range(25):
        offset, cursor = await _commit_racing_reconcile(tmp_path / f"run{attempt}")
        assert offset == 999_999, (
            f"attempt {attempt}: the reconcile rolled the committed offset back "
            f"to {offset} -- the session re-drains and duplicates those records"
        )
        assert cursor == {"dl2": {"orch_run_seq": 3}, "dl3": {}}, (
            f"attempt {attempt}: committed cursor lost to a concurrent write"
        )


async def test_offset_writers_do_not_share_a_staging_path(tmp_path) -> None:
    """Concurrent writers must stage to different temp files.

    A shared staging name lets one writer rename a file the other is still
    filling, publishing a half-written record -- corruption that no amount of
    caller-side locking would prevent. The name still ends in ``.offset.tmp``
    so ``reclaim_orphans`` keeps reaping strays.
    """
    qm = FileSystemQueueManager(queues_dir=tmp_path)
    await qm.append("s1", b"a")
    paths = {qm._offset_tmp_path("s1") for _ in range(50)}

    assert len(paths) == 50, "staging paths collided between writes"
    assert all(p.name.endswith(".offset.tmp") for p in paths)
    assert all(qm._offset_tmp_owner(p) == "s1" for p in paths), (
        "reclaim_orphans could no longer tell which session a staging file "
        "belongs to, and would reap one belonging to a live session"
    )

    # A staging file beside a live log is kept; a log-less stray is reaped.
    live = qm._offset_tmp_path("s1")
    live.write_text("{}", encoding="utf-8")
    stray = qm._offset_tmp_path("s-gone")
    stray.write_text("{}", encoding="utf-8")

    await qm.reclaim_orphans(before_ts=time.time() + 60)

    assert live.exists(), "reclaim reaped a staging file belonging to a live session"
    assert not stray.exists(), "reclaim left a log-less staging file behind"


# ---------------------------------------------------------------------------
# Fix 9: every drain-loop read quarantines a corrupt offset, not just the first
# ---------------------------------------------------------------------------


async def test_corrupt_offset_at_idle_recheck_quarantines_worker(
    caplog, monkeypatch
) -> None:
    """The dry-exit recheck read must quarantine like the main-loop read.

    A recovered worker with no backlog rechecks before exiting; an offset that
    goes unparseable between the two reads used to escape to the supervisor and
    crash-loop the session.
    """
    reg = SessionRegistry()
    qm = cast(FileSystemQueueManager, reg.queue_manager)
    sid = "s-corrupt-recheck"
    worker = _make_worker(sid, _InertGraph())
    worker.live_event_seen = False
    reg._register_for_test(worker)

    reads = {"n": 0}
    real_read = qm._read_offset_record

    def _corrupt_after_first(session_id: str):
        reads["n"] += 1
        if reads["n"] > 1:
            raise ValueError(f"unparseable offset document for {session_id!r}")
        return real_read(session_id)

    monkeypatch.setattr(qm, "_read_offset_record", _corrupt_after_first)

    with caplog.at_level(logging.ERROR, logger="context_intelligence_server"):
        await reg.drain_worker(worker, flush_timeout=0.05)

    assert any("drain_worker_quarantined" in r.getMessage() for r in caplog.records)
    assert sid not in reg._workers


async def test_corrupt_offset_during_finalize_tail_quarantines_worker(
    caplog,
) -> None:
    """The finalize tail-drain read must quarantine like the main-loop read.

    This is the common ``session:end`` path; an unguarded ValueError here
    escaped to the supervisor instead of quarantining the one bad session.
    """
    reg = SessionRegistry()
    qm = cast(FileSystemQueueManager, reg.queue_manager)
    sid = "s-corrupt-finalize"
    await qm.append(sid, _line("session:end", {"session_id": sid}))
    qm._offset_path(sid).write_text('{"v":1,"offset":"NOT-AN-INT"}', encoding="utf-8")

    worker = _make_worker(sid, _InertGraph())
    reg._register_for_test(worker)

    with (
        caplog.at_level(logging.ERROR, logger="context_intelligence_server"),
        pytest.raises(_SessionQuarantined),
    ):
        await reg._drain_to_eof(worker, handlers=None)

    assert any("drain_worker_quarantined" in r.getMessage() for r in caplog.records)
    assert sid not in reg._workers
    assert worker.store_closed is True


# ---------------------------------------------------------------------------
# Fix 10: a finalized session leaves no dead state behind for a reused id
# ---------------------------------------------------------------------------


async def test_delete_drained_retires_dead_letters_for_id_reuse(tmp_path) -> None:
    """Finalizing must retire the dead letters out of the session's own name.

    A session id can be reused. Left in place, the previous session's dead
    payloads make the new session's first read skip its own leading lines --
    committing past events that were never processed.
    """
    qm = FileSystemQueueManager(queues_dir=tmp_path)
    sid = "s-reuse"
    poison = b"poison-line"

    await qm.append(sid, poison)
    await qm.dead_letter(sid, poison, "boom")
    await qm.commit(sid, len(poison) + 1, None)
    assert await qm.delete_drained(sid) is True

    assert not (tmp_path / f"{sid}.dead.jsonl").exists()
    retired = list(tmp_path.glob(f"{sid}.finalized-*.dead.jsonl"))
    assert len(retired) == 1, "dead-letter payloads must be retained, not deleted"
    assert sid not in qm._dead_unreconciled

    # The reused id must read its own first line, not skip it as already-dead.
    await qm.append(sid, poison)
    batch = await qm.read_batch(sid, max_items=10)
    assert [r.raw for r in batch.records] == [poison], (
        "the reused session skipped its own line against the previous "
        "session's dead payloads"
    )


# ---------------------------------------------------------------------------
# Fix 11: the boot-sweep reconcile is COUNTED, so delete_drained's eviction
# cannot drop the guard out from under it (the guard-eviction clobber, one
# layer down from Fix 8). Also: reclaim_orphans unlinks under the key lock,
# and the finalize offset reads degrade on a corrupt .offset.
# ---------------------------------------------------------------------------


def _seed_reconcilable(qm: FileSystemQueueManager, key: str, dead_lines: int) -> int:
    """Seed a fully-committed log of ``dead_lines`` already-dead lines + a dead
    file naming that payload, so ``_reconcile_dead_key`` does a real RMW and
    ``delete_drained`` accepts (size == committed) and evicts. Returns the log
    byte size."""
    body = b"D\n" * dead_lines
    qm._log_path(key).write_bytes(body)
    qm._offset_path(key).write_text(
        json.dumps({"v": 1, "offset": len(body), "cursor": None}), encoding="utf-8"
    )
    qm._dead_path(key).write_text(
        json.dumps({"ts": time.time(), "error": "poison", "payload": "D"}) + "\n",
        encoding="utf-8",
    )
    return len(body)


async def test_boot_reconcile_counted_guard_survives_eviction_race(
    tmp_path, monkeypatch
) -> None:
    """The boot-sweep reconcile must hold the COUNTED guard.

    ``recovery_reconcile_dead`` reaches ``_reconcile_dead_key`` on a worker
    thread. Before the fix it took ``file_lock`` through the RAW ``_key_guard``
    accessor, so ``delete_drained``'s ``waiters == 1`` eviction gate could not
    see it, evicted the guard-map entry mid-reconcile, and the next writer
    minted a SECOND ``_KeyLock`` over the same file -- two locks, no mutual
    exclusion, clobber. Drive the reconcile concurrently with an
    eviction-then-remint on the same key, under maximally adverse scheduling,
    and assert NO key ever had two distinct ``_KeyLock`` objects held at once.
    """
    # Passive detector: two distinct _KeyLock objects held for one key at the
    # same instant is exactly the bypass. Patched on the class, auto-restored.
    from context_intelligence_server.queue_manager import filesystem as fsmod

    held: dict[str, set[int]] = {}
    held_lock = threading.Lock()
    violations: list[str] = []
    real_acquire = fsmod._KeyLock.acquire
    real_release = fsmod._KeyLock.release

    def spy_acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        ok = real_acquire(self, blocking, timeout)
        key = getattr(self, "_probe_key", None)
        if ok and key is not None:
            with held_lock:
                s = held.setdefault(key, set())
                s.add(id(self))
                if len(s) > 1:
                    violations.append(f"{key}:{sorted(s)}")
        return ok

    def spy_release(self) -> None:
        key = getattr(self, "_probe_key", None)
        if key is not None:
            with held_lock:
                held.get(key, set()).discard(id(self))
        real_release(self)

    real_key_guard = FileSystemQueueManager._key_guard

    def tagging_key_guard(self, worker_key: str):
        g = real_key_guard(self, worker_key)
        # Tag the lock so the spy can attribute it to a key.
        g.file_lock._probe_key = worker_key  # type: ignore[attr-defined]
        return g

    monkeypatch.setattr(fsmod._KeyLock, "acquire", spy_acquire)
    monkeypatch.setattr(fsmod._KeyLock, "release", spy_release)
    monkeypatch.setattr(FileSystemQueueManager, "_key_guard", tagging_key_guard)

    old_interval = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)  # maximally adverse: switch every few bytecodes
    torn = 0
    rounds = 300
    try:
        qm = FileSystemQueueManager(queues_dir=tmp_path)
        for r in range(rounds):
            key = f"k{r}"
            _seed_reconcilable(qm, key, dead_lines=64)
            barrier = threading.Barrier(2)

            def _reconcile_thread(k: str = key, b: threading.Barrier = barrier) -> None:
                b.wait()
                # The boot-sweep entry point (iterates keys off worker thread).
                qm._reconcile_dead_key(k)

            t = threading.Thread(target=_reconcile_thread, name="boot-reconcile")
            t.start()

            async def _evict_then_remint(
                k: str = key, b: threading.Barrier = barrier
            ) -> None:
                await asyncio.to_thread(b.wait)
                await qm.delete_drained(k)  # the eviction path
                await qm.append(k, b"D\n")  # the next writer that could re-mint

            await _evict_then_remint()
            t.join(10)

            # The final .offset must be a parseable record, never torn.
            try:
                qm._read_offset_record(key)
            except (ValueError, OSError):
                torn += 1
            for p in tmp_path.glob(f"{key}*"):
                p.unlink(missing_ok=True)
    finally:
        sys.setswitchinterval(old_interval)

    assert violations == [], (
        f"two distinct _KeyLock objects were held for one key at once over "
        f"{rounds} rounds -- the guard was evicted underneath a live reconcile: "
        f"{violations[:3]}"
    )
    assert torn == 0, f"{torn}/{rounds} rounds left a torn .offset record"


async def test_reclaim_orphans_unlinks_offset_under_key_lock(
    tmp_path, monkeypatch
) -> None:
    """``reclaim_orphans`` must unlink an orphan ``.offset`` while holding the
    key's ``file_lock`` -- the same lock every offset writer takes -- so the
    unlink can never race a concurrent offset write. Observed by checking the
    lock state at the moment of the unlink."""
    qm = FileSystemQueueManager(queues_dir=tmp_path)
    stem = "orphan-key"
    # An orphan .offset (NO .log beside it) -> a reclaim candidate.
    qm._offset_path(stem).write_text(
        json.dumps({"v": 1, "offset": 5, "cursor": None}), encoding="utf-8"
    )
    # Same guard object reclaim's counted _guard(stem) will resolve to.
    guard = qm._key_guard(stem)
    target = qm._offset_path(stem)

    observed: dict[str, bool] = {}
    real_unlink = Path.unlink

    def spy_unlink(self: Path, *args: Any, **kwargs: Any) -> None:
        if self == target:
            observed["locked_at_unlink"] = guard.file_lock.locked()
        return real_unlink(self, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", spy_unlink)

    result = await qm.reclaim_orphans(before_ts=time.time() + 60)

    assert observed.get("locked_at_unlink") is True, (
        "reclaim_orphans unlinked the orphan .offset WITHOUT holding the key's "
        "file_lock -- a concurrent offset write could race the unlink"
    )
    assert not target.exists()
    assert result["reclaimed"] >= 1


async def test_finalize_offset_reads_degrade_on_corrupt_offset(tmp_path) -> None:
    """``delete_drained`` and ``is_fully_drained`` read the committed offset on
    the finalize path (``session:end``). A corrupt ``.offset`` must DEGRADE
    (retain / report-not-drained) instead of raising out to the drain
    supervisor and crashing the worker."""
    qm = FileSystemQueueManager(queues_dir=tmp_path)
    sid = "s-finalize-corrupt"
    await qm.append(sid, b"one-real-line\n")
    # A genuinely corrupt committed offset (non-int) beside a present log.
    qm._offset_path(sid).write_text('{"v":1,"offset":"NOT-AN-INT"}', encoding="utf-8")

    # Neither call may raise; both degrade to the safe, conservative answer.
    drained = await qm.is_fully_drained(sid)
    assert drained is False, (
        "is_fully_drained must report NOT drained (conservative) on a corrupt "
        ".offset, never raise"
    )

    deleted = await qm.delete_drained(sid)
    assert deleted is False, (
        "delete_drained must retain (return False) on a corrupt .offset so "
        "finalize takes its bounded retain/give-up path -- never crash"
    )
    # Files retained for the boot RESET_OFFSET pass to heal, not destroyed.
    assert qm._log_path(sid).exists()
    assert qm._offset_path(sid).exists()
