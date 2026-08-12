"""Tier 3 -- REAL Neo4j end-to-end proof for I5b (durable handler cursor).

Reproduces a realistic "server restart mid-session" / "stale-session reap
mid-session" against a LIVE Neo4j container and asserts the spec's Sec 10.3
DTU acceptance criteria:

  1. Duplicate Iteration nodes == 0 across the restart (no bare-shape
     re-pooling).
  2. Edge parity: E06 HAS_PART, E09 CAUSED, E14 TRIGGERS, E15 ENABLES present
     across the restart boundary (proves FULL-cursor persistence -- not just
     the node_id -- because E09 specifically requires
     ``pending_tool_block_ids`` to have survived the rebuild).
  3. ``iteration_scope`` tallies: every Iteration node carries "run" or
     "unscoped"; none are missing/neither.

This test drives the REAL code path a crash-restart/reap hits:
``get_or_create -> start_drain -> drain_worker -> restore_cursor(read_cursor)``.
It does NOT use ``_register_for_test`` to bypass ``get_or_create`` (unlike
most other tests/neo4j/ files) -- the whole point here is to exercise
``SessionRegistry.get_or_create``'s settings-derived construction path, which
is where BOTH triggers (T1 crash-restart via main.py's recovery loop, T2
stale-reap via drain_worker's own idle branch) actually spawn their rebuilt
worker in production.

Two scenarios:
  - test_cursor_durability_survives_crash_restart_e2e (T1): the pre-restart
    registry/worker object is simply discarded (its drain task cancelled)
    WITHOUT calling delete_drained -- .log/.offset survive on disk exactly
    as they would after a process crash. A brand-new SessionRegistry, over
    the SAME on-disk queues dir and the SAME Neo4j, picks the session back
    up.
  - test_cursor_durability_survives_stale_reap_e2e (T2): the registry's own
    ``_deregister`` is called directly (mirrors the real idle-reap branch in
    ``drain_worker``, which calls ``_safe_close`` + ``_deregister`` + returns)
    -- this is the sharper trigger because it has NO recovery path today
    without I5b: ``_deregister`` intentionally leaves .log/.offset on disk.

Run:
    uv run pytest tests/neo4j/test_cursor_durability_e2e.py -v -m neo4j
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest
from context_intelligence_server.config import Neo4jClientConfig
from context_intelligence_server.neo4j_store import (
    ensure_neo4j_schema,
    ensure_schema_version_baseline,
)
from context_intelligence_server.registry import SessionRegistry, SessionWorker
from context_intelligence_server.status import SCHEMA_VERSION
from neo4j import AsyncGraphDatabase

pytestmark = pytest.mark.neo4j

WORKSPACE = "cursor-durability-e2e"

# Fixed timestamps -- deterministic node_ids, no wall-clock flakiness.
T0 = "2026-08-11T09:00:00+00:00"  # session:start
TP1 = "2026-08-11T09:00:01+00:00"  # prompt:submit #1
T1 = "2026-08-11T10:00:00+00:00"  # execution:start (the run-scoping ts)
TPR1 = "2026-08-11T10:00:01+00:00"  # provider:request iter1
TLQ1 = "2026-08-11T10:00:02+00:00"  # llm:request iter1
TLR1 = "2026-08-11T10:00:03+00:00"  # llm:response iter1
TCB0S = "2026-08-11T10:00:04+00:00"  # content_block:start block0
TCB0E = "2026-08-11T10:00:05+00:00"  # content_block:end block0 (tool_call)
TPR2 = "2026-08-11T10:00:06+00:00"  # provider:request iter2
TLQ2 = "2026-08-11T10:00:07+00:00"  # llm:request iter2
TLR2 = "2026-08-11T10:00:08+00:00"  # llm:response iter2

# --- post-restart timestamps ---
TTPRE = "2026-08-11T10:05:00+00:00"  # tool:pre (fires E09 -- pending_tool_block_ids)
TTPOST = "2026-08-11T10:05:01+00:00"  # tool:post
TPR3 = "2026-08-11T10:05:02+00:00"  # provider:request iter3 (continuation, not reset)
TLQ3 = "2026-08-11T10:05:03+00:00"  # llm:request iter3
TLR3 = "2026-08-11T10:05:04+00:00"  # llm:response iter3
TOC = "2026-08-11T10:05:05+00:00"  # orchestrator:complete
TP2 = "2026-08-11T10:05:06+00:00"  # prompt:submit #2 (fires E15)

TOOL_CALL_ID = "toolblock-1"


def _line(event: str, workspace: str, data: dict[str, Any]) -> bytes:
    """Encode an appended event line exactly as POST /events stores it."""
    return json.dumps({"event": event, "workspace": workspace, "data": data}).encode(
        "utf-8"
    )


def _pre_restart_lines(sid: str) -> list[bytes]:
    """session:start .. provider:request/llm:*(iter2) -- the pre-restart half.

    Deliberately stops with content_block:end (tool_call) cached in
    ``pending_tool_block_ids`` but WITHOUT the matching tool:pre -- that pop
    happens post-restart, so E09 can only be created if the FULL DataLayer2State
    (not just execution_start_ts/iteration_count) survived the rebuild.
    """
    return [
        _line("session:start", WORKSPACE, {"session_id": sid, "timestamp": T0}),
        _line(
            "prompt:submit",
            WORKSPACE,
            {"session_id": sid, "timestamp": TP1, "prompt": "do the thing"},
        ),
        _line("execution:start", WORKSPACE, {"session_id": sid, "timestamp": T1}),
        _line(
            "provider:request",
            WORKSPACE,
            {"session_id": sid, "timestamp": TPR1},
        ),
        _line(
            "llm:request",
            WORKSPACE,
            {
                "session_id": sid,
                "timestamp": TLQ1,
                "provider": "anthropic",
                "model": "claude",
                "message_count": 1,
            },
        ),
        _line(
            "llm:response",
            WORKSPACE,
            {
                "session_id": sid,
                "timestamp": TLR1,
                "usage": {"input_tokens": 10, "output_tokens": 5},
            },
        ),
        _line(
            "content_block:start",
            WORKSPACE,
            {"session_id": sid, "timestamp": TCB0S, "block_index": 0},
        ),
        _line(
            "content_block:end",
            WORKSPACE,
            {
                "session_id": sid,
                "timestamp": TCB0E,
                "block_index": 0,
                "block": {"type": "tool_call", "id": TOOL_CALL_ID},
            },
        ),
        _line(
            "provider:request",
            WORKSPACE,
            {"session_id": sid, "timestamp": TPR2},
        ),
        _line(
            "llm:request",
            WORKSPACE,
            {
                "session_id": sid,
                "timestamp": TLQ2,
                "provider": "anthropic",
                "model": "claude",
                "message_count": 2,
            },
        ),
        _line(
            "llm:response",
            WORKSPACE,
            {
                "session_id": sid,
                "timestamp": TLR2,
                "usage": {"input_tokens": 20, "output_tokens": 8},
            },
        ),
    ]


def _post_restart_lines(sid: str) -> list[bytes]:
    """tool:pre/post(iter2's block) -> provider:request(iter3) -> orchestrator:complete
    -> prompt:submit#2 -- the post-restart half, all for the SAME orch run T1.
    """
    return [
        _line(
            "tool:pre",
            WORKSPACE,
            {
                "session_id": sid,
                "timestamp": TTPRE,
                "tool_call_id": TOOL_CALL_ID,
                "tool_name": "bash",
                "tool_input": "echo hi",
            },
        ),
        _line(
            "tool:post",
            WORKSPACE,
            {
                "session_id": sid,
                "timestamp": TTPOST,
                "tool_call_id": TOOL_CALL_ID,
                "result": {"output": "hi"},
            },
        ),
        _line(
            "provider:request",
            WORKSPACE,
            {"session_id": sid, "timestamp": TPR3},
        ),
        _line(
            "llm:request",
            WORKSPACE,
            {
                "session_id": sid,
                "timestamp": TLQ3,
                "provider": "anthropic",
                "model": "claude",
                "message_count": 3,
            },
        ),
        _line(
            "llm:response",
            WORKSPACE,
            {
                "session_id": sid,
                "timestamp": TLR3,
                "usage": {"input_tokens": 30, "output_tokens": 12},
            },
        ),
        _line(
            "orchestrator:complete",
            WORKSPACE,
            {
                "session_id": sid,
                "timestamp": TOC,
                "orchestrator": "test-orchestrator",
                "turn_count": 1,
            },
        ),
        _line(
            "prompt:submit",
            WORKSPACE,
            {"session_id": sid, "timestamp": TP2, "prompt": "do another thing"},
        ),
    ]


class _SettingsProxy:
    """Minimal settings stand-in pointed at the REAL Neo4j fixture + a tmp queues dir.

    ``SessionRegistry.get_or_create`` calls ``settings.resolve_neo4j_admin()``
    directly (doc 12, the Neo4j two-client split) and reads several scalar
    fields off ``get_settings()`` -- this mirrors tests/conftest.py's
    ``safe_settings`` proxy shape exactly, but points ``neo4j_url`` /
    ``neo4j_user`` / ``neo4j_password`` at the LIVE test container instead of
    the (unreachable) real default settings, so ``get_or_create`` builds a
    genuine ``Neo4jGraphStore`` -- not a stub, not ``_register_for_test``.
    """

    def __init__(
        self, queues_dir: Path, blob_dir: Path, container: dict[str, Any]
    ) -> None:
        self.queues_path = str(queues_dir)
        self.blob_path = str(blob_dir)
        self.neo4j_url = container["bolt_url"]
        self.neo4j_user = container["user"]
        self.neo4j_password = container["password"]
        self.stale_session_timeout = 3600.0
        self.write_concurrency = 4
        self.max_delivery_attempts = 3
        self.neo4j_flush_chunk_rows = 100
        self.neo4j_flush_chunk_bytes = 4_194_304
        self.neo4j_lock_timeout: float | None = None

    def resolve_neo4j_admin(self) -> Neo4jClientConfig:
        return Neo4jClientConfig(
            url=self.neo4j_url,
            username=self.neo4j_user,
            password=self.neo4j_password,
            access_mode="WRITE",
        )


async def _drain_until(
    predicate: Any,
    *,
    timeout: float = 30.0,
    interval: float = 0.05,
) -> bool:
    """Poll *predicate* (a zero-arg callable) until truthy or *timeout* elapses."""
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while True:
        if predicate():
            return True
        if loop.time() >= deadline:
            return False
        await asyncio.sleep(interval)


async def _cancel_and_await(worker: SessionWorker) -> None:
    """Cancel a worker's drain task and await its (CancelledError) completion."""
    if worker.task is None:
        return
    worker.task.cancel()
    try:
        await worker.task
    except asyncio.CancelledError:
        pass


async def _append_all(qm: Any, sid: str, lines: list[bytes]) -> None:
    for raw in lines:
        await qm.append(sid, raw)


def _make_settings_proxy(
    tmp_path: Path, neo4j_container: dict[str, Any]
) -> _SettingsProxy:
    return _SettingsProxy(
        queues_dir=tmp_path / "queues",
        blob_dir=tmp_path / "blobs",
        container=neo4j_container,
    )


async def _run_query(
    neo4j_container: dict[str, Any], query: str, **params: Any
) -> list[Any]:
    """One-shot query against the live container; returns all result rows."""
    driver = AsyncGraphDatabase.driver(
        neo4j_container["bolt_url"],
        auth=(neo4j_container["user"], neo4j_container["password"]),
    )
    try:
        async with driver.session() as session:
            result = await session.run(query, params)
            return [record async for record in result]
    finally:
        await driver.close()


# ---------------------------------------------------------------------------
# T1 -- crash restart
# ---------------------------------------------------------------------------


async def test_cursor_durability_survives_crash_restart_e2e(
    neo4j_container: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    sid = "cursor-e2e-crash"
    orch_run_id = f"{sid}::orch_run::{T1}"

    # Schema (indexes/constraints) active before any MERGE -- mirrors lifespan.
    admin_driver = AsyncGraphDatabase.driver(
        neo4j_container["bolt_url"],
        auth=(neo4j_container["user"], neo4j_container["password"]),
    )
    try:
        await ensure_neo4j_schema(admin_driver)
        await ensure_schema_version_baseline(admin_driver)
    finally:
        await admin_driver.close()

    proxy = _make_settings_proxy(tmp_path, neo4j_container)
    monkeypatch.setattr(
        "context_intelligence_server.registry.get_settings", lambda: proxy
    )

    # ---------------------------------------------------------------
    # PHASE 1 -- pre-restart: real registry_A, real get_or_create, real store.
    # ---------------------------------------------------------------
    reg_a = SessionRegistry()
    worker_a = reg_a.get_or_create(sid, WORKSPACE)
    assert worker_a.task is not None

    pre_lines = _pre_restart_lines(sid)
    await _append_all(reg_a.queue_manager, sid, pre_lines)

    wrote = await _drain_until(
        lambda: reg_a.pipeline_counters()["written_total"] >= len(pre_lines),
        timeout=30.0,
    )
    assert wrote, "pre-restart batch did not commit within the window"

    # --- Evidence requirement: .offset now contains a JSON record with a
    #     non-null cursor (execution_start_ts=T1, iteration_count>0). ---
    offset_path = tmp_path / "queues" / f"{sid}.offset"
    rec = json.loads(offset_path.read_text(encoding="utf-8"))
    assert rec["v"] == 1
    assert rec["cursor"] is not None, ".offset cursor must be non-null after commit"
    assert rec["cursor"]["dl2"]["execution_start_ts"] == T1
    assert rec["cursor"]["dl2"]["iteration_count"] == 2
    assert rec["cursor"]["dl2"]["pending_tool_block_ids"] == {
        TOOL_CALL_ID: f"{sid}::block::1::0"
    }

    # ---------------------------------------------------------------
    # "CRASH": discard reg_a/worker_a WITHOUT delete_drained -- .log/.offset
    # survive on disk. Cancel the task (test-harness cleanup only; production
    # would have the OS kill the process instead).
    # ---------------------------------------------------------------
    await _cancel_and_await(worker_a)

    # ---------------------------------------------------------------
    # PHASE 2 -- "restart": a BRAND NEW SessionRegistry + fresh
    # HookStateService/DataLayer2State + new Neo4jGraphStore over the SAME
    # queues dir + SAME Neo4j, via get_or_create -> start_drain -> drain_worker.
    # ---------------------------------------------------------------
    # Mirrors lifespan calling ensure_schema_version_baseline on every
    # "startup" -- a fresh admin driver, used once, then closed.
    restart_driver = AsyncGraphDatabase.driver(
        neo4j_container["bolt_url"],
        auth=(neo4j_container["user"], neo4j_container["password"]),
    )
    try:
        await ensure_schema_version_baseline(restart_driver)
    finally:
        await restart_driver.close()

    reg_b = SessionRegistry()
    worker_b = reg_b.get_or_create(sid, WORKSPACE)
    assert worker_b.task is not None

    # HONESTY CHECK: the fresh worker's DataLayer2State is genuinely at
    # defaults THIS INSTANT -- the drain task has been scheduled but the
    # event loop has not yet given it a turn to run restore_cursor. If this
    # assertion ever fails, the "fresh state" claim below is not proven.
    assert worker_b.services.data_layer_2.execution_start_ts is None
    assert worker_b.services.data_layer_2.iteration_count == 0
    assert worker_b.services.data_layer_2.pending_tool_block_ids == {}

    # Let the drain task actually run far enough to call restore_cursor
    # (read_cursor is asyncio.to_thread -- needs a real await, not just
    # asyncio.sleep(0)).
    restored = await _drain_until(
        lambda: worker_b.services.data_layer_2.execution_start_ts is not None,
        timeout=10.0,
    )
    assert restored, "restore_cursor did not populate execution_start_ts in time"
    assert worker_b.services.data_layer_2.execution_start_ts == T1
    assert worker_b.services.data_layer_2.iteration_count == 2
    assert worker_b.services.data_layer_2.pending_tool_block_ids == {
        TOOL_CALL_ID: f"{sid}::block::1::0"
    }

    # ---------------------------------------------------------------
    # PHASE 3 -- append MORE events for the SAME run after the restart.
    # ---------------------------------------------------------------
    post_lines = _post_restart_lines(sid)
    await _append_all(reg_b.queue_manager, sid, post_lines)

    wrote2 = await _drain_until(
        lambda: reg_b.pipeline_counters()["written_total"] >= len(post_lines),
        timeout=30.0,
    )
    assert wrote2, "post-restart batch did not commit within the window"

    await _cancel_and_await(worker_b)

    # =================================================================
    # Sec 10.3 acceptance-criteria assertions (real Cypher, real Neo4j)
    # =================================================================
    iter_rows = await _run_query(
        neo4j_container,
        "MATCH (i:Iteration {session_id: $sid}) "
        "RETURN i.node_id AS node_id, i.iteration_number AS n, "
        "i.iteration_scope AS scope ORDER BY i.iteration_number",
        sid=sid,
    )

    # (a) Duplicate-free.
    node_ids = [r["node_id"] for r in iter_rows]
    assert len(node_ids) == len(set(node_ids)), (
        f"duplicate Iteration node_ids for session {sid}: {node_ids}"
    )
    bare = [nid for nid in node_ids if "::orch_run::" not in nid]
    assert bare == [], f"bare (pre-fix-shape) Iteration node_ids found: {bare}"
    assert len(node_ids) == 3, f"expected exactly 3 Iteration nodes, got {node_ids}"

    # (b) Run-scoped continuity: same run prefix, iteration_number continues
    #     1, 2, 3 across the restart boundary (no restart-to-1 collision).
    expected_prefix = f"{orch_run_id}::iteration::"
    for r in iter_rows:
        assert r["node_id"].startswith(expected_prefix), (
            f"Iteration {r['node_id']} is not scoped to {orch_run_id}"
        )
    assert [r["n"] for r in iter_rows] == [1, 2, 3], (
        f"iteration_number must continue 1,2,3 across the restart, got "
        f"{[r['n'] for r in iter_rows]}"
    )

    # (d) iteration_scope: every Iteration node carries a value (never
    #     missing/null), and since execution_start_ts was active throughout,
    #     all three must be "run" (never "unscoped").
    scopes = [r["scope"] for r in iter_rows]
    assert all(s is not None for s in scopes), f"missing iteration_scope: {scopes}"
    assert scopes == ["run", "run", "run"], f"expected all-'run' scopes, got {scopes}"

    # (c) Edge parity across the restart boundary.
    iter3_id = f"{orch_run_id}::iteration::3"

    e06_rows = await _run_query(
        neo4j_container,
        "MATCH (o:OrchestratorRun {node_id: $orid})-[r:HAS_PART]->(i:Iteration) "
        "WHERE i.node_id = $iter3 RETURN count(r) AS c",
        orid=orch_run_id,
        iter3=iter3_id,
    )
    assert e06_rows[0]["c"] >= 1, (
        "E06 HAS_PART edge missing from the post-restart (iteration 3) "
        "OrchestratorRun -> Iteration"
    )

    # E09: ContentBlock -[:CAUSED]-> ToolCall. This can ONLY exist if
    # pending_tool_block_ids (cached pre-restart at content_block:end)
    # survived the rebuild and was consumed by the post-restart tool:pre --
    # i.e. it proves FULL DataLayer2State restore, not just the node_id.
    e09_rows = await _run_query(
        neo4j_container,
        "MATCH (b:ContentBlock {session_id: $sid})-[r:CAUSED]->(t:ToolCall) "
        "WHERE t.tool_call_id = $tcid RETURN count(r) AS c",
        sid=sid,
        tcid=TOOL_CALL_ID,
    )
    assert e09_rows[0]["c"] >= 1, (
        "E09 CAUSED edge missing -- pending_tool_block_ids did not survive "
        "the worker rebuild"
    )

    # E14: Prompt -[:TRIGGERS]-> OrchestratorRun (created at execution:start
    # from the pre-restart prompt:submit's last_prompt_id cursor).
    e14_rows = await _run_query(
        neo4j_container,
        "MATCH (p:Prompt {session_id: $sid})-[r:TRIGGERS]->(o:OrchestratorRun) "
        "WHERE o.node_id = $orid RETURN count(r) AS c",
        sid=sid,
        orid=orch_run_id,
    )
    assert e14_rows[0]["c"] >= 1, "E14 TRIGGERS edge missing"

    # E15: OrchestratorRun -[:ENABLES]-> Prompt (created at the post-restart
    # second prompt:submit from orchestrator:complete's
    # last_completed_orch_run_id cursor).
    e15_rows = await _run_query(
        neo4j_container,
        "MATCH (o:OrchestratorRun {node_id: $orid})-[r:ENABLES]->(p:Prompt) "
        "WHERE p.session_id = $sid RETURN count(r) AS c",
        orid=orch_run_id,
        sid=sid,
    )
    assert e15_rows[0]["c"] >= 1, "E15 ENABLES edge missing"

    # (e) SchemaMeta: exactly ONE singleton node exists.
    schema_rows = await _run_query(
        neo4j_container,
        "MATCH (m:SchemaMeta {id: 'singleton'}) "
        "RETURN count(m) AS c, m.schema_version AS v",
    )
    assert schema_rows[0]["c"] == 1, (
        f"expected exactly one :SchemaMeta singleton, got {schema_rows[0]['c']}"
    )
    assert schema_rows[0]["v"] == SCHEMA_VERSION


# ---------------------------------------------------------------------------
# T2 -- stale-worker reap (the sharper trigger: no recovery path today)
# ---------------------------------------------------------------------------


async def test_cursor_durability_survives_stale_reap_e2e(
    neo4j_container: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    sid = "cursor-e2e-reap"
    orch_run_id = f"{sid}::orch_run::{T1}"

    admin_driver = AsyncGraphDatabase.driver(
        neo4j_container["bolt_url"],
        auth=(neo4j_container["user"], neo4j_container["password"]),
    )
    try:
        await ensure_neo4j_schema(admin_driver)
    finally:
        await admin_driver.close()

    proxy = _make_settings_proxy(tmp_path, neo4j_container)
    monkeypatch.setattr(
        "context_intelligence_server.registry.get_settings", lambda: proxy
    )

    reg = SessionRegistry()
    worker1 = reg.get_or_create(sid, WORKSPACE)
    assert worker1.task is not None

    pre_lines = _pre_restart_lines(sid)
    await _append_all(reg.queue_manager, sid, pre_lines)
    wrote = await _drain_until(
        lambda: reg.pipeline_counters()["written_total"] >= len(pre_lines),
        timeout=30.0,
    )
    assert wrote, "pre-reap batch did not commit within the window"

    # ---------------------------------------------------------------
    # T2 trigger: the registry's OWN reap path -- _deregister pops the
    # worker from the dict WITHOUT touching .log/.offset (registry.py's
    # real idle-reap branch calls _safe_close + _deregister + return; we
    # call _deregister directly and cancel the task ourselves to mirror
    # that same-coroutine self-termination without waiting out the real
    # 30s idle-detection window).
    # ---------------------------------------------------------------
    reg._deregister(sid)
    assert sid not in reg._workers, "_deregister must have removed the worker"
    await _cancel_and_await(worker1)

    # Files must still be on disk -- this IS what makes T2 have no recovery
    # path without I5b.
    assert (tmp_path / "queues" / f"{sid}.log").exists()
    assert (tmp_path / "queues" / f"{sid}.offset").exists()

    # The "next event" rebuilds a fresh worker via the SAME registry (mirrors
    # the real production flow: the next POST /events calls get_or_create
    # again on a registry that no longer has this session_id).
    worker2 = reg.get_or_create(sid, WORKSPACE)
    assert worker2.task is not None
    assert worker2 is not worker1, "get_or_create must have built a NEW worker"

    # HONESTY CHECK: genuinely fresh before restore.
    assert worker2.services.data_layer_2.execution_start_ts is None
    assert worker2.services.data_layer_2.iteration_count == 0

    restored = await _drain_until(
        lambda: worker2.services.data_layer_2.execution_start_ts is not None,
        timeout=10.0,
    )
    assert restored, "restore_cursor did not populate execution_start_ts in time"
    assert worker2.services.data_layer_2.execution_start_ts == T1
    assert worker2.services.data_layer_2.iteration_count == 2

    post_lines = _post_restart_lines(sid)
    await _append_all(reg.queue_manager, sid, post_lines)
    wrote2 = await _drain_until(
        lambda: (
            reg.pipeline_counters()["written_total"] >= len(pre_lines) + len(post_lines)
        ),
        timeout=30.0,
    )
    assert wrote2, "post-reap batch did not commit within the window"

    await _cancel_and_await(worker2)

    # Same duplicate=0 / run-scoped invariant as the crash-restart scenario.
    iter_rows = await _run_query(
        neo4j_container,
        "MATCH (i:Iteration {session_id: $sid}) "
        "RETURN i.node_id AS node_id, i.iteration_number AS n, "
        "i.iteration_scope AS scope ORDER BY i.iteration_number",
        sid=sid,
    )
    node_ids = [r["node_id"] for r in iter_rows]
    assert len(node_ids) == len(set(node_ids)), (
        f"duplicate Iteration node_ids for session {sid}: {node_ids}"
    )
    bare = [nid for nid in node_ids if "::orch_run::" not in nid]
    assert bare == [], f"bare (pre-fix-shape) Iteration node_ids found: {bare}"
    assert len(node_ids) == 3, f"expected exactly 3 Iteration nodes, got {node_ids}"

    expected_prefix = f"{orch_run_id}::iteration::"
    for r in iter_rows:
        assert r["node_id"].startswith(expected_prefix), (
            f"Iteration {r['node_id']} is not scoped to {orch_run_id}"
        )
    assert [r["n"] for r in iter_rows] == [1, 2, 3], (
        f"iteration_number must continue 1,2,3 across the reap, got "
        f"{[r['n'] for r in iter_rows]}"
    )
    scopes = [r["scope"] for r in iter_rows]
    assert all(s is not None for s in scopes), f"missing iteration_scope: {scopes}"
    assert scopes == ["run", "run", "run"], f"expected all-'run' scopes, got {scopes}"

    # E06 for the post-reap iteration -- proves execution_start_ts (not just
    # iteration_count) survived the reap.
    iter3_id = f"{orch_run_id}::iteration::3"
    e06_rows = await _run_query(
        neo4j_container,
        "MATCH (o:OrchestratorRun {node_id: $orid})-[r:HAS_PART]->(i:Iteration) "
        "WHERE i.node_id = $iter3 RETURN count(r) AS c",
        orid=orch_run_id,
        iter3=iter3_id,
    )
    assert e06_rows[0]["c"] >= 1, "E06 HAS_PART edge missing across the reap boundary"

    # E09 across the reap boundary -- pending_tool_block_ids survival proof.
    e09_rows = await _run_query(
        neo4j_container,
        "MATCH (b:ContentBlock {session_id: $sid})-[r:CAUSED]->(t:ToolCall) "
        "WHERE t.tool_call_id = $tcid RETURN count(r) AS c",
        sid=sid,
        tcid=TOOL_CALL_ID,
    )
    assert e09_rows[0]["c"] >= 1, (
        "E09 CAUSED edge missing across the reap boundary -- "
        "pending_tool_block_ids did not survive"
    )
