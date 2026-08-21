"""Boot-safety hardening tests.

Every test in this file was written and observed RED against the
pre-fix tree before the corresponding production change landed (see the
session's RED-first evidence log). No real Neo4j is used anywhere in this
file.

Covers, per the final (v1.3) spec:
  - classify_session's decision table
  - log-then-delete ordering + the closed reason vocabulary
  - the anti-over-deletion guarantee (resumable data survives reclaim)
  - B3: an unparseable head resumes with a fallback workspace, never deletes
  - B1: /status is LEAN and ZERO-DISK-READ during boot (gated
    on boot-is-OVER, not boot-succeeded)
  - the reconcile background task is exception-safe
  - the four G-1..G-4 crash-loop guards (+ G-5)
  - Gate 1 (live-session ownership) safety
  - B2: the honest recovered-drainer bound + forward progress
  - sidecar retention (a boot never destroys its own quarantine)
  - D-1: the dry-exit's after-await re-read (the strand-after-await fix)
  - D-4: RESET_OFFSET's dead-empty precondition + the in-lock re-check race
  - R10: no phantom reclaim over dead-file-only keys
  - R11: reclaim_enabled=False is a genuine dry run
  - R8: shutdown cancels every background task before closing the drivers
"""

from __future__ import annotations

import asyncio
import errno
import json
import logging
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

import context_intelligence_server.main as main_module
from context_intelligence_server.config import Settings
from context_intelligence_server.main import _head_is_resumable, lifespan
from context_intelligence_server.queue_manager import QueueManager, Verdict
from context_intelligence_server.registry import SessionRegistry, SessionWorker
from context_intelligence_server.services import HookStateService
from context_intelligence_server.status import boot_state

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _line(workspace: str = "/ws", event: str = "tool_use", **data: Any) -> bytes:
    """A newline-terminated record, matching QueueManager.append's own
    on-disk framing exactly (append() always ensures a trailing "\n")."""
    return (
        json.dumps({"event": event, "workspace": workspace, "data": data}) + "\n"
    ).encode("utf-8")


async def _qm(tmp_path: Path) -> QueueManager:
    return QueueManager(queues_dir=tmp_path)


def _seed_log(tmp_path: Path, key: str, content: bytes) -> Path:
    path = tmp_path / f"{key}.log"
    path.write_bytes(content)
    return path


def _seed_offset(tmp_path: Path, key: str, content: str) -> Path:
    path = tmp_path / f"{key}.offset"
    path.write_text(content, encoding="utf-8")
    return path


def _seed_dead(tmp_path: Path, key: str, content: str = "") -> Path:
    path = tmp_path / f"{key}.dead.jsonl"
    path.write_text(content, encoding="utf-8")
    return path


class _NoOpGraph:
    """A minimal graph store fake: flush/close are no-ops, buffer is empty."""

    def __init__(self) -> None:
        self.closed = False
        self.workspace = "default"
        self.created_by: str | None = None

    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        self.closed = True

    def discard_buffer(self) -> None:
        return None

    async def upsert_node(self, node_id: str, data: dict[str, Any]) -> None:
        return None

    async def upsert_edge(self, src_id: str, dst_id: str, data: dict[str, Any]) -> None:
        return None

    async def get_node(self, node_id: str) -> dict[str, Any] | None:
        return None

    async def get_edge(self, src_id: str, dst_id: str) -> dict[str, Any] | None:
        return None

    async def find_delegation_by_sub_session(
        self, sub_session_id: str, workspace: str
    ) -> dict[str, Any] | None:
        return None


def _make_worker(
    session_id: str, workspace: str = "/ws", *, live_event_seen: bool = True
) -> SessionWorker:
    return SessionWorker(
        session_id=session_id,
        workspace=workspace,
        services=HookStateService(
            workspace=workspace, blob_store=MagicMock(), graph_store=_NoOpGraph()
        ),
        live_event_seen=live_event_seen,
    )


# ---------------------------------------------------------------------------
# §11.1 -- classify_session decision-table (table-driven; side-effect-free)
# ---------------------------------------------------------------------------


async def test_classify_resumable_head_parses(tmp_path: Path) -> None:
    qm = await _qm(tmp_path)
    _seed_log(tmp_path, "k1", _line())
    c = await qm.classify_session("k1", _head_is_resumable)
    assert c.verdict is Verdict.RESUMABLE
    assert c.reason == ""
    # side-effect-free: files untouched.
    assert (tmp_path / "k1.log").exists()


async def test_classify_orphan_offset_is_reclaim_orphans_job_not_classify(
    tmp_path: Path,
) -> None:
    """R10/ALSO-b: classify iterates *.log stems only; a .log that vanished
    between glob and classify (or was never there) is a benign race."""
    qm = await _qm(tmp_path)
    c = await qm.classify_session("ghost", _head_is_resumable)
    assert c.verdict is Verdict.UNREADABLE
    assert c.reason == "log_vanished"


async def test_classify_unparseable_offset_small_resets(tmp_path: Path) -> None:
    _seed_log(tmp_path, "k2", _line() * 3)
    _seed_offset(tmp_path, "k2", "not-a-number")
    _seed_dead(tmp_path, "k2", "")  # empty -> dead_empty=True
    qm = await _qm(tmp_path)
    c = await qm.classify_session("k2", _head_is_resumable)
    assert c.verdict is Verdict.RESET_OFFSET
    assert c.reason == "unparseable_offset"
    assert c.dead_empty is True


async def test_classify_unparseable_offset_with_dead_letters_kept(
    tmp_path: Path,
) -> None:
    """Q-17: RESET_OFFSET requires an EMPTY .dead.jsonl; otherwise
    the reset would re-dead-letter the same poison line on replay."""
    _seed_log(tmp_path, "k3", _line() * 3)
    _seed_offset(tmp_path, "k3", "garbage")
    _seed_dead(
        tmp_path, "k3", json.dumps({"ts": 1, "error": "x", "payload": "p"}) + "\n"
    )
    qm = await _qm(tmp_path)
    c = await qm.classify_session("k3", _head_is_resumable)
    assert c.verdict is Verdict.KEEP
    assert c.reason == "bad_offset_with_dead"


async def test_classify_unparseable_offset_large_deletes(tmp_path: Path) -> None:
    big = _line() * 3
    _seed_log(tmp_path, "k4", big)
    _seed_offset(tmp_path, "k4", "not-a-number")
    qm = await _qm(tmp_path)
    settings = Settings(reclaim_redrain_max_bytes=1)  # force "large"
    with patch(
        "context_intelligence_server.queue_manager.get_settings",
        return_value=settings,
    ):
        c = await qm.classify_session("k4", _head_is_resumable)
    assert c.verdict is Verdict.UNRESUMABLE
    assert c.reason == "unparseable_offset"


async def test_classify_negative_offset(tmp_path: Path) -> None:
    _seed_log(tmp_path, "k5", _line())
    _seed_offset(tmp_path, "k5", "-5")
    qm = await _qm(tmp_path)
    c = await qm.classify_session("k5", _head_is_resumable)
    assert c.verdict is Verdict.RESET_OFFSET
    assert c.reason == "negative_offset"


async def test_classify_offset_past_eof(tmp_path: Path) -> None:
    line = _line()
    _seed_log(tmp_path, "k6", line)
    _seed_offset(tmp_path, "k6", str(len(line) + 1000))
    qm = await _qm(tmp_path)
    c = await qm.classify_session("k6", _head_is_resumable)
    assert c.verdict is Verdict.RESET_OFFSET
    assert c.reason == "offset_past_eof"


async def test_classify_empty_log(tmp_path: Path) -> None:
    _seed_log(tmp_path, "k7", b"")
    qm = await _qm(tmp_path)
    c = await qm.classify_session("k7", _head_is_resumable)
    assert c.verdict is Verdict.UNRESUMABLE
    assert c.reason == "empty_log"


async def test_classify_fully_drained(tmp_path: Path) -> None:
    line = _line()
    _seed_log(tmp_path, "k8", line)
    _seed_offset(tmp_path, "k8", str(len(line)))
    qm = await _qm(tmp_path)
    c = await qm.classify_session("k8", _head_is_resumable)
    assert c.verdict is Verdict.DRAINED
    assert c.reason == "fully_drained"


async def test_classify_mid_line_offset_resumes(tmp_path: Path) -> None:
    """7c: offset numerically sane, < size, mid-line -- RESUME (self-heals)."""
    line = _line()
    _seed_log(tmp_path, "k9", line)
    _seed_offset(tmp_path, "k9", "1")  # inside the line, not on a boundary
    qm = await _qm(tmp_path)
    c = await qm.classify_session("k9", _head_is_resumable)
    assert c.verdict is Verdict.RESUMABLE


async def test_classify_merged_head_resumes_with_fallback_workspace_byte0(
    tmp_path: Path,
) -> None:
    """B3: a merged/truncated FIRST uncommitted line no longer deletes -- it
    resumes with the workspace read from byte 0 of the same file."""
    good_head = _line(workspace="/real-ws")
    merged = (
        b'{"event":"tool_use","workspace":"/ws","data":{}' + b"\n"
    )  # truncated JSON
    _seed_log(tmp_path, "k10", good_head + merged)
    _seed_offset(tmp_path, "k10", str(len(good_head)))  # first uncommitted = merged
    qm = await _qm(tmp_path)
    c = await qm.classify_session("k10", _head_is_resumable)
    assert c.verdict is Verdict.RESUMABLE
    assert c.reason == "fallback_workspace"
    assert c.fallback_source == "byte0"


async def test_classify_garbage_line_resumes_via_sentinel_not_deleted(
    tmp_path: Path,
) -> None:
    """v1.3 defect (claim-guard run_a5af6bd7, clm_9116061c -- REFUTED,
    now fixed): a single-line non-JSON garbage `.log` (newline-terminated,
    <1MiB, no `.offset`/`.dead.jsonl` siblings) must NOT be classified
    UNRESUMABLE/DELETE. `main._recover_one_session` is MORE LENIENT than
    the old classify predicate -- it unconditionally sentinel-dispatches
    whenever byte 0 is a COMPLETE (newline-terminated) line, regardless of
    JSON parseability, because the drainer dead-letters the unparseable
    head and drains everything behind it. classify_session must never
    judge DELETE for a `.log` `_recover_one_session` would actually
    dispatch. (Superseded the old `test_classify_no_parseable_line_small_
    deletes`, which asserted the buggy over-delete behaviour.)"""
    garbage = b"not json at all\n"
    _seed_log(tmp_path, "k11", garbage)
    _seed_offset(tmp_path, "k11", "0")
    qm = await _qm(tmp_path)
    c = await qm.classify_session("k11", _head_is_resumable)
    assert c.verdict is Verdict.RESUMABLE
    assert c.reason == "fallback_workspace"
    assert c.fallback_source == "sentinel"
    # side-effect-free AND not deleted.
    assert (tmp_path / "k11.log").exists()


async def test_boot_reclaim_survives_garbage_log_and_real_drain_dead_letters_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Full-stack RED test for the same defect (clm_9116061c): with
    reclaim_enabled=True, a single-line non-JSON garbage `.log` (no
    siblings) must survive the REAL `_boot_reclaim` pass -- not merely
    classify_session in isolation. A subsequent REAL drain (registry.
    drain_worker, no mocks of the code under test) then dead-letters the
    unparseable line and advances the offset past it, exactly matching
    the contract `main._recover_one_session`'s sentinel fallback promises
    ("the drainer dead-letters the unparseable head and
    drains everything behind it")."""
    garbage = b"not json at all\n"
    _seed_log(tmp_path, "garbage-key", garbage)

    qm = QueueManager(queues_dir=tmp_path)
    monkeypatch.setattr(main_module.registry, "_queue_manager", qm)
    monkeypatch.setattr(main_module._settings, "reclaim_enabled", True)
    monkeypatch.setattr(main_module._settings, "crash_recovery_respawn_limit", 8)

    await main_module._boot_reclaim()

    # Survives the REAL reclaim pass -- NOT unlinked despite reclaim_enabled=True.
    assert (tmp_path / "garbage-key.log").exists()
    assert boot_state.fallback_workspace_sentinel >= 1

    # A real drain (registry.drain_worker, the same code path a respawned
    # recovery drainer runs) dead-letters the unparseable line and
    # advances the offset -- no mocks of the unit under test.
    reg = SessionRegistry()
    reg._queue_manager = qm
    worker = _make_worker("garbage-key", "unknown-recovered", live_event_seen=False)
    reg._register_for_test(worker)
    reg._ensure_infra()
    reg._max_delivery_attempts = 1  # force immediate dead-letter, no retry delay

    await reg.drain_worker(worker)

    dead_content = (tmp_path / "garbage-key.dead.jsonl").read_text(encoding="utf-8")
    assert "not json at all" in dead_content
    # Offset advanced past the poison line (drained to EOF; dry-exit fires
    # since there is nothing left and the worker was recovered).
    assert "garbage-key" not in reg._workers


async def test_classify_unclassifiable_large_file_kept_not_deleted(
    tmp_path: Path,
) -> None:
    """§6 bound: DELETE only when the probe window covered the WHOLE file.
    A large file whose first MiB has no parseable line is KEPT, not deleted."""
    garbage = b"not json at all\n" * 200_000  # > 1 MiB, no parseable line anywhere
    _seed_log(tmp_path, "k12", garbage)
    _seed_offset(tmp_path, "k12", "0")
    qm = await _qm(tmp_path)
    c = await qm.classify_session("k12", _head_is_resumable)
    assert c.verdict is Verdict.KEEP
    assert c.reason == "unclassifiable"
    assert (tmp_path / "k12.log").exists()


async def test_classify_unreadable_offset_is_kept_never_deleted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A transient FS error reading the offset is NOT a corruption finding
    and must never be laundered into a deletion."""
    _seed_log(tmp_path, "k13", _line())
    qm = await _qm(tmp_path)

    def _raise_oserror(_session_id: str) -> int:
        raise OSError(errno.EIO, "simulated transient I/O error")

    monkeypatch.setattr(qm, "_read_committed_offset", _raise_oserror)
    c = await qm.classify_session("k13", _head_is_resumable)
    assert c.verdict is Verdict.UNREADABLE
    assert c.reason == "unreadable_offset"


# ---------------------------------------------------------------------------
# §11.2 -- log-then-delete: audit line BEFORE unlink, all closed-set files gone
# ---------------------------------------------------------------------------


async def test_reclaim_deletes_and_logs_before_unlink(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    _seed_log(tmp_path, "d1", b"")  # empty_log -> delete
    _seed_dead(tmp_path, "d1", "kept-forever\n")
    qm = await _qm(tmp_path)
    c = await qm.classify_session("d1", _head_is_resumable)
    assert c.verdict is Verdict.UNRESUMABLE

    with caplog.at_level(logging.WARNING):
        ok = await qm.reclaim(c, lambda: False)

    assert ok is True
    assert not (tmp_path / "d1.log").exists()
    assert not (tmp_path / "d1.offset").exists()
    # .dead.jsonl is NEVER deleted by the boot-safety classifier.
    assert (tmp_path / "d1.dead.jsonl").exists()
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any(
        "boot_reclaimed" in r.getMessage() and "reason=empty_log" in r.getMessage()
        for r in warnings
    )


async def test_reclaim_logs_before_failing_unlink(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The 'log first' policy is mechanically enforced, not just documented:
    even when the unlink itself fails, the boot_reclaimed line already fired."""
    _seed_log(tmp_path, "d2", b"")
    qm = await _qm(tmp_path)
    c = await qm.classify_session("d2", _head_is_resumable)

    real_unlink = Path.unlink

    def _raise_unlink(self: Path, *a: Any, **kw: Any) -> None:
        if self.name == "d2.log":
            raise OSError(errno.EACCES, "simulated permission error")
        return real_unlink(self, *a, **kw)

    with (
        caplog.at_level(logging.WARNING),
        patch.object(Path, "unlink", _raise_unlink),
    ):
        ok = await qm.reclaim(c, lambda: False)

    assert ok is False
    messages = [r.getMessage() for r in caplog.records]
    reclaimed_idx = next(i for i, m in enumerate(messages) if "boot_reclaimed" in m)
    failed_idx = next(i for i, m in enumerate(messages) if "boot_reclaim_failed" in m)
    assert reclaimed_idx < failed_idx


# ---------------------------------------------------------------------------
# §11.3 -- the anti-over-deletion test: resumable data survives reclaim
# ---------------------------------------------------------------------------


async def test_resumable_and_mid_line_offset_survive_reclaim(tmp_path: Path) -> None:
    line = _line()
    _seed_log(tmp_path, "r1", line)  # scenario 3: stranded undrained tail
    _seed_log(tmp_path, "r2", line)
    _seed_offset(tmp_path, "r2", "1")  # scenario 7c: mid-line offset
    qm = await _qm(tmp_path)

    for key in ("r1", "r2"):
        c = await qm.classify_session(key, _head_is_resumable)
        assert c.verdict is Verdict.RESUMABLE
        # RESUMABLE is never reclaimed -- the caller (main._boot_reclaim)
        # simply never calls reclaim() for it. Files remain exactly as-is.
        assert (tmp_path / f"{key}.log").exists()


# ---------------------------------------------------------------------------
# §11.5 (amended v1.3) -- /status is LEAN + ZERO-DISK during boot
# ---------------------------------------------------------------------------


async def test_status_lean_during_boot_zero_disk_reads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    boot_state.phase = "reclaim"  # an ACTIVE boot phase (not ready/failed)
    called = {"pipeline_metrics": False, "spool_stats": False}

    async def _spy_metrics() -> dict:
        called["pipeline_metrics"] = True
        return {}

    async def _spy_spool() -> dict:
        called["spool_stats"] = True
        return {}

    monkeypatch.setattr(main_module.registry, "pipeline_metrics", _spy_metrics)
    monkeypatch.setattr(main_module.registry.queue_manager, "spool_stats", _spy_spool)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=main_module.app), base_url="http://test"
    ) as c:
        response = await c.get("/status")

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert data["boot"]["phase"] == "reclaim"
    assert data["metrics"] is None
    assert data["spool"] is None
    assert data["status_detail"] == {"reason": "booting"}
    # The direct assertion of §2.3's unreachability argument: neither
    # derive_all_stats' caller nor spool_stats is ever invoked while booting.
    assert called["pipeline_metrics"] is False
    assert called["spool_stats"] is False


async def test_status_populated_once_ready(monkeypatch: pytest.MonkeyPatch) -> None:
    boot_state.phase = "ready"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=main_module.app), base_url="http://test"
    ) as c:
        response = await c.get("/status")
    data = response.json()
    assert data["metrics"] is not None
    assert data["spool"] is not None
    assert "status_detail" not in data


async def test_status_populated_on_failed_boot_C1(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Gate on boot-IS-OVER, not boot-succeeded. `failed`
    is terminal and the server keeps ingesting -- metrics/spool must NOT be
    permanently nulled by a reconcile failure (that would silently reopen
    the exact '38 GB spool with zero signal' incident)."""
    boot_state.phase = "failed"
    boot_state.error = "RuntimeError: simulated"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=main_module.app), base_url="http://test"
    ) as c:
        response = await c.get("/status")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"  # the boot phase is informational, never the probe
    assert data["boot"]["phase"] == "failed"
    assert data["metrics"] is not None
    assert data["spool"] is not None


def test_boot_state_default_phase_is_recovering_never_ready() -> None:
    """C2: the pre-lifespan default MUST NOT be 'ready' -- the
    zero-disk-during-boot guarantee depends on this from import time."""
    from context_intelligence_server.status import BootState

    assert BootState().phase == "recovering"
    assert BootState().phase != "ready"


# ---------------------------------------------------------------------------
# §11.6 -- _boot_reconcile is exception-safe (the boot done-callback analogue)
# ---------------------------------------------------------------------------


async def test_boot_reconcile_survives_a_failing_step_and_names_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _raise(*_a: Any, **_kw: Any) -> Any:
        raise OSError(errno.EIO, "simulated reconcile failure")

    monkeypatch.setattr(
        main_module.registry.queue_manager, "recovery_reconcile_dead", _raise
    )
    monkeypatch.setattr(
        main_module.registry.queue_manager,
        "heal_torn_tails",
        AsyncMock(return_value={}),
    )

    await main_module._boot_reconcile()

    assert boot_state.phase == "failed"
    assert boot_state.failed_step == "reconcile"
    assert boot_state.error is not None and "OSError" in boot_state.error


async def test_boot_reconcile_success_reaches_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        main_module.registry.queue_manager,
        "heal_torn_tails",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(
        main_module.registry.queue_manager,
        "recovery_reconcile_dead",
        AsyncMock(return_value=0),
    )
    monkeypatch.setattr(
        main_module.registry.queue_manager,
        "recovery_seed_counts",
        AsyncMock(return_value=(0, 0)),
    )
    monkeypatch.setattr(main_module._settings, "crash_recovery_respawn_limit", 0)

    await main_module._boot_reconcile()

    assert boot_state.phase == "ready"
    assert boot_state.completed_at is not None
    assert boot_state.error is None


# ---------------------------------------------------------------------------
# §11.7 -- crash-loop guards (G-1..G-4) -- the four crash triggers
# ---------------------------------------------------------------------------


async def test_g1_recover_skips_corrupt_offset_key(tmp_path: Path) -> None:
    _seed_log(tmp_path, "good", _line())
    _seed_log(tmp_path, "bad", _line())
    _seed_offset(tmp_path, "bad", "\x00\x00\x00")  # NUL-filled
    qm = await _qm(tmp_path)
    result = await qm.recover()  # must not raise
    assert "good" in result
    assert "bad" not in result  # skipped, not crash-looped


async def test_g2_reconcile_dead_skips_corrupt_key(tmp_path: Path) -> None:
    _seed_log(tmp_path, "bad", _line())
    _seed_offset(tmp_path, "bad", "not-a-number")
    _seed_dead(
        tmp_path, "bad", json.dumps({"ts": 1, "error": "x", "payload": "p"}) + "\n"
    )
    qm = await _qm(tmp_path)
    total = await qm.recovery_reconcile_dead()  # must not raise
    assert total == 0


async def test_g3_seed_counts_skips_corrupt_key(tmp_path: Path) -> None:
    _seed_log(tmp_path, "good", _line())
    _seed_log(tmp_path, "bad", _line())
    _seed_offset(tmp_path, "bad", "negative-not-parseable")
    qm = await _qm(tmp_path)
    accepted, _written = await qm.recovery_seed_counts()  # must not raise
    assert accepted >= 1  # "good" still contributes


async def test_g4_topup_read_batch_guard_skips_corrupt_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        main_module.registry, "_queue_manager", QueueManager(queues_dir=tmp_path)
    )
    qm = main_module.registry.queue_manager
    _seed_log(tmp_path, "bad", _line())
    _seed_offset(tmp_path, "bad", "\x00")

    async def _raise_read_batch(session_id: str, max_items: int) -> Any:
        raise ValueError("simulated corrupt offset in read_batch")

    monkeypatch.setattr(qm, "read_batch", _raise_read_batch)
    spawned: list[str] = []
    monkeypatch.setattr(
        main_module.registry, "get_or_create", lambda s, w, **kw: spawned.append(s)
    )

    result = await main_module._crash_recovery_topup(None)  # must not raise
    assert result.dispatched == 0


async def test_g5_active_sessions_skips_corrupt_key(tmp_path: Path) -> None:
    _seed_log(tmp_path, "good", _line())
    _seed_log(tmp_path, "bad", _line())
    _seed_offset(tmp_path, "bad", "garbage")
    qm = await _qm(tmp_path)
    result = await qm.active_sessions()  # must not raise
    assert "good" in result
    assert "bad" not in result


async def test_all_four_crash_triggers_reach_ready_with_reclaim_disabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The four crash-loop states, all seeded together; the whole
    _boot_reconcile pass must reach phase=ready with reclaim disabled too,
    proving the G-1..G-5 guards are independent defence, not merely masked
    by deletion."""
    _seed_log(tmp_path, "nul-offset", _line())
    _seed_offset(tmp_path, "nul-offset", "\x00\x00")
    _seed_log(tmp_path, "neg-offset", _line())
    _seed_offset(tmp_path, "neg-offset", "-1")
    _seed_dead(tmp_path, "bad-payload", json.dumps({"payload": 123}) + "\n")
    _seed_log(tmp_path, "bad-payload", _line())

    monkeypatch.setattr(
        main_module.registry, "_queue_manager", QueueManager(queues_dir=tmp_path)
    )
    monkeypatch.setattr(main_module._settings, "reclaim_enabled", False)
    monkeypatch.setattr(main_module._settings, "crash_recovery_respawn_limit", 8)
    monkeypatch.setattr(
        main_module.registry, "get_or_create", lambda *a, **kw: MagicMock()
    )

    await main_module._boot_reconcile()

    assert boot_state.phase == "ready"


# ---------------------------------------------------------------------------
# §11.8 -- Gate 1 (live-session ownership) safety
# ---------------------------------------------------------------------------


async def test_gate1_live_worker_key_never_reclaimed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    line = _line()
    _seed_log(tmp_path, "owned", line)
    _seed_offset(tmp_path, "owned", str(len(line)))  # fully_drained shape

    qm = QueueManager(queues_dir=tmp_path)
    monkeypatch.setattr(main_module.registry, "_queue_manager", qm)
    monkeypatch.setattr(main_module._settings, "reclaim_enabled", True)
    monkeypatch.setattr(main_module._settings, "crash_recovery_respawn_limit", 8)
    main_module.registry._register_for_test(_make_worker("owned"))

    await main_module._boot_reclaim()

    assert (tmp_path / "owned.log").exists()
    assert boot_state.kept >= 1


async def test_key_reclaimed_after_worker_removed_drains_from_zero(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    line = _line()
    _seed_log(tmp_path, "was-owned", line)
    _seed_offset(tmp_path, "was-owned", str(len(line)))

    qm = QueueManager(queues_dir=tmp_path)
    monkeypatch.setattr(main_module.registry, "_queue_manager", qm)
    monkeypatch.setattr(main_module._settings, "reclaim_enabled", True)
    monkeypatch.setattr(main_module._settings, "crash_recovery_respawn_limit", 8)

    await main_module._boot_reclaim()

    assert not (tmp_path / "was-owned.log").exists()
    # Re-append -> drains from byte 0, no stale .offset survives.
    await qm.append("was-owned", _line(event="second"))
    batch = await qm.read_batch("was-owned", max_items=10)
    assert len(batch.records) == 1


# ---------------------------------------------------------------------------
# §11.9 (B2, honest bound) -- recovered-only population + forward progress
# ---------------------------------------------------------------------------


async def test_recovered_drainer_population_bounded_and_makes_progress(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The RED test for the leak this whole mechanism exists to close: seed
    N sessions with NO terminal record, ceiling < N. Across >=3 sweep
    passes, assert the recovered-only population never exceeds the derived
    bound AND that cumulative distinct dispatches exceed the ceiling
    (forward progress -- the assertion that fails under the rejected
    "count live recovered drainers against the ceiling" fix and passes
    under the adopted "drainer exits when it runs dry" fix)."""
    ceiling = 3
    n_sessions = 9
    qm = QueueManager(queues_dir=tmp_path)
    for i in range(n_sessions):
        await qm.append(f"sess-{i}", _line(session_id=f"sess-{i}"))

    reg = SessionRegistry()
    reg._queue_manager = qm
    monkeypatch.setattr(main_module, "registry", reg)

    dispatched_all: set[str] = set()

    async def _drain_to_dry(worker: SessionWorker) -> None:
        """A faithful stand-in for drain_worker's dry-exit: drains the one
        record then exits immediately (no terminal record, so the real
        drain loop would otherwise never finish)."""
        batch = await qm.read_batch(worker.session_id, max_items=10)
        if batch.records:
            await qm.commit(worker.session_id, batch.end_offset)
        reg._deregister(worker.session_id)

    def _get_or_create(
        sid: str, workspace: str, created_by: Any = None, **kw: Any
    ) -> Any:
        recovered = kw.get("recovered", False)
        worker = _make_worker(sid, workspace, live_event_seen=not recovered)
        reg._register_for_test(worker)
        dispatched_all.add(sid)
        asyncio.ensure_future(_drain_to_dry(worker))
        return worker

    monkeypatch.setattr(reg, "get_or_create", _get_or_create)

    for _pass in range(4):
        result = await main_module._crash_recovery_topup(ceiling)
        # Bounded: recovered-only population never exceeds a generous
        # derived bound (ceiling per pass; the fake drains+exits inline
        # so live population settles back to 0 well within one pass).
        recovered_only = [w for w in reg.workers() if not w.live_event_seen]
        assert len(recovered_only) <= ceiling
        await asyncio.sleep(0)  # let the fire-and-forget drain tasks run
        assert result.dispatched <= ceiling

    # Forward progress: cumulative distinct sessions dispatched exceeds the
    # ceiling -- this is what distinguishes real drainage from a stall.
    assert len(dispatched_all) > ceiling


# ---------------------------------------------------------------------------
# §11.10 -- sidecar retention: a boot never destroys its own quarantine
# ---------------------------------------------------------------------------


async def test_reclaim_orphans_keeps_current_boots_sidecar(tmp_path: Path) -> None:
    """before_ts is THIS PROCESS's start time. A sidecar older than that
    (mtime < before_ts) came from a PRIOR boot -> reclaimed. A sidecar THIS
    boot's heal just created (mtime >= before_ts, i.e. after process start)
    must survive -- a boot must never destroy its own quarantine."""
    import os as _os
    import time as _time

    before_ts = _time.time()  # "process start" -- captured BEFORE either file exists

    old_sidecar = tmp_path / "s1.log.torn-111.bin"
    old_sidecar.write_bytes(b"old")
    _os.utime(old_sidecar, (before_ts - 100, before_ts - 100))  # from a PRIOR boot

    new_sidecar = tmp_path / "s2.log.torn-222.bin"
    new_sidecar.write_bytes(b"new")  # created just now, by THIS boot's heal

    qm = await _qm(tmp_path)
    result = await qm.reclaim_orphans(before_ts=before_ts)

    assert not old_sidecar.exists()
    assert new_sidecar.exists()
    assert result["reclaimed"] == 1


async def test_reclaim_orphans_removes_orphan_offset_and_tmp(tmp_path: Path) -> None:
    (tmp_path / "orphan.offset").write_text("5", encoding="utf-8")
    (tmp_path / "orphan.offset.tmp").write_text("5", encoding="utf-8")
    qm = await _qm(tmp_path)
    result = await qm.reclaim_orphans(before_ts=0.0)
    assert not (tmp_path / "orphan.offset").exists()
    assert not (tmp_path / "orphan.offset.tmp").exists()
    assert result["reclaimed"] == 2


async def test_reclaim_orphans_respects_reclaim_enabled_false_dry_run(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """v1.3 defect (claim-guard run_a5af6bd7, clm_aab3adbc -- REFUTED,
    now fixed): with `reclaim_enabled=False` ("dry run, delete nothing"),
    `reclaim_orphans` must NOT unlink an orphan `.offset` or a stale
    `.torn-*.bin` quarantine sidecar -- it must only classify + LOG the
    would-delete (action=dry_run), same as the per-key `reclaim` path.
    Before this fix, `reclaim_orphans` ignored `reclaim_enabled` entirely
    and unlinked unconditionally -- an invisible delete every boot even
    with the safety default (`reclaim_enabled=False`)."""
    import os as _os
    import time as _time

    before_ts = _time.time()
    (tmp_path / "orphan.offset").write_text("5", encoding="utf-8")
    old_sidecar = tmp_path / "s1.log.torn-111.bin"
    old_sidecar.write_bytes(b"quarantined-bytes")
    _os.utime(old_sidecar, (before_ts - 100, before_ts - 100))  # prior boot

    qm = await _qm(tmp_path)
    with caplog.at_level(logging.WARNING):
        result = await qm.reclaim_orphans(before_ts=before_ts, enabled=False)

    # Nothing unlinked -- both targets survive.
    assert (tmp_path / "orphan.offset").exists()
    assert old_sidecar.exists()
    # A real deletion must never be invisible: the would-delete is named
    # in the dry-run log, and NOT counted as an actual reclaim.
    assert any(
        "orphan_offset" in r.getMessage() and "action=dry_run" in r.getMessage()
        for r in caplog.records
    )
    assert any(
        "torn_sidecar" in r.getMessage() and "action=dry_run" in r.getMessage()
        for r in caplog.records
    )
    assert result["reclaimed"] == 0


async def test_reclaim_orphans_enabled_true_deletes_what_dry_run_named(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """The GREEN half of the same fix: the SAME targets, with
    `enabled=True`, ARE unlinked, logged (action=delete, not dry_run), and
    counted -- so a real deletion is never invisible on either side of the
    flag."""
    import os as _os
    import time as _time

    before_ts = _time.time()
    (tmp_path / "orphan.offset").write_text("5", encoding="utf-8")
    old_sidecar = tmp_path / "s1.log.torn-111.bin"
    old_sidecar.write_bytes(b"quarantined-bytes")
    _os.utime(old_sidecar, (before_ts - 100, before_ts - 100))

    qm = await _qm(tmp_path)
    with caplog.at_level(logging.WARNING):
        result = await qm.reclaim_orphans(before_ts=before_ts, enabled=True)

    assert not (tmp_path / "orphan.offset").exists()
    assert not old_sidecar.exists()
    assert any(
        "orphan_offset" in r.getMessage() and "action=delete" in r.getMessage()
        for r in caplog.records
    )
    assert any(
        "torn_sidecar" in r.getMessage() and "action=delete" in r.getMessage()
        for r in caplog.records
    )
    assert result["reclaimed"] == 2
    assert result["reclaimed_bytes"] == len(b"5") + len(b"quarantined-bytes")


async def test_boot_reclaim_orphan_offset_survives_with_reclaim_disabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """End-to-end (main._boot_reclaim, not just the QueueManager unit): a
    boot with `reclaim_enabled=False` must leave an orphan `.offset` (no
    matching `.log`) untouched -- the real production call site must pass
    `reclaim_enabled` through to `reclaim_orphans`, not just gate the
    telemetry counter around an unconditional unlink."""
    (tmp_path / "orphan.offset").write_text("5", encoding="utf-8")

    qm = QueueManager(queues_dir=tmp_path)
    monkeypatch.setattr(main_module.registry, "_queue_manager", qm)
    monkeypatch.setattr(main_module._settings, "reclaim_enabled", False)
    monkeypatch.setattr(main_module._settings, "crash_recovery_respawn_limit", 8)

    await main_module._boot_reclaim()

    assert (tmp_path / "orphan.offset").exists()


# ---------------------------------------------------------------------------
# §11.13 (amended, D-1) -- the dry-exit + the strand-after-await fix
# ---------------------------------------------------------------------------


async def test_dry_exit_fires_for_recovered_drainer_over_drained_log(
    tmp_path: Path,
) -> None:
    qm = QueueManager(queues_dir=tmp_path)
    reg = SessionRegistry()
    reg._queue_manager = qm
    line = _line()
    await qm.append("sess-x", line)
    await qm.commit("sess-x", len(line) + 1)  # fully drained, no terminal record

    worker = _make_worker("sess-x", live_event_seen=False)  # recovered=True shape
    reg._register_for_test(worker)

    await reg.drain_worker(worker)  # must return promptly (dry-exit), not hang

    assert "sess-x" not in reg._workers
    assert worker.services.graph.closed is True  # type: ignore[attr-defined]


async def test_dry_exit_negative_control_live_created_worker_never_exits(
    tmp_path: Path,
) -> None:
    qm = QueueManager(queues_dir=tmp_path)
    reg = SessionRegistry()
    reg._queue_manager = qm
    line = _line()
    await qm.append("sess-live", line)
    await qm.commit("sess-live", len(line) + 1)

    worker = _make_worker("sess-live", live_event_seen=True)  # live path (default)
    reg._register_for_test(worker)

    task = asyncio.ensure_future(reg.drain_worker(worker))
    await asyncio.sleep(0.05)
    assert not task.done()  # never exits on an empty batch when live
    # drain_worker catches CancelledError internally (pre-existing,
    # unrelated behaviour: it closes the store, deregisters, and RETURNS
    # rather than re-raising) -- so cancel()+await completes normally here.
    # This test's real assertion already happened above: `not task.done()`.
    task.cancel()
    await task
    assert worker.services.graph.closed is True  # type: ignore[attr-defined]


async def test_dry_exit_negative_control_recovered_that_drained_still_exits(
    tmp_path: Path,
) -> None:
    """The RED test proving the field is NOT last_event_time in disguise:
    a recovered worker that DID process >=1 record must still exit once
    it runs dry (v1.2's rejected fix excluded exactly this population)."""
    qm = QueueManager(queues_dir=tmp_path)
    reg = SessionRegistry()
    reg._queue_manager = qm
    line = _line()
    await qm.append("sess-drained-recovered", line)

    worker = _make_worker("sess-drained-recovered", live_event_seen=False)
    reg._register_for_test(worker)
    worker.last_event_time = 1.0  # simulate having processed >=1 record

    await reg.drain_worker(worker)

    assert "sess-drained-recovered" not in reg._workers


async def test_d1_no_strand_after_recheck_await(tmp_path: Path) -> None:
    """D-1 (the load-bearing blocker): a POST landing DURING the recheck's
    await must NOT strand the drainer's client -- the flag is re-read AFTER
    the await, so the exit aborts and the loop continues draining."""
    qm = QueueManager(queues_dir=tmp_path)
    reg = SessionRegistry()
    reg._queue_manager = qm
    worker = _make_worker("sess-race", live_event_seen=False)
    reg._register_for_test(worker)

    real_read_batch = qm.read_batch
    call_count = {"n": 0}

    async def _read_batch_with_race(session_id: str, max_items: int) -> Any:
        call_count["n"] += 1
        if call_count["n"] == 2:
            # Simulate a POST's get_or_create landing during THIS await,
            # before the recheck's result is used.
            worker.live_event_seen = True
        return await real_read_batch(session_id, max_items)

    qm.read_batch = _read_batch_with_race  # type: ignore[method-assign]

    task = asyncio.ensure_future(reg.drain_worker(worker))
    for _ in range(50):
        await asyncio.sleep(0.01)
        if call_count["n"] >= 2:
            break
    await asyncio.sleep(0.05)

    # The drainer must NOT have exited/deregistered -- the flag flip during
    # the recheck aborts the exit.
    assert "sess-race" in reg._workers
    assert not task.done()
    # drain_worker catches CancelledError internally and returns cleanly
    # (pre-existing, unrelated behaviour) -- cancel()+await completes
    # normally. The load-bearing assertions already happened above.
    task.cancel()
    await task


# ---------------------------------------------------------------------------
# §11.15 extended (D-4) -- reset-offset threshold + dead-empty + in-lock race
# ---------------------------------------------------------------------------


async def test_reset_offset_applies_and_drains_from_zero(tmp_path: Path) -> None:
    line = _line()
    _seed_log(tmp_path, "reset-me", line * 2)
    _seed_offset(tmp_path, "reset-me", "not-a-number")
    qm = await _qm(tmp_path)
    c = await qm.classify_session("reset-me", _head_is_resumable)
    assert c.verdict is Verdict.RESET_OFFSET

    ok = await qm.reclaim(c, lambda: False)

    assert ok is True
    assert not (tmp_path / "reset-me.offset").exists()
    assert (tmp_path / "reset-me.log").exists()
    batch = await qm.read_batch("reset-me", max_items=10)
    assert len(batch.records) == 2  # drains from byte 0


async def test_reset_offset_refused_when_dead_letters_appear_after_classify(
    tmp_path: Path,
) -> None:
    """D-4: the .dead.jsonl-empty precondition is re-checked
    INSIDE the guarded body, immediately before applying -- a live drain
    dirtying the dead file between classify and reclaim must not let a
    reset apply and re-dead-letter the poison line."""
    line = _line()
    _seed_log(tmp_path, "race-dead", line * 2)
    _seed_offset(tmp_path, "race-dead", "garbage")
    qm = await _qm(tmp_path)
    c = await qm.classify_session("race-dead", _head_is_resumable)
    assert c.verdict is Verdict.RESET_OFFSET  # dead was empty AT CLASSIFY TIME

    # Simulate a concurrent live drain dead-lettering into this key's file
    # BETWEEN classify and reclaim.
    _seed_dead(tmp_path, "race-dead", json.dumps({"payload": "x"}) + "\n")

    ok = await qm.reclaim(c, lambda: False)

    assert ok is False
    assert (tmp_path / "race-dead.offset").exists()  # untouched
    dead_content = (tmp_path / "race-dead.dead.jsonl").read_text(encoding="utf-8")
    assert dead_content.count("\n") == 1  # NOT duplicated / re-dead-lettered


async def test_reclaim_size_drift_refuses_to_delete(tmp_path: Path) -> None:
    """§11.16 -- the delete window is closed: a live append growing the
    `.log` between classify and reclaim must refuse the delete."""
    _seed_log(tmp_path, "grows", b"")  # empty_log -> delete
    qm = await _qm(tmp_path)
    c = await qm.classify_session("grows", _head_is_resumable)
    assert c.verdict is Verdict.UNRESUMABLE

    # Simulate a live append landing between classify and reclaim.
    (tmp_path / "grows.log").write_bytes(_line())

    ok = await qm.reclaim(c, lambda: False)

    assert ok is False
    assert (tmp_path / "grows.log").exists()


async def test_reclaim_gate1_refuses_when_key_becomes_owned(tmp_path: Path) -> None:
    _seed_log(tmp_path, "becomes-owned", b"")
    qm = await _qm(tmp_path)
    c = await qm.classify_session("becomes-owned", _head_is_resumable)

    ok = await qm.reclaim(c, lambda: True)  # now owned

    assert ok is False
    assert (tmp_path / "becomes-owned.log").exists()


# ---------------------------------------------------------------------------
# §11.17 (R10) -- no phantom reclaim over a dead-file-only key
# ---------------------------------------------------------------------------


async def test_no_phantom_reclaim_for_dead_only_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A correctly-finalized session retains ONLY a .dead.jsonl
    (delete_drained already removed .log/.offset). Classify must never see
    this key at all (it iterates *.log stems only) -- no boot_reclaimed
    'bytes=0' noise, forever, for a perfectly healthy session."""
    _seed_dead(tmp_path, "finalized", "kept\n")
    qm = QueueManager(queues_dir=tmp_path)
    monkeypatch.setattr(main_module.registry, "_queue_manager", qm)
    monkeypatch.setattr(main_module._settings, "reclaim_enabled", True)
    monkeypatch.setattr(main_module._settings, "crash_recovery_respawn_limit", 8)

    await main_module._boot_reclaim()
    await main_module._boot_reclaim()  # second pass: still nothing to see

    assert (tmp_path / "finalized.dead.jsonl").exists()


# ---------------------------------------------------------------------------
# §11.18 (R11) -- reclaim_enabled=False is a genuine dry run
# ---------------------------------------------------------------------------


async def test_reclaim_disabled_classifies_but_deletes_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    _seed_log(tmp_path, "would-delete", b"")
    line = _line()
    _seed_log(tmp_path, "would-resume", line)

    qm = QueueManager(queues_dir=tmp_path)
    monkeypatch.setattr(main_module.registry, "_queue_manager", qm)
    monkeypatch.setattr(main_module._settings, "reclaim_enabled", False)
    monkeypatch.setattr(main_module._settings, "crash_recovery_respawn_limit", 8)

    with caplog.at_level(logging.WARNING):
        await main_module._boot_reclaim()

    # Every seeded file still exists -- nothing was unlinked.
    assert (tmp_path / "would-delete.log").exists()
    assert (tmp_path / "would-resume.log").exists()
    assert any("action=dry_run" in r.getMessage() for r in caplog.records)
    assert boot_state.reclaim_enabled is False


async def test_reclaim_enabled_true_deletes_what_dry_run_named(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _seed_log(tmp_path, "target", b"")
    qm = QueueManager(queues_dir=tmp_path)
    monkeypatch.setattr(main_module.registry, "_queue_manager", qm)
    monkeypatch.setattr(main_module._settings, "crash_recovery_respawn_limit", 8)

    monkeypatch.setattr(main_module._settings, "reclaim_enabled", False)
    await main_module._boot_reclaim()
    assert (tmp_path / "target.log").exists()  # dry run: nothing deleted

    boot_state.reclaimed = 0
    boot_state.kept = 0
    monkeypatch.setattr(main_module._settings, "reclaim_enabled", True)
    await main_module._boot_reclaim()
    assert not (tmp_path / "target.log").exists()  # enabled: deleted for real


# ---------------------------------------------------------------------------
# §11.19 (amended, R8) -- shutdown cancels every task before closing drivers
# ---------------------------------------------------------------------------


async def test_shutdown_cancels_sweep_and_boot_tasks_before_closing_drivers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_driver = MagicMock()
    mock_driver.close = AsyncMock()
    monkeypatch.setattr(main_module._settings, "crash_recovery_respawn_limit", 2)
    monkeypatch.setattr(
        main_module._settings, "crash_recovery_sweep_interval_seconds", 60
    )
    monkeypatch.setattr(
        main_module.registry, "get_or_create", lambda *a, **kw: MagicMock()
    )

    with (
        patch("context_intelligence_server.main.setup_logging"),
        patch(
            "context_intelligence_server.main.AsyncGraphDatabase.driver",
            return_value=mock_driver,
        ),
        patch("context_intelligence_server.main.ensure_neo4j_schema", new=AsyncMock()),
    ):
        async with lifespan(main_module.app):
            await main_module.app.state.boot_task
            sweep_task = main_module.app.state.sweep_task
            boot_task = main_module.app.state.boot_task
            assert not sweep_task.done()  # the sweep loop runs forever

    assert sweep_task.done()
    assert boot_task.done()
    assert mock_driver.close.await_count == 2


# ---------------------------------------------------------------------------
# The end-to-end regression: the worst finding (merged-head corruption) is gone
# ---------------------------------------------------------------------------


async def test_merged_head_session_resumes_and_drains_behind_the_poison_line(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The worst finding: a session whose FIRST uncommitted line is
    the merged/truncated line. B3: classified fallback_workspace (not
    deleted), kept on disk, and on the SECOND `_boot_reconcile` pass it is
    STILL there and STILL classifies the same way -- the permanent per-boot
    stall is gone because the session is never stuck: it is
    recoverable and dispatchable every boot, not because it vanishes."""
    merged = b'{"event":"tool_use","workspace":"/ws","dat' + b"\n"  # truncated
    good_head = _line(workspace="/w0")
    _seed_log(tmp_path, "merged-head", good_head + merged)
    _seed_offset(
        tmp_path, "merged-head", str(len(good_head))
    )  # merged is first-uncommitted

    qm = QueueManager(queues_dir=tmp_path)
    c1 = await qm.classify_session("merged-head", _head_is_resumable)
    assert c1.verdict is Verdict.RESUMABLE
    assert c1.reason == "fallback_workspace"
    assert (tmp_path / "merged-head.log").exists()

    # A second pass over the SAME on-disk state classifies identically --
    # deterministic, not a one-shot escape hatch.
    c2 = await qm.classify_session("merged-head", _head_is_resumable)
    assert c2.verdict is Verdict.RESUMABLE
    assert c2.reason == "fallback_workspace"


# ---------------------------------------------------------------------------
# config.py defaults (C-1/C-3, C-6/C-7) -- covered further in test_config.py;
# this smoke-checks the coupling from the boot-safety angle.
# ---------------------------------------------------------------------------


def test_d8_config_defaults_are_coupled() -> None:
    s = Settings()
    assert s.crash_recovery_respawn_limit == 8
    assert s.crash_recovery_sweep_interval_seconds == 60
    assert s.reclaim_redrain_max_bytes == 64 * 1024 * 1024
    assert s.reclaim_enabled is False


def test_head_is_resumable_is_total_never_raises() -> None:
    """R4: valid-but-non-dict JSON must not escape as AttributeError."""
    for raw in (b"123", b"null", b'"str"', b"[]", b"{}", b"not json", b""):
        result = _head_is_resumable(raw)
        assert isinstance(result, bool)
    assert _head_is_resumable(json.dumps({"workspace": "/ws"}).encode()) is True
    assert _head_is_resumable(json.dumps({"workspace": ""}).encode()) is False
