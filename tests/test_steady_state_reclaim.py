"""Steady-state queue reclaim + dead-letter retention.

Covers undrained-tail protection, crash-atomic compaction ordering,
dead-letter retention/expiry, /status non-blocking during compaction,
reclaim-on-finalize composing with compaction, and boot-vs-sweep expiry
accounting. No real Neo4j is used anywhere in this file (see
``tests/neo4j/test_steady_state_reclaim_neo4j.py`` for the Neo4j-backed cases).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest

import context_intelligence_server.main as main_module
import context_intelligence_server.queue_manager.filesystem as queue_manager_module
from context_intelligence_server.config import Settings
from context_intelligence_server.queue_manager import FileSystemQueueManager, QueueManager
from context_intelligence_server.registry import SessionRegistry, SessionWorker
from context_intelligence_server.services import HookStateService

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fixed(i: int) -> bytes:
    """A 9-byte, fixed-width record payload (10 bytes on disk incl. '\\n').

    Fixed width makes byte-offset arithmetic in the crash-atomicity tests
    exact and easy to reason about (event i occupies bytes [10*i, 10*i+10)).
    """
    return f"{i:09d}".encode("ascii")


def _line(event: str, workspace: str, data: dict) -> bytes:
    """Encode an appended event line exactly as POST /events stores it."""
    return json.dumps({"event": event, "workspace": workspace, "data": data}).encode(
        "utf-8"
    )


def _seed_dead(tmp_path: Path, key: str, content: str = "") -> Path:
    path = tmp_path / f"{key}.dead.jsonl"
    path.write_text(content, encoding="utf-8")
    return path


def _seed_log(tmp_path: Path, key: str, content: bytes = b"") -> Path:
    path = tmp_path / f"{key}.log"
    path.write_bytes(content)
    return path


_G_TS = "2026-08-21T00:00:00+00:00"


def _dead_record(payload: str, error: str = "boom") -> str:
    return json.dumps({"ts": time.time(), "error": error, "payload": payload}) + "\n"


# ---------------------------------------------------------------------------
# (b) undrained tail is never prefix-reclaimed past the committed offset
# ---------------------------------------------------------------------------


async def test_b_undrained_tail_never_reclaimed_past_committed_c_less_than_tail(
    tmp_path: Path,
) -> None:
    """C < E-C: commit 40 of 100 events, compact, and prove events 41..100
    survive in order, untouched, with committed rebased to 0."""
    qm = FileSystemQueueManager(queues_dir=tmp_path)
    sid = "s-tail-c-lt-tail"
    events = [_fixed(i) for i in range(100)]
    for ev in events:
        await qm.append(sid, ev)

    first_batch = await qm.read_batch(sid, max_items=40)
    assert len(first_batch.records) == 40
    await qm.commit(sid, first_batch.end_offset, None)
    c = first_batch.end_offset
    log_path = tmp_path / f"{sid}.log"
    e = log_path.stat().st_size
    assert c < e - c  # this sub-case: C < E-C

    reclaimed = await qm.compact_committed_prefix(sid, 0)
    assert reclaimed == c

    assert log_path.stat().st_size == e - c
    assert qm._read_committed_offset(sid) == 0

    remaining = await qm.read_batch(sid, max_items=1000)
    assert [r.raw for r in remaining.records] == events[40:]


async def test_b_undrained_tail_never_reclaimed_past_committed_c_greater_than_tail(
    tmp_path: Path,
) -> None:
    """C > E-C: commit 70 of 100 events -- the tail is now the SMALLER side."""
    qm = FileSystemQueueManager(queues_dir=tmp_path)
    sid = "s-tail-c-gt-tail"
    events = [_fixed(i) for i in range(100)]
    for ev in events:
        await qm.append(sid, ev)

    first_batch = await qm.read_batch(sid, max_items=70)
    assert len(first_batch.records) == 70
    await qm.commit(sid, first_batch.end_offset, None)
    c = first_batch.end_offset
    log_path = tmp_path / f"{sid}.log"
    e = log_path.stat().st_size
    assert c > e - c  # this sub-case: C > E-C

    reclaimed = await qm.compact_committed_prefix(sid, 0)
    assert reclaimed == c
    assert log_path.stat().st_size == e - c
    assert qm._read_committed_offset(sid) == 0

    remaining = await qm.read_batch(sid, max_items=1000)
    assert [r.raw for r in remaining.records] == events[70:]


async def test_b_below_min_prefix_bytes_is_a_noop(tmp_path: Path) -> None:
    """C below min_prefix_bytes bails without touching the file at all."""
    qm = FileSystemQueueManager(queues_dir=tmp_path)
    sid = "s-below-threshold"
    for i in range(10):
        await qm.append(sid, _fixed(i))
    batch = await qm.read_batch(sid, max_items=5)
    await qm.commit(sid, batch.end_offset, None)
    log_path = tmp_path / f"{sid}.log"
    before = log_path.read_bytes()

    reclaimed = await qm.compact_committed_prefix(sid, min_prefix_bytes=10_000)
    assert reclaimed == 0
    assert log_path.read_bytes() == before
    assert qm._read_committed_offset(sid) == batch.end_offset


# ---------------------------------------------------------------------------
# (c) crash atomicity
# ---------------------------------------------------------------------------


async def test_c_mid_copy_oserror_is_a_pure_noop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An OSError raised mid-copy (Precision 1) must not mutate anything and
    must not escape -- it is caught and the method returns 0."""
    qm = FileSystemQueueManager(queues_dir=tmp_path)
    sid = "s-mid-copy-fault"
    for i in range(9):
        await qm.append(sid, _fixed(i))
    batch = await qm.read_batch(sid, max_items=3)
    await qm.commit(sid, batch.end_offset, None)

    log_path = tmp_path / f"{sid}.log"
    offset_path = tmp_path / f"{sid}.offset"
    log_before = log_path.read_bytes()
    offset_before = offset_path.read_text(encoding="utf-8")
    assert offset_before.strip() == f'{{"v":1,"offset":{batch.end_offset},"cursor":null}}'

    def _raise(fd: int, data: bytes) -> None:
        raise OSError("simulated mid-copy failure")

    monkeypatch.setattr(
        queue_manager_module.FileSystemQueueManager, "_write_all", staticmethod(_raise)
    )

    reclaimed = await qm.compact_committed_prefix(sid, 0)  # must not raise

    assert reclaimed == 0
    assert log_path.read_bytes() == log_before
    assert offset_path.read_text(encoding="utf-8") == offset_before


async def test_c_window2_offset_rebased_before_log_replaced_bounded_redrive(
    tmp_path: Path,
) -> None:
    """Simulates a crash after the offset was rebased to 0 but before the log
    was replaced. A reader resuming from this on-disk state must see a
    bounded re-drive (the committed prefix duplicated) -- never a loss."""
    qm = FileSystemQueueManager(queues_dir=tmp_path)
    sid = "s-window2"
    events = [_fixed(i) for i in range(9)]
    for ev in events:
        await qm.append(sid, ev)
    batch = await qm.read_batch(sid, max_items=3)
    await qm.commit(sid, batch.end_offset, None)  # committed = 3 events (30 bytes)

    # Simulate the crash: offset already rebased to 0 (step 5 completed),
    # log NOT yet replaced (step 6 never ran).
    offset_path = tmp_path / f"{sid}.offset"
    offset_path.write_text("0", encoding="utf-8")

    resumed = await qm.read_batch(sid, max_items=100)
    resumed_raw = [r.raw for r in resumed.records]

    # Bounded re-drive: every original event is present -- zero loss.
    assert resumed_raw == events
    # the duplicated set is exactly the already-committed prefix
    assert resumed_raw[:3] == events[:3]


async def test_c_control_rejected_log_then_offset_order_loses_data(
    tmp_path: Path,
) -> None:
    """CONTROL: applies the alternative log-then-offset ordering by hand and
    stops mid-window (log replaced, offset not yet rewritten), proving that
    order silently drops undrained data -- why offset-before-log is used."""
    qm = FileSystemQueueManager(queues_dir=tmp_path)
    sid = "s-rejected-order"
    events = [_fixed(i) for i in range(9)]
    for ev in events:
        await qm.append(sid, ev)
    batch = await qm.read_batch(sid, max_items=3)
    await qm.commit(sid, batch.end_offset, None)  # committed = 30 bytes (C == 30)

    log_path = tmp_path / f"{sid}.log"
    c = qm._read_committed_offset(sid)
    e = log_path.stat().st_size
    assert c <= e - c  # precondition for the table's "lands inside the tail" case

    # alternative order, applied by hand: replace the log with the tail first
    tail_bytes = log_path.read_bytes()[c:e]
    rejected_tmp = tmp_path / f"{sid}.log.rejected.tmp"
    rejected_tmp.write_bytes(tail_bytes)
    os.replace(rejected_tmp, log_path)
    # crash here -- offset file still says C (unchanged)

    resumed = await qm.read_batch(sid, max_items=100)
    resumed_raw = [r.raw for r in resumed.records]

    # silent loss: the drainer resumes at byte C inside the already-shortened
    # file, skipping the first C bytes of real undrained data
    skipped_events = events[3:6]
    surviving_events = events[6:9]
    assert resumed_raw == surviving_events
    assert resumed_raw != events[3:9], (
        "control failed to reproduce the loss: the rejected ordering was "
        "expected to silently drop the leading undrained events"
    )
    for skipped in skipped_events:
        assert skipped not in resumed_raw


# ---------------------------------------------------------------------------
# (i) os.replace failure restores the offset -- pure no-op, zero drift
# ---------------------------------------------------------------------------


async def test_i_replace_failure_restores_offset_zero_accounting_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    qm = FileSystemQueueManager(queues_dir=tmp_path)
    sid = "s-replace-fails"
    events = [_fixed(i) for i in range(9)]
    for ev in events:
        await qm.append(sid, ev)
    batch = await qm.read_batch(sid, max_items=3)
    await qm.commit(sid, batch.end_offset, None)
    c = batch.end_offset

    log_path = tmp_path / f"{sid}.log"
    log_before = log_path.read_bytes()

    real_replace = os.replace

    def _flaky_replace(src: Any, dst: Any) -> None:
        # Fail ONLY the log replace (step 6); let the offset writes through.
        if str(dst) == str(log_path):
            raise OSError("simulated os.replace failure on the log")
        real_replace(src, dst)

    monkeypatch.setattr(
        queue_manager_module.os, "replace", _flaky_replace, raising=True
    )

    with caplog.at_level(logging.ERROR):
        reclaimed = await qm.compact_committed_prefix(sid, 0)  # must not raise

    assert reclaimed == 0
    # Offset restored to C -- a pure no-op, not a re-drive. Read via the
    # committed-offset accessor: the restore now writes the same atomic
    # ``{"v":1,"offset":C,"cursor":...}`` record every other .offset writer
    # uses, so the value (not the raw byte shape) is what must equal C.
    assert qm._read_committed_offset(sid) == c
    # Log completely untouched.
    assert log_path.read_bytes() == log_before
    assert any(
        "compact_replace_failed" in r.getMessage()
        and "offset_restored" in r.getMessage()
        for r in caplog.records
    )

    # Load-bearing proof of zero accounting drift: the NEXT read_batch
    # resumes from the restored offset C, not from 0 -- no duplicate
    # processing, hence no double record_written downstream.
    resumed = await qm.read_batch(sid, max_items=100)
    assert [r.raw for r in resumed.records] == events[3:]


async def test_i_double_replace_failure_logs_restore_failed_honestly(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """If the RESTORE itself also fails, the honest (documented) fallback is
    a logged `compact_restore_failed ... redrive_expected=true` -- never a
    silent success claim."""
    qm = FileSystemQueueManager(queues_dir=tmp_path)
    sid = "s-double-fail"
    for i in range(9):
        await qm.append(sid, _fixed(i))
    batch = await qm.read_batch(sid, max_items=3)
    await qm.commit(sid, batch.end_offset, None)

    log_path = tmp_path / f"{sid}.log"

    def _always_raise(src: Any, dst: Any) -> None:
        # let the first offset rebase-to-0 write through, then fail both the
        # log replace and the subsequent restore-to-C write. Both offset writes
        # are now atomic ``{"v":1,"offset":N,...}`` records, so the rebase-to-0
        # is the one carrying offset 0 -- everything else is the restore.
        src_content = (
            Path(src).read_text(encoding="utf-8") if Path(src).exists() else ""
        )
        if str(dst) == str(log_path):
            raise OSError("simulated persistent log-replace failure")
        if str(dst).endswith(".offset") and '"offset":0' not in src_content:
            raise OSError("simulated persistent offset-restore failure")

    monkeypatch.setattr(queue_manager_module.os, "replace", _always_raise, raising=True)

    with caplog.at_level(logging.ERROR):
        reclaimed = await qm.compact_committed_prefix(sid, 0)  # must not raise

    assert reclaimed == 0
    assert any(
        "compact_restore_failed" in r.getMessage()
        and "redrive_expected=true" in r.getMessage()
        for r in caplog.records
    )


# ---------------------------------------------------------------------------
# (j) a large tail no longer blocks reclaiming the committed prefix
# ---------------------------------------------------------------------------


async def test_j_large_tail_does_not_block_prefix_reclaim(
    tmp_path: Path,
) -> None:
    qm = FileSystemQueueManager(queues_dir=tmp_path)
    sid = "s-huge-tail"
    # 20 events * 10 bytes = 200 bytes total.
    for i in range(20):
        await qm.append(sid, _fixed(i))
    batch = await qm.read_batch(sid, max_items=2)  # C = 20 bytes
    await qm.commit(sid, batch.end_offset, None)
    log_path = tmp_path / f"{sid}.log"
    assert batch.end_offset == 20

    # Tail is 180 bytes -- well above min_prefix_bytes (10) and above what
    # used to be a tail cap. The prefix must still be reclaimed.
    reclaimed = await qm.compact_committed_prefix(sid, min_prefix_bytes=10)

    assert reclaimed == 20
    assert log_path.stat().st_size == 180
    assert qm._read_committed_offset(sid) == 0

    # A concurrent append for the same key must complete promptly.
    await asyncio.wait_for(qm.append(sid, _fixed(999)), timeout=2.0)
    assert log_path.stat().st_size == 190


# ---------------------------------------------------------------------------
# (e) dead-letter retention: log-less keys, whole-file mtime expiry
# ---------------------------------------------------------------------------


async def test_e_dead_letters_older_than_retention_expired_newer_kept(
    tmp_path: Path,
) -> None:
    qm = FileSystemQueueManager(queues_dir=tmp_path)
    now = time.time()
    retention = 30 * 86400.0

    old_path = _seed_dead(tmp_path, "old-log-less", _dead_record("p1"))
    old_mtime = now - retention - 3600  # 1h past the window
    os.utime(old_path, (old_mtime, old_mtime))

    new_path = _seed_dead(tmp_path, "new-log-less", _dead_record("p2"))
    new_mtime = now - 3600  # 1h old, well within the window
    os.utime(new_path, (new_mtime, new_mtime))

    # Old on age, but has a LIVE .log -- must NEVER be touched (boot-safety rule).
    log_backed_dead = _seed_dead(
        tmp_path, "old-but-log-present", _dead_record("p3") + _dead_record("p4")
    )
    os.utime(log_backed_dead, (old_mtime, old_mtime))
    _seed_log(tmp_path, "old-but-log-present", b"")

    result = await qm.expire_dead_letters(now, retention, enabled=True)

    assert not old_path.exists()
    assert new_path.exists()
    assert log_backed_dead.exists()
    assert result["expired_keys"] == 1
    assert result["expired_records"] == 1  # old-log-less had 1 record
    assert result["failed"] == 0


async def test_e_dry_run_deletes_nothing_but_still_classifies(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    qm = FileSystemQueueManager(queues_dir=tmp_path)
    now = time.time()
    retention = 30 * 86400.0
    old_path = _seed_dead(tmp_path, "would-expire", _dead_record("p"))
    old_mtime = now - retention - 3600
    os.utime(old_path, (old_mtime, old_mtime))

    with caplog.at_level(logging.WARNING):
        result = await qm.expire_dead_letters(now, retention, enabled=False)

    assert old_path.exists()  # nothing unlinked
    assert result["expired_keys"] == 0  # dry-run never counts as "expired"
    assert any(
        "dead_letter_expired" in r.getMessage() and "action=dry_run" in r.getMessage()
        for r in caplog.records
    )


async def test_e_retention_zero_disables_expiry(tmp_path: Path) -> None:
    qm = FileSystemQueueManager(queues_dir=tmp_path)
    now = time.time()
    old_path = _seed_dead(tmp_path, "ancient", _dead_record("p"))
    os.utime(old_path, (now - 10_000_000, now - 10_000_000))

    result = await qm.expire_dead_letters(now, retention_seconds=0, enabled=True)

    assert old_path.exists()
    assert result == {
        "expired_keys": 0,
        "expired_records": 0,
        "expired_bytes": 0,
        "failed": 0,
    }


# ---------------------------------------------------------------------------
# (k) dead-letter expiry works under SHIPPED DEFAULTS
# ---------------------------------------------------------------------------


async def test_k_expiry_is_opt_in_disabled_under_shipped_defaults(
    tmp_path: Path,
) -> None:
    """No overrides at all: `dead_letter_expiry_enabled` ships False -- an
    un-recovered dead-letter's last surviving copy must never be silently
    deleted out of the box. Explicit opt-in still deletes (mechanism
    unchanged, only the default flipped)."""
    settings = Settings()
    assert settings.reclaim_enabled is False
    assert settings.dead_letter_expiry_enabled is False

    qm = FileSystemQueueManager(queues_dir=tmp_path)
    now = time.time()
    old_path = _seed_dead(tmp_path, "shipped-default-no-expire", _dead_record("p"))
    old_mtime = now - settings.dead_letter_retention_seconds - 3600
    os.utime(old_path, (old_mtime, old_mtime))

    result = await qm.expire_dead_letters(
        now, settings.dead_letter_retention_seconds, settings.dead_letter_expiry_enabled
    )

    assert old_path.exists(), "shipped default must never auto-delete the last copy"
    assert result["expired_keys"] == 0  # dry-run only, no override

    # Opt-in (enabled=True) still deletes -- the mechanism itself is untouched.
    result = await qm.expire_dead_letters(
        now, settings.dead_letter_retention_seconds, enabled=True
    )
    assert not old_path.exists()
    assert result["expired_keys"] == 1


async def test_k_dead_letter_expiry_enabled_false_still_dry_runs(
    tmp_path: Path,
) -> None:
    qm = FileSystemQueueManager(queues_dir=tmp_path)
    now = time.time()
    old_path = _seed_dead(tmp_path, "would-expire-2", _dead_record("p"))
    os.utime(old_path, (now - (31 * 86400.0), now - (31 * 86400.0)))

    result = await qm.expire_dead_letters(now, 30 * 86400.0, enabled=False)
    assert old_path.exists()
    assert result["expired_keys"] == 0


# ---------------------------------------------------------------------------
# (m) boot-vs-sweep dead-letter accounting: boot expiry runs before seed
# counting and must never call record_purged (nothing was counted yet);
# sweep expiry runs on live counters and must call it, or residual drifts.
# ---------------------------------------------------------------------------


async def test_m_boot_phase_expiry_never_calls_record_purged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        main_module.registry, "_queue_manager", FileSystemQueueManager(queues_dir=tmp_path)
    )
    main_module.registry._accepted_total = 0
    main_module.registry._written_total = 0

    monkeypatch.setattr(main_module._settings, "reclaim_enabled", False)
    monkeypatch.setattr(main_module._settings, "dead_letter_expiry_enabled", True)
    monkeypatch.setattr(
        main_module._settings, "dead_letter_retention_seconds", 30 * 86400.0
    )
    # Prevent a background sweep task from starting at the end of
    # _boot_reconcile -- it would otherwise run concurrently and race this
    # test's own assertions (and outlive the test).
    monkeypatch.setattr(
        main_module._settings, "crash_recovery_sweep_interval_seconds", 0
    )

    old_path = _seed_dead(
        tmp_path, "boot-expire-me", _dead_record("p1") + _dead_record("p2")
    )
    old_mtime = time.time() - main_module._settings.dead_letter_retention_seconds - 3600
    os.utime(old_path, (old_mtime, old_mtime))

    purge_calls: list[int] = []
    monkeypatch.setattr(
        main_module.registry, "record_purged", lambda n: purge_calls.append(n)
    )

    await main_module._boot_reconcile()

    assert not old_path.exists(), "boot phase never expired the dead-letter file"
    assert purge_calls == [], (
        "boot's expire step must NEVER call record_purged -- it runs BEFORE "
        "recovery_seed_counts, so expired lines are simply never counted "
        "into accepted_seed in the first place (main.py's own comment at "
        "the boot expire call site); calling record_purged here would "
        "subtract records that were never added, driving residual negative"
    )
    metrics = await main_module.registry.pipeline_metrics()
    assert metrics["residual"] == 0
    assert metrics["degraded"] is False


async def test_m_sweep_tick_expiry_applies_record_purged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        main_module.registry, "_queue_manager", FileSystemQueueManager(queues_dir=tmp_path)
    )
    main_module.registry._accepted_total = 0
    main_module.registry._written_total = 0

    monkeypatch.setattr(main_module._settings, "dead_letter_expiry_enabled", True)
    monkeypatch.setattr(
        main_module._settings, "dead_letter_retention_seconds", 30 * 86400.0
    )

    old_path = _seed_dead(
        tmp_path,
        "sweep-expire-me",
        _dead_record("p1") + _dead_record("p2") + _dead_record("p3"),
    )
    old_mtime = time.time() - main_module._settings.dead_letter_retention_seconds - 3600
    os.utime(old_path, (old_mtime, old_mtime))

    real_record_purged = main_module.registry.record_purged
    purge_calls: list[int] = []

    def _spy_record_purged(n: int) -> None:
        purge_calls.append(n)
        real_record_purged(n)

    monkeypatch.setattr(main_module.registry, "record_purged", _spy_record_purged)
    # Seed accepted_total > written_total so a real record_purged call is
    # OBSERVABLE in the counters (not masked by record_purged's own
    # accepted-can-never-fall-below-written clamp at a 0/0 baseline).
    main_module.registry.record_accepted(3)

    sweep_task = asyncio.create_task(main_module._crash_recovery_sweep_loop(0, 100))
    try:
        for _ in range(300):
            if purge_calls:
                break
            await asyncio.sleep(0.02)
    finally:
        sweep_task.cancel()
        try:
            await sweep_task
        except asyncio.CancelledError:
            pass

    assert purge_calls == [3], (
        "sweep-tick expiry must call record_purged with the expired-record "
        "count -- unlike boot, the sweep runs on LIVE counters and must "
        "keep `accepted` conserved (main.py's own comment at the sweep "
        "expire call site)"
    )
    assert not old_path.exists()
    assert main_module.registry.pipeline_counters()["accepted_total"] == 0
    metrics = await main_module.registry.pipeline_metrics()
    assert metrics["residual"] == 0
    assert metrics["degraded"] is False


# ---------------------------------------------------------------------------
# (f) neither compaction nor expiry can block /status
# ---------------------------------------------------------------------------


async def test_f_status_not_blocked_by_an_in_progress_compaction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Hold a key's file_lock (as an in-progress compaction would) from a
    background thread; /status must still return promptly -- it never
    acquires guard.file_lock for any key."""
    monkeypatch.setattr(
        main_module.registry, "_queue_manager", FileSystemQueueManager(queues_dir=tmp_path)
    )
    qm = main_module.registry.queue_manager
    sid = "s-status-lock"
    await qm.append(sid, _fixed(0))
    await qm.commit(sid, 10, None)

    with qm._guard(sid) as guard:
        loop = asyncio.get_event_loop()
        held = asyncio.Event()
        release = asyncio.Event()

        def _hold_lock() -> None:
            guard.file_lock.acquire()
            loop.call_soon_threadsafe(held.set)
            while not release.is_set():
                time.sleep(0.01)
            guard.file_lock.release()

        thread_task = loop.run_in_executor(None, _hold_lock)
        await asyncio.wait_for(held.wait(), timeout=5.0)

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=main_module.app), base_url="http://test"
        ) as c:
            response = await asyncio.wait_for(c.get("/status"), timeout=2.0)

        assert response.status_code == 200

        release.set()
        await thread_task


async def test_f_status_not_blocked_by_a_concurrent_dead_letter_expiry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        main_module.registry, "_queue_manager", FileSystemQueueManager(queues_dir=tmp_path)
    )
    qm = main_module.registry.queue_manager
    for i in range(50):
        _seed_dead(tmp_path, f"dead-{i}", _dead_record("p"))

    expire_task = asyncio.create_task(
        qm.expire_dead_letters(time.time(), 30 * 86400.0, enabled=True)
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=main_module.app), base_url="http://test"
    ) as c:
        response = await asyncio.wait_for(c.get("/status"), timeout=2.0)
    assert response.status_code == 200
    await expire_task


# ---------------------------------------------------------------------------
# (g) regression: reclaim-on-finalize unchanged, composes with compaction
# ---------------------------------------------------------------------------


@pytest.fixture
async def reg_qm(tmp_path: Path):
    reg = SessionRegistry()
    reg._queue_manager = FileSystemQueueManager(queues_dir=tmp_path)
    reg._write_semaphore = asyncio.Semaphore(8)
    reg._max_delivery_attempts = 3
    yield reg, reg._queue_manager
    for w in list(reg._workers.values()):
        if w.task and not w.task.done():
            w.task.cancel()
    tasks = [w.task for w in reg._workers.values() if w.task and not w.task.done()]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def test_g_reclaim_on_finalize_still_removes_log_and_offset(
    reg_qm: tuple[SessionRegistry, QueueManager],
) -> None:
    reg, qm = reg_qm
    sid = "s-finalize-regression"
    worker = SessionWorker(
        session_id=sid, workspace="/ws", services=HookStateService(workspace="/ws")
    )
    worker.services.graph.flush = AsyncMock()  # type: ignore[method-assign]
    worker.services.graph.close = AsyncMock()  # type: ignore[method-assign]
    reg._register_for_test(worker)

    for i in range(5):
        await qm.append(
            sid,
            _line("tool:pre", "/ws", {"session_id": sid, "i": i, "timestamp": _G_TS}),
        )
    await qm.append(
        sid, _line("session:end", "/ws", {"session_id": sid, "timestamp": _G_TS})
    )

    reg.start_drain(worker)
    assert worker.task is not None
    await asyncio.wait_for(asyncio.shield(worker.task), timeout=10.0)

    assert not (qm.queues_dir / f"{sid}.log").exists()
    assert not (qm.queues_dir / f"{sid}.offset").exists()


async def test_g_compaction_then_finalize_compose_correctly(
    reg_qm: tuple[SessionRegistry, QueueManager],
) -> None:
    """A compaction that already ran on an open session must not interfere
    with a LATER finalize -- both paths must compose."""
    reg, qm = reg_qm
    sid = "s-compact-then-finalize"
    worker = SessionWorker(
        session_id=sid, workspace="/ws", services=HookStateService(workspace="/ws")
    )
    worker.services.graph.flush = AsyncMock()  # type: ignore[method-assign]
    worker.services.graph.close = AsyncMock()  # type: ignore[method-assign]
    reg._register_for_test(worker)

    for i in range(5):
        await qm.append(
            sid,
            _line("tool:pre", "/ws", {"session_id": sid, "i": i, "timestamp": _G_TS}),
        )

    reg.start_drain(worker)
    assert worker.task is not None

    async def _drained() -> bool:
        b = await qm.read_batch(sid, max_items=1)
        return not b.records

    for _ in range(200):
        if await _drained():
            break
        await asyncio.sleep(0.02)
    assert await _drained()

    # The AUTOMATIC path (Trigger I, wired via reg_qm's real drain loop +
    # the safe_settings proxy) has already been compacting on every idle
    # poll tick since backlog hit 0 -- assert the composition it produced:
    # the log has already collapsed to its (empty) undrained tail, entirely
    # without a manual call.
    log_path = qm.queues_dir / f"{sid}.log"

    async def _log_collapsed() -> bool:
        return not log_path.exists() or log_path.stat().st_size == 0

    for _ in range(100):
        if await _log_collapsed():
            break
        await asyncio.sleep(0.02)
    assert await _log_collapsed()

    # A manual call on top finds nothing left -- idempotent (I7).
    reclaimed = await qm.compact_committed_prefix(sid, 0)
    assert reclaimed == 0
    assert not log_path.exists() or log_path.stat().st_size == 0

    await qm.append(
        sid, _line("session:end", "/ws", {"session_id": sid, "timestamp": _G_TS})
    )
    await asyncio.wait_for(asyncio.shield(worker.task), timeout=10.0)

    assert not (qm.queues_dir / f"{sid}.log").exists()
    assert not (qm.queues_dir / f"{sid}.offset").exists()
