"""Online retention against real threads, scratch files/SQLite and external flock."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import cast

import httpx
import pytest
from fastapi import FastAPI

from context_intelligence_server import blob_retention as r
from context_intelligence_server.auth import BearerTokenMiddleware
from context_intelligence_server.blob_coordination import (
    Coordinator, coordinator_for, filesystem, wait_filesystem,
)
from context_intelligence_server.blob_store import AsyncDiskBlobStore
from context_intelligence_server.retention_client import ClientError, RetentionClient
from context_intelligence_server.retention_scheduler import run_scheduled
from context_intelligence_server.retention_service import (
    JobConflict, JobNotFound, RetentionService, SNAPSHOT, TERMINAL,
)
from context_intelligence_server.routers.admin import router


def payload(root: Path, key: str = "old", session: str = "session") -> Path:
    file = root / session / "blobs" / f"{key}.json"
    file.parent.mkdir(parents=True, exist_ok=True)
    file.write_text('{"value":1}')
    old = time.time_ns() - 90 * 86400 * 10**9
    os.utime(file, ns=(old, old))
    return file


async def until(predicate, timeout=5):
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.002)


async def ready(service):
    job = await service.preview("30d")
    await service.task
    assert service.status()["state"] == "ready", service.status()
    return job["id"]


async def finish(service) -> None:
    """Await the service's current job task."""
    task = service.task
    assert task is not None
    await task


def external_lock(root):
    code = """
import sys
from context_intelligence_server.blob_retention import RootLock, RetentionError
try:
    with RootLock(sys.argv[1], exclusive=True):
        pass
except RetentionError:
    sys.exit(7)
"""
    return subprocess.run([sys.executable, "-c", code, str(root)], timeout=5).returncode


def test_fifo_cohorts_under_shared_and_exclusive_load():
    coord = Coordinator()
    entered, release = threading.Event(), threading.Event()
    order = []
    threads = []

    def hold():
        with coord.lease():
            entered.set()
            assert release.wait(5)

    initial = threading.Thread(target=hold)
    initial.start()
    assert entered.wait(5)
    # Force queue order while actual admissions use the real coordinator.
    for i, exclusive in enumerate([True, False, False, True, False, True]):
        def run(index=i, write=exclusive):
            with coord.lease(exclusive=write):
                order.append(index)
                time.sleep(0.002)
        thread = threading.Thread(target=run)
        thread.start()
        threads.append(thread)
        deadline = time.monotonic() + 5
        with coord.condition:
            while len(coord.queue) != i + 1:
                assert time.monotonic() < deadline
                coord.condition.wait(0.002)
    release.set()
    initial.join(5)
    for thread in threads:
        thread.join(5)
        assert not thread.is_alive()
    assert order[0] == 0 and set(order[1:3]) == {1, 2} and order[3:] == [3, 4, 5]
    assert not coord.queue and not coord.readers and not coord.writer


@pytest.mark.parametrize("operation", ["write", "read", "list", "size", "dump", "delete_session"])
async def test_all_operations_automatically_coordinate_aliases(tmp_path, operation):
    root = tmp_path / "root"
    root.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(root, target_is_directory=True)
    payload(root)
    store = AsyncDiskBlobStore(alias)
    coord = await asyncio.to_thread(coordinator_for, root)
    entered, release = threading.Event(), threading.Event()

    def exclusive():
        with coord.lease(exclusive=True):
            entered.set()
            assert release.wait(5)

    holder = asyncio.create_task(asyncio.to_thread(exclusive))
    await until(entered.is_set)
    calls = {
        "write": lambda: store.write("session", "new", {}),
        "read": lambda: store.read("ci-blob://session/old"),
        "list": lambda: store.list("session"),
        "size": lambda: store.size("ci-blob://session/old"),
        "dump": lambda: store.dump("ci-blob://session/old", tmp_path / "dump"),
        "delete_session": lambda: store.delete_session("session"),
    }
    task = asyncio.create_task(calls[operation]())
    try:
        await until(lambda: len(coord.queue) == 1)
        assert not task.done()
        assert coord.queue[0][1] == (operation == "delete_session")
    finally:
        release.set()
        await holder
    await task


async def test_cancel_waiting_and_running_io_and_shutdown_external_exclusion(tmp_path):
    service = await RetentionService.open(tmp_path)
    coord = service._coordinator
    held, release = threading.Event(), threading.Event()
    touched = []

    def blocking():
        held.set()
        assert release.wait(5)
        touched.append("finished")

    task = asyncio.create_task(filesystem(tmp_path, blocking))
    await until(held.is_set)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    waiting = asyncio.create_task(filesystem(tmp_path, lambda: touched.append("must-not-run"), exclusive=True))
    await until(lambda: len(coord.queue) == 1)
    waiting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting
    closing = asyncio.create_task(service.close())
    try:
        await asyncio.sleep(0.06)
        assert not closing.done()
        assert await asyncio.to_thread(external_lock, tmp_path) == 7
    finally:
        release.set()
        await closing
    assert touched == ["finished"]
    assert await asyncio.to_thread(external_lock, tmp_path) == 0
    assert not coord.queue and coord.readers == 0


def test_scan_windows_close_descriptors_and_static_multi_directory_coverage(tmp_path, monkeypatch):
    for i in range(1100):
        payload(tmp_path, f"{i:04}", f"s{i // 550}")
    cutoff, started = time.time_ns() - 30 * 86400 * 10**9, time.time_ns()
    cursor, report = r.ScanCursor(), r.Report(str(tmp_path), cutoff)
    def root_descriptors():
        descriptors = []
        for number in os.listdir("/proc/self/fd"):
            try:
                target = os.readlink("/proc/self/fd/" + number)
            except FileNotFoundError:
                continue
            if target.startswith(str(tmp_path)):
                descriptors.append(target)
        return descriptors

    assert not root_descriptors()
    all_items = []
    stat_count = 0
    original = os.stat

    def counted(*args, **kwargs):
        nonlocal stat_count
        stat_count += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(os, "stat", counted)
    while not cursor.done:
        stat_count = 0
        items = r.scan_window(tmp_path, cursor, report, started, max_entries=19, budget_ns=10**9)
        assert len(items) <= 19 and stat_count < 40
        assert not root_descriptors()
        all_items.extend(items)
    assert len(all_items) == len({item.uri for item in all_items}) == 1100
    assert report.files == 1100 and len(report.samples) == 5
    assert not (tmp_path / r.METADATA).exists()


def test_scan_cookie_survives_consumed_entry_deletion_and_directory_replacement(tmp_path):
    for i in range(60):
        payload(tmp_path, f"{i:04}")
    report = r.Report(str(tmp_path), time.time_ns())
    started, cursor = time.time_ns(), r.ScanCursor()
    first = r.scan_window(tmp_path, cursor, report, started, max_entries=10, budget_ns=10**9)
    assert first
    for item in first:
        session, key = r.parse_uri(item.uri)
        (tmp_path / session / "blobs" / f"{key}.json").unlink()
    all_items = list(first)
    while not cursor.done:
        all_items.extend(r.scan_window(tmp_path, cursor, report, started, max_entries=10, budget_ns=10**9))
    assert len({item.uri for item in all_items}) == 60
    cursor = r.ScanCursor()
    r.scan_window(tmp_path, cursor, report, started, max_entries=10, budget_ns=10**9)
    blobs = tmp_path / "session" / "blobs"
    blobs.rename(blobs.with_name("old-blobs"))
    blobs.mkdir()
    payload(tmp_path, "replacement")
    previous = report.changed
    assert not r.scan_window(tmp_path, cursor, report, started, max_entries=10, budget_ns=10**9)
    assert report.changed == previous + 1


async def test_preview_side_effect_free_duplicate_expiry_and_cancel(tmp_path):
    old = payload(tmp_path)
    service = await RetentionService.open(tmp_path)
    try:
        before = {str(p): p.stat().st_mtime_ns for p in tmp_path.rglob("*")}
        job = await ready(service)
        after = {str(p): p.stat().st_mtime_ns for p in tmp_path.rglob("*")}
        assert before == after
        with pytest.raises(JobConflict):
            await service.preview("30d")
        with pytest.raises(JobConflict):
            await RetentionService.open(tmp_path / ".")
        with pytest.raises(JobNotFound):
            await service.start("wrong", "prune")
        with pytest.raises(JobConflict):
            await service.start(job, "yes")
        service._ready_deadline = time.monotonic() - 1
        with pytest.raises(JobConflict, match="expired"):
            await service.start(job, "prune")
        assert (await service.cancel(job))["state"] == "cancelled"
        assert (await service.cancel(job))["state"] == "cancelled"
        with pytest.raises(JobConflict):
            await service.start(job, "prune")
        assert old.exists() and not (tmp_path / r.METADATA).exists()
        new = await ready(service)
        assert new != job
    finally:
        await service.close()


async def test_online_apply_retains_new_changed_files_and_replay_across_instances(tmp_path, monkeypatch):
    for i in range(96):
        payload(tmp_path, f"{i:04}")
    # Alias lives outside root so scanner doesn't mistake it for a session.
    alias = tmp_path.parent / (tmp_path.name + "-alias")
    alias.symlink_to(tmp_path, target_is_directory=True)
    first_store, other = AsyncDiskBlobStore(tmp_path), AsyncDiskBlobStore(alias)
    service = await RetentionService.open(tmp_path)
    sizes, admission_times = [], []
    real_batch = r.apply_batch

    def tracking(path, items, report, **kwargs):
        sizes.append(len(items))
        admission_times.append(time.monotonic())
        real_batch(path, items, report, **kwargs)

    monkeypatch.setattr(r, "apply_batch", tracking)
    try:
        job = await ready(service)
        await other.write("session", "0000", {"changed": True})
        newer = payload(tmp_path, "post-preview-old-mtime")
        await service.start(job, "prune")
        writes = 0
        while not service.task.done():
            uri = await other.write("live", f"new-{writes}", {"n": writes})
            assert await first_store.read(uri) == {"n": writes}
            assert await first_store.size(uri) > 0
            await other.list("live")
            writes += 1
            await asyncio.sleep(0.002)
        await service.task
        result = service.status()
        assert result["state"] == "completed", result
        assert result["report"]["deleted"] == 95
        assert result["report"]["errors"] == 0 and writes > 5
        assert await first_store.read("ci-blob://session/0000") == {"changed": True}
        assert newer.exists()
        assert sizes and max(sizes) <= 32
        assert all(b - a >= 0.045 for a, b in zip(admission_times, admission_times[1:]))
        expired = "ci-blob://session/0001"
        with pytest.raises(r.BlobExpiredError):
            await other.read(expired)
        assert await other.write("session", "0001", {"replayed": True}) == expired
        assert not (tmp_path / "session/blobs/0001.json").exists()
        with r.Root(tmp_path) as root, r.Ledger(root) as ledger:
            assert tuple(row[1] for row in ledger.db.execute("PRAGMA table_info(expiry)")) == r._COLUMNS
            assert ledger.db.execute("SELECT count(*) FROM expiry WHERE pending=1").fetchone()[0] == 0
    finally:
        await service.close()


async def test_dedicated_worker_progress_when_ordinary_pool_waits_for_exclusive(tmp_path):
    payload(tmp_path)
    loop = asyncio.get_running_loop()
    pool = ThreadPoolExecutor(max_workers=1)
    loop.set_default_executor(pool)
    service = await RetentionService.open(tmp_path)
    coord = service._coordinator
    entered, release = threading.Event(), threading.Event()

    def hold():
        with coord.lease(exclusive=True):
            entered.set()
            assert release.wait(5)

    thread = threading.Thread(target=hold)
    thread.start()
    await until(entered.is_set)
    write = asyncio.create_task(AsyncDiskBlobStore(tmp_path).write("live", "new", {}))
    await until(lambda: len(coord.queue) == 1)
    maintenance = asyncio.create_task(service._work(lambda: "dedicated", coordinated=False))
    assert await asyncio.wait_for(maintenance, 1) == "dedicated"
    ticks = 0
    for _ in range(10):
        await asyncio.sleep(0.002)
        ticks += 1
    assert ticks == 10 and not write.done()
    release.set()
    thread.join(5)
    await write
    await service.close()
    # Test's one-thread pool is joined by the event-loop runner at teardown.


async def test_cancel_finishes_admitted_batch_and_durable_checkpoint(tmp_path, monkeypatch):
    for i in range(80):
        payload(tmp_path, str(i))
    service = await RetentionService.open(tmp_path)
    entered, release = threading.Event(), threading.Event()
    journal = r.Ledger.journal
    calls = []

    def blocked(self, items):
        journal(self, items)
        calls.append(len(items))
        entered.set()
        assert release.wait(5)

    monkeypatch.setattr(r.Ledger, "journal", blocked)
    try:
        job = await ready(service)
        await service.start(job, "prune")
        await until(entered.is_set)
        assert (await service.cancel(job))["state"] == "cancelling"
        closing = asyncio.create_task(service.close())
        assert await asyncio.to_thread(external_lock, tmp_path) == 7
        assert not closing.done()
        release.set()
        await closing
        status = service.status()
        assert status["state"] == "cancelled"
        assert all(not thread.is_alive() for thread in service.executor._threads)
        assert status["report"]["deleted"] == calls[0] <= 32
        assert len(calls) == 1
        saved = json.loads((tmp_path / r.METADATA / SNAPSHOT).read_text())
        assert saved["state"] == "cancelled" and saved["confirmed"]
    finally:
        release.set()
        await service.close()
    reopened = await RetentionService.open(tmp_path)
    assert reopened.status()["state"] == "cancelled"
    await reopened.close()


def test_large_nonpending_recovery_pages_use_primary_key_without_filter(tmp_path):
    with r.RootLock(tmp_path) as lock, r.Ledger(lock.root, writable=True) as ledger:
        ledger.db.executemany("INSERT INTO expiry VALUES (?,1,1,1,1,1,1,0)",
                             ((f"ci-blob://s/{i:08}",) for i in range(12000)))
        ledger.commit()
    after, total, pages = "", 0, 0
    while True:
        after, items, count = r.recovery_page(tmp_path, after, time.time_ns(), time.time_ns())
        assert not items and count <= 512
        total += count
        pages += 1
        if count < 512:
            break
    assert total == 12000 and pages == 24
    with r.Root(tmp_path) as root, r.Ledger(root) as ledger:
        plan = str(ledger.db.execute(
            "EXPLAIN QUERY PLAN SELECT * FROM expiry WHERE uri>? ORDER BY uri LIMIT 512", ("",)
        ).fetchall())
        assert "PRIMARY KEY" in plan


async def test_backward_clock_refuses_apply_without_deletion(tmp_path):
    old = payload(tmp_path)
    service = await RetentionService.open(tmp_path)
    try:
        job = await ready(service)
        service._last_wall = time.time_ns() + 10**12
        with pytest.raises(r.RetentionError, match="backwards"):
            await service.start(job, "prune")
        assert old.exists() and not (tmp_path / r.METADATA).exists()
    finally:
        await service.close()


async def test_restarted_confirmed_job_is_interrupted_never_resumed(tmp_path):
    old = payload(tmp_path)
    service = await RetentionService.open(tmp_path)
    job = await ready(service)
    # Simulate crash at the durable pre-deletion checkpoint, no auto apply.
    service.job.update(confirmed=True, state="running")
    await service._work(service._save, exclusive=True)
    await service.close()
    reopened = await RetentionService.open(tmp_path)
    try:
        assert reopened.status(job)["state"] == "interrupted"
        with pytest.raises(JobConflict):
            await reopened.start(job, "prune")
        assert old.exists()
        assert json.loads((tmp_path / r.METADATA / SNAPSHOT).read_text())["state"] == "interrupted"
    finally:
        await reopened.close()


async def test_admin_auth_validation_and_status_during_slow_scan(tmp_path, monkeypatch):
    payload(tmp_path)
    service = await RetentionService.open(tmp_path)
    app = FastAPI()
    app.include_router(router)
    app.state.auth_mode = "static"
    app.state.admin_api_key_configured = True
    app.state.retention_service = service
    wrapped = BearerTokenMiddleware(
        app, keystore={hashlib.sha256(b"fixture-data").hexdigest(): "test"},
        admin_api_key_digest=hashlib.sha256(b"fixture-admin").hexdigest(),
    )
    entered, release = threading.Event(), threading.Event()
    original = r.scan_window

    def slow(*args, **kwargs):
        entered.set()
        assert release.wait(5)
        return original(*args, **kwargs)

    monkeypatch.setattr(r, "scan_window", slow)
    transport = httpx.ASGITransport(app=wrapped)
    try:
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            base = "/admin/blobs/prune"
            for token, expected in [(None, 401), ("wrong", 401), ("fixture-data", 403)]:
                headers = {"Authorization": f"Bearer {token}"} if token else {}
                assert (await client.post(base, json={"older_than": "30d"}, headers=headers)).status_code == expected
            client.headers["Authorization"] = "Bearer fixture-admin"
            for body in [{"older_than": "30d", "root": "/elsewhere"},
                         {"older_than": "30d", "cutoff_ns": 1}, {"older_than": "0d"}]:
                assert (await client.post(base, json=body)).status_code == 422
            replies = await asyncio.gather(*(client.post(base, json={"older_than": "30d"}) for _ in range(2)))
            assert sorted(reply.status_code for reply in replies) == [202, 409]
            job = next(reply.json()["id"] for reply in replies if reply.status_code == 202)
            for suffix, body in [("/start", {"confirmation": "prune"}), ("/cancel", {})]:
                for token, expected in [(None, 401), ("wrong", 401), ("fixture-data", 403)]:
                    headers = {"Authorization": f"Bearer {token}"} if token else {"Authorization": ""}
                    assert (await client.post(base + "/" + job + suffix, json=body, headers=headers)).status_code == expected
            await until(entered.is_set)
            status = await asyncio.wait_for(client.get(base + "/" + job), 0.5)
            assert status.json()["state"] == "previewing"
            assert (await client.post(base + "/" + job + "/start", json={"confirmation": "prune"})).status_code == 409
            release.set()
            await service.task
            assert (await client.get(base)).json()["state"] == "ready"
            assert (await client.post(base + "/" + job + "/start",
                                      json={"confirmation": "prune", "root": "/tmp"})).status_code == 422
            assert (await client.post(base + "/" + job + "/cancel")).status_code == 202
            app.state.admin_api_key_configured = False
            assert (await client.get(base)).status_code == 503
    finally:
        release.set()
        await service.close()


def _final_save_in_flight(service) -> bool:
    """True while the job's LAST checkpoint runs (old code: terminal state; new: finalizing)."""
    job = service.job
    return job["state"] in TERMINAL or job.get("phase") in {"finalizing", "finished"}


def _hold_final_save(service, monkeypatch, *, fail: bool = False):
    """Wrap the zero-arg ``_save``; pause/fail only the final checkpoint.

    Public state is captured at the instant the final checkpoint is in flight.
    """
    entered, release = threading.Event(), threading.Event()
    seen: dict = {}
    save = service._save

    def wrapped():
        if _final_save_in_flight(service):
            seen["state"], seen["phase"] = service.job["state"], service.job.get("phase")
            entered.set()
            assert release.wait(5)
            if fail:
                raise OSError("injected final checkpoint failure")
        save()
        if _final_save_in_flight(service):
            seen["durable"] = json.loads((service.path / r.METADATA / SNAPSHOT).read_text())["state"]
            seen["public_after_save"] = service.job["state"]

    monkeypatch.setattr(service, "_save", wrapped)
    return entered, release, seen


async def test_final_checkpoint_blocks_replacement_preview(tmp_path, monkeypatch):
    payload(tmp_path)
    service = await RetentionService.open(tmp_path)
    entered, release, _seen = _hold_final_save(service, monkeypatch)
    try:
        job = await ready(service)
        await service.start(job, "prune")
        await until(entered.is_set)
        status = service.status()
        assert status["state"] == "running" and status["phase"] == "finalizing", status
        assert not service.task.done()
        with pytest.raises(JobConflict):
            await service.preview("30d")
        assert service.status()["id"] == job
        release.set()
        await service.task
        assert service.status()["state"] == "completed"
        new = await ready(service)
        assert new != job and service.status()["state"] == "ready"
    finally:
        release.set()
        await service.close()


async def test_terminal_state_is_durable_before_it_is_public(tmp_path, monkeypatch):
    payload(tmp_path)
    service = await RetentionService.open(tmp_path)
    entered, release, seen = _hold_final_save(service, monkeypatch)
    try:
        job = await ready(service)
        await service.start(job, "prune")
        await until(entered.is_set)
        release.set()
        await finish(service)
        assert seen["durable"] == "completed", seen
        assert seen["public_after_save"] == "running", seen
        assert service.status()["state"] == "completed"
    finally:
        release.set()
        await service.close()


async def test_final_checkpoint_failure_is_failed_and_never_observed_completed(
    tmp_path, monkeypatch
):
    payload(tmp_path)
    service = await RetentionService.open(tmp_path)
    entered, release, seen = _hold_final_save(service, monkeypatch, fail=True)
    observed: list[str] = []

    async def poll():
        while True:
            if service.job is not None:
                observed.append(service.status()["state"])
            await asyncio.sleep(0.001)

    poller = None
    try:
        job = await ready(service)
        poller = asyncio.create_task(poll())
        await service.start(job, "prune")
        await until(entered.is_set)
        assert seen["state"] == "running", seen
        release.set()
        await finish(service)
        status = service.status()
        assert status["state"] == "failed" and status["error"].startswith(
            "Checkpoint failed"
        ), status
        assert "completed" not in observed, observed
        assert (
            json.loads((tmp_path / r.METADATA / SNAPSHOT).read_text())["state"]
            != "completed"
        )
    finally:
        release.set()
        if poller is not None:
            poller.cancel()
        await service.close()


async def test_directory_fsync_failure_after_final_replace_is_ambiguous_not_certified(
    tmp_path, monkeypatch
):
    """Characterization of observed behavior (green on arrival; no production change).

    Contract: the LIVE state becomes completed only after _save fully succeeds, so a
    failure of the directory fsync that follows the final os.replace reports failed.
    The replaced terminal checkpoint may nevertheless be visible to a later read or
    reopen. That outcome is valid: deletion had already finished. It is an uncertain
    fsync outcome, NOT a durability certification, and both readings are allowed.
    """
    old = payload(tmp_path)
    service = await RetentionService.open(tmp_path)
    real_replace, real_fsync = os.replace, os.fsync
    hit = {"replaced": False, "failed": False}

    def replace(src, dst, **kwargs):
        real_replace(src, dst, **kwargs)
        if dst == SNAPSHOT and _final_save_in_flight(service):
            hit["replaced"] = True

    def fsync(fd):
        # The first fsync after the final snapshot replace is the directory fsync.
        if hit["replaced"] and not hit["failed"]:
            hit["failed"] = True
            raise OSError("injected directory fsync failure")
        real_fsync(fd)

    monkeypatch.setattr(os, "replace", replace)
    monkeypatch.setattr(os, "fsync", fsync)
    observed: list[str] = []

    async def poll():
        while True:
            if service.job is not None:
                observed.append(service.status()["state"])
            await asyncio.sleep(0.001)

    poller = None
    reopened = None
    try:
        job = await ready(service)
        poller = asyncio.create_task(poll())
        await service.start(job, "prune")
        await finish(service)
        # the real post-replace path ran
        assert hit == {"replaced": True, "failed": True}, hit
        status = service.status()
        assert status["state"] == "failed" and status["error"].startswith(
            "Checkpoint failed"
        ), status
        assert "completed" not in observed, observed
        assert not old.exists(), "deletion finished before the ambiguous checkpoint"
        on_disk = json.loads((tmp_path / r.METADATA / SNAPSHOT).read_text())["state"]
        # replaced terminal checkpoint is readable
        assert on_disk == "completed", on_disk
        await service.close()
        reopened = await RetentionService.open(tmp_path)
        assert reopened.status()["state"] == "completed", reopened.status()
    finally:
        if poller is not None:
            poller.cancel()
        monkeypatch.undo()
        await service.close()
        if reopened is not None:
            await reopened.close()


async def test_cancel_during_finalizing_does_not_change_outcome(tmp_path, monkeypatch):
    payload(tmp_path)
    service = await RetentionService.open(tmp_path)
    entered, release, _seen = _hold_final_save(service, monkeypatch)
    try:
        job = await ready(service)
        await service.start(job, "prune")
        await until(entered.is_set)
        status = await service.cancel(job)
        assert status["state"] == "running" and status["phase"] == "finalizing", status
        assert not service.cancel_requested.is_set()
        release.set()
        await finish(service)
        assert service.status()["state"] == "completed"
    finally:
        release.set()
        await service.close()


async def test_service_opens_fresh_tilde_root(tmp_path, monkeypatch):
    home, cwd = tmp_path / "home", tmp_path / "cwd"
    home.mkdir()
    cwd.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.chdir(cwd)
    service = await RetentionService.open("~/fresh-blobs")
    try:
        assert service.path == (home / "fresh-blobs").resolve()
        assert (home / "fresh-blobs" / r.LOCK).is_file()
        assert list(cwd.iterdir()) == [], "a literal './~' directory must never be created"
    finally:
        await service.close()


class _SchedulerClient:
    """Sync RetentionClient stand-in that drives a real service on the test loop."""

    timeout = 5.0

    def __init__(self, service, loop):
        self.service, self.loop = service, loop

    def _call(self, coro):
        try:
            return asyncio.run_coroutine_threadsafe(coro, self.loop).result(10)
        except JobNotFound as exc:
            raise ClientError(str(exc), status_code=404) from None
        except JobConflict as exc:
            raise ClientError(str(exc), status_code=409) from None

    def status(self, job_id=None):
        try:
            return self.service.status(job_id)
        except JobNotFound as exc:
            raise ClientError(str(exc), status_code=404) from None

    def preview(self, older_than):
        return self._call(self.service.preview(older_than))

    def start(self, job_id, confirmation):
        return self._call(self.service.start(job_id, confirmation))

    def cancel(self, job_id):
        return self._call(self.service.cancel(job_id))


async def test_scheduler_waits_for_final_checkpoint(tmp_path, monkeypatch):
    payload(tmp_path)
    service = await RetentionService.open(tmp_path)
    entered, release, _ = _hold_final_save(service, monkeypatch)
    try:
        client = _SchedulerClient(service, asyncio.get_running_loop())
        run = asyncio.create_task(
            asyncio.to_thread(
                run_scheduled,
                cast(RetentionClient, client),
                older_than="30d",
                poll_interval=0.02,
                max_runtime=30,
            )
        )
        await until(entered.is_set, timeout=10)
        # several polls while the final checkpoint is in flight
        await asyncio.sleep(0.3)
        assert not run.done(), (
            "scheduler must not report success before the final checkpoint"
        )
        release.set()
        result = await run
        assert result.outcome == "completed" and result.exit_code == 0, result
        assert (
            json.loads((tmp_path / r.METADATA / SNAPSHOT).read_text())["state"]
            == "completed"
        )
    finally:
        release.set()
        await service.close()


async def test_scheduler_nonzero_when_final_checkpoint_fails(tmp_path, monkeypatch):
    payload(tmp_path)
    service = await RetentionService.open(tmp_path)
    entered, release, _ = _hold_final_save(service, monkeypatch, fail=True)
    try:
        client = _SchedulerClient(service, asyncio.get_running_loop())
        run = asyncio.create_task(
            asyncio.to_thread(
                run_scheduled,
                cast(RetentionClient, client),
                older_than="30d",
                poll_interval=0.02,
                max_runtime=30,
            )
        )
        await until(entered.is_set, timeout=10)
        await asyncio.sleep(0.3)
        assert not run.done()
        release.set()
        result = await run
        assert result.exit_code != 0 and result.outcome != "completed", result
        assert service.status()["state"] == "failed"
    finally:
        release.set()
        await service.close()


async def test_exact_fingerprint_revalidated_after_shared_scan(tmp_path):
    old = payload(tmp_path)
    service = await RetentionService.open(tmp_path)
    try:
        cutoff = time.time_ns() - 30 * 86400 * 10**9
        report = r.Report(str(tmp_path), cutoff)
        cursor, started = r.ScanCursor(), time.time_ns()
        items = await service._work(lambda: r.scan_window(
            tmp_path, cursor, report, started, budget_ns=10**9,
        ))
        assert len(items) == 1
        await AsyncDiskBlobStore(tmp_path / ".").write("session", "old", {"changed": True})
        await service._work(lambda: r.apply_batch(tmp_path, items, report), exclusive=True)
        assert report.deleted == 0 and report.changed == 1
        assert json.loads(old.read_text()) == {"changed": True}
    finally:
        await service.close()


@pytest.mark.parametrize("boundary", ["checkpoint", "journal", "unlink"])
def test_real_process_crash_leaves_durable_interrupted_job(tmp_path, boundary):
    for i in range(35):
        payload(tmp_path, str(i))
    code = f"""
import asyncio, os
from context_intelligence_server import blob_retention as r
from context_intelligence_server.retention_service import RetentionService
async def main():
    service = await RetentionService.open({str(tmp_path)!r})
    job = await service.preview("30d")
    await service.task
    if {boundary!r} == "checkpoint":
        save = service._save
        def crash():
            save()
            os._exit(73)
        service._save = crash
    elif {boundary!r} == "journal":
        journal = r.Ledger.journal
        def crash(self, items):
            journal(self, items)
            os._exit(73)
        r.Ledger.journal = crash
    else:
        unlink = r.os.unlink
        def crash(*args, **kwargs):
            unlink(*args, **kwargs)
            os._exit(73)
        r.os.unlink = crash
    await service.start(job["id"], "prune")
    await service.task
asyncio.run(main())
"""
    child = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=8)
    assert child.returncode == 73, child.stderr
    snapshot = tmp_path / r.METADATA / SNAPSHOT
    saved = json.loads(snapshot.read_text())
    assert saved["confirmed"] and saved["state"] == "running"
    remaining = list((tmp_path / "session/blobs").glob("*.json"))
    assert len(remaining) == (34 if boundary == "unlink" else 35)

    async def reopen():
        service = await RetentionService.open(tmp_path)
        try:
            assert service.status()["state"] == "interrupted"
            await asyncio.sleep(0.06)
            assert len(list((tmp_path / "session/blobs").glob("*.json"))) == len(remaining)
            job = await ready(service)
            await service.start(job, "prune")
            await service.task
            assert service.status()["state"] == "completed", service.status()
            assert not list((tmp_path / "session/blobs").glob("*.json"))
        finally:
            await service.close()
    asyncio.run(reopen())


async def test_hot_rollback_recovery_only_after_confirmed_online_apply(tmp_path):
    # Force a genuine hot rollback with dirty pages spilled from a tiny cache.
    with r.RootLock(tmp_path) as lock, r.Ledger(lock.root, writable=True):
        pass
    old = payload(tmp_path)
    code = f"""
import os
from context_intelligence_server import blob_retention as r
with r.RootLock({str(tmp_path)!r}, exclusive=True) as lock, r.Ledger(lock.root, writable=True) as ledger:
    ledger.db.execute("PRAGMA cache_size=1")
    ledger.db.executemany("INSERT INTO expiry VALUES (?,1,1,1,1,1,1,0)",
        ((f"ci-blob://s/{{i:08}}" + "x"*180,) for i in range(1000)))
    os._exit(73)
"""
    child = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=5)
    assert child.returncode == 73, child.stderr
    directory = tmp_path / r.METADATA
    before = {p.name: p.read_bytes() for p in directory.iterdir()}
    service = await RetentionService.open(tmp_path)
    try:
        job = await ready(service)
        assert service.status()["report"]["recovery_required"]
        assert before == {p.name: p.read_bytes() for p in directory.iterdir()}
        await service.cancel(job)
        assert old.exists() and before == {p.name: p.read_bytes() for p in directory.iterdir()}
        job = await ready(service)
        await service.start(job, "prune")
        await service.task
        assert service.status()["state"] == "completed", service.status()
        assert not old.exists()
    finally:
        await service.close()


async def test_real_http_events_graph_and_new_blobs_remain_available(tmp_path, monkeypatch):
    """Real ingress/drainer and blob paths; only the graph boundary is fake."""
    from unittest.mock import AsyncMock
    import context_intelligence_server.main as main
    from context_intelligence_server.neo4j_store import Neo4jGraphStore
    from tests.conftest import MockNeo4jDriver

    root = tmp_path / "blobs"
    for i in range(96):
        payload(root, str(i))
    settings = main.app.state.settings
    monkeypatch.setattr(main, "_settings", settings)
    # Root matches registry's already-scoped settings fixture (no global env).
    service = await RetentionService.open(root)
    monkeypatch.setattr(main.app.state, "retention_service", service, raising=False)
    monkeypatch.setattr(main.app.state, "auth_mode", "static", raising=False)
    monkeypatch.setattr(main.app.state, "admin_api_key_configured", True, raising=False)
    monkeypatch.setattr(main.app.state, "neo4j_query_driver", MockNeo4jDriver(rows=[{"n": 1}]), raising=False)
    monkeypatch.setattr(
        main.app.state, "neo4j_query_access_mode", "READ", raising=False
    )
    flushed = AsyncMock()
    monkeypatch.setattr(Neo4jGraphStore, "flush", flushed)
    monkeypatch.setattr(Neo4jGraphStore, "close", AsyncMock())
    dispatched = []

    async def fake_graph_dispatch(*args, **kwargs):
        # Exercise the real blob store that ordinary ingestion uses. The graph
        # itself is disposable/fake here; manager owns real Neo4j qualification.
        uri = await AsyncDiskBlobStore(root).write(
            "ingest", str(len(dispatched)), {"value": 1}
        )
        dispatched.append(uri)

    monkeypatch.setattr("context_intelligence_server.registry.process_event", fake_graph_dispatch)
    wrapped = BearerTokenMiddleware(
        main.app, keystore={hashlib.sha256(b"fixture-data").hexdigest(): "test"},
        admin_api_key_digest=hashlib.sha256(b"fixture-admin").hexdigest(),
    )
    client = httpx.AsyncClient(transport=httpx.ASGITransport(app=wrapped), base_url="http://test")
    try:
        admin = {"Authorization": "Bearer fixture-admin"}
        data = {"Authorization": "Bearer fixture-data"}
        response = await client.post(
            "/admin/blobs/prune", json={"older_than": "30d"}, headers=admin
        )
        assert response.status_code == 202
        job = response.json()["id"]
        await service.task
        assert (await client.post(f"/admin/blobs/prune/{job}/start",
                                 json={"confirmation": "prune"}, headers=admin)).status_code == 202
        accepted = 0
        while not service.task.done():
            reply = await client.post("/events", json={
                "event": "tool:pre", "workspace": "/fixture",
                "data": {
                    "session_id": "ingest",
                    "timestamp": "2024-01-01T00:00:00+00:00",
                },
            }, headers=data)
            assert reply.status_code == 202
            graph = await client.post("/cypher", json={"query": "RETURN 1 AS n"}, headers=data)
            assert graph.status_code == 200
            accepted += 1
            await asyncio.sleep(0.005)
        await service.task
        await until(lambda: main.registry._written_total >= accepted)
        assert accepted > 3 and len(dispatched) == accepted and flushed.await_count > 0
        assert service.status()["state"] == "completed" and service.report.errors == 0
        assert service.report.deleted == 96
        for uri in dispatched:
            assert await AsyncDiskBlobStore(root).read(uri) == {"value": 1}
        gone = await client.get("/blobs/session/0", headers=data)
        missing = await client.get("/blobs/session/never", headers=data)
        assert gone.status_code == 410 and missing.status_code == 404
    finally:
        await main.registry.shutdown_workers()
        await main.registry.close_neo4j_driver()
        await client.aclose()
        await service.close()


# --- Relocated-root startup: a terminal snapshot from another root is history ---


async def _completed_root(root: Path) -> None:
    """Run a REAL job to completion at *root*, leaving one expired blob."""
    payload(root)
    service = await RetentionService.open(root)
    try:
        job = await ready(service)
        await service.start(job, "prune")
        await finish(service)
        assert service.status()["state"] == "completed", service.status()
        assert service.report is not None
        assert service.report.deleted == 1
    finally:
        await service.close()


def _metadata_state(root: Path) -> dict[str, tuple[bytes, int]]:
    base = root / r.METADATA
    return {
        str(p.relative_to(base)): (p.read_bytes(), p.stat().st_mtime_ns)
        for p in sorted(base.rglob("*"))
        if p.is_file()
    }


def _rewrite_snapshot(
    base: Path, mode: int = 0o600, drop: tuple[str, ...] = (), **changes
) -> None:
    file = base / r.METADATA / SNAPSHOT
    saved = json.loads(file.read_text())
    # A real service writes report.root == snapshot root; keep a rewritten root
    # coherent unless the caller supplies its own report.
    if isinstance(changes.get("root"), str):
        saved["report"] = {**saved["report"], "root": changes["root"]}
    saved.update(changes)
    for key in drop:
        del saved[key]
    file.write_text(json.dumps(saved))
    file.chmod(mode)


@pytest.mark.parametrize("relocate", ["rename", "copy"])
async def test_relocated_completed_root_reopens_without_job_and_keeps_expiry(
    tmp_path, relocate
):
    import shutil

    old_root, new_root = tmp_path / "old", tmp_path / "new"
    await _completed_root(old_root)
    if relocate == "rename":
        old_root.rename(new_root)
    else:
        shutil.copytree(old_root, new_root)
    before = _metadata_state(new_root)

    service = await RetentionService.open(new_root)
    try:
        assert service.job is None and service.report is None
        with pytest.raises(JobNotFound):
            service.status()
        with pytest.raises(r.BlobExpiredError):
            await AsyncDiskBlobStore(new_root).read("ci-blob://session/old")
        assert _metadata_state(new_root) == before

        payload(new_root, "fresh")
        job = await ready(service)
        await service.start(job, "prune")
        await finish(service)
        status = service.status()
        assert status["state"] == "completed" and status["id"] == job, status
        assert status["root"] == str(new_root.resolve())
    finally:
        await service.close()
    saved = json.loads((new_root / r.METADATA / SNAPSHOT).read_text())
    assert saved["root"] == str(new_root.resolve()) and saved["id"] == job
    with pytest.raises(r.BlobExpiredError):
        await AsyncDiskBlobStore(new_root).read("ci-blob://session/old")


@pytest.mark.parametrize("state", sorted(TERMINAL))
async def test_foreign_terminal_snapshot_of_every_terminal_state_is_history(
    tmp_path, state
):
    await _completed_root(tmp_path)
    _rewrite_snapshot(tmp_path, state=state, root=str(tmp_path.parent / "elsewhere"))
    before = _metadata_state(tmp_path)
    service = await RetentionService.open(tmp_path)
    try:
        assert service.job is None and service.report is None
        with pytest.raises(JobNotFound):
            service.status()
    finally:
        await service.close()
    assert _metadata_state(tmp_path) == before


@pytest.mark.parametrize("state", ["running", "cancelling", "ready", "previewing"])
async def test_foreign_active_or_executable_snapshot_is_rejected_untouched(
    tmp_path, state
):
    await _completed_root(tmp_path)
    _rewrite_snapshot(tmp_path, state=state, root=str(tmp_path.parent / "elsewhere"))
    before = _metadata_state(tmp_path)
    with pytest.raises(r.RetentionError):
        await RetentionService.open(tmp_path)
    assert coordinator_for(tmp_path).service is None
    assert _metadata_state(tmp_path) == before


@pytest.mark.parametrize(
    "changes, mode, expected",
    [
        pytest.param(
            {"version": 2}, 0o600, r.RetentionError, id="same-root-bad-version"
        ),
        pytest.param(
            {"confirmed": False}, 0o600, r.RetentionError, id="same-root-unconfirmed"
        ),
        pytest.param({"id": "not-a-uuid"}, 0o600, ValueError, id="same-root-bad-uuid"),
        pytest.param(
            {"state": "bogus"}, 0o600, r.RetentionError, id="same-root-unknown-state"
        ),
        pytest.param(
            {"state": "bogus", "root": "/elsewhere"},
            0o600,
            r.RetentionError,
            id="foreign-unknown-state",
        ),
        pytest.param(
            {"root": "/elsewhere", "version": 2},
            0o600,
            r.RetentionError,
            id="foreign-bad-version",
        ),
        pytest.param(
            {"root": "/elsewhere", "confirmed": False},
            0o600,
            r.RetentionError,
            id="foreign-unconfirmed",
        ),
        pytest.param(
            {"root": "/elsewhere", "id": "not-a-uuid"},
            0o600,
            ValueError,
            id="foreign-bad-uuid",
        ),
        pytest.param(
            {"root": "/elsewhere"}, 0o644, r.RetentionError, id="foreign-unsafe-mode"
        ),
        pytest.param({}, 0o644, r.RetentionError, id="same-root-unsafe-mode"),
    ],
)
async def test_malformed_or_unsafe_snapshots_still_fail_closed(
    tmp_path, changes, mode, expected
):
    await _completed_root(tmp_path)
    _rewrite_snapshot(tmp_path, mode=mode, **changes)
    before = _metadata_state(tmp_path)
    with pytest.raises(expected):
        await RetentionService.open(tmp_path)
    assert coordinator_for(tmp_path).service is None
    assert _metadata_state(tmp_path) == before


@pytest.mark.parametrize(
    "remove, expected",
    [("report", r.RetentionError), ("id", KeyError)],
)
async def test_foreign_terminal_snapshot_missing_required_field_fails_closed(
    tmp_path, remove, expected
):
    await _completed_root(tmp_path)
    _rewrite_snapshot(tmp_path, root="/elsewhere", drop=(remove,))
    before = _metadata_state(tmp_path)
    with pytest.raises(expected):
        await RetentionService.open(tmp_path)
    assert coordinator_for(tmp_path).service is None
    assert _metadata_state(tmp_path) == before


@pytest.mark.raw_identity_factory
async def test_outer_lifespan_starts_for_relocated_completed_root(
    tmp_path, monkeypatch
):
    """The real lifespan reaches the (mocked) Neo4j driver boundary, not a retention failure."""
    import shutil

    from context_intelligence_server import main
    from context_intelligence_server.config import RecoveryConfig, Settings

    await _completed_root(tmp_path / "original")
    moved = tmp_path / "moved"
    shutil.copytree(tmp_path / "original", moved)
    settings = Settings(
        queues_path=str(tmp_path / "state" / "queues"),
        blob_path=str(moved),
        log_path=str(tmp_path / "logs" / "server.jsonl"),
        api_keys_store_path=str(tmp_path / "identity" / "keys.json"),
        allow_unauthenticated=True,
        recovery=RecoveryConfig(enabled=False),
    )

    class DriverBoundary(Exception):
        pass

    calls: list[str] = []

    def stop_before_driver(*args, **kwargs):
        calls.append("driver")
        raise DriverBoundary("mock Neo4j boundary; no connection created")

    monkeypatch.setattr(main, "_settings", settings)
    monkeypatch.setattr(main, "setup_logging", lambda: None)
    monkeypatch.setattr(main, "build_neo4j_driver", stop_before_driver)
    main.create_asgi_app(settings)
    with pytest.raises(DriverBoundary):
        async with main.lifespan(main.app):
            raise AssertionError("lifespan must stop at the mocked driver boundary")
    assert calls == ["driver"]


_MISSING = object()


@pytest.mark.parametrize(
    "stored",
    [
        pytest.param(_MISSING, id="missing"),
        pytest.param(None, id="null"),
        pytest.param(7, id="integer"),
        pytest.param(1.5, id="float"),
        pytest.param(True, id="boolean"),
        pytest.param(["/old/root"], id="list"),
        pytest.param({"path": "/old/root"}, id="object"),
        pytest.param("", id="empty"),
        pytest.param("   ", id="blank"),
        pytest.param("old/root", id="relative"),
        pytest.param("./old/root", id="dot-relative"),
        pytest.param("/old\x00/root", id="nul"),
    ],
)
@pytest.mark.parametrize("state", sorted(TERMINAL))
async def test_malformed_stored_root_is_rejected_for_every_terminal_state(
    tmp_path, state, stored
):
    await _completed_root(tmp_path)
    if stored is _MISSING:
        _rewrite_snapshot(tmp_path, state=state, drop=("root",))
    else:
        _rewrite_snapshot(tmp_path, state=state, root=stored)
    before = _metadata_state(tmp_path)
    with pytest.raises(r.RetentionError, match="Invalid retention job snapshot"):
        await RetentionService.open(tmp_path)
    assert coordinator_for(tmp_path).service is None
    assert _metadata_state(tmp_path) == before


@pytest.mark.parametrize(
    "old_root",
    ["/definitely/not/there/old-root", "/definitely/not/there/../old-root", "/"],
)
async def test_valid_absolute_foreign_root_need_not_exist_and_is_never_touched(
    tmp_path, old_root
):
    await _completed_root(tmp_path)
    _rewrite_snapshot(tmp_path, root=old_root)
    before = _metadata_state(tmp_path)
    service = await RetentionService.open(tmp_path)
    try:
        assert service.job is None and service.report is None
    finally:
        await service.close()
    assert _metadata_state(tmp_path) == before
    assert not Path("/definitely").exists()


STORED = "/old/foreign/root"
CUTOFF = 1_700_000_000_000_000_000
COUNTERS = (
    "files",
    "logical_bytes",
    "allocated_bytes",
    "retained",
    "skipped",
    "changed",
    "errors",
    "deleted",
)


def _report(**fields) -> dict:
    return {"root": STORED, "cutoff_ns": CUTOFF, **fields}


async def _open_rejected(tmp_path, state, report) -> None:
    """A foreign snapshot with this report must fail closed, untouched."""
    await _completed_root(tmp_path)
    if report is _MISSING:
        _rewrite_snapshot(tmp_path, state=state, root=STORED, drop=("report",))
    else:
        _rewrite_snapshot(tmp_path, state=state, root=STORED, report=report)
    before = _metadata_state(tmp_path)
    with pytest.raises(r.RetentionError, match=r"^Invalid retention job snapshot$"):
        await RetentionService.open(tmp_path)
    assert coordinator_for(tmp_path).service is None
    assert _metadata_state(tmp_path) == before


async def _open_accepted_as_history(tmp_path, state, report) -> None:
    await _completed_root(tmp_path)
    _rewrite_snapshot(tmp_path, state=state, root=STORED, report=report)
    before = _metadata_state(tmp_path)
    service = await RetentionService.open(tmp_path)
    try:
        assert service.job is None and service.report is None
        with pytest.raises(JobNotFound):
            service.status()
    finally:
        await service.close()
    assert coordinator_for(tmp_path).service is None
    assert _metadata_state(tmp_path) == before


@pytest.mark.parametrize("state", sorted(TERMINAL))
@pytest.mark.parametrize(
    "report",
    [
        pytest.param(
            {"root": [], "cutoff_ns": "not-time"}, id="confirmed-root-list-cutoff-str"
        ),
        pytest.param(_MISSING, id="missing-report"),
        pytest.param([], id="report-list"),
        pytest.param({"cutoff_ns": CUTOFF}, id="missing-root"),
        pytest.param({"root": STORED}, id="missing-cutoff"),
        pytest.param(
            {"root": "/some/other/root", "cutoff_ns": CUTOFF},
            id="root-differs-from-snapshot",
        ),
        pytest.param(_report(cutoff_ns=True), id="cutoff-bool"),
        pytest.param(_report(files=-1), id="counter-negative"),
        pytest.param(_report(deleted=True), id="counter-bool"),
        pytest.param(_report(recovery_required=1), id="recovery-int"),
        pytest.param(_report(samples=["ok", 1]), id="samples-non-str-element"),
        pytest.param(_report(unexpected=1), id="unknown-field"),
    ],
)
async def test_foreign_malformed_report_is_rejected_for_every_terminal_state(
    tmp_path, state, report
):
    await _open_rejected(tmp_path, state, report)


_BAD_FIELD_VALUES = {
    "cutoff_ns": {"bool": True, "float": 1.5, "str": "1", "null": None, "list": []},
    "root": {
        "list": [],
        "null": None,
        "int": 7,
        "empty": "",
        "relative": "relative/root",
        "other": "/other/root",
        "trailing-slash": STORED + "/",
        "unnormalized": "/x/.." + STORED,
    },
    "recovery_required": {"int-0": 0, "int-1": 1, "str": "true", "null": None},
    "samples": {
        "str": "uri",
        "null": None,
        "int": 5,
        "non-str": ["ok", 1],
        "null-element": [None],
    },
    **{
        name: {"bool": True, "str": "1", "float": 1.5, "null": None, "negative": -1}
        for name in COUNTERS
    },
}


@pytest.mark.parametrize(
    "field_name, value",
    [
        pytest.param(name, value, id=f"{name}-{label}")
        for name, labelled in _BAD_FIELD_VALUES.items()
        for label, value in labelled.items()
    ],
)
async def test_foreign_report_field_with_wrong_value_is_rejected(
    tmp_path, field_name, value
):
    await _open_rejected(tmp_path, "completed", _report(**{field_name: value}))


@pytest.mark.parametrize(
    "report",
    ["text", 5, None, [_report()]],
    ids=["str", "int", "null", "list-of-report"],
)
async def test_foreign_non_object_report_is_rejected(tmp_path, report):
    await _open_rejected(tmp_path, "completed", report)


async def test_foreign_report_rejection_never_leaks_snapshot_values(tmp_path):
    await _completed_root(tmp_path)
    marker = "SECRET-MARKER-9f3a"
    _rewrite_snapshot(
        tmp_path, root=STORED, report=_report(samples=[marker, 1], **{marker: marker})
    )
    with pytest.raises(r.RetentionError) as caught:
        await RetentionService.open(tmp_path)
    assert caught.value.args == ("Invalid retention job snapshot",)
    assert marker not in repr(caught.value)


@pytest.mark.parametrize("state", sorted(TERMINAL))
async def test_foreign_minimal_report_is_history_for_every_terminal_state(
    tmp_path, state
):
    await _open_accepted_as_history(tmp_path, state, _report())


@pytest.mark.parametrize(
    "report",
    [
        pytest.param(_report(cutoff_ns=-5), id="negative-cutoff"),
        pytest.param(_report(cutoff_ns=0), id="zero-cutoff"),
        pytest.param(_report(recovery_required=True), id="recovery-true-only"),
        pytest.param(_report(recovery_required=False), id="recovery-false-only"),
        pytest.param(_report(samples=[]), id="empty-samples"),
        pytest.param(
            _report(samples=["ci-blob://a/b", "ci-blob://c/d"]), id="str-samples"
        ),
        pytest.param(
            _report(**dict.fromkeys(COUNTERS, 0), recovery_required=False, samples=[]),
            id="full-zero-counters",
        ),
        pytest.param(_report(**dict.fromkeys(COUNTERS, 10**18)), id="large-counters"),
        pytest.param(_report(files=3, deleted=1, errors=0), id="partial-counters"),
    ],
)
async def test_foreign_valid_report_shapes_are_history(tmp_path, report):
    await _open_accepted_as_history(tmp_path, "completed", report)


@pytest.mark.parametrize(
    "mutate, expected",
    [
        pytest.param(lambda saved: saved.pop("report"), KeyError, id="missing-report"),
        pytest.param(
            lambda saved: saved["report"].update(unexpected=1),
            TypeError,
            id="unknown-field",
        ),
    ],
)
async def test_same_root_report_failures_keep_their_original_types(
    tmp_path, mutate, expected
):
    await _completed_root(tmp_path)
    file = tmp_path / r.METADATA / SNAPSHOT
    saved = json.loads(file.read_text())
    mutate(saved)
    file.write_text(json.dumps(saved))
    file.chmod(0o600)
    before = _metadata_state(tmp_path)
    with pytest.raises(expected):
        await RetentionService.open(tmp_path)
    assert coordinator_for(tmp_path).service is None
    assert _metadata_state(tmp_path) == before


async def test_same_root_report_values_are_not_newly_validated(tmp_path):
    """Only the foreign-terminal exemption is new; same-root loading is unchanged."""
    await _completed_root(tmp_path)
    _rewrite_snapshot(tmp_path, report={"root": "/other/root", "cutoff_ns": "not-time"})
    service = await RetentionService.open(tmp_path)
    try:
        status = service.status()
        assert status["state"] == "completed"
        assert status["report"]["cutoff_ns"] == "not-time"
    finally:
        await service.close()


# --- Mapped (.blob) and pre-upgrade backslash (.json) names (PR126) ---------


async def test_online_service_prunes_mapped_legacy_and_ordinary_files_and_replay_stays_expired(
    tmp_path,
):
    store = AsyncDiskBlobStore(tmp_path)
    mapped_uri = await store.write("session", "a/b", {"v": "mapped"})
    old = time.time_ns() - 90 * 86400 * 10**9
    os.utime(store.blob_path("session", "a/b"), ns=(old, old))
    payload(tmp_path, "a\\b")  # pre-upgrade file: a literal backslash in its name
    payload(tmp_path, "plain")
    legacy_uri = "ci-blob://session/a\\b"
    uris = sorted([mapped_uri, legacy_uri, "ci-blob://session/plain"])

    service = await RetentionService.open(tmp_path)
    try:
        job = await ready(service)
        assert sorted(service.status()["report"]["samples"]) == uris
        await service.start(job, "prune")
        await finish(service)
        result = service.status()
    finally:
        await service.close()

    assert result["state"] == "completed", result
    assert (result["report"]["deleted"], result["report"]["errors"]) == (3, 0)
    assert list((tmp_path / "session" / "blobs").iterdir()) == []
    for uri in uris:
        with pytest.raises(r.BlobExpiredError) as expired:
            await store.read(uri)
        assert expired.value.uri == uri
    # Replay keeps the recorded reference and never rehydrates a pruned payload.
    assert await store.write("session", "a/b", {"replayed": True}) == mapped_uri
    assert await store.write("session", "a\\b", {"replayed": True}) == legacy_uri
    assert list((tmp_path / "session" / "blobs").iterdir()) == []


_MAPPED_DIGEST = "5998a4c7b7280b4eb36c9f88878ed97c34b2cbeb7f138372a02365515c97bde9"  # sha256 of "a/b" under the key domain


@pytest.mark.parametrize("kind", ["mapped", "legacy"])
@pytest.mark.parametrize(
    "operation", ["write", "read", "list", "size", "dump", "delete_session"]
)
async def test_mapped_and_legacy_operations_queue_behind_an_exclusive_lease(
    tmp_path, kind, operation
):
    store = AsyncDiskBlobStore(tmp_path)
    if kind == "mapped":
        uri, key = await store.write("session", "a/b", {"seed": kind}), "a/b"
    else:
        payload(tmp_path, "a\\b")  # pre-upgrade file with a literal backslash
        uri, key = "ci-blob://session/a\\b", "a\\b"
    coord = await asyncio.to_thread(coordinator_for, tmp_path)
    entered, release = threading.Event(), threading.Event()

    def exclusive():
        with coord.lease(exclusive=True):
            entered.set()
            assert release.wait(5)

    holder = asyncio.create_task(asyncio.to_thread(exclusive))
    await until(entered.is_set)
    calls = {
        "write": lambda: store.write("session", key, {"new": kind}),
        "read": lambda: store.read(uri),
        "list": lambda: store.list("session"),
        "size": lambda: store.size(uri),
        "dump": lambda: store.dump(uri, tmp_path / "dump"),
        "delete_session": lambda: store.delete_session("session"),
    }
    task = asyncio.create_task(calls[operation]())
    try:
        await until(lambda: len(coord.queue) == 1)
        assert not task.done()
        assert coord.queue[0][1] == (operation == "delete_session")
    finally:
        release.set()
        await holder
    result = await task
    if operation == "read":
        assert result == ({"seed": "mapped"} if kind == "mapped" else {"value": 1})
    if operation == "list":
        assert result == [uri]
    if operation == "delete_session":
        assert result == 1


async def test_a_cancelled_waiting_mapped_write_creates_nothing(tmp_path):
    store = AsyncDiskBlobStore(tmp_path)
    coord = await asyncio.to_thread(coordinator_for, tmp_path)
    entered, release = threading.Event(), threading.Event()

    def exclusive():
        with coord.lease(exclusive=True):
            entered.set()
            assert release.wait(5)

    holder = asyncio.create_task(asyncio.to_thread(exclusive))
    await until(entered.is_set)
    waiting = asyncio.create_task(store.write("session", "a/b", {"v": 1}))
    try:
        await until(lambda: len(coord.queue) == 1)
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
    finally:
        release.set()
        await holder

    assert not (tmp_path / "session").exists()
    assert not coord.queue and coord.readers == 0


async def test_an_admitted_mapped_write_finishes_after_cancellation_and_excludes_retention(
    tmp_path, monkeypatch
):
    store = AsyncDiskBlobStore(tmp_path)
    coord = await asyncio.to_thread(coordinator_for, tmp_path)
    entered, release = threading.Event(), threading.Event()
    real_fsync = os.fsync

    def gated_fsync(fd):
        # Real fsync, but the FIRST one (inside the admitted write) waits at a barrier.
        if not entered.is_set():
            entered.set()
            assert release.wait(5)
        return real_fsync(fd)

    monkeypatch.setattr(os, "fsync", gated_fsync)
    write = asyncio.create_task(store.write("session", "a/b", {"v": 1}))
    await until(entered.is_set)
    write.cancel()
    seen: list[list[str]] = []
    maintenance = asyncio.create_task(
        filesystem(
            tmp_path,
            lambda: seen.append(
                sorted(p.name for p in (tmp_path / "session" / "blobs").iterdir())
            ),
            exclusive=True,
        )
    )
    try:
        await until(
            lambda: len(coord.queue) == 1
        )  # retention-style exclusive work waits
        assert seen == []
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await write
    await maintenance

    # The admitted write completed under its lease; exclusive work never saw a temp file.
    assert seen == [[f"~ci-key-v1~{_MAPPED_DIGEST}.blob"]]
    assert await store.read(f"ci-blob://session/~ci-key-v1/{_MAPPED_DIGEST}") == {
        "v": 1
    }
    assert not coord.queue and coord.readers == 0
