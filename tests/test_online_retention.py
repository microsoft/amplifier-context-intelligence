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