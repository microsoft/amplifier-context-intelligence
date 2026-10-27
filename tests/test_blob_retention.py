"""Retention acceptance tests, exclusively against isolated scratch roots."""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import shlex
import sqlite3
import subprocess
import sys
import threading
from pathlib import Path

import pytest

from context_intelligence_server import blob_retention as r
from context_intelligence_server.blob_store import AsyncDiskBlobStore

CUTOFF = 1_700_000_000_000_000_000
CLI = Path(sys.executable).parent / "context-intelligence-server"


def payload(root: Path, key: str = "old", mtime: int = CUTOFF - 1) -> Path:
    path = root / "session" / "blobs" / f"{key}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('{"value": 1}')
    os.utime(path, ns=(mtime, mtime))
    return path


def run_cli(root: Path, *args: str, tty_input: str | None = None, env=None):
    command = [str(CLI), "blobs", "prune", "--blob-root", str(root), *args]
    if tty_input is not None:
        # script owns/reaps its PTY child synchronously. No detached process.
        command = ["script", "-q", "-e", "-c", shlex.join(command), "/dev/null"]
    return subprocess.run(command, input=tty_input or "", text=True, capture_output=True,
                          timeout=10, env=env)


@pytest.mark.parametrize("age,seconds", [("1s", 1), ("2m", 120), ("3h", 10800), ("30d", 2592000), ("2w", 1209600)])
def test_duration(age, seconds):
    assert r.duration_ns(age) == seconds * 10**9


@pytest.mark.parametrize("age", ["0d", "-1d", "1.2d", "30", "d", "1D", "1 d", "01d", "9999999999999999999999999w", "9999999999w"])
def test_bad_duration(age):
    with pytest.raises(ValueError):
        r.duration_ns(age)


def test_cutoff_range():
    with pytest.raises(ValueError):
        r.cutoff_for(10, now_ns=9)
    assert r.cutoff_for(1, now_ns=10) == 9


def test_preview_boundary_future_event_names_and_no_artifacts(tmp_path, monkeypatch):
    old = payload(tmp_path, "2099-01-01__raw")
    payload(tmp_path, "boundary", CUTOFF)
    payload(tmp_path, "new", CUTOFF + 1)
    payload(tmp_path, "1900-01-01__raw", CUTOFF + 10**12)
    before = {p.relative_to(tmp_path): p.lstat() for p in tmp_path.rglob("*")}
    real_open = os.open

    def no_body(path, *args, **kwargs):
        assert not str(path).endswith(".json"), "preview opened a blob body"
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(os, "open", no_body)
    report = r.preview(tmp_path, CUTOFF)
    assert report.files == 1 and report.retained == 3
    assert report.logical_bytes == old.stat().st_size
    assert report.allocated_bytes == old.stat().st_blocks * 512
    assert report.samples == ["ci-blob://session/2099-01-01__raw"]
    assert "America/Los_Angeles" in report.text()
    after = {p.relative_to(tmp_path): p.lstat() for p in tmp_path.rglob("*")}
    assert before.keys() == after.keys()
    assert all(before[p].st_mtime_ns == after[p].st_mtime_ns for p in before)
    assert not (tmp_path / r.LOCK).exists()
    assert not (tmp_path / r.METADATA).exists()


def test_samples_bounded_and_batches_bounded(tmp_path, monkeypatch):
    for i in range(1100):
        payload(tmp_path, str(i))
    sizes = []
    journal = r.Ledger.journal

    def tracking(self, batch):
        sizes.append(len(batch))
        journal(self, batch)

    monkeypatch.setattr(r.Ledger, "journal", tracking)
    report = r.apply(tmp_path, CUTOFF, lambda _: True)
    assert report.deleted == 1100 and report.errors == 0
    assert sizes == [512, 512, 76]
    assert len(report.samples) == 5
    assert (tmp_path / "session" / "blobs").is_dir()


def test_apply_preserves_graph_queue_unknowns_and_directories(tmp_path):
    old = payload(tmp_path)
    boundary = payload(tmp_path, "boundary", CUTOFF)
    sentinels = [tmp_path / "graph.db", tmp_path / "session" / "queue.jsonl",
                 old.parent / "unknown.tmp", old.parent / "nested" / "keep.json"]
    for path in sentinels:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"sentinel")
    report = r.apply(tmp_path, CUTOFF, lambda plan: plan.files == 1)
    assert report.deleted == 1 and not old.exists()
    assert boundary.exists() and old.parent.is_dir()
    assert all(p.read_bytes() == b"sentinel" for p in sentinels)
    assert (tmp_path / r.LOCK).is_file()
    with r.Root(tmp_path) as root, r.Ledger(root) as ledger:
        assert ledger.contains("ci-blob://session/old")
        assert ledger.db.execute("PRAGMA query_only").fetchone() == (0,)  # mode=ro enforces access
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            ledger.db.execute("DELETE FROM expiry")


@pytest.mark.parametrize("answer", [False, None])
def test_decline_creates_no_ledger(tmp_path, answer):
    old = payload(tmp_path)
    with pytest.raises(r.RetentionError, match="not confirmed"):
        r.apply(tmp_path, CUTOFF, lambda _: answer)
    assert old.exists() and not (tmp_path / r.METADATA).exists()


def test_symlinks_hardlinks_and_unknown_layout_skipped(tmp_path):
    outside = tmp_path / "outside.json"
    outside.write_text("{}")
    old = payload(tmp_path)
    (tmp_path / "alias-session").symlink_to(old.parent.parent, target_is_directory=True)
    (old.parent / "link.json").symlink_to(outside)
    os.link(outside, old.parent / "hardlink.json")
    (old.parent / "directory.json").mkdir()
    (tmp_path / "fake-session").mkdir()
    (tmp_path / "fake-session" / "blobs").symlink_to(old.parent, target_is_directory=True)
    payload(tmp_path, "bad\nname")
    result = r.apply(tmp_path, CUTOFF, lambda _: True)
    assert result.deleted == 1
    assert result.skipped >= 6
    assert outside.read_text() == "{}"


@pytest.mark.parametrize("uri", [
    "ci-blob://../key", "ci-blob://session/../escape", "ci-blob://session/..",
    "ci-blob://session/a\nb",
])
async def test_uri_escape_rejected(tmp_path, uri):
    store = AsyncDiskBlobStore(tmp_path)
    with pytest.raises(ValueError):
        await store.read(uri)
    with pytest.raises(ValueError):
        await store.dump(uri, tmp_path / "dump")
    assert not (tmp_path / r.LOCK).exists()


@pytest.mark.parametrize("entry", ["lock", "metadata", "database", "journal"])
def test_symlinked_metadata_rejected(tmp_path, entry):
    old = payload(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    if entry == "lock":
        (tmp_path / r.LOCK).symlink_to(outside / "lock")
    elif entry == "metadata":
        (tmp_path / r.METADATA).symlink_to(outside, target_is_directory=True)
    else:
        metadata = tmp_path / r.METADATA
        metadata.mkdir(mode=0o700)
        (metadata / (r.DATABASE if entry == "database" else r.DATABASE + "-journal")).symlink_to(outside / "db")
    with pytest.raises((OSError, r.RetentionError)):
        r.apply(tmp_path, CUTOFF, lambda _: True)
    assert old.exists() and not list(outside.iterdir())


def test_corrupt_ledger_fails_loud_without_unlink(tmp_path):
    old = payload(tmp_path)
    metadata = tmp_path / r.METADATA
    metadata.mkdir(mode=0o700)
    database = metadata / r.DATABASE
    database.write_bytes(b"not SQLite")
    before = database.read_bytes()
    with pytest.raises(sqlite3.DatabaseError):
        r.preview(tmp_path, CUTOFF)
    with pytest.raises(sqlite3.DatabaseError):
        r.apply(tmp_path, CUTOFF, lambda _: True)
    assert old.exists() and database.read_bytes() == before


def test_existing_empty_ledger_still_fails_closed(tmp_path):
    old = payload(tmp_path)
    metadata = tmp_path / r.METADATA
    metadata.mkdir(mode=0o700)
    database = metadata / r.DATABASE
    database.touch(mode=0o600)
    for operation in (
        lambda: r.preview(tmp_path, CUTOFF),
        lambda: r.apply(tmp_path, CUTOFF, lambda _: True),
        lambda: asyncio.run(AsyncDiskBlobStore(tmp_path).write("session", "new", {})),
    ):
        with pytest.raises(r.RetentionError, match="Invalid expiry ledger schema"):
            operation()
    assert old.exists() and database.read_bytes() == b""
    assert not (old.parent / "new.json").exists()


@pytest.mark.parametrize("boundary", [
    "file_created", "sqlite_opened", "schema_created", "schema_committed",
    "file_synced", "before_publish", "after_publish", "directory_synced",
])
def test_first_ledger_publication_process_crash_and_rerun(tmp_path, boundary):
    old = payload(tmp_path)
    code = f"""
import os
import stat
from pathlib import Path
from context_intelligence_server import blob_retention as r

root = Path({str(tmp_path)!r})
boundary = {boundary!r}
def crash(at):
    if boundary == at:
        os._exit(73)

real_open, real_connect = os.open, r.sqlite3.connect
real_fsync, real_rename = os.fsync, os.rename
def opened(name, *args, **kwargs):
    fd = real_open(name, *args, **kwargs)
    if (name == r.DATABASE or str(name).startswith(r.DATABASE + ".init-")) and args[0] & os.O_CREAT:
        crash("file_created")
    return fd
class Connection(r.sqlite3.Connection):
    def execute(self, sql, *args, **kwargs):
        result = super().execute(sql, *args, **kwargs)
        if sql.startswith("CREATE TABLE expiry"):
            crash("schema_created")
        return result
    def commit(self):
        super().commit()
        crash("schema_committed")
def connected(*args, **kwargs):
    db = real_connect(*args, factory=Connection, **kwargs)
    crash("sqlite_opened")
    return db
def synced(fd):
    real_fsync(fd)
    st = os.fstat(fd)
    if stat.S_ISREG(st.st_mode):
        crash("file_synced")
    elif (root / r.METADATA / r.DATABASE).exists():
        crash("directory_synced")
def renamed(*args, **kwargs):
    crash("before_publish")
    real_rename(*args, **kwargs)
    crash("after_publish")
os.open, r.sqlite3.connect = opened, connected
os.fsync, os.rename = synced, renamed
r.apply(root, {CUTOFF}, lambda _: True)
"""
    child = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=10)
    assert child.returncode == 73, child.stderr
    assert old.exists()
    metadata = tmp_path / r.METADATA
    database = metadata / r.DATABASE
    published = boundary in ("after_publish", "directory_synced")
    assert database.exists() == published
    if published:
        with sqlite3.connect(database) as db:
            assert tuple(row[1] for row in db.execute("PRAGMA table_info(expiry)")) == r._COLUMNS
            assert db.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert not (metadata / (r.DATABASE + "-journal")).exists()
    else:
        # A zero-byte file or hot journal is harmless under a private temp name.
        assert list(metadata.glob(r.DATABASE + ".init-*"))
    before = {p.name: p.read_bytes() for p in metadata.iterdir()}
    assert r.preview(tmp_path, CUTOFF).files == 1
    with pytest.raises(r.RetentionError, match="not confirmed"):
        r.apply(tmp_path, CUTOFF, lambda _: False)
    assert {p.name: p.read_bytes() for p in metadata.iterdir()} == before
    store = AsyncDiskBlobStore(tmp_path)
    assert asyncio.run(store.write("session", "new", {"value": 2})) == "ci-blob://session/new"
    assert r.apply(tmp_path, CUTOFF, lambda report: report.files == 1).deleted == 1
    assert not old.exists()
    assert asyncio.run(store.read("ci-blob://session/new")) == {"value": 2}
    with pytest.raises(r.BlobExpiredError):
        asyncio.run(store.read("ci-blob://session/old"))
    assert r.apply(tmp_path, CUTOFF, lambda report: report.files == 0).deleted == 0
    # Reruns do not mutate or adopt incomplete initialization remnants.
    for name, contents in before.items():
        if name != r.DATABASE:
            assert (metadata / name).read_bytes() == contents


@pytest.mark.parametrize("boundary", ["file", "before_publish", "after_publish"])
def test_first_ledger_fsync_failure_never_unlinks_and_can_retry(tmp_path, monkeypatch, boundary):
    old = payload(tmp_path)
    metadata = tmp_path / r.METADATA
    database = metadata / r.DATABASE
    real_fsync = os.fsync

    def refuse(fd):
        import stat
        st = os.fstat(fd)
        if stat.S_ISREG(st.st_mode) and boundary == "file":
            raise OSError("initialization fsync refused")
        if metadata.exists() and r._identity(st) == r._identity(metadata.stat()):
            if database.exists() == (boundary == "after_publish") and boundary != "file":
                raise OSError("initialization fsync refused")
        return real_fsync(fd)

    with monkeypatch.context() as m:
        m.setattr(os, "fsync", refuse)
        with pytest.raises(OSError, match="initialization fsync refused"):
            r.apply(tmp_path, CUTOFF, lambda _: True)
    assert old.exists() and database.exists() == (boundary == "after_publish")
    assert r.preview(tmp_path, CUTOFF).files == 1
    assert r.apply(tmp_path, CUTOFF, lambda _: True).deleted == 1
    assert r.apply(tmp_path, CUTOFF, lambda _: True).deleted == 0


def test_commit_failure_before_unlink(tmp_path, monkeypatch):
    old = payload(tmp_path)
    real_commit = r.Ledger.commit

    def refuse(self):
        if self.db.execute("SELECT 1 FROM expiry WHERE pending=1").fetchone():
            raise sqlite3.OperationalError("simulated disk full")
        return real_commit(self)

    monkeypatch.setattr(r.Ledger, "commit", refuse)
    with pytest.raises(sqlite3.OperationalError, match="disk full"):
        r.apply(tmp_path, CUTOFF, lambda _: True)
    assert old.exists()


def test_metadata_fsync_failure_prevents_unlink(tmp_path, monkeypatch):
    old = payload(tmp_path)
    real_journal = r.Ledger.journal
    real_fsync = os.fsync

    def journal(self, batch):
        def refuse_metadata(fd):
            if fd == self.fd:
                raise OSError("metadata fsync refused")
            return real_fsync(fd)
        with monkeypatch.context() as m:
            m.setattr(os, "fsync", refuse_metadata)
            real_journal(self, batch)

    monkeypatch.setattr(r.Ledger, "journal", journal)
    with pytest.raises(OSError, match="fsync refused"):
        r.apply(tmp_path, CUTOFF, lambda _: True)
    assert old.exists()


def test_batch_directory_sync_is_deduplicated_and_precedes_completion(tmp_path, monkeypatch):
    paths = [payload(tmp_path, key=f"old-{i}") for i in range(5)]
    directory = r._identity(paths[0].parent.stat())
    events = []
    real_sync, real_unlink, real_commit = os.fsync, os.unlink, r.Ledger.commit
    monkeypatch.setattr(r, "BATCH_SIZE", 2)

    def synced(fd):
        if r._identity(os.fstat(fd)) == directory:
            events.append("directory-sync")
        real_sync(fd)

    def unlinked(name, *args, **kwargs):
        result = real_unlink(name, *args, **kwargs)
        if str(name).endswith(".json"):
            events.append("unlink")
        return result

    def committed(self):
        if self.db.execute("SELECT 1 FROM expiry WHERE pending=0").fetchone():
            assert events[-1] in ("directory-sync", "completion")
            events.append("completion")
        return real_commit(self)

    monkeypatch.setattr(os, "fsync", synced)
    monkeypatch.setattr(os, "unlink", unlinked)
    monkeypatch.setattr(r.Ledger, "commit", committed)
    assert r.apply(tmp_path, CUTOFF, lambda _: True).deleted == 5
    assert events.count("directory-sync") == 3  # one per batch, not per payload
    assert not any(path.exists() for path in paths)


def test_directory_sync_failure_keeps_durable_pending_and_closes_all_fds(tmp_path, monkeypatch):
    paths = []
    for session in ("first", "second", "third"):
        path = tmp_path / session / "blobs" / "old.json"
        path.parent.mkdir(parents=True)
        path.write_text("{}")
        os.utime(path, ns=(CUTOFF - 1, CUTOFF - 1))
        paths.append(path)
    identities = {r._identity(path.parent.stat()) for path in paths}
    attempts, duplicates = [], []
    real_sync, real_dup = os.fsync, os.dup

    def duplicated(fd):
        result = real_dup(fd)
        duplicates.append(result)
        return result

    def fail_second(fd):
        identity = r._identity(os.fstat(fd))
        if identity in identities:
            attempts.append(identity)
            if len(attempts) == 2:
                raise OSError("batch directory sync failed")
        return real_sync(fd)

    with monkeypatch.context() as m:
        m.setattr(os, "dup", duplicated)
        m.setattr(os, "fsync", fail_second)
        with pytest.raises(OSError, match="batch directory sync failed"):
            r.apply(tmp_path, CUTOFF, lambda _: True)
    assert len(attempts) == 3  # later directories are still synced
    for fd in duplicates:
        with pytest.raises(OSError):
            os.fstat(fd)
    with r.Root(tmp_path) as root, r.Ledger(root) as ledger:
        assert ledger.db.execute("SELECT pending FROM expiry").fetchall() == [(1,)] * 3
    assert not any(path.exists() for path in paths)
    attempts.clear()

    def record_sync(fd):
        identity = r._identity(os.fstat(fd))
        if identity in identities:
            attempts.append(identity)
        return real_sync(fd)

    with monkeypatch.context() as m:
        m.setattr(os, "fsync", record_sync)
        assert r.apply(tmp_path, CUTOFF, lambda _: True).deleted == 0
    assert set(attempts) == identities  # recovered absences synced before commit
    with r.Root(tmp_path) as root, r.Ledger(root) as ledger:
        assert ledger.db.execute("SELECT pending FROM expiry").fetchall() == [(0,)] * 3


def test_directory_pin_failure_precedes_unlink(tmp_path, monkeypatch):
    old = payload(tmp_path)
    with monkeypatch.context() as m:
        def refuse(fd):
            raise OSError("no descriptor available")
        m.setattr(os, "dup", refuse)
        result = r.apply(tmp_path, CUTOFF, lambda _: True)
    assert result.deleted == 0 and result.errors == 1 and old.exists()
    with r.Root(tmp_path) as root, r.Ledger(root) as ledger:
        assert ledger.db.execute("SELECT pending FROM expiry").fetchone() == (1,)
    assert r.apply(tmp_path, CUTOFF, lambda _: True).deleted == 1


def test_directory_close_failure_still_drains_other_descriptors(tmp_path, monkeypatch):
    first = os.open(tmp_path, r._DIR)
    second = os.open(tmp_path, r._DIR)
    synced, closed = [], []
    real_sync, real_close = os.fsync, os.close

    def record_sync(fd):
        synced.append(fd)
        return real_sync(fd)

    def fail_first_close(fd):
        closed.append(fd)
        real_close(fd)
        if fd == first:
            raise OSError("close result uncertain")

    monkeypatch.setattr(os, "fsync", record_sync)
    monkeypatch.setattr(os, "close", fail_first_close)
    with pytest.raises(OSError, match="close result uncertain"):
        with r._sync_dirs() as directories:
            directories[(1, 1)] = first
            directories[(1, 2)] = second
    assert synced == [first, second] and closed == [first, second]
    for fd in (first, second):
        with pytest.raises(OSError):
            os.fstat(fd)


def test_directory_descriptors_are_bounded_by_batch(tmp_path, monkeypatch):
    for i in range(7):
        path = tmp_path / f"session-{i}" / "blobs" / "old.json"
        path.parent.mkdir(parents=True)
        path.write_text("{}")
        os.utime(path, ns=(CUTOFF - 1, CUTOFF - 1))
    active = set()
    maximum = 0
    real_dup, real_close = os.dup, os.close

    def duplicated(fd):
        nonlocal maximum
        result = real_dup(fd)
        active.add(result)
        maximum = max(maximum, len(active))
        return result

    def closed(fd):
        active.discard(fd)
        return real_close(fd)

    monkeypatch.setattr(r, "BATCH_SIZE", 3)
    monkeypatch.setattr(os, "dup", duplicated)
    monkeypatch.setattr(os, "close", closed)
    assert r.apply(tmp_path, CUTOFF, lambda _: True).deleted == 7
    assert maximum == 3 and not active


def test_checked_batch_restores_checks_and_rejects_replaced_root(tmp_path):
    original = tmp_path / "root"
    original.mkdir()
    with r.Root(original) as root:
        with pytest.raises(RuntimeError, match="body failed"):
            with root.checked_batch():
                raise RuntimeError("body failed")
        assert not root._inside_checked_batch
        root.check()
        with pytest.raises(r.RetentionError, match="root changed"):
            with root.checked_batch():
                original.rename(tmp_path / "previous-root")
                original.mkdir()
        assert not root._inside_checked_batch


def test_checked_batches_keep_root_walks_bounded(tmp_path, monkeypatch):
    for i in range(10):
        payload(tmp_path, key=f"old-{i}")
    real_open = r._open_absolute
    walks = []

    def counted(path):
        walks.append(path)
        return real_open(path)

    monkeypatch.setattr(r, "_open_absolute", counted)
    assert r.apply(tmp_path, CUTOFF, lambda _: True).deleted == 10
    # Preview/session boundaries, initialization and commits still check fully;
    # individual candidates no longer walk every absolute ancestor repeatedly.
    assert len(walks) < 50


def test_process_crash_after_unlink_recovers_and_syncs_absence(tmp_path, monkeypatch):
    old = payload(tmp_path)
    directory = r._identity(old.parent.stat())
    code = f"""
import os
from context_intelligence_server import blob_retention as r
real_unlink = os.unlink
def crash(name, *args, **kwargs):
    real_unlink(name, *args, **kwargs)
    if name == 'old.json':
        os._exit(73)
os.unlink = crash
r.apply({str(tmp_path)!r}, {CUTOFF}, lambda _: True)
"""
    child = subprocess.run([sys.executable, "-c", code], capture_output=True, timeout=10)
    assert child.returncode == 73 and not old.exists()
    with r.Root(tmp_path) as root, r.Ledger(root) as ledger:
        assert ledger.db.execute("SELECT pending FROM expiry").fetchone() == (1,)
    synced = []
    real_sync = os.fsync

    def record(fd):
        if r._identity(os.fstat(fd)) == directory:
            synced.append(fd)
        return real_sync(fd)

    monkeypatch.setattr(os, "fsync", record)
    assert r.apply(tmp_path, CUTOFF, lambda _: True).deleted == 0
    assert len(synced) == 1
    with r.Root(tmp_path) as root, r.Ledger(root) as ledger:
        assert ledger.db.execute("SELECT pending FROM expiry").fetchone() == (0,)


def test_interrupted_commit_then_rerun_and_decline_pending(tmp_path, monkeypatch):
    old = payload(tmp_path)

    def crash(*args, **kwargs):
        raise RuntimeError("interrupted after durable commit")

    with monkeypatch.context() as m:
        m.setattr(r, "_unlink", crash)
        with pytest.raises(RuntimeError, match="interrupted"):
            r.apply(tmp_path, CUTOFF, lambda _: True)
    assert old.exists()
    with r.Root(tmp_path) as root, r.Ledger(root) as ledger:
        assert ledger.contains("ci-blob://session/old")
        assert ledger.db.execute("SELECT pending FROM expiry").fetchone() == (1,)
    with pytest.raises(r.RetentionError):
        r.apply(tmp_path, CUTOFF, lambda _: False)
    assert old.exists()
    result = r.apply(tmp_path, CUTOFF, lambda _: True)
    assert result.deleted == 1 and not old.exists()
    assert r.apply(tmp_path, CUTOFF, lambda _: True).deleted == 0


def test_pending_45d_payload_respects_fresh_90d_confirmation_then_30d(tmp_path):
    day = r.duration_ns("1d")
    now = CUTOFF
    cutoff_30d, cutoff_90d = now - 30 * day, now - 90 * day
    old = payload(tmp_path, mtime=now - 45 * day)
    code = f"""
import os
from context_intelligence_server import blob_retention as r
r._unlink = lambda *args, **kwargs: os._exit(73)
r.apply({str(tmp_path)!r}, {cutoff_30d}, lambda report: report.files == 1)
"""
    child = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=10)
    assert child.returncode == 73, child.stderr
    before_payload = old.read_bytes(), r.fingerprint(old.stat())
    database = tmp_path / r.METADATA / r.DATABASE
    before_database = database.read_bytes()
    with r.Root(tmp_path) as root, r.Ledger(root) as ledger:
        row = ledger.db.execute("SELECT * FROM expiry").fetchone()
        assert row[-1] == 1
    seen = []

    def confirm_90d(report):
        seen.append((report.files, report.retained, report.samples))
        return True

    with pytest.raises(r.RetentionError, match="not confirmed"):
        r.apply(tmp_path, cutoff_90d, lambda _: False)
    assert database.read_bytes() == before_database
    assert (old.read_bytes(), r.fingerprint(old.stat())) == before_payload
    result = r.apply(tmp_path, cutoff_90d, confirm_90d)
    assert seen == [(0, 1, [])]
    assert result.deleted == 0 and result.retained == 1
    assert (old.read_bytes(), r.fingerprint(old.stat())) == before_payload
    with r.Root(tmp_path) as root, r.Ledger(root) as ledger:
        assert ledger.db.execute("SELECT * FROM expiry").fetchone() == row
    seen.clear()

    def confirm_30d(report):
        seen.append((report.files, report.retained, report.samples))
        return True

    assert r.apply(tmp_path, cutoff_30d, confirm_30d).deleted == 1
    assert seen == [(1, 0, ["ci-blob://session/old"])]
    assert not old.exists()
    with r.Root(tmp_path) as root, r.Ledger(root) as ledger:
        assert ledger.db.execute("SELECT * FROM expiry").fetchone() == (*row[:-1], 0)
    assert r.apply(tmp_path, cutoff_30d, lambda _: True).deleted == 0


@pytest.mark.parametrize("change", ["boundary", "replaced"])
def test_pending_recovery_keeps_boundary_and_replaced_fingerprints(tmp_path, monkeypatch, change):
    old = payload(tmp_path)
    with monkeypatch.context() as m:
        def interrupt(*args, **kwargs):
            raise RuntimeError("interrupted")
        m.setattr(r, "_unlink", interrupt)
        with pytest.raises(RuntimeError, match="interrupted"):
            r.apply(tmp_path, CUTOFF, lambda _: True)
    with r.Root(tmp_path) as root, r.Ledger(root) as ledger:
        row = ledger.db.execute("SELECT * FROM expiry").fetchone()
    if change == "boundary":
        cutoff = row[4]  # original mtime is exactly the current cutoff
    else:
        replacement = old.with_suffix(".tmp")
        replacement.write_bytes(old.read_bytes())
        os.utime(replacement, ns=(row[4], row[4]))
        os.replace(replacement, old)
        cutoff = CUTOFF
    before = old.read_bytes(), r.fingerprint(old.stat())
    result = r.apply(tmp_path, cutoff, lambda _: True)
    assert result.deleted == 0
    assert (old.read_bytes(), r.fingerprint(old.stat())) == before
    with r.Root(tmp_path) as root, r.Ledger(root) as ledger:
        assert ledger.db.execute("SELECT * FROM expiry").fetchone() == row


@pytest.mark.parametrize("when", ["before_journal", "before_unlink"])
def test_changed_fingerprint_retained(tmp_path, monkeypatch, when):
    old = payload(tmp_path)
    real_matches = r._matches
    real_journal = r.Ledger.journal

    def rewrite():
        old.write_text('{"new": true}')
        os.utime(old, ns=(CUTOFF + 1, CUTOFF + 1))

    if when == "before_journal":
        def matches(root, item):
            rewrite()
            return real_matches(root, item)
        monkeypatch.setattr(r, "_matches", matches)
    else:
        def journal(self, batch):
            real_journal(self, batch)
            rewrite()
        monkeypatch.setattr(r.Ledger, "journal", journal)
    result = r.apply(tmp_path, CUTOFF, lambda _: True)
    assert result.deleted == 0 and result.changed == 1
    assert old.read_text() == '{"new": true}'


def test_replaced_ancestor_detected_before_journal(tmp_path, monkeypatch):
    root = tmp_path / "root"
    root.mkdir()
    old = payload(root)
    original = r._matches

    def moved(handle, item):
        (root / "session").rename(tmp_path / "moved")
        (root / "session").symlink_to(tmp_path / "moved", target_is_directory=True)
        return original(handle, item)

    monkeypatch.setattr(r, "_matches", moved)
    with pytest.raises((OSError, r.RetentionError)):
        r.apply(root, CUTOFF, lambda _: True)
    assert (tmp_path / "moved" / "blobs" / old.name).exists()


def test_lock_cross_process_alias_coordination_and_release(tmp_path):
    payload(tmp_path)
    alias = tmp_path.parent / (tmp_path.name + "-alias")
    alias.symlink_to(tmp_path, target_is_directory=True)
    with r.RootLock(tmp_path):
        result = run_cli(alias, "--apply")
        assert result.returncode != 0 and "busy" in result.stderr
    with r.RootLock(alias, exclusive=True):
        code = (
            "import asyncio; from context_intelligence_server.blob_store import AsyncDiskBlobStore;"
            f"asyncio.run(AsyncDiskBlobStore({str(tmp_path)!r}).write('session', 'new', {{}}))"
        )
        result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=10)
        assert result.returncode != 0 and "busy" in result.stderr
    with r.RootLock(tmp_path, exclusive=True):
        pass


async def test_worker_lifespan_refusal_hold_shutdown_and_release(tmp_path, monkeypatch):
    import context_intelligence_server.main as main
    from types import SimpleNamespace
    root = tmp_path / "root"
    root.mkdir()
    app = SimpleNamespace(state=SimpleNamespace(
        settings=SimpleNamespace(blob_path=str(root), stateful_root=lambda: tmp_path / "state")
    ))
    entered = []

    @contextlib.asynccontextmanager
    async def fake_lifespan(app):
        entered.append(True)
        yield
        with pytest.raises(r.RetentionError):
            with r.RootLock(root, exclusive=True):
                pass

    monkeypatch.setattr(main, "_server_lifespan", fake_lifespan)
    with r.RootLock(root, exclusive=True):
        with pytest.raises(r.RetentionError):
            async with main.lifespan(app):
                pytest.fail("worker started during apply")
    assert not entered
    async with main.lifespan(app):
        assert entered
        result = run_cli(root, "--apply")
        assert result.returncode != 0 and "busy" in result.stderr
    with r.RootLock(root, exclusive=True):
        pass


async def test_cancelled_write_holds_lock_until_thread_exits(tmp_path, monkeypatch):
    import context_intelligence_server.blob_store as module
    store = AsyncDiskBlobStore(tmp_path)
    entered, release = threading.Event(), threading.Event()
    original = module.os.replace

    def pause(*args, **kwargs):
        entered.set()
        assert release.wait(5)
        return original(*args, **kwargs)

    monkeypatch.setattr(module.os, "replace", pause)
    task = asyncio.create_task(store.write("session", "key", {}))
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        with pytest.raises(r.RetentionError):
            with r.RootLock(tmp_path, exclusive=True):
                pass
    finally:
        release.set()
        # Poll a concrete lock condition rather than assuming the thread ended.
        for _ in range(100):
            try:
                with r.RootLock(tmp_path, exclusive=True):
                    break
            except r.RetentionError:
                await asyncio.sleep(0.01)
        else:
            pytest.fail("thread failed to release shared lock")


def test_cli_default_preview_and_no_asgi_or_drivers(tmp_path):
    old = payload(tmp_path)
    result = run_cli(tmp_path)
    assert result.returncode == 0, result.stderr
    assert "Eligible files: 1" in result.stdout
    assert old.exists() and not (tmp_path / r.LOCK).exists()
    code = f"""
import sys
from context_intelligence_server.cli import main
main(['blobs', 'prune', '--blob-root', {str(tmp_path)!r}])
assert not any(x in sys.modules for x in ['context_intelligence_server.main', 'neo4j', 'fastapi'])
"""
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("answer", ["decline\n", "\x04", "prune\n"])
def test_cli_real_interactive_confirmation(tmp_path, answer):
    old = payload(tmp_path)
    result = run_cli(tmp_path, "--apply", tty_input=answer)
    if answer == "prune\n":
        assert result.returncode == 0, result.stdout + result.stderr
        assert not old.exists() and "deleted: 1" in result.stdout
    else:
        assert result.returncode != 0
        assert old.exists() and not (tmp_path / r.METADATA).exists()


def test_cli_noninteractive_and_no_yes_bypass(tmp_path):
    old = payload(tmp_path)
    result = run_cli(tmp_path, "--apply")
    assert result.returncode != 0 and "interactive terminal" in result.stderr
    result = run_cli(tmp_path, "--apply", "--yes")
    assert result.returncode != 0 and "unrecognized arguments" in result.stderr
    assert old.exists() and not (tmp_path / r.METADATA).exists()


def test_cli_config_resolution_missing_explicit_config_and_root(tmp_path):
    payload(tmp_path)
    config = tmp_path / "server.yaml"
    config.write_text(f"blob_path: {tmp_path}\n")
    env = {**os.environ, "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE": str(config)}
    result = subprocess.run([str(CLI), "blobs", "prune"], env=env, capture_output=True, text=True, timeout=10)
    assert result.returncode == 0 and f"Root: {tmp_path}" in result.stdout
    env["AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_BLOB_PATH"] = str(tmp_path / "absent")
    result = subprocess.run([str(CLI), "blobs", "prune"], env=env, capture_output=True, text=True, timeout=10)
    assert result.returncode != 0
    env["AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE"] = str(tmp_path / "missing.yaml")
    result = run_cli(tmp_path, env=env)
    assert result.returncode != 0 and "config file does not exist" in result.stderr
    assert not (tmp_path / "absent").exists()


async def test_expired_read_dump_replay_restore_and_physical_listing(tmp_path):
    from context_intelligence_server.blob_processor import process_event_data
    store = AsyncDiskBlobStore(tmp_path)
    data = {"raw": {"value": 1}}
    await process_event_data(data, store, "session", "node")
    uri = data["raw"]["$blob_ref"]
    path = store.blob_path("session", "node__raw")
    os.utime(path, ns=(CUTOFF - 1, CUTOFF - 1))
    assert r.apply(tmp_path, CUTOFF, lambda _: True).deleted == 1
    for operation in [store.read(uri), store.dump(uri, tmp_path / "dump")]:
        with pytest.raises(r.BlobExpiredError) as exc:
            await operation
        assert exc.value.uri == uri
    data = {"raw": {"new": 2}}
    await process_event_data(data, store, "session", "node")
    assert data["raw"] == {"$blob_ref": uri} and not path.exists()
    assert await store.list("session") == []
    with pytest.raises(FileNotFoundError) as exc:
        await store.read("ci-blob://session/never")
    assert not isinstance(exc.value, r.BlobExpiredError)
    path.write_text('{"restored": true}')
    assert await store.read(uri) == {"restored": True}
    dumped = await store.dump(uri, tmp_path / "dump")
    assert Path(dumped).read_text() == path.read_text()
    before = path.stat()
    assert await store.write("session", "node__raw", {"overwrite": "no"}) == uri
    assert path.stat().st_mtime_ns == before.st_mtime_ns
    assert await store.read(uri) == {"restored": True}
    assert await store.list("session") == [uri]


async def test_authenticated_http_410_vs_404_and_auth_unchanged(tmp_path, auth_client, monkeypatch):
    import context_intelligence_server.main as main
    from types import SimpleNamespace
    monkeypatch.setattr(main, "_settings", SimpleNamespace(blob_path=str(tmp_path)))
    payload(tmp_path)
    r.apply(tmp_path, CUTOFF, lambda _: True)
    response = await auth_client.get("/blobs/session/old")
    assert response.status_code == 401
    headers = {"Authorization": "Bearer test-secret"}
    response = await auth_client.get("/blobs/session/old", headers=headers)
    assert response.status_code == 410
    assert response.json()["detail"] == {"code": "blob_expired", "uri": "ci-blob://session/old"}
    assert (await auth_client.get("/blobs/session/unknown", headers=headers)).status_code == 404


def test_unlink_partial_failure_is_honest_and_cli_nonzero(tmp_path, monkeypatch):
    old = payload(tmp_path)
    real_unlink = os.unlink

    def refuse(name, *args, **kwargs):
        if name == old.name:
            raise PermissionError("unlink refused")
        return real_unlink(name, *args, **kwargs)

    monkeypatch.setattr(os, "unlink", refuse)
    report = r.apply(tmp_path, CUTOFF, lambda _: True)
    assert report.deleted == 0 and report.errors == 1 and old.exists()
    from context_intelligence_server import cli
    monkeypatch.setattr(r, "apply", lambda *args: report)
    with pytest.raises(SystemExit) as exc:
        cli.main(["blobs", "prune", "--blob-root", str(tmp_path), "--apply"])
    assert exc.value.code == 1


def test_process_crash_retains_durable_record_then_cli_decline_and_resume(tmp_path):
    old = payload(tmp_path)
    code = f"""
import os
from context_intelligence_server import blob_retention as r
r._unlink = lambda *args, **kwargs: os._exit(73)
r.apply({str(tmp_path)!r}, {CUTOFF}, lambda _: True)
"""
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, timeout=10)
    assert result.returncode == 73 and old.exists()
    with r.Root(tmp_path) as root, r.Ledger(root) as ledger:
        assert ledger.contains("ci-blob://session/old")
    result = run_cli(tmp_path, "--apply", tty_input="no\n")
    assert result.returncode != 0 and old.exists()
    result = run_cli(tmp_path, "--apply", tty_input="prune\n")
    assert result.returncode == 0, result.stdout + result.stderr
    assert not old.exists() and "deleted: 1" in result.stdout


async def test_physical_read_precedes_corrupt_ledger(tmp_path):
    old = payload(tmp_path)
    metadata = tmp_path / r.METADATA
    metadata.mkdir(mode=0o700)
    (metadata / r.DATABASE).write_bytes(b"corrupt")
    store = AsyncDiskBlobStore(tmp_path)
    assert await store.read("ci-blob://session/old") == {"value": 1}
    assert Path(await store.dump("ci-blob://session/old", tmp_path / "dump")).read_bytes() == old.read_bytes()
    with pytest.raises(sqlite3.DatabaseError):
        await store.read("ci-blob://session/missing")


async def test_standalone_operations_refuse_symlink_ancestor(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    outside = tmp_path / "outside"
    old = payload(outside)
    (root / "session").symlink_to(outside / "session", target_is_directory=True)
    store = AsyncDiskBlobStore(root)
    for operation in [
        store.read("ci-blob://session/old"),
        store.write("session", "old", {"overwrite": True}),
        store.dump("ci-blob://session/old", root / "dump"),
        store.list("session"),
        store.size("ci-blob://session/old"),
        store.delete_session("session"),
    ]:
        with pytest.raises(OSError):
            await operation
    assert old.read_text() == '{"value": 1}'


def test_changed_apply_result_nonzero(tmp_path, monkeypatch):
    from context_intelligence_server import cli
    report = r.Report(str(tmp_path), CUTOFF, changed=1)
    monkeypatch.setattr(r, "apply", lambda *args: report)
    with pytest.raises(SystemExit) as exc:
        cli.main(["blobs", "prune", "--blob-root", str(tmp_path), "--apply"])
    assert exc.value.code == 1


@pytest.mark.parametrize("reserved", [r.METADATA, r.LOCK])
async def test_reserved_sessions_cannot_damage_lock_or_ledger(tmp_path, reserved):
    old = payload(tmp_path)
    r.apply(tmp_path, CUTOFF, lambda _: True)
    database = tmp_path / r.METADATA / r.DATABASE
    before = database.read_bytes()
    store = AsyncDiskBlobStore(tmp_path)
    for operation in [
        store.write(reserved, "key", {}),
        store.read(f"ci-blob://{reserved}/key"),
        store.dump(f"ci-blob://{reserved}/key", tmp_path / "dump"),
        store.list(reserved),
        store.size(f"ci-blob://{reserved}/key"),
        store.delete_session(reserved),
    ]:
        with pytest.raises(ValueError):
            await operation
    assert database.read_bytes() == before
    assert await store.write("session", "old", {}) == "ci-blob://session/old"
    assert not old.exists()


@pytest.mark.parametrize("alias", ["same_directory", "directory_symlink", "hardlink", "file_symlink"])
async def test_dump_never_truncates_source_alias(tmp_path, alias):
    store = AsyncDiskBlobStore(tmp_path)
    uri = await store.write("session", "key", {"payload": "preserve"})
    path = store.blob_path("session", "key")
    before = path.read_bytes()
    dest_dir = tmp_path / "dump"
    if alias == "same_directory":
        dest_dir = path.parent
    elif alias == "directory_symlink":
        dest_dir.symlink_to(path.parent, target_is_directory=True)
    else:
        dest_dir.mkdir()
        if alias == "hardlink":
            os.link(path, dest_dir / path.name)
        else:
            (dest_dir / path.name).symlink_to(path)
    with pytest.raises(OSError):
        await store.dump(uri, dest_dir)
    assert path.read_bytes() == before


def test_hot_sqlite_journal_recovered_only_after_confirmation(tmp_path):
    for i in range(512):
        payload(tmp_path, str(i) + "x" * 180)
    r.apply(tmp_path, 0, lambda _: True)  # durably initialize only the schema
    code = f"""
import os
from context_intelligence_server import blob_retention as r
original = r.Ledger.__enter__
def small_cache(self):
    result = original(self)
    if self.db is not None:
        self.db.execute("PRAGMA cache_size=1")
    return result
r.Ledger.__enter__ = small_cache
r.Ledger.commit = lambda self: os._exit(73)
r.apply({str(tmp_path)!r}, {CUTOFF}, lambda _: True)
"""
    child = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=10)
    assert child.returncode == 73, child.stderr
    journal = tmp_path / r.METADATA / (r.DATABASE + "-journal")
    assert journal.stat().st_size > 0
    before = {p.name: p.read_bytes() for p in journal.parent.iterdir()}
    with pytest.raises(sqlite3.OperationalError) as exc:
        r.preview(tmp_path, CUTOFF)
    assert exc.value.sqlite_errorcode == sqlite3.SQLITE_READONLY_ROLLBACK
    seen = []
    with pytest.raises(r.RetentionError, match="not confirmed"):
        r.apply(tmp_path, CUTOFF, lambda report: seen.append(report.recovery_required))
    assert seen == [True]
    assert {p.name: p.read_bytes() for p in journal.parent.iterdir()} == before
    result = r.apply(tmp_path, CUTOFF, lambda _: True)
    assert result.deleted == 512 and result.errors == 0
    assert not journal.exists()


# --- Tilde-prefixed roots (Copilot r4329246656) ---------------------------------


def _tilde_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path]:
    """Isolated HOME and a *different* cwd, so a stray literal ``~`` is detectable."""
    home, cwd = tmp_path / "home", tmp_path / "cwd"
    home.mkdir()
    cwd.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.chdir(cwd)
    return home, cwd


def test_rootlock_expands_tilde_before_creating_fresh_root(tmp_path, monkeypatch):
    home, cwd = _tilde_env(tmp_path, monkeypatch)
    with r.RootLock("~/fresh-blobs", create_root=True) as lock:
        assert lock.root.path == (home / "fresh-blobs").resolve()
    assert (home / "fresh-blobs" / r.LOCK).is_file()
    assert list(cwd.iterdir()) == [], "a literal './~' directory must never be created"


def test_rootlock_existing_expanded_root_control(tmp_path, monkeypatch):
    """Control: an already-existing expanded root opens (true pre- and post-fix)."""
    home, _cwd = _tilde_env(tmp_path, monkeypatch)
    (home / "existing").mkdir()
    with r.RootLock("~/existing", create_root=True) as lock:
        assert lock.root.path == (home / "existing").resolve()


def test_rootlock_existing_expanded_root_leaves_no_literal_tilde_dir(
    tmp_path, monkeypatch
):
    home, cwd = _tilde_env(tmp_path, monkeypatch)
    (home / "existing").mkdir()
    with r.RootLock("~/existing", create_root=True):
        pass
    assert list(cwd.iterdir()) == [], "a literal './~' directory must never be created"


async def test_worker_lifespan_fresh_tilde_blob_root(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from typing import cast

    from fastapi import FastAPI

    from context_intelligence_server import main

    home, cwd = _tilde_env(tmp_path, monkeypatch)
    app = SimpleNamespace(
        state=SimpleNamespace(
            settings=SimpleNamespace(
                blob_path="~/fresh-blobs", stateful_root=lambda: tmp_path / "state"
            )
        )
    )
    entered = []

    @contextlib.asynccontextmanager
    async def fake_lifespan(app):
        entered.append(True)
        yield

    monkeypatch.setattr(main, "_server_lifespan", fake_lifespan)
    async with main.lifespan(cast(FastAPI, app)):
        assert entered
        assert app.state.retention_service.path == (home / "fresh-blobs").resolve()
    assert (home / "fresh-blobs" / r.LOCK).is_file()
    assert list(cwd.iterdir()) == [], (
        "startup must not create a literal './~' directory"
    )


# --- Legacy '%', '?', '#' identifiers (PR126 lane A) -----------------------
# URIs are parsed by literal scheme-stripping; nothing is ever URL-decoded, so
# these characters are ordinary component bytes, never traversal.

_LITERAL_NAMES = ["a%b", "a?b", "a#b", "q?x=1#f", "a%2fb", "%2e%2e", "%", "?", "#"]


@pytest.mark.parametrize("name", _LITERAL_NAMES)
def test_component_accepts_percent_query_fragment_literally(name):
    assert r.component(name) == name


@pytest.mark.parametrize(
    "uri, expected",
    [
        ("ci-blob://s%2fx/k%2e%2e", ("s%2fx", "k%2e%2e")),
        ("ci-blob://a?b/c#d", ("a?b", "c#d")),
        ("ci-blob://s%1/k%00", ("s%1", "k%00")),
    ],
)
def test_parse_uri_is_literal_with_no_url_decoding(uri, expected):
    assert r.parse_uri(uri) == expected


@pytest.mark.parametrize(
    "uri",
    [
        "ci-blob://a/b/c",
        "ci-blob://a",
        "ci-blob://a/",
        "ci-blob:///k",
        "ci-blob://s/..",
        "ci-blob://s/a\x00b",
        f"ci-blob://s/{r.METADATA}",
        f"ci-blob://{r.LOCK}/k",
        "http://s/k",
    ],
)
def test_parse_uri_still_rejects_structurally_unsafe_uris(uri):
    with pytest.raises(ValueError):
        r.parse_uri(uri)


def _legacy_payloads(root: Path) -> list[str]:
    layout = {"s%1?2#3": ["k%?#a", "plain"], "legacy": ["x%2fy"]}
    uris = []
    for session, keys in layout.items():
        for key in keys:
            path = root / session / "blobs" / f"{key}.json"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('{"value": 1}')
            os.utime(path, ns=(CUTOFF - 1, CUTOFF - 1))
            uris.append(f"ci-blob://{session}/{key}")
    return sorted(uris)


async def test_offline_retention_enumerates_and_prunes_legacy_names(tmp_path):
    uris = _legacy_payloads(tmp_path)
    store = AsyncDiskBlobStore(tmp_path)

    report = r.preview(tmp_path, CUTOFF)
    assert (report.files, report.skipped, report.errors) == (3, 0, 0)

    applied = r.apply(tmp_path, CUTOFF, lambda _: True)
    assert (applied.deleted, applied.errors) == (3, 0)
    assert list(tmp_path.rglob("*.json")) == []
    with r.Root(tmp_path) as root, r.Ledger(root) as ledger:
        assert all(ledger.contains(uri) for uri in uris)
    for uri in uris:
        with pytest.raises(r.BlobExpiredError) as exc:
            await store.read(uri)
        assert exc.value.uri == uri


async def test_online_retention_scan_and_batch_prune_legacy_names(tmp_path):
    import time

    uris = _legacy_payloads(tmp_path)
    store = AsyncDiskBlobStore(tmp_path)
    report = r.Report(str(tmp_path), CUTOFF)
    cursor, items = r.ScanCursor(), []
    started = time.time_ns()
    while not cursor.done:
        items.extend(
            r.scan_window(
                tmp_path, cursor, report, started, max_entries=10, budget_ns=10**9
            )
        )

    assert sorted(item.uri for item in items) == uris
    assert (report.skipped, report.errors) == (0, 0)

    r.apply_batch(tmp_path, items, report)
    assert (report.deleted, report.errors) == (3, 0)
    for uri in uris:
        with pytest.raises(r.BlobExpiredError):
            await store.read(uri)


# --- Mapped (.blob) and pre-upgrade backslash (.json) names (PR126) ---------

_MARK = "~ci-key-v1"
_SLASH = "5998a4c7b7280b4eb36c9f88878ed97c34b2cbeb7f138372a02365515c97bde9"
_BACKSLASH = "f3af24d320ab49ad3ecaddd0d48f69cb7c393eb657ffbf1b80416af26a1346b4"
_MAPPED_URI = f"ci-blob://s/{_MARK}/{_SLASH}"
_LEGACY_URI = "ci-blob://s/a\\b"


def _put(root: Path, session: str, filename: str, mtime: int = CUTOFF - 1) -> Path:
    path = root / session / "blobs" / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('{"value": 1}', encoding="utf-8")
    os.utime(path, ns=(mtime, mtime))
    return path


def _mixed_layout(root: Path) -> list[str]:
    _put(root, "s", "a\\b.json")
    _put(root, "s", f"{_MARK}~{_SLASH}.blob")
    _put(root, "s", "plain.json")
    for junk in ("x.tmp", "foo.blob", ".json"):
        _put(root, "s", junk)
    return sorted([_LEGACY_URI, _MAPPED_URI, "ci-blob://s/plain"])


def test_parse_uri_accepts_a_pre_upgrade_backslash_name_on_posix():
    assert r.parse_uri("ci-blob://s/a\\b") == ("s", "a\\b")


@pytest.mark.parametrize("uri", [_LEGACY_URI, _MAPPED_URI, "ci-blob://s/plain"])
def test_ledger_contains_accepts_every_address_kind(tmp_path, uri):
    _put(tmp_path, "s", "plain.json")
    with (
        r.RootLock(tmp_path, exclusive=True) as lock,
        r.Ledger(lock.root, writable=True) as ledger,
    ):
        ledger.journal([r.Candidate(uri, (1, 2, 3, 4, 5), 0)])
    with r.Root(tmp_path) as root, r.Ledger(root) as ledger:
        assert ledger.contains(uri) is True
        assert ledger.contains("ci-blob://s/absent") is False
        assert ledger.contains(f"ci-blob://s/{_MARK}/{'1' * 64}") is False


@pytest.mark.parametrize(
    "uri",
    [
        f"ci-blob://s/{_MARK}/{'A' * 64}",
        f"ci-blob://s/{_MARK}/short",
        "ci-blob://s/a/b",
    ],
)
def test_ledger_contains_still_rejects_malformed_uris(tmp_path, uri):
    with (
        r.Root(tmp_path) as root,
        r.Ledger(root) as ledger,
        pytest.raises(ValueError),
    ):
        ledger.contains(uri)


async def test_offline_retention_prunes_backslash_json_and_mapped_blob_files(tmp_path):
    uris = _mixed_layout(tmp_path)
    store = AsyncDiskBlobStore(tmp_path)

    report = r.preview(tmp_path, CUTOFF)
    assert (report.files, report.skipped, report.errors) == (3, 3, 0)
    assert sorted(report.samples) == uris

    applied = r.apply(tmp_path, CUTOFF, lambda _: True)

    assert (applied.deleted, applied.errors) == (3, 0)
    assert sorted(p.name for p in (tmp_path / "s" / "blobs").iterdir()) == [
        ".json",
        "foo.blob",
        "x.tmp",
    ]
    with r.Root(tmp_path) as root, r.Ledger(root) as ledger:
        assert all(ledger.contains(uri) for uri in uris)
    for uri in uris:
        with pytest.raises(r.BlobExpiredError) as exc:
            await store.read(uri)
        assert exc.value.uri == uri


async def test_online_retention_scans_and_prunes_backslash_json_and_mapped_blob_files(
    tmp_path,
):
    import time

    uris = _mixed_layout(tmp_path)
    store = AsyncDiskBlobStore(tmp_path)
    report = r.Report(str(tmp_path), CUTOFF)
    cursor, items = r.ScanCursor(), []
    started = time.time_ns()
    while not cursor.done:
        items.extend(
            r.scan_window(
                tmp_path, cursor, report, started, max_entries=10, budget_ns=10**9
            )
        )

    assert sorted(item.uri for item in items) == uris
    assert (report.skipped, report.errors) == (3, 0)

    r.apply_batch(tmp_path, items, report)

    assert (report.deleted, report.errors) == (3, 0)
    assert sorted(p.name for p in (tmp_path / "s" / "blobs").iterdir()) == [
        ".json",
        "foo.blob",
        "x.tmp",
    ]
    for uri in uris:
        with pytest.raises(r.BlobExpiredError) as exc:
            await store.read(uri)
        assert exc.value.uri == uri


def test_a_hardlinked_mapped_blob_is_skipped_not_pruned(tmp_path):
    mapped = _put(tmp_path, "s", f"{_MARK}~{_SLASH}.blob")
    os.link(mapped, tmp_path / "alias")

    assert r.preview(tmp_path, CUTOFF).files == 0
    assert r.apply(tmp_path, CUTOFF, lambda _: True).deleted == 0
    assert mapped.exists()


@pytest.mark.parametrize("byte_count", range(243, 251))
@pytest.mark.parametrize("multibyte", [False, True], ids=["ascii", "utf8"])
def test_on_disk_names_are_never_folded_when_enumerated(tmp_path, byte_count, multibyte):
    long_name = (
        "é" * (byte_count // 2) + "k" * (byte_count % 2)
        if multibyte else "k" * byte_count
    )
    _put(tmp_path, "s", f"{long_name}.json")

    applied = r.apply(tmp_path, CUTOFF, lambda report: report.files == 1)

    assert (applied.deleted, applied.errors) == (1, 0)
    with r.Root(tmp_path) as root, r.Ledger(root) as ledger:
        assert ledger.contains(f"ci-blob://s/{long_name}") is True


def test_recovery_unlinks_a_pending_mapped_blob_by_its_exact_filename(tmp_path):
    mapped = _put(tmp_path, "s", f"{_MARK}~{_SLASH}.blob")
    item = r.Candidate(_MAPPED_URI, r.fingerprint(mapped.stat()), 0)
    with (
        r.RootLock(tmp_path, exclusive=True) as lock,
        r.Ledger(lock.root, writable=True) as ledger,
    ):
        ledger.journal([item])  # journalled (pending) but never unlinked: a crash

    assert mapped.exists()
    applied = r.apply(tmp_path, CUTOFF, lambda _: True)

    assert (applied.deleted, applied.errors) == (1, 0)
    assert not mapped.exists()


def test_a_restored_mapped_blob_keeps_its_tombstone_and_is_not_pruned_again(tmp_path):
    mapped = _put(tmp_path, "s", f"{_MARK}~{_SLASH}.blob")
    assert r.apply(tmp_path, CUTOFF, lambda _: True).deleted == 1
    restored = _put(tmp_path, "s", f"{_MARK}~{_SLASH}.blob")

    result = r.apply(tmp_path, CUTOFF, lambda _: True)

    assert result.deleted == 0 and result.skipped >= 1
    assert restored.exists() and mapped == restored


async def test_replay_after_a_legacy_prune_returns_the_old_uri_and_creates_nothing(
    tmp_path,
):
    _put(tmp_path, "s", "a\\b.json")
    store = AsyncDiskBlobStore(tmp_path)
    assert r.apply(tmp_path, CUTOFF, lambda _: True).deleted == 1

    assert await store.write("s", "a\\b", {"replayed": True}) == _LEGACY_URI

    assert list((tmp_path / "s" / "blobs").iterdir()) == []
    assert await store.list("s") == []
    with pytest.raises(r.BlobExpiredError) as exc:
        await store.read(_LEGACY_URI)
    assert exc.value.uri == _LEGACY_URI


async def test_replay_after_a_mapped_prune_does_not_rehydrate(tmp_path):
    store = AsyncDiskBlobStore(tmp_path)
    uri = await store.write("s", "a/b", {"v": 1})
    assert uri == _MAPPED_URI
    os.utime(store.blob_path("s", "a/b"), ns=(CUTOFF - 1, CUTOFF - 1))
    assert r.apply(tmp_path, CUTOFF, lambda _: True).deleted == 1

    assert await store.write("s", "a/b", {"v": 2}) == uri

    assert list((tmp_path / "s" / "blobs").iterdir()) == []
    with pytest.raises(r.BlobExpiredError) as exc:
        await store.read(uri)
    assert exc.value.uri == uri


@pytest.mark.parametrize(
    "kind",
    ["legacy", "mapped", *[
        f"{encoding}-{count}"
        for encoding in ("ascii", "utf8") for count in range(243, 251)
    ]],
)
async def test_each_phase_through_a_fresh_store_sees_the_same_disk_truth(
    tmp_path, kind
):
    """Prune -> expired read -> replay -> physical restore, reopening the store every phase."""
    if kind == "legacy":
        path = _put(tmp_path, "s", "a\\b.json")
        uri, logical = _LEGACY_URI, "a\\b"
    elif kind == "mapped":
        path = _put(tmp_path, "s", f"{_MARK}~{_SLASH}.blob")
        uri, logical = _MAPPED_URI, "a/b"
    else:
        encoding, count = kind.split("-")
        byte_count = int(count)
        logical = (
            "é" * (byte_count // 2) + "k" * (byte_count % 2)
            if encoding == "utf8" else "k" * byte_count
        )
        assert len(logical.encode("utf-8")) == byte_count
        path = _put(tmp_path, "s", f"{logical}.json")
        uri = f"ci-blob://s/{logical}"

    assert await AsyncDiskBlobStore(tmp_path).read(uri) == {"value": 1}
    assert await AsyncDiskBlobStore(tmp_path).list("s") == [uri]
    assert await AsyncDiskBlobStore(tmp_path).size(uri) == path.stat().st_size
    dumped = Path(await AsyncDiskBlobStore(tmp_path).dump(uri, tmp_path / "dump"))
    assert dumped.name == path.name
    assert json.loads(dumped.read_text(encoding="utf-8")) == {"value": 1}
    assert r.apply(tmp_path, CUTOFF, lambda _: True).deleted == 1
    assert not path.exists()
    with r.Root(tmp_path) as root, r.Ledger(root) as ledger:
        assert ledger.contains(uri)

    with pytest.raises(r.BlobExpiredError) as expired:
        await AsyncDiskBlobStore(tmp_path).read(uri)
    assert expired.value.uri == uri
    with pytest.raises(r.BlobExpiredError) as expired:
        await AsyncDiskBlobStore(tmp_path).dump(uri, tmp_path / "expired-dump")
    assert expired.value.uri == uri
    assert await AsyncDiskBlobStore(tmp_path).size(uri) == 0
    assert not (tmp_path / "expired-dump").exists()

    # Replay (a brand-new writer instance) keeps the recorded URI and rehydrates nothing,
    # and never creates the other address kind either.
    assert (
        await AsyncDiskBlobStore(tmp_path).write("s", logical, {"replayed": True})
        == uri
    )
    assert list((tmp_path / "s" / "blobs").iterdir()) == []

    # A physically restored file wins over the tombstone for every later reader.
    path.write_text('{"restored": true}', encoding="utf-8")
    assert await AsyncDiskBlobStore(tmp_path).read(uri) == {"restored": True}
    assert await AsyncDiskBlobStore(tmp_path).list("s") == [uri]
    assert await AsyncDiskBlobStore(tmp_path).size(uri) == path.stat().st_size
    restored = Path(await AsyncDiskBlobStore(tmp_path).dump(uri, tmp_path / "restored"))
    assert json.loads(restored.read_text(encoding="utf-8")) == {"restored": True}


@pytest.mark.parametrize(
    "logical", ["a/" + "k" * 300, "a\\" + "k" * 300, "a\0" + "k" * 300, "k" * 300 + "\n"],
    ids=["slash", "backslash", "nul", "newline"],
)
async def test_real_prune_of_historical_fold_still_blocks_mapped_replay(tmp_path, logical):
    from context_intelligence_server.utils import fold_name

    folded = fold_name(logical, 242)
    path = _put(tmp_path, "s", f"{folded}.json")
    historical_uri = f"ci-blob://s/{folded}"
    assert r.apply(tmp_path, CUTOFF, lambda _: True).deleted == 1
    assert not path.exists()
    with r.Root(tmp_path) as root, r.Ledger(root) as ledger:
        assert ledger.contains(historical_uri)
    store = AsyncDiskBlobStore(tmp_path)
    assert await store.write("s", logical, {"replay": True}) == historical_uri
    assert list(path.parent.iterdir()) == []
    with pytest.raises(r.BlobExpiredError) as exc:
        await store.read(historical_uri)
    assert exc.value.uri == historical_uri
