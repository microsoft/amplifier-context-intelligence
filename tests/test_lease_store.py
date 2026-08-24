"""Filesystem lease-store: the writer-lease persistence backend.

The writer-lease detector (``writer_lease.py``) reaches the lease only through
this store, so these tests pin the persistence contract independently of the
detector's policy.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from context_intelligence_server.lease_store import LeaseRecord, create_lease_store
from context_intelligence_server.lease_store.filesystem import (
    LEASE_FILENAME,
    FileSystemLeaseStore,
)

pytestmark = pytest.mark.integration


def _store(directory: Path) -> FileSystemLeaseStore:
    store = create_lease_store(lambda: directory)
    assert isinstance(store, FileSystemLeaseStore)
    return store


def _record(owner: str = "me", heartbeat: float = 100.0) -> LeaseRecord:
    return LeaseRecord(
        owner=owner,
        host="h",
        pid=7,
        started_at=1.0,
        heartbeat=heartbeat,
        revision="rev",
        server_version="6.7.3",
        lease_version=1,
    )


def test_read_missing_is_none(tmp_path: Path) -> None:
    """A missing lease reads as None (a free directory), never an error."""
    assert _store(tmp_path).read() is None


def test_write_then_read_roundtrips(tmp_path: Path) -> None:
    store = _store(tmp_path)
    store.write(_record(owner="alice", heartbeat=42.0))
    got = store.read()
    assert got is not None
    assert got.owner == "alice"
    assert got.heartbeat == 42.0
    assert got.unreadable is False


def test_write_is_atomic_no_tmp_left(tmp_path: Path) -> None:
    store = _store(tmp_path)
    store.write(_record())
    assert (tmp_path / LEASE_FILENAME).exists()
    assert list(tmp_path.glob("*.tmp")) == []


def test_torn_lease_reads_unreadable(tmp_path: Path) -> None:
    """A hand-mangled lease is a synthetic unreadable record (fresh-foreign
    strength), not None and not a crash."""
    (tmp_path / LEASE_FILENAME).write_text("{not json", encoding="utf-8")
    got = _store(tmp_path).read()
    assert got is not None
    assert got.unreadable is True


def test_delete_if_owned_only_deletes_own(tmp_path: Path) -> None:
    store = _store(tmp_path)
    store.write(_record(owner="mine"))

    # A foreign lease is never deleted -- deleting it would hand the directory
    # to a third writer.
    store.delete_if_owned("someone_else")
    assert store.read() is not None

    store.delete_if_owned("mine")
    assert store.read() is None


def test_delete_if_owned_absent_is_noop(tmp_path: Path) -> None:
    """Best-effort: deleting an already-absent lease is not an error."""
    _store(tmp_path).delete_if_owned("mine")  # no raise


def test_dir_source_resolved_lazily(tmp_path: Path) -> None:
    """The store constructs nothing and reads no path at build time -- the
    directory is resolved per operation, so a store built before its directory
    exists still works once it does."""
    target = tmp_path / "queues"
    store = create_lease_store(lambda: target)
    target.mkdir()  # created AFTER the store was built
    store.write(_record(owner="late"))
    got = store.read()
    assert got is not None and got.owner == "late"
