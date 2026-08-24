"""The POST /admin/blobs/reclaim orchestration: dry-run vs apply, the
blast-radius cap, the destructive-apply single-flight, and fenced deletion.

The *selection* logic (which blobs are orphans) is covered against a real
graph elsewhere; these tests pin the endpoint's own contract by stubbing the
one selection call, so they run without neo4j.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from context_intelligence_server.blob_store import BlobReference
from context_intelligence_server.routers import admin
from context_intelligence_server.routers.admin import BlobReclaimBody, reclaim_blobs
from fastapi import HTTPException

pytestmark = pytest.mark.integration


def _ref(uri: str, size: int = 10) -> BlobReference:
    session_id, _, key = uri.removeprefix("ci-blob://").partition("/")
    return BlobReference(
        uri=uri, session_id=session_id, key=key, size=size, last_modified=1.0
    )


def _request() -> Any:
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))


def _stub_selection(monkeypatch, candidates: list[BlobReference]) -> None:
    async def _fake_select(_request: Any, *, min_age_minutes: int) -> dict[str, Any]:
        return {
            "scanned_disk_blobs": len(candidates),
            "referenced_uris": 0,
            "orphans_found": len(candidates),
            "reclaimable_bytes": sum(c.size for c in candidates),
            "skipped_recent": 0,
            "skipped_pending_session": 0,
            "candidates": list(candidates),
        }

    monkeypatch.setattr(admin, "_select_orphans", _fake_select)


class _FakeStore:
    """A blob store whose delete() honours a per-uri fence verdict."""

    def __init__(self, deletable: set[str]) -> None:
        self._deletable = deletable
        self.deleted: list[str] = []

    async def delete(self, uri: str, if_unmodified: BlobReference | None = None) -> bool:
        if uri in self._deletable:
            self.deleted.append(uri)
            return True
        return False  # absent or changed since scan -- fenced out


@pytest.fixture(autouse=True)
def _reset_single_flight():
    admin._reclaim_apply_inflight = False
    yield
    admin._reclaim_apply_inflight = False


async def test_apply_without_max_delete_is_422(monkeypatch) -> None:
    """F2: a destructive apply must name its blast radius."""
    with pytest.raises(HTTPException) as exc:
        await reclaim_blobs(
            BlobReclaimBody(dry_run=False, max_delete=None), _request()
        )
    assert exc.value.status_code == 422


async def test_dry_run_reports_without_deleting(monkeypatch) -> None:
    """F1: dry-run (the default) previews and deletes nothing."""
    cands = [_ref("ci-blob://s1/a"), _ref("ci-blob://s1/b")]
    _stub_selection(monkeypatch, cands)
    store = _FakeStore({"ci-blob://s1/a", "ci-blob://s1/b"})
    monkeypatch.setattr(admin, "create_blob_store", lambda _s: store)

    resp = await reclaim_blobs(BlobReclaimBody(dry_run=True), _request())

    assert resp["dry_run"] is True
    assert resp["rescanned"] is False
    assert resp["orphans_found"] == 2
    assert resp["deleted"] == 0
    assert store.deleted == []  # nothing touched


async def test_apply_deletes_through_fenced_protocol(monkeypatch) -> None:
    """Happy-path apply: fresh scan (rescanned), fenced delete, audit per delete."""
    cands = [_ref("ci-blob://s1/a"), _ref("ci-blob://s1/b")]
    _stub_selection(monkeypatch, cands)
    store = _FakeStore({"ci-blob://s1/a", "ci-blob://s1/b"})
    monkeypatch.setattr(admin, "create_blob_store", lambda _s: store)
    audited: list[str] = []
    monkeypatch.setattr(
        admin, "_audit_blob_reclaim_delete", lambda _r, *, uri: audited.append(uri)
    )

    resp = await reclaim_blobs(
        BlobReclaimBody(dry_run=False, max_delete=10), _request()
    )

    assert resp["rescanned"] is True
    assert resp["deleted"] == 2
    assert sorted(store.deleted) == ["ci-blob://s1/a", "ci-blob://s1/b"]
    assert sorted(audited) == ["ci-blob://s1/a", "ci-blob://s1/b"]
    assert admin._reclaim_apply_inflight is False  # released


async def test_fenced_delete_refusal_is_not_counted(monkeypatch) -> None:
    """R3/R4: a blob changed/re-referenced since the scan is fenced out --
    delete() returns False, it stays on disk and is NOT counted as deleted."""
    cands = [_ref("ci-blob://s1/a"), _ref("ci-blob://s1/b")]
    _stub_selection(monkeypatch, cands)
    # Only 'a' is still deletable; 'b' was re-minted since the scan.
    store = _FakeStore({"ci-blob://s1/a"})
    monkeypatch.setattr(admin, "create_blob_store", lambda _s: store)
    monkeypatch.setattr(
        admin, "_audit_blob_reclaim_delete", lambda _r, *, uri: None
    )

    resp = await reclaim_blobs(
        BlobReclaimBody(dry_run=False, max_delete=10), _request()
    )

    assert resp["deleted"] == 1
    assert store.deleted == ["ci-blob://s1/a"]  # 'b' left intact


async def test_max_delete_caps_blast_radius(monkeypatch) -> None:
    """F4: orphans_found reflects the FULL set; only max_delete are removed."""
    cands = [_ref(f"ci-blob://s1/{k}") for k in "abcde"]
    _stub_selection(monkeypatch, cands)
    store = _FakeStore({c.uri for c in cands})
    monkeypatch.setattr(admin, "create_blob_store", lambda _s: store)
    monkeypatch.setattr(
        admin, "_audit_blob_reclaim_delete", lambda _r, *, uri: None
    )

    resp = await reclaim_blobs(
        BlobReclaimBody(dry_run=False, max_delete=2), _request()
    )

    assert resp["orphans_found"] == 5  # full candidate set
    assert resp["deleted"] == 2  # capped
    assert len(store.deleted) == 2


async def test_concurrent_apply_is_single_flighted(monkeypatch) -> None:
    """E1: a second apply while one is in flight is refused (409) before it
    even scans -- two applies would each honour max_delete and jointly exceed
    the operator's intended blast radius."""
    admin._reclaim_apply_inflight = True  # simulate an apply already running
    scanned = False

    async def _should_not_run(_request: Any, *, min_age_minutes: int) -> dict[str, Any]:
        nonlocal scanned
        scanned = True
        return {"candidates": []}

    monkeypatch.setattr(admin, "_select_orphans", _should_not_run)

    with pytest.raises(HTTPException) as exc:
        await reclaim_blobs(
            BlobReclaimBody(dry_run=False, max_delete=1), _request()
        )

    assert exc.value.status_code == 409
    assert scanned is False  # fail-fast: rejected before the authoritative scan


async def test_dry_run_is_never_single_flighted(monkeypatch) -> None:
    """A preview must never be blocked by an in-flight apply."""
    admin._reclaim_apply_inflight = True
    _stub_selection(monkeypatch, [_ref("ci-blob://s1/a")])

    resp = await reclaim_blobs(BlobReclaimBody(dry_run=True), _request())

    assert resp["dry_run"] is True
    assert resp["orphans_found"] == 1
