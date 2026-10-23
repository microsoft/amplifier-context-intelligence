"""NAME_MAX integration with retention, replay and the recovery spool."""

from __future__ import annotations

import json
import hashlib
import os
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from context_intelligence_server import blob_retention as retention
from context_intelligence_server.blob_store import AsyncDiskBlobStore
from context_intelligence_server.queue_manager import (
    MAX_SESSION_ID_BYTES, QueueManager, fold_worker_key,
)
from context_intelligence_server.retention_service import RetentionService


@pytest.mark.parametrize(
    "session,key",
    [
        ("s" * 255, "k" * 242),
        ("s" * 256, "k" * 243),
        ("界" * 85, "é" * 121),
        ("界" * 86, "é" * 122),
        ("nested-" * 70, "event-" * 90),
    ],
)
async def test_disk_boundaries_and_canonical_lifecycle(tmp_path, monkeypatch, session, key):
    store = AsyncDiskBlobStore(tmp_path / "blobs")
    real_open = os.open
    temporary_names = []

    def track_open(name, flags, *args, **kwargs):
        if str(name).endswith(".tmp"):
            assert flags & os.O_EXCL and flags & os.O_NOFOLLOW
            assert len(os.fsencode(name)) <= 255
            temporary_names.append(name)
        return real_open(name, flags, *args, **kwargs)

    monkeypatch.setattr(os, "open", track_open)
    uri = await store.write(session, key, {"payload": "preserved"})
    folded_session, folded_key = store.parse_uri(uri)
    path = store.blob_path(session, key)
    assert path == store._root / folded_session / "blobs" / f"{folded_key}.json"
    assert path.is_file() and temporary_names
    assert len(os.fsencode(temporary_names[0])) == len(folded_key.encode()) + 13
    assert f"ci-blob://{folded_session}/{folded_key}" == uri
    assert await store.read(uri) == {"payload": "preserved"}
    assert await store.list(session) == await store.list(folded_session) == [uri]
    assert await store.size(uri) == path.stat().st_size
    dumped = await store.dump(uri, tmp_path / "dump")
    assert Path(dumped).read_bytes() == path.read_bytes()
    assert await store.delete_session(session) == 1
    assert await store.delete_session(folded_session) == 0


@pytest.mark.parametrize("online", [False, True])
async def test_folded_expiry_replay_and_http(tmp_path, monkeypatch, auth_client, online):
    import context_intelligence_server.main as main

    root = tmp_path / "storage"
    store = AsyncDiskBlobStore(root)
    session, key = "nested-" * 70, "raw-" * 90
    uri = await store.write(session, key, {"old": True})
    physical_session, physical_key = store.parse_uri(uri)
    path = store.blob_path(session, key)
    old = time.time_ns() - 90 * 86400 * 10**9
    os.utime(path, ns=(old, old))
    monkeypatch.setattr(main, "_settings", SimpleNamespace(blob_path=str(root)))
    headers = {"Authorization": "Bearer test-secret"}
    url = f"/blobs/{physical_session}/{physical_key}"
    assert (await auth_client.get(url, headers=headers)).status_code == 200
    if online:
        service = await RetentionService.open(root)
        try:
            job = await service.preview("30d")
            await service.task
            assert service.status()["state"] == "ready"
            assert service.status()["report"]["samples"] == [uri]
            assert path.exists()
            await service.start(job["id"], "prune")
            await service.task
            result = service.status()
            assert result["state"] == "completed" and result["report"]["deleted"] == 1
        finally:
            await service.close()
    else:
        cutoff = time.time_ns() - 30 * 86400 * 10**9
        assert retention.preview(root, cutoff).samples == [uri]
        assert path.exists()
        result = retention.apply(root, cutoff, lambda plan: plan.samples == [uri])
        assert result.deleted == 1 and result.errors == 0

    with pytest.raises(retention.BlobExpiredError):
        await store.read(uri)
    # Replay uses the ORIGINAL input names, not the physical names.
    assert await store.write(session, key, {"replayed": True}) == uri
    assert not path.exists() and await store.list(session) == []
    raw_uri = f"ci-blob://{session}/{key}"
    with pytest.raises(retention.BlobExpiredError) as expired:
        await store.read(raw_uri)
    assert expired.value.uri == uri
    with pytest.raises(retention.BlobExpiredError):
        await store.dump(raw_uri, tmp_path / "dump")
    assert await store.size(uri) == 0
    response = await auth_client.get(url, headers=headers)
    assert response.status_code == 410
    assert response.json()["detail"] == {"code": "blob_expired", "uri": uri}
    response = await auth_client.get(f"/blobs/{session}/{key}", headers=headers)
    assert response.status_code == 410
    assert response.json()["detail"] == {"code": "blob_expired", "uri": uri}
    assert (await auth_client.get(f"/blobs/{physical_session}/absent", headers=headers)).status_code == 404
    assert (await auth_client.get(url)).status_code == 401
    # Physical-first read remains stronger than a tombstone.
    path.write_text('{"restored": true}')
    assert (await auth_client.get(url, headers=headers)).json() == {"restored": True}


async def test_temp_collision_does_not_follow_or_unlink_existing_symlink(tmp_path, monkeypatch):
    import context_intelligence_server.blob_store as blob_store

    store = AsyncDiskBlobStore(tmp_path / "root")
    directory = store.blob_path("session", "k" * 242).parent
    directory.mkdir(parents=True)
    outside = tmp_path / "sentinel"
    outside.write_text("untouched")
    collision = directory / f"{'k' * 242}.AAAAAAAA.tmp"
    collision.symlink_to(outside)
    names = iter(["AAAAAAAA", "BBBBBBBB"])
    monkeypatch.setattr(blob_store.secrets, "token_urlsafe", lambda _: next(names))
    uri = await store.write("session", "k" * 242, {"safe": True})
    assert collision.is_symlink() and outside.read_text() == "untouched"
    assert await store.read(uri) == {"safe": True}
    assert not (directory / f"{'k' * 242}.BBBBBBBB.tmp").exists()


async def test_recovery_retry_at_exact_worker_filename_budget(tmp_path):
    queue = QueueManager(tmp_path / "recovery")
    worker = "x" * MAX_SESSION_ID_BYTES
    origin = {"source_handle": "a" * 64, "ordinal": 0, "source_line_sha256": "b" * 64}
    other = {**origin, "ordinal": 1}
    selected = json.dumps({"_recovery_origin": origin}).encode()
    retained = json.dumps({"_recovery_origin": other}).encode()
    await queue.dead_letter(worker, selected, "synthetic poison")
    await queue.dead_letter(worker, retained, "separate poison")
    assert await queue.archive_recovery_dead_letters(origin) == 1
    assert not await queue.recovery_origin_is_dead(origin)
    assert await queue.recovery_origin_is_dead(other)
    assert await queue.recovery_origin_has_retry_audit(origin)
    assert all(len(path.name.encode()) <= 255 for path in queue._dir.iterdir())
    assert not list(queue._dir.glob("*.tmp"))


async def test_native_spool_migrates_before_reconcile_without_rewriting_identity(tmp_path, monkeypatch):
    import context_intelligence_server.main as main

    queue = QueueManager(tmp_path / "recovery")
    raw_worker = "worker-" + "x" * 239  # production band: .dead.jsonl cannot exist
    session = "source-" + "s" * 180  # admitted original identity stays distinct
    origin = {"source_handle": "a" * 64, "ordinal": 0, "source_line_sha256": "b" * 64}
    envelope = {
        "event": "session:start", "workspace": "synthetic",
        "data": {"session_id": session, "timestamp": "2026-01-01T00:00:00"},
        "created_by": "owner", "_recovery_origin": origin,
    }
    raw = json.dumps(envelope).encode() + b"\n"
    (queue._dir / f"{raw_worker}.log").write_bytes(raw)
    (queue._dir / f"{raw_worker}.offset").write_text("0")
    recovery_registry = SimpleNamespace(queue_manager=queue, get_or_create=MagicMock())
    app = SimpleNamespace(state=SimpleNamespace(recovery_registry=recovery_registry))

    async def reconcile(_):
        assert await queue.recovery_origin_commit_state(origin) == "queued"
        assert not (queue._dir / f"{raw_worker}.log").exists()

    monkeypatch.setattr(main, "_reconcile_native_recovery_outbox", reconcile)
    await main._resume_native_recovery(app)
    folded = fold_worker_key(raw_worker)
    recovery_registry.get_or_create.assert_called_once_with(
        folded, "synthetic", created_by="owner"
    )
    batch = await queue.read_batch(folded, 1)
    assert batch.lines == [raw[:-1]]
    assert json.loads(batch.lines[0])["data"]["session_id"] == session
    await queue.commit(folded, batch.end_offset)
    assert await queue.recovery_origin_commit_state(origin) == "committed"
    assert await queue.migrate_overlong_keys() == (0, 0, 0)


@pytest.mark.parametrize("sibling", [".offset", ".dead.jsonl"])
async def test_migration_refuses_any_existing_destination_sibling(tmp_path, sibling):
    queue = QueueManager(tmp_path / "queues")
    original = "s" * 220
    folded = fold_worker_key(original)
    source = queue._dir / f"{original}.log"
    source.write_bytes(b'{"event":"uncommitted"}\n')
    target = queue._dir / f"{folded}{sibling}"
    target.write_bytes(b"17" if sibling == ".offset" else b"forensic-record\n")
    before = {path.name: path.read_bytes() for path in queue._dir.iterdir()}
    assert await queue.migrate_overlong_keys() == (0, 1, 0)
    assert {path.name: path.read_bytes() for path in queue._dir.iterdir()} == before


@pytest.mark.parametrize("alias_kind", ["blob", "queue"])
@pytest.mark.parametrize("actor,expected", [("owner", 200), ("other", 403), ("reader", 200)])
async def test_scoped_claims_preserve_folded_blob_ownership(
    tmp_path, monkeypatch, auth_client, actor, expected, alias_kind
):
    """A physical alias must neither hide the owner nor grant another workspace access."""
    import context_intelligence_server.main as main
    from context_intelligence_server.auth import StaticKeyResolver
    from context_intelligence_server.config import ContributorGrant
    from context_intelligence_server.session_claims import SessionClaimStore

    store = AsyncDiskBlobStore(tmp_path / "storage")
    original = "nested-" * 70
    uri = await store.write(original, "raw", {"private": "synthetic-owner-data"})
    folded, key = store.parse_uri(uri)
    alias = folded if alias_kind == "blob" else fold_worker_key(original)
    claims = SessionClaimStore(tmp_path / "claims.sqlite3")
    queue = QueueManager(tmp_path / "queues")
    monkeypatch.setattr(main.app.state, "append_live_record", queue.append)
    monkeypatch.setattr(main.registry, "get_or_create", MagicMock())
    monkeypatch.setattr(main, "_settings", SimpleNamespace(blob_path=str(store._root)))
    monkeypatch.setattr(main.app.state, "access_control_mode", "scoped")
    monkeypatch.setattr(main.app.state, "session_claims", claims)
    monkeypatch.setattr(main.app.state, "contributor_grants", {
        "owner": ContributorGrant(workspaces=["workspace-one"], capabilities={"live:write", "data:read"}),
        "other": ContributorGrant(workspaces=["workspace-two"], capabilities={"live:write", "data:read"}),
        "reader": ContributorGrant(workspaces=["workspace-one"], capabilities={"data:read"}),
    })
    tokens = {"owner": "test-owner-token", "other": "test-other-token",
              "reader": "test-reader-token"}
    resolver = StaticKeyResolver({
        hashlib.sha256(token.encode()).hexdigest(): identity
        for identity, token in tokens.items()
    })
    monkeypatch.setattr(main.asgi_app, "resolver", resolver)
    # Exercise real scoped admission, not a manually seeded claim. A physical
    # alias is refused before either claim insertion or queue append.
    for identity, session, workspace, status in [
        ("owner", original, "workspace-one", 202),
        ("other", alias, "workspace-two", 409),
    ]:
        accepted = await auth_client.post("/events", json={
            "event": "session:start", "workspace": workspace,
            "data": {"session_id": session, "timestamp": "2026-01-01T00:00:00"},
        }, headers={"Authorization": f"Bearer {tokens[identity]}"})
        assert accepted.status_code == status
    assert await claims.get_workspace(original) == "workspace-one"
    assert await claims.get_workspace(alias) is None
    assert await queue.pending_count(fold_worker_key(original)) == 1
    if alias_kind == "blob":
        assert not (queue._dir / f"{fold_worker_key(alias)}.log").exists()
    # A fresh store simulates process restart, including the lazy read-only path.
    monkeypatch.setattr(main.app.state, "session_claims", None)
    monkeypatch.setattr(main.app.state, "session_claims_path", claims.path)
    headers = {"Authorization": f"Bearer {tokens[actor]}"}
    raw = await auth_client.get(f"/blobs/{original}/{key}", headers=headers)
    assert raw.status_code == expected
    canonical = await auth_client.get(f"/blobs/{folded}/{key}", headers=headers)
    assert canonical.status_code == expected
    for spelling in (original, folded):
        listed = await auth_client.get(f"/blobs/{spelling}", headers=headers)
        assert listed.status_code == expected
        if expected == 200:
            assert listed.json()["blobs"] == [uri]