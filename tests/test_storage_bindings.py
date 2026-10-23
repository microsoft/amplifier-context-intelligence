"""Physical aliases never grant a second original ownership of scoped storage."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from context_intelligence_server.blob_store import AsyncDiskBlobStore
from context_intelligence_server.queue_manager import QueueManager, fold_worker_key
from context_intelligence_server.session_claims import SessionClaimStore


def _legacy(path, rows):
    with sqlite3.connect(path) as db:
        db.execute(
            """CREATE TABLE session_claims (
                session_id TEXT PRIMARY KEY, contributor_id TEXT NOT NULL,
                workspace TEXT NOT NULL, reservation_token TEXT
            )"""
        )
        db.executemany("INSERT INTO session_claims VALUES (?, ?, ?, ?)", rows)


@pytest.mark.parametrize("family", ["blob", "queue"])
@pytest.mark.parametrize("reverse", [False, True])
async def test_alias_claims_refused_in_either_order_without_partial_rows(tmp_path, family, reverse):
    original = "session-" * 70
    alias = (
        AsyncDiskBlobStore._fold_session_id(original)
        if family == "blob" else fold_worker_key(original)
    )
    first, second = (alias, original) if reverse else (original, alias)
    claims = SessionClaimStore(tmp_path / "claims.sqlite3")
    assert await claims.claim(first, "owner", "one")
    with sqlite3.connect(claims.path) as db:
        before = db.execute("SELECT * FROM storage_bindings ORDER BY kind, component").fetchall()
    # Even the SAME owner/workspace cannot claim a different original identity.
    assert not await claims.claim(second, "owner", "one")
    assert not await claims.claim(second, "other", "two")
    assert await claims.get_workspace(second) is None
    with sqlite3.connect(claims.path) as db:
        assert db.execute("SELECT * FROM storage_bindings ORDER BY kind, component").fetchall() == before
        assert db.execute("SELECT count(*) FROM session_claims").fetchone() == (1,)


@pytest.mark.parametrize("family", ["blob", "queue"])
async def test_alias_claim_race_across_independent_stores(tmp_path, family):
    path = tmp_path / "claims.sqlite3"
    stores = [SessionClaimStore(path), SessionClaimStore(path)]
    original = "nested-" * 70
    alias = (
        AsyncDiskBlobStore._fold_session_id(original)
        if family == "blob" else fold_worker_key(original)
    )
    barrier = threading.Barrier(2)
    async def compete(store, session, owner):
        await asyncio.to_thread(barrier.wait, 5)
        return await store.claim(session, owner, owner)
    results = await asyncio.gather(
        compete(stores[0], original, "one"), compete(stores[1], alias, "two")
    )
    assert sorted(results) == [False, True]
    with sqlite3.connect(path) as db:
        winner = original if results[0] else alias
        assert db.execute("SELECT session_id FROM session_claims").fetchall() == [(winner,)]
        assert db.execute("SELECT DISTINCT session_id FROM storage_bindings").fetchall() == [(winner,)]


async def test_additive_backfill_bounded_clean_and_provisional_rows(tmp_path):
    path = tmp_path / "claims.sqlite3"
    original = "source-" * 70
    _legacy(path, [
        (f"ordinary-{i}", "owner", "one", None) for i in range(600)
    ] + [(original, "owner", "one", None), ("provisional", "owner", "one", "token"),
         ("invalid/component", "owner", "one", None)])
    claims = SessionClaimStore(path)
    assert await claims.get_blob_workspace(original) == "one"
    assert await claims.get_blob_workspace(AsyncDiskBlobStore._fold_session_id(original)) == "one"
    assert await claims.storage_identity_matches(original)
    assert await claims.get_blob_workspace("provisional") is None
    assert await claims.get_blob_workspace("invalid/component") is None
    assert not await claims.claim("invalid/component", "owner", "one")
    assert await claims.get_workspace("invalid/component") == "one"
    assert await claims.finalize_recovery("provisional", "owner", "one", "token")
    assert await SessionClaimStore(path).get_blob_workspace("provisional") == "one"
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT count(*) FROM session_claims").fetchone() == (603,)
        assert db.execute(
            "SELECT session_id FROM storage_bindings WHERE kind='queue' AND component=?",
            ("invalid/component",),
        ).fetchone() == ("invalid/component",)


async def test_legacy_percent_query_fragment_ids_get_both_bindings(tmp_path):
    """'%', '?' and '#' are literal, valid storage names: a legacy claim is
    backfilled with BOTH a blob and a queue binding to the original id."""
    path = tmp_path / "claims.sqlite3"
    legacy_id = "legacy%sess?id#1"
    _legacy(path, [(legacy_id, "owner", "one", None)])
    claims = SessionClaimStore(path)
    assert await claims.get_blob_workspace(legacy_id) == "one"
    assert await claims.storage_identity_matches(legacy_id)
    assert await claims.claim(legacy_id, "owner", "one")
    with sqlite3.connect(path) as db:
        assert db.execute(
            "SELECT kind, component, session_id FROM storage_bindings ORDER BY kind"
        ).fetchall() == [
            ("blob", legacy_id, legacy_id),
            ("queue", legacy_id, legacy_id),
        ]


@pytest.mark.parametrize("family", ["blob", "queue"])
@pytest.mark.parametrize("provisional", [False, True])
async def test_ambiguous_backfill_keeps_originals_and_blocks_storage(tmp_path, family, provisional):
    path = tmp_path / "claims.sqlite3"
    original = "long-" * 100
    alias = (
        AsyncDiskBlobStore._fold_session_id(original)
        if family == "blob" else fold_worker_key(original)
    )
    rows = [(original, "owner", "one", "token" if provisional else None),
            (alias, "other", "two", None)]
    _legacy(path, rows)
    for _ in range(2):
        claims = SessionClaimStore(path)
        assert not await claims.storage_identity_matches(original)
        assert not await claims.storage_identity_matches(alias)
        assert not await claims.claim(original, "owner", "one")
        assert not await claims.claim(alias, "other", "two")
        assert await claims.reserve_recovery(original, "owner", "one", "token") == "conflict"
        assert not await claims.finalize_recovery(original, "owner", "one", "token")
        if family == "blob":
            assert await claims.get_blob_workspace(original) is None
            assert await claims.get_blob_workspace(alias) is None
        with sqlite3.connect(path) as db:
            assert db.execute(
                "SELECT session_id FROM storage_bindings WHERE kind=? AND component=?",
                (family, alias),
            ).fetchone() == (None,)
            assert sorted(db.execute("SELECT * FROM session_claims").fetchall()) == sorted(rows)


@pytest.mark.parametrize("family", ["blob", "queue"])
async def test_recovery_reservation_bindings_follow_exact_token_lifecycle(tmp_path, family):
    path = tmp_path / "claims.sqlite3"
    claims = SessionClaimStore(path)
    original = "recovery-" * 60
    alias = (
        AsyncDiskBlobStore._fold_session_id(original)
        if family == "blob" else fold_worker_key(original)
    )
    assert await claims.reserve_recovery(original, "owner", "one", "token") == "reserved"
    assert await claims.get_blob_workspace(original) is None
    assert not await claims.claim(alias, "other", "two")
    assert await claims.reserve_recovery(alias, "other", "two", "another") == "conflict"
    assert not await claims.finalize_recovery(original, "owner", "one", "wrong")
    await claims.release_recovery(original, "owner", "one", "wrong")
    assert not await claims.claim(alias, "other", "two")
    await claims.release_recovery(original, "owner", "one", "token")
    assert await claims.claim(alias, "other", "two")
    assert await claims.reserve_recovery(original, "owner", "one", "token") == "conflict"
    # A separate original finalizes, survives restart, and cannot be unbound.
    assert await claims.reserve_recovery("clean", "owner", "one", "token") == "reserved"
    assert await claims.finalize_recovery("clean", "owner", "one", "token")
    claims = SessionClaimStore(path)
    await claims.release_recovery("clean", "owner", "one", "token")
    assert await claims.storage_identity_matches("clean")
    assert await claims.finalize_recovery("clean", "owner", "one", "retry")


@pytest.mark.parametrize("committed", [False, True])
async def test_native_resume_does_not_dispatch_ambiguous_or_provisional_queue(
    tmp_path, monkeypatch, committed
):
    import context_intelligence_server.main as main

    original = "nested-" * 70
    alias = fold_worker_key(original)
    path = tmp_path / "claims.sqlite3"
    _legacy(path, [(original, "owner", "one", None if committed else "token"),
                   (alias, "other", "two", None)])
    queue = QueueManager(tmp_path / "recovery")
    raw = json.dumps({"workspace": "one", "created_by": "owner",
                      "data": {"session_id": original}}).encode()
    await queue.append(alias, raw)
    worker_registry = SimpleNamespace(queue_manager=queue, get_or_create=MagicMock())
    app = SimpleNamespace(state=SimpleNamespace(
        access_control_mode="scoped", session_claims=SessionClaimStore(path),
        recovery_registry=worker_registry,
    ))
    monkeypatch.setattr(main, "_reconcile_native_recovery_outbox", AsyncMock())
    await main._resume_native_recovery(app)
    worker_registry.get_or_create.assert_not_called()
    assert (await queue.read_batch(alias, 1)).lines == [raw]
    assert await queue.pending_count(alias) == 1


@pytest.mark.parametrize("method,suffix", [("GET", "/summary"), ("DELETE", "")])
@pytest.mark.parametrize("via_root", [False, True])
@pytest.mark.parametrize("family", ["blob", "queue"])
async def test_ambiguous_legacy_storage_is_not_read_or_deleted(
    tmp_path, monkeypatch, auth_client, method, suffix, via_root, family
):
    """Negative seam check: exact graph ownership must not delete a shared alias."""
    import context_intelligence_server.main as main
    import context_intelligence_server.routers.deletion as routes
    from context_intelligence_server.config import ContributorGrant
    from context_intelligence_server.services import GraphState

    original = "original-" * 60
    alias = (
        AsyncDiskBlobStore._fold_session_id(original)
        if family == "blob" else fold_worker_key(original)
    )
    path = tmp_path / "claims.sqlite3"
    _legacy(path, [(original, "owner", "one", None),
                   (alias, "other", "two", None)])
    claims = SessionClaimStore(path)
    assert not await claims.storage_identity_matches(original)
    if family == "blob":
        assert await claims.get_blob_workspace(original) is None
    store = AsyncDiskBlobStore(tmp_path / "blobs")
    uri = await store.write(original, "raw", {"private": True})
    graph = GraphState(workspace="one")
    await graph.upsert_node(original, {"labels": ["Session", "RootSession"]})
    requested = original
    if via_root:
        requested = "clean-root"
        assert await claims.claim(requested, "owner", "one")
        await graph.upsert_node(requested, {"labels": ["Session", "RootSession"]})
        await graph.upsert_edge(requested, original, {"type": "HAS_SUBSESSION"})
    queue = QueueManager(tmp_path / "queues")
    await queue.dead_letter(fold_worker_key(original), b"synthetic", "retained audit")
    queue_before = {p.name: p.read_bytes() for p in queue._dir.iterdir()}
    monkeypatch.setattr(main.app.state, "access_control_mode", "scoped")
    monkeypatch.setattr(main.app.state, "session_claims", claims)
    monkeypatch.setattr(main.app.state, "contributor_grants", {
        "owner": ContributorGrant(workspaces=["one"], capabilities={"live:write", "data:read"})
    })
    monkeypatch.setattr(main.app.state, "registry", SimpleNamespace(queue_manager=queue))
    monkeypatch.setattr(main.app.state, "neo4j_driver", object(), raising=False)
    monkeypatch.setattr(main.app.state, "neo4j_query_driver", object(), raising=False)
    monkeypatch.setattr(routes, "get_settings", lambda: SimpleNamespace(blob_path=str(store._root)))
    monkeypatch.setattr(routes, "Neo4jGraphStore", lambda **_: graph)
    monkeypatch.setattr(routes, "AsyncDiskBlobStore", lambda **_: store)
    blob_list = AsyncMock(wraps=store.list)
    pending_count = AsyncMock(wraps=queue.pending_count)
    monkeypatch.setattr(store, "list", blob_list)
    monkeypatch.setattr(queue, "pending_count", pending_count)
    # Production route + production service factory, scratch stores only.
    response = await auth_client.request(
        method, f"/sessions/{requested}{suffix}",
        headers={"Authorization": "Bearer test-secret"},
    )
    preserved = (
        store.blob_path(original, "raw").exists(),
        await graph.resolve_session_graph(original) is not None,
        {p.name: p.read_bytes() for p in queue._dir.iterdir()} == queue_before,
    )
    assert (response.status_code, preserved) == (403, (True, True, True))
    blob_list.assert_not_awaited()
    pending_count.assert_not_awaited()
    assert await store.read(uri) == {"private": True}


@pytest.mark.parametrize("method,suffix", [("GET", "/summary"), ("DELETE", "")])
@pytest.mark.parametrize("child_claim", ["committed", "missing", "provisional", "other-workspace"])
async def test_whole_graph_guard_preserves_workspace_and_capability_policy(
    tmp_path, monkeypatch, auth_client, method, suffix, child_claim
):
    """Authorized nonowners still work; each descendant must independently pass."""
    import context_intelligence_server.main as main
    import context_intelligence_server.routers.deletion as routes
    from context_intelligence_server.config import ContributorGrant
    from context_intelligence_server.services import GraphState

    root, child = "clean-root", "child-" * 80
    claims = SessionClaimStore(tmp_path / "claims.sqlite3")
    assert await claims.claim(root, "source-author", "one")
    if child_claim == "provisional":
        assert await claims.reserve_recovery(child, "source-author", "one", "token") == "reserved"
    elif child_claim != "missing":
        workspace = "two" if child_claim == "other-workspace" else "one"
        assert await claims.claim(child, "source-author", workspace)
    graph = GraphState(workspace="one")
    await graph.upsert_node(root, {"labels": ["Session", "RootSession"]})
    await graph.upsert_node(child, {"labels": ["Session", "SubSession"]})
    await graph.upsert_edge(root, child, {"type": "HAS_SUBSESSION"})
    store = AsyncDiskBlobStore(tmp_path / "blobs")
    queue = QueueManager(tmp_path / "queues")
    for sid in (root, child):
        await store.write(sid, "raw", {"synthetic": sid})
        await queue.dead_letter(fold_worker_key(sid), b"audit", "synthetic")
    queue_before = {p.name: p.read_bytes() for p in queue._dir.iterdir()}
    capability = "data:read" if method == "GET" else "live:write"
    monkeypatch.setattr(main.app.state, "access_control_mode", "scoped")
    monkeypatch.setattr(main.app.state, "session_claims", claims)
    # The authenticated owner is NOT the claims' source-author, and has only
    # the capability this route requires. No owner-only or read+write shortcut.
    monkeypatch.setattr(main.app.state, "contributor_grants", {
        "owner": ContributorGrant(workspaces=["one"], capabilities={capability})
    })
    monkeypatch.setattr(main.app.state, "registry", SimpleNamespace(queue_manager=queue))
    monkeypatch.setattr(main.app.state, "neo4j_driver", object(), raising=False)
    monkeypatch.setattr(main.app.state, "neo4j_query_driver", object(), raising=False)
    monkeypatch.setattr(routes, "get_settings", lambda: SimpleNamespace(blob_path=str(store._root)))
    monkeypatch.setattr(routes, "Neo4jGraphStore", lambda **_: graph)
    monkeypatch.setattr(routes, "AsyncDiskBlobStore", lambda **_: store)
    blob_list = AsyncMock(wraps=store.list)
    pending_count = AsyncMock(wraps=queue.pending_count)
    monkeypatch.setattr(store, "list", blob_list)
    monkeypatch.setattr(queue, "pending_count", pending_count)
    response = await auth_client.request(
        method, f"/sessions/{root}{suffix}",
        headers={"Authorization": "Bearer test-secret"},
    )
    if child_claim != "committed":
        assert response.status_code == 403
        blob_list.assert_not_awaited()
        pending_count.assert_not_awaited()
    else:
        assert response.status_code == 200
        if method == "DELETE":
            assert response.json()["blobs_deleted"] == 2
            assert response.json()["queue_sessions_cleaned"] == 2
            assert await graph.resolve_session_graph(root) is None
            assert not store.blob_path(root, "raw").exists()
            assert not store.blob_path(child, "raw").exists()
            return
        assert response.json()["blob_count"] == 2
        assert set(response.json()["session_ids"]) == {root, child}
    assert await graph.resolve_session_graph(root) is not None
    assert store.blob_path(root, "raw").exists()
    assert store.blob_path(child, "raw").exists()
    assert {p.name: p.read_bytes() for p in queue._dir.iterdir()} == queue_before


async def test_migration_holds_canonical_admission_and_preserves_concurrent_append(tmp_path, monkeypatch):
    queue = QueueManager(tmp_path / "queues")
    original = "s" * 220
    folded = fold_worker_key(original)
    (queue._dir / f"{original}.log").write_bytes(b"original\n")
    paused, release = threading.Event(), threading.Event()
    real_exists = queue._exists_or_too_long_is_absent

    def exists(path):
        result = real_exists(path)
        if path.name == f"{folded}.dead.jsonl":
            paused.set()
            assert release.wait(5)
        return result

    monkeypatch.setattr(queue, "_exists_or_too_long_is_absent", exists)
    migration = asyncio.create_task(queue.migrate_overlong_keys())
    append = None
    try:
        assert await asyncio.to_thread(paused.wait, 5)
        destination = queue._guards.get(folded)
        assert destination is not None and destination.admission.locked()
        assert destination.file_lock.locked()
        append = asyncio.create_task(queue.append(folded, b"new"))
    finally:
        release.set()
        assert await migration == (1, 0, 0)
        if append is not None:
            await append
    assert (await queue.read_batch(folded, 10)).lines == [b"original", b"new"]


# --- Summary and deletion see every blob address kind (PR126) ---------------


@pytest.mark.parametrize("with_legacy_twin", [False, True])
async def test_summary_and_deletion_cover_ordinary_mapped_and_legacy_backslash_blobs(
    tmp_path, with_legacy_twin
):
    """Real stores; only the graph is the in-memory fake.  Sizes come from the filesystem."""
    from context_intelligence_server.deletion import DeletionService
    from context_intelligence_server.services import GraphState

    store = AsyncDiskBlobStore(tmp_path / "blobs")
    await store.write("root", "plain", {"v": 1})
    await store.write("root", "a/b", {"v": 2})
    blob_dir = tmp_path / "blobs" / "root" / "blobs"
    (blob_dir / "pre-upgrade\\x.json").write_text(
        '{"legacy": "only"}', encoding="utf-8"
    )
    if with_legacy_twin:
        # The same logical name exists both as a legacy file and (via a new write) as a mapped one.
        (blob_dir / "a\\b.json").write_text('{"legacy": "twin"}', encoding="utf-8")
        await store.write("root", "a\\b", {"v": 3})
    expected_files = len(list(blob_dir.iterdir()))
    expected_bytes = sum(path.stat().st_size for path in blob_dir.iterdir())
    assert expected_files == (5 if with_legacy_twin else 3)

    graph = GraphState(workspace="one")
    await graph.upsert_node("root", {"labels": ["Session", "RootSession"]})
    service = DeletionService(graph, store, QueueManager(tmp_path / "queues"))

    preview = await service.preview("root")
    assert preview is not None
    assert (preview.blob_count, preview.blob_bytes) == (expected_files, expected_bytes)

    result = await service.apply("root")
    assert result is not None and result.blobs_deleted == expected_files
    assert not (tmp_path / "blobs" / "root").exists()
    assert await store.list("root") == []
