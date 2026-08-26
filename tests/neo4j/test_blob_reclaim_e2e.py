"""End-to-end blob-reclaim against a REAL Neo4j graph and a REAL filesystem
blob store -- no mocks, no stubs.

Proves the reshaped reclaim GC for real: the reference scan runs as Cypher
against a live graph, orphan selection uses the real QueueManager drain state,
and deletion goes through the real fenced BlobStore.delete -- the orphan file
actually disappears from disk while the referenced blob survives.

    uv run pytest tests/neo4j/test_blob_reclaim_e2e.py -q -m neo4j
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from neo4j import READ_ACCESS, AsyncGraphDatabase

from context_intelligence_server.blob_store import create_blob_store
from context_intelligence_server.config import Settings
from context_intelligence_server.neo4j_store import ensure_neo4j_schema
from context_intelligence_server.queue_manager import FileSystemQueueManager
from context_intelligence_server.registry import SessionRegistry
from context_intelligence_server.routers import admin
from context_intelligence_server.routers.admin import BlobReclaimBody, reclaim_blobs

pytestmark = pytest.mark.neo4j

_WS = "reclaim_e2e"


def _settings(tmp_path: Path) -> Settings:
    # The reclaim path reaches Neo4j via app.state.neo4j_query_driver, so only
    # the filesystem roots matter here.
    s = Settings()
    s.blob_path = str(tmp_path / "blobs")
    s.queues_path = str(tmp_path / "queues")
    return s


def _build_registry(queues_dir: Path) -> SessionRegistry:
    reg = SessionRegistry()
    reg._queue_manager = FileSystemQueueManager(queues_dir=queues_dir)
    reg._write_semaphore = asyncio.Semaphore(8)
    reg._max_delivery_attempts = 3
    return reg


def _backdate(path: Path, minutes: int) -> None:
    """Age a blob file past the reclaim min-age floor (>= 15 min)."""
    old = time.time() - minutes * 60
    os.utime(path, (old, old))


def _blob_file(blob_root: Path, session_id: str, key: str) -> Path:
    return blob_root / session_id / "blobs" / f"{key}.json"


@pytest.fixture
async def _driver(neo4j_container: dict[str, Any]):
    driver = AsyncGraphDatabase.driver(
        neo4j_container["bolt_url"],
        auth=(neo4j_container["user"], neo4j_container["password"]),
    )
    await ensure_neo4j_schema(driver)
    async with driver.session() as session:
        await session.run("MATCH (n) DETACH DELETE n")
    yield driver
    async with driver.session() as session:
        await session.run("MATCH (n) DETACH DELETE n")
    await driver.close()


async def test_reclaim_deletes_orphan_keeps_referenced_e2e(
    tmp_path: Path, neo4j_container: dict[str, Any], _driver, monkeypatch
) -> None:
    settings = _settings(tmp_path)
    monkeypatch.setattr(admin, "get_settings", lambda: settings)

    # Real filesystem blob store: write two real blobs.
    store = create_blob_store(settings)
    ref_kept = await store.write("sess_ref", "kept", {"payload": "referenced"})
    ref_orphan = await store.write("sess_orphan", "orphan", {"payload": "dangling"})

    blob_root = Path(settings.blob_path)
    kept_file = _blob_file(blob_root, "sess_ref", "kept")
    orphan_file = _blob_file(blob_root, "sess_orphan", "orphan")
    assert kept_file.exists() and orphan_file.exists()

    # Age both past the 15-minute floor so neither is skipped_recent.
    _backdate(kept_file, 60)
    _backdate(orphan_file, 60)

    # Reference ONLY the kept blob in the real graph, via a carrier property.
    async with _driver.session() as session:
        await session.run(
            "CREATE (:Event {node_id: 'e1', workspace: $ws, session_id: 'sess_ref', "
            "data: $data})",
            ws=_WS,
            data=json.dumps({"$blob_ref": ref_kept.uri}),
        )

    # Real registry + real queue manager; both sessions fully drained (no logs).
    registry = _build_registry(tmp_path / "queues")

    request = SimpleNamespace(
        # request.scope["state"]["contributor_id"] is read by the audit logger.
        scope={"state": {"contributor_id": "admin"}},
        app=SimpleNamespace(
            state=SimpleNamespace(
                registry=registry,
                neo4j_query_driver=_driver,
                neo4j_query_access_mode="READ",
            )
        ),
    )
    # Sanity: the exact access-mode constant the endpoint will use.
    assert admin._access_mode_const("READ") is READ_ACCESS

    # 1) DRY-RUN: finds exactly the orphan, deletes nothing.
    admin._reclaim_apply_inflight = False
    dry = await reclaim_blobs(
        BlobReclaimBody(dry_run=True, min_age_minutes=15), request
    )
    assert dry["dry_run"] is True
    assert dry["orphans_found"] == 1, dry
    assert ref_orphan.uri in dry["sample"]
    assert ref_kept.uri not in dry["sample"]
    assert orphan_file.exists()  # nothing deleted in dry-run

    # 2) APPLY: the orphan file actually disappears; the referenced one survives.
    applied = await reclaim_blobs(
        BlobReclaimBody(dry_run=False, min_age_minutes=15, max_delete=10), request
    )
    assert applied["deleted"] == 1, applied
    assert not orphan_file.exists(), "orphan blob must be gone from disk"
    assert kept_file.exists(), "referenced blob must survive"
    assert admin._reclaim_apply_inflight is False  # single-flight released
