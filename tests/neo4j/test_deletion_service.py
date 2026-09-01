"""Tier 3 -- Neo4j integration proof for DeletionService (A4).

DeletionService composed with a REAL Neo4jGraphStore, a REAL
AsyncDiskBlobStore, and a REAL QueueManager. Ingests a multi-session graph
(root + 2 subsessions + 1 fork) with blobs on several of its sessions that
shares a :SST_CONCEPT node (Agent) with a SEPARATE, unrelated graph, drains
each session's queue, then applies deletion via a SUB-session id and proves:

  (a) every session node in the graph is gone (root + all descendants),
  (b) every graph blob is purged (count reconciles to the whole-graph total),
  (c) queue/dead-letter artifacts for the graph are gone,
  (d) the shared concept node survives with only its edges-into-graph removed,
  (e) the unrelated graph reachable only through the shared concept is untouched.

Run: uv run --group dev pytest tests/neo4j/test_deletion_service.py -q -m neo4j
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from context_intelligence_server.blob_store import AsyncDiskBlobStore
from context_intelligence_server.deletion import DeletionService
from context_intelligence_server.queue_manager import QueueManager

pytestmark = pytest.mark.neo4j


async def _drain(queue_manager: QueueManager, session_id: str, raw: bytes) -> None:
    await queue_manager.append(session_id, raw)
    batch = await queue_manager.read_batch(session_id, max_items=10)
    await queue_manager.commit(session_id, batch.end_offset)


async def _build_graph(
    store: Any, blob_store: AsyncDiskBlobStore, queue_manager: QueueManager
) -> frozenset[str]:
    """Seed a root + 2 subsessions + 1 fork, with blobs on several sessions.

    Tree shape:  root -[HAS_SUBSESSION]-> sub1 -[HAS_SUBSESSION]-> sub2
                 root -[FORKED]-> fork1

    A shared Agent (:SST_CONCEPT) is reached from fork1's Delegation. Every
    session's queue is drained. Returns the whole-graph blob URI set.
    """
    store.created_by = "colombod"

    blob_root = await blob_store.write("ds-fam-root", "orch1", {"v": "root"})
    blob_sub2 = await blob_store.write("ds-fam-sub2", "tool1", {"v": "sub2"})
    blob_fork1 = await blob_store.write("ds-fam-fork1", "del1", {"v": "fork1"})

    await store.upsert_node(
        "ds-fam-root",
        {"labels": ["Session", "RootSession"], "started_at": "2026-01-01T00:00:00Z"},
    )
    await store.upsert_node(
        "ds-fam-sub1",
        {
            "labels": ["Session", "SubSession"],
            "parent_id": "ds-fam-root",
            "started_at": "2026-01-01T00:01:00Z",
        },
    )
    await store.upsert_edge("ds-fam-root", "ds-fam-sub1", {"type": "HAS_SUBSESSION"})
    await store.upsert_node(
        "ds-fam-sub2",
        {
            "labels": ["Session", "SubSession"],
            "parent_id": "ds-fam-sub1",
            "started_at": "2026-01-01T00:02:00Z",
        },
    )
    await store.upsert_edge("ds-fam-sub1", "ds-fam-sub2", {"type": "HAS_SUBSESSION"})
    await store.upsert_node(
        "ds-fam-fork1",
        {
            "labels": ["Session", "ForkedSession"],
            "parent_id": "ds-fam-root",
            "started_at": "2026-01-01T00:03:00Z",
        },
    )
    await store.upsert_edge("ds-fam-root", "ds-fam-fork1", {"type": "FORKED"})

    await store.upsert_node(
        "ds-fam-root::orch::1",
        {"labels": ["OrchestratorRun", "SST_EVENT"], "raw": {"$blob_ref": blob_root}},
    )
    await store.upsert_edge(
        "ds-fam-root", "ds-fam-root::orch::1", {"type": "HAS_EXECUTION"}
    )

    await store.upsert_node(
        "ds-fam-sub2::tool::1",
        {"labels": ["ToolCall", "SST_EVENT"], "result": {"$blob_ref": blob_sub2}},
    )
    await store.upsert_edge(
        "ds-fam-sub2", "ds-fam-sub2::tool::1", {"type": "HAS_TOOL_CALL"}
    )

    await store.upsert_node(
        "ds-fam-fork1::delegation::1",
        {"labels": ["Delegation", "SST_EVENT"], "messages": {"$blob_ref": blob_fork1}},
    )
    await store.upsert_edge(
        "ds-fam-fork1", "ds-fam-fork1::delegation::1", {"type": "TRIGGERED"}
    )

    # Shared concept node -- must survive the delete of ds-fam-* below.
    await store.upsert_node("ds-agent-shared", {"labels": ["Agent", "SST_CONCEPT"]})
    await store.upsert_edge(
        "ds-fam-fork1::delegation::1", "ds-agent-shared", {"type": "HAS_AGENT"}
    )

    await store.flush()

    for sid in ("ds-fam-root", "ds-fam-sub1", "ds-fam-sub2", "ds-fam-fork1"):
        await _drain(
            queue_manager, sid, json.dumps({"event": "tool:pre"}).encode("utf-8")
        )
    await queue_manager.dead_letter("ds-fam-sub2", b"bad-line", "boom")

    return frozenset({blob_root, blob_sub2, blob_fork1})


async def _build_unrelated_graph_sharing_concept(store: Any) -> None:
    """A SEPARATE, unrelated graph reaching the SAME shared Agent node.

    Reachable ONLY through the shared concept -- deleting ds-fam-* must not
    touch any of this.
    """
    await store.upsert_node(
        "ds-other-fam-root",
        {"labels": ["Session", "RootSession"], "started_at": "2026-02-01T00:00:00Z"},
    )
    await store.upsert_node(
        "ds-other-fam-root::delegation::1",
        {"labels": ["Delegation", "SST_EVENT"]},
    )
    await store.upsert_edge(
        "ds-other-fam-root",
        "ds-other-fam-root::delegation::1",
        {"type": "TRIGGERED"},
    )
    await store.upsert_edge(
        "ds-other-fam-root::delegation::1", "ds-agent-shared", {"type": "HAS_AGENT"}
    )
    await store.flush()


class TestDeletionServiceNeo4j:
    """DeletionService composed with a real Neo4jGraphStore + real blob/queue."""

    async def test_preview_reports_whole_graph_facts(
        self, neo4j_services: Any, tmp_path: Path
    ) -> None:
        store = neo4j_services.graph
        blob_store = AsyncDiskBlobStore(tmp_path / "blobs")
        queue_manager = QueueManager(tmp_path / "queues")
        service = DeletionService(store, blob_store, queue_manager)

        blob_refs = await _build_graph(store, blob_store, queue_manager)
        await _build_unrelated_graph_sharing_concept(store)

        preview = await service.preview("ds-fam-sub2")
        assert preview is not None
        assert preview.root_id == "ds-fam-root"
        assert preview.session_ids == frozenset(
            {"ds-fam-root", "ds-fam-sub1", "ds-fam-sub2", "ds-fam-fork1"}
        )
        assert preview.blob_count == len(blob_refs) == 3
        assert preview.deletable is True
        assert preview.pending_sessions == []
        assert preview.created_by == "colombod"

    async def test_apply_deletes_whole_graph_and_preserves_shared_concept(
        self, neo4j_services: Any, tmp_path: Path
    ) -> None:
        store = neo4j_services.graph
        blob_store = AsyncDiskBlobStore(tmp_path / "blobs")
        queue_manager = QueueManager(tmp_path / "queues")
        service = DeletionService(store, blob_store, queue_manager)

        blob_refs = await _build_graph(store, blob_store, queue_manager)
        await _build_unrelated_graph_sharing_concept(store)

        result = await service.apply("ds-fam-sub1", requested_by="tester")

        assert result is not None
        assert result.root_id == "ds-fam-root"
        assert result.session_count == 4
        # Owned = 4 sessions + orch + tool + delegation = 7 (agent excluded).
        assert result.nodes_deleted == 7
        assert result.relationships_deleted == 7
        assert result.blobs_deleted == len(blob_refs) == 3
        assert result.queue_sessions_cleaned == 4

        # (a) every session node in the graph is gone.
        for node_id in (
            "ds-fam-root",
            "ds-fam-sub1",
            "ds-fam-sub2",
            "ds-fam-fork1",
            "ds-fam-root::orch::1",
            "ds-fam-sub2::tool::1",
            "ds-fam-fork1::delegation::1",
        ):
            assert await store.get_node(node_id) is None, f"{node_id} should be gone"

        # (b) every graph blob is purged.
        for sid in ("ds-fam-root", "ds-fam-sub1", "ds-fam-sub2", "ds-fam-fork1"):
            assert await blob_store.list(sid) == []

        # (c) queue/dead-letter artifacts for the graph are gone.
        for sid in ("ds-fam-root", "ds-fam-sub1", "ds-fam-sub2", "ds-fam-fork1"):
            assert not queue_manager._log_path(sid).exists()
            assert not queue_manager._offset_path(sid).exists()
            assert not queue_manager._dead_path(sid).exists()

        # (d) the shared concept node survives.
        agent = await store.get_node("ds-agent-shared")
        assert agent is not None
        assert "SST_CONCEPT" in agent.get("labels", [])

        # (e) the unrelated graph, reachable only via the shared concept, is untouched.
        assert await store.get_node("ds-other-fam-root") is not None
        assert await store.get_node("ds-other-fam-root::delegation::1") is not None
        assert (
            await store.get_edge("ds-other-fam-root::delegation::1", "ds-agent-shared")
            is not None
        )

    async def test_apply_refuses_when_a_graph_session_has_pending_records(
        self, neo4j_services: Any, tmp_path: Path
    ) -> None:
        store = neo4j_services.graph
        blob_store = AsyncDiskBlobStore(tmp_path / "blobs")
        queue_manager = QueueManager(tmp_path / "queues")
        service = DeletionService(store, blob_store, queue_manager)

        await _build_graph(store, blob_store, queue_manager)
        # Leave sub2 with an uncommitted append after the drain above.
        await queue_manager.append("ds-fam-sub2", b'{"event": "late"}')

        with pytest.raises(RuntimeError, match="pending"):
            await service.apply("ds-fam-root")

        # Nothing deleted.
        assert await store.get_node("ds-fam-root") is not None
        assert await store.get_node("ds-fam-sub2") is not None

    async def test_apply_unknown_session_returns_none(
        self, neo4j_services: Any, tmp_path: Path
    ) -> None:
        store = neo4j_services.graph
        blob_store = AsyncDiskBlobStore(tmp_path / "blobs")
        queue_manager = QueueManager(tmp_path / "queues")
        service = DeletionService(store, blob_store, queue_manager)

        assert await service.apply("does-not-exist") is None
