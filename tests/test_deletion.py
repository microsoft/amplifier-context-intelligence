"""Unit tests for DeletionService (A4).

Uses the in-memory GraphState (services.py) alongside a REAL
AsyncDiskBlobStore and a REAL QueueManager rooted at pytest's tmp_path --
only Neo4j itself is faked. See tests/neo4j/test_deletion_service.py for the
real-Neo4j proof.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest
from context_intelligence_server.blob_store import AsyncDiskBlobStore
from context_intelligence_server.deletion import DeletionService, SessionsPendingError
from context_intelligence_server.queue_manager import QueueManager
from context_intelligence_server.services import GraphState

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def graph() -> GraphState:
    return GraphState(workspace="test")


@pytest.fixture
def blob_store(tmp_path: Path) -> AsyncDiskBlobStore:
    return AsyncDiskBlobStore(tmp_path / "blobs")


@pytest.fixture
def queue_manager(tmp_path: Path) -> QueueManager:
    return QueueManager(tmp_path / "queues")


@pytest.fixture
def service(
    graph: GraphState, blob_store: AsyncDiskBlobStore, queue_manager: QueueManager
) -> DeletionService:
    return DeletionService(graph, blob_store, queue_manager)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _drain(queue_manager: QueueManager, session_id: str, raw: bytes) -> None:
    """Append one record and commit it -- session ends up with pending_count == 0."""
    await queue_manager.append(session_id, raw)
    batch = await queue_manager.read_batch(session_id, max_items=10)
    await queue_manager.commit(session_id, batch.end_offset)


async def _build_drained_multi_session_graph(
    graph: GraphState,
    blob_store: AsyncDiskBlobStore,
    queue_manager: QueueManager,
    *,
    root: str,
    sub1: str,
    sub2: str,
    concept: str,
) -> frozenset[str]:
    """Seed root -> sub1 -> sub2, with 4 blobs total and a shared SST_CONCEPT node.

    Every session's queue is drained (committed). sub2 additionally carries a
    dead-letter record, to prove ``.dead.jsonl`` is also removed by
    ``QueueManager.delete_session`` (unlike ``delete_drained``).

    Returns the set of blob URIs written.
    """
    blob_root = await blob_store.write(root, "k1", {"v": "root"})
    blob_sub1 = await blob_store.write(sub1, "k1", {"v": "sub1"})
    blob_sub2a = await blob_store.write(sub2, "k1", {"v": "sub2a"})
    blob_sub2b = await blob_store.write(sub2, "k2", {"v": "sub2b"})

    await graph.upsert_node(
        root,
        {
            "labels": ["Session", "RootSession"],
            "started_at": "2026-01-01T00:00:00",
            "created_by": "colombod",
        },
    )
    await graph.upsert_node(
        sub1,
        {
            "labels": ["Session", "SubSession"],
            "started_at": "2026-01-01T00:01:00",
        },
    )
    await graph.upsert_edge(root, sub1, {"type": "HAS_SUBSESSION"})
    await graph.upsert_node(
        sub2,
        {
            "labels": ["Session", "SubSession"],
            "started_at": "2026-01-01T00:02:00",
        },
    )
    await graph.upsert_edge(sub1, sub2, {"type": "HAS_SUBSESSION"})
    await graph.upsert_node(concept, {"labels": ["Agent", "SST_CONCEPT"]})
    await graph.upsert_edge(sub2, concept, {"type": "HAS_AGENT"})

    for sid in (root, sub1, sub2):
        await _drain(
            queue_manager, sid, json.dumps({"event": "tool:pre"}).encode("utf-8")
        )
    await queue_manager.dead_letter(sub2, b"bad-line", "boom")

    return frozenset({blob_root, blob_sub1, blob_sub2a, blob_sub2b})


# ---------------------------------------------------------------------------
# preview()
# ---------------------------------------------------------------------------


async def test_preview_unknown_session_returns_none(service: DeletionService) -> None:
    assert await service.preview("does-not-exist") is None


async def test_preview_returns_facts_and_mutates_nothing(
    service: DeletionService,
    graph: GraphState,
    blob_store: AsyncDiskBlobStore,
    queue_manager: QueueManager,
) -> None:
    root, sub1, sub2, concept = "p-root", "p-sub1", "p-sub2", "p-agent"
    blob_refs = await _build_drained_multi_session_graph(
        graph,
        blob_store,
        queue_manager,
        root=root,
        sub1=sub1,
        sub2=sub2,
        concept=concept,
    )

    preview = await service.preview(sub1)
    assert preview is not None
    assert preview.root_id == root
    assert preview.session_ids == frozenset({root, sub1, sub2})
    assert preview.blob_count == len(blob_refs) == 4
    assert preview.node_count == 4  # root, sub1, sub2, concept (boundary, included)
    assert preview.edge_count == 3  # root->sub1, sub1->sub2, sub2->concept
    assert preview.subsession_count == 2
    assert preview.created_by == "colombod"
    assert preview.deletable is True
    assert preview.pending_sessions == []

    # Nothing mutated: same facts on a second call, graph/blobs/queue untouched.
    preview_again = await service.preview(root)
    assert preview_again == preview
    assert await graph.get_node(root) is not None
    assert await graph.get_node(sub2) is not None
    assert await blob_store.list(sub2) != []
    assert queue_manager._log_path(sub2).exists()


async def test_preview_deletable_false_when_session_has_pending(
    service: DeletionService, graph: GraphState, queue_manager: QueueManager
) -> None:
    root, sub1 = "pp-root", "pp-sub1"
    await graph.upsert_node(root, {"labels": ["Session", "RootSession"]})
    await graph.upsert_node(sub1, {"labels": ["Session", "SubSession"]})
    await graph.upsert_edge(root, sub1, {"type": "HAS_SUBSESSION"})

    # sub1 has an appended-but-uncommitted record -> pending_count > 0.
    await queue_manager.append(sub1, b'{"event": "tool:pre"}')

    preview = await service.preview(root)
    assert preview is not None
    assert preview.deletable is False
    assert preview.pending_sessions == [sub1]


# ---------------------------------------------------------------------------
# apply() -- refusal on pending
# ---------------------------------------------------------------------------


async def test_apply_refuses_and_deletes_nothing_when_pending(
    service: DeletionService,
    graph: GraphState,
    blob_store: AsyncDiskBlobStore,
    queue_manager: QueueManager,
) -> None:
    root, sub1 = "ap-root", "ap-sub1"
    blob_uri = await blob_store.write(root, "k1", {"v": 1})
    await graph.upsert_node(root, {"labels": ["Session", "RootSession"]})
    await graph.upsert_node(sub1, {"labels": ["Session", "SubSession"]})
    await graph.upsert_edge(root, sub1, {"type": "HAS_SUBSESSION"})
    await queue_manager.append(sub1, b'{"event": "tool:pre"}')  # uncommitted

    with pytest.raises(SessionsPendingError, match="pending") as excinfo:
        await service.apply(root)
    # The retryable refusal names exactly which sessions are still draining.
    assert excinfo.value.pending_sessions == [sub1]
    assert excinfo.value.root_id == root

    # Nothing deleted: graph, blob, and queue artifacts all survive.
    assert await graph.get_node(root) is not None
    assert await graph.get_node(sub1) is not None
    assert await blob_store.list(root) == [blob_uri]
    assert queue_manager._log_path(sub1).exists()


# ---------------------------------------------------------------------------
# apply() -- full multi-session deletion
# ---------------------------------------------------------------------------


async def test_apply_deletes_graph_blobs_and_queue_for_every_session(
    service: DeletionService,
    graph: GraphState,
    blob_store: AsyncDiskBlobStore,
    queue_manager: QueueManager,
) -> None:
    root, sub1, sub2, concept = "ad-root", "ad-sub1", "ad-sub2", "ad-agent"
    blob_refs = await _build_drained_multi_session_graph(
        graph,
        blob_store,
        queue_manager,
        root=root,
        sub1=sub1,
        sub2=sub2,
        concept=concept,
    )

    result = await service.apply(sub1, requested_by="tester")

    assert result is not None
    assert result.root_id == root
    assert result.session_count == 3
    assert result.nodes_deleted == 3  # root, sub1, sub2 -- concept excluded
    assert result.relationships_deleted == 3  # root->sub1, sub1->sub2, sub2->concept
    assert result.blobs_deleted == len(blob_refs) == 4
    assert result.queue_sessions_cleaned == 3

    # Graph: every owned session node gone, shared concept survives.
    for sid in (root, sub1, sub2):
        assert await graph.get_node(sid) is None
    concept_node = await graph.get_node(concept)
    assert concept_node is not None
    assert "SST_CONCEPT" in concept_node.get("labels", [])

    # Blobs: every graph session's blob dir gone.
    for sid in (root, sub1, sub2):
        assert await blob_store.list(sid) == []

    # Queue: log/offset/dead-letter gone for every graph session.
    for sid in (root, sub1, sub2):
        assert not queue_manager._log_path(sid).exists()
        assert not queue_manager._offset_path(sid).exists()
        assert not queue_manager._dead_path(sid).exists()


async def test_apply_unknown_session_returns_none(service: DeletionService) -> None:
    assert await service.apply("does-not-exist") is None


async def test_apply_logs_the_deletion(
    service: DeletionService,
    graph: GraphState,
    blob_store: AsyncDiskBlobStore,
    queue_manager: QueueManager,
    caplog: pytest.LogCaptureFixture,
) -> None:
    root, sub1, sub2, concept = "al-root", "al-sub1", "al-sub2", "al-agent"
    await _build_drained_multi_session_graph(
        graph,
        blob_store,
        queue_manager,
        root=root,
        sub1=sub1,
        sub2=sub2,
        concept=concept,
    )

    with caplog.at_level(logging.INFO, logger="context_intelligence_server.deletion"):
        result = await service.apply(root, requested_by="colombod")

    assert result is not None
    assert any(
        r.levelno == logging.INFO
        and "session_deletion_applied" in r.getMessage()
        and f"root_id={root}" in r.getMessage()
        and "requested_by=colombod" in r.getMessage()
        and getattr(r, "session_id", None) == root
        for r in caplog.records
    ), "the applied deletion must be logged once at INFO with root_id/requested_by"


async def test_preview_and_apply_count_every_blob_the_store_holds(
    service: DeletionService,
    graph: GraphState,
    blob_store: AsyncDiskBlobStore,
    queue_manager: QueueManager,
) -> None:
    """Blob counting goes through the blob store, not through node data.

    A blob written for a graph session is counted and deleted even though no
    node property points at it -- the blob store already knows which blobs
    belong to a session (``BlobStore.list``), so there is nothing left to
    "reconcile" against a separate, node-derived count.
    """
    root, sub1, sub2, concept = "rm-root", "rm-sub1", "rm-sub2", "rm-agent"
    blob_refs = await _build_drained_multi_session_graph(
        graph,
        blob_store,
        queue_manager,
        root=root,
        sub1=sub1,
        sub2=sub2,
        concept=concept,
    )
    # A blob with no corresponding node property -- still a real file the
    # blob store holds for this session.
    await blob_store.write(sub2, "extra", {"v": "extra"})

    preview = await service.preview(root)
    assert preview is not None
    assert preview.blob_count == len(blob_refs) + 1

    result = await service.apply(root, requested_by="tester")

    assert result is not None
    assert result.blobs_deleted == len(blob_refs) + 1
    for sid in (root, sub1, sub2):
        assert await blob_store.list(sid) == []
