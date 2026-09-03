"""DeletionService -- composes the storage primitives to delete a session graph.

See docs/02-server-design.md (DELETE section, "Abstraction principle",
"Whole-graph, not one session") for the design this implements.

This service composes ONLY the public Protocol-level APIs of ``GraphStore``,
``BlobStore``, and ``QueueManager`` -- it never talks to Neo4j or the
filesystem directly. Each backend's delete lives at its own storage
abstraction; this module holds orchestration, precondition enforcement,
ordering, and logging only.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from context_intelligence_server.blob_store import BlobStore
from context_intelligence_server.graph_store import GraphStore

logger = logging.getLogger(__name__)


class _QueueManagerLike(Protocol):
    """The subset of ``QueueManager`` this service composes."""

    async def pending_count(self, session_id: str) -> int: ...

    async def delete_session(self, session_id: str) -> bool: ...


class SessionsPendingError(Exception):
    """Raised by ``apply`` when the graph still has undrained queue records.

    The delete is refused (nothing is written) because one or more sessions in
    the graph still have complete-but-uncommitted queue lines left to drain.
    This is a *transient, retryable* condition: once the drain finishes,
    ``pending_count`` returns to 0 and the same delete succeeds. The offending
    session ids are carried on ``pending_sessions`` so the caller can report
    exactly what is still in flight and can be told, machine-readably, to retry.
    """

    def __init__(self, root_id: str, pending_sessions: list[str]) -> None:
        self.root_id = root_id
        self.pending_sessions = pending_sessions
        super().__init__(
            f"apply refused: graph root={root_id!r} has pending "
            f"(uncommitted) session(s) {pending_sessions!r}; drain before "
            "deleting -- nothing was deleted"
        )


@dataclass(frozen=True)
class DeletionPreview:
    """Dry-run facts for a whole session graph -- mutates nothing.

    Exactly what the router surfaces before ``apply``. ``blob_count`` and the
    node/edge counts are the whole-graph totals (see ``SessionGraph``), not
    the passed session's alone. ``deletable`` is False whenever any session in
    the graph still has pending (uncommitted) queue records; ``pending_sessions``
    names them.
    """

    root_id: str
    session_ids: frozenset[str]
    node_count: int
    edge_count: int
    blob_count: int
    created_by: str | None
    started_at: datetime | None
    last_change: datetime | None
    subsession_count: int
    workspace: str
    working_dir: str | None
    deletable: bool
    pending_sessions: list[str]


@dataclass(frozen=True)
class DeletionResult:
    """Per-backend counts for an applied whole-graph deletion."""

    root_id: str
    session_count: int
    nodes_deleted: int
    relationships_deleted: int
    blobs_deleted: int
    queue_sessions_cleaned: int


class DeletionService:
    """Composes ``GraphStore``, ``BlobStore``, and ``QueueManager`` to delete
    a whole session graph -- root + all descendants -- plus every blob and
    queue artifact it references.

    Constructed via dependency injection; never constructs or reaches into
    Neo4j/the filesystem itself.
    """

    def __init__(
        self,
        graph_store: GraphStore,
        blob_store: BlobStore,
        queue_manager: _QueueManagerLike,
    ) -> None:
        self._graph = graph_store
        self._blobs = blob_store
        self._queue = queue_manager

    async def _pending_sessions(self, session_ids: frozenset[str]) -> list[str]:
        """Return the sorted subset of *session_ids* with pending queue records."""
        pending: list[str] = []
        for sid in sorted(session_ids):
            if await self._queue.pending_count(sid) > 0:
                pending.append(sid)
        return pending

    async def _blob_count(self, session_ids: frozenset[str]) -> int:
        """Return the total number of blobs stored for every session in *session_ids*.

        The blob store is keyed by session id and already knows every blob it
        holds for a session (``BlobStore.list``), so this asks the blob store
        directly instead of trying to find blob markers hidden inside graph
        node data. This also means a blob is counted even if no node happens
        to reference it -- the blob store is the one place that knows what it
        is actually holding.
        """
        total = 0
        for sid in session_ids:
            total += len(await self._blobs.list(sid))
        return total

    async def preview(self, session_id: str) -> DeletionPreview | None:
        """Resolve the whole session graph for *session_id* and report what
        deleting it would do -- mutates nothing.

        Returns ``None`` if *session_id* does not resolve to any known session.
        """
        graph = await self._graph.resolve_session_graph(session_id)
        if graph is None:
            return None

        pending_sessions = await self._pending_sessions(graph.session_ids)
        blob_count = await self._blob_count(graph.session_ids)
        return DeletionPreview(
            root_id=graph.root_id,
            session_ids=graph.session_ids,
            node_count=graph.node_count,
            edge_count=graph.edge_count,
            blob_count=blob_count,
            created_by=graph.created_by,
            started_at=graph.started_at,
            last_change=graph.last_change,
            subsession_count=graph.subsession_count,
            workspace=graph.workspace,
            working_dir=graph.working_dir,
            deletable=not pending_sessions,
            pending_sessions=pending_sessions,
        )

    async def apply(
        self, session_id: str, *, requested_by: str | None = None
    ) -> DeletionResult | None:
        """Permanently delete the whole session graph for *session_id*.

        Resolves the graph, enforces the drain precondition (every session in
        the graph must have zero pending queue records) across the WHOLE
        graph, then deletes in order: graph -> blobs (per session) -> queue
        artifacts (per session). ``session_ids`` is captured from the
        resolution BEFORE any delete, so losing the graph node set first does
        not lose track of what else must be removed.

        Returns ``None`` if *session_id* does not resolve to any known
        session -- no writes occur in that case.

        Raises:
            SessionsPendingError: If any session in the graph still has pending
                (uncommitted) queue records -- refuses, deletes nothing. This is
                retryable: it carries ``pending_sessions`` and clears once drained.
            RuntimeError: If the graph vanishes between resolve and delete.
        """
        graph = await self._graph.resolve_session_graph(session_id)
        if graph is None:
            return None

        session_ids = graph.session_ids

        pending_sessions = await self._pending_sessions(session_ids)
        if pending_sessions:
            raise SessionsPendingError(graph.root_id, pending_sessions)

        graph_result = await self._graph.delete_session_graph(session_id)
        if graph_result is None:
            raise RuntimeError(
                f"apply: graph for root={graph.root_id!r} vanished between "
                "resolve and delete -- no writes were attempted"
            )

        blobs_deleted = 0
        for sid in session_ids:
            blobs_deleted += await self._blobs.delete_session(sid)

        queue_sessions_cleaned = 0
        for sid in session_ids:
            if await self._queue.delete_session(sid):
                queue_sessions_cleaned += 1

        result = DeletionResult(
            root_id=graph.root_id,
            session_count=len(session_ids),
            nodes_deleted=graph_result.nodes_deleted,
            relationships_deleted=graph_result.relationships_deleted,
            blobs_deleted=blobs_deleted,
            queue_sessions_cleaned=queue_sessions_cleaned,
        )

        logger.info(
            "session_deletion_applied root_id=%s created_by=%s requested_by=%s "
            "session_count=%d nodes_deleted=%d relationships_deleted=%d "
            "blobs_deleted=%d queue_sessions_cleaned=%d",
            result.root_id,
            graph.created_by,
            requested_by,
            result.session_count,
            result.nodes_deleted,
            result.relationships_deleted,
            result.blobs_deleted,
            result.queue_sessions_cleaned,
            extra={"session_id": result.root_id},
        )
        return result
