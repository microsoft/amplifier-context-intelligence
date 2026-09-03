"""Routes for deleting a session's stored data.

These routes are a thin layer over ``DeletionService``. Each route reads the
request, builds the graph store, blob store, and queue manager, creates a
``DeletionService`` with them, calls it, and turns the answer into JSON. The
routes do not talk to Neo4j or the file system on their own --
``DeletionService`` already knows how to do that.

There are two routes, split by HTTP method instead of a query flag:

- ``GET /sessions/{session_id}/summary`` -- read-only. Reports what deleting
  this session's data would do, without deleting anything. This is the
  preview step.
- ``DELETE /sessions/{session_id}`` -- always performs the delete. There is
  no dry-run flag here any more: the GET route above is the preview, and
  this route is the one that actually removes the data.

Neither route takes a ``workspace`` query parameter. A session id is the
unique identifier the caller passes; the server looks up which workspace
that session lives in on its own (see ``Neo4jGraphStore.resolve_session_graph``).
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request

from context_intelligence_server.authz import require_read, require_write
from context_intelligence_server.blob_store import AsyncDiskBlobStore
from context_intelligence_server.config import get_settings
from context_intelligence_server.deletion import (
    DeletionPreview,
    DeletionResult,
    DeletionService,
    SessionsPendingError,
)
from context_intelligence_server.graph_store import AmbiguousSessionError
from context_intelligence_server.neo4j_store import Neo4jGraphStore

logger = logging.getLogger(__name__)

router = APIRouter()

# Plain-language detail shown to the caller when a session id is found in
# more than one workspace. This should not happen in practice (session ids
# are unique) -- see AmbiguousSessionError for why the server refuses to
# guess rather than picking one workspace.
_AMBIGUOUS_SESSION_DETAIL = (
    "the session id exists in more than one workspace; this should not "
    "happen and needs to be looked into before it can be resolved"
)


def _iso(value: datetime | None) -> str | None:
    """Turn a datetime into a plain ISO-8601 string, or leave ``None`` as ``None``."""
    return value.isoformat() if value is not None else None


def _preview_to_dict(preview: DeletionPreview) -> dict[str, Any]:
    """Turn a DeletionPreview into a plain dict, ready to send as JSON."""
    return {
        "root_id": preview.root_id,
        "session_ids": sorted(preview.session_ids),
        "node_count": preview.node_count,
        "edge_count": preview.edge_count,
        "blob_count": preview.blob_count,
        "created_by": preview.created_by,
        "started_at": _iso(preview.started_at),
        "last_change": _iso(preview.last_change),
        "subsession_count": preview.subsession_count,
        "workspace": preview.workspace,
        "working_dir": preview.working_dir,
        "deletable": preview.deletable,
        "pending_sessions": preview.pending_sessions,
    }


def _result_to_dict(result: DeletionResult) -> dict[str, Any]:
    """Turn a DeletionResult into a plain dict, ready to send as JSON."""
    return {
        "root_id": result.root_id,
        "session_count": result.session_count,
        "nodes_deleted": result.nodes_deleted,
        "relationships_deleted": result.relationships_deleted,
        "blobs_deleted": result.blobs_deleted,
        "queue_sessions_cleaned": result.queue_sessions_cleaned,
    }


def _build_service(graph_store: Neo4jGraphStore, request: Request) -> DeletionService:
    """Assemble a DeletionService from one graph store plus the blob store
    and queue manager every route uses the same way."""
    settings = get_settings()
    blob_store = AsyncDiskBlobStore(root=settings.blob_path)
    queue_manager = request.app.state.registry.queue_manager
    return DeletionService(graph_store, blob_store, queue_manager)


async def read_deletion_service(request: Request) -> DeletionService:
    """Build a DeletionService that only reads, through the read-only Neo4j
    connection (``app.state.neo4j_query_driver``).

    Used by the summary route. No workspace is supplied here -- the graph
    store looks up which workspace a session id belongs to on its own.
    """
    graph_store = Neo4jGraphStore(
        uri="",
        driver=request.app.state.neo4j_query_driver,
    )
    return _build_service(graph_store, request)


async def delete_route_service(request: Request) -> DeletionService:
    """Build the DeletionService the delete route uses.

    The delete route always changes stored data, so it always uses the
    admin Neo4j connection (``app.state.neo4j_driver``) -- unlike the
    summary route, there is no read-only path here any more, because there
    is no more dry run on this route. No workspace is supplied -- the graph
    store looks up which workspace a session id belongs to on its own.
    """
    graph_store = Neo4jGraphStore(uri="", driver=request.app.state.neo4j_driver)
    return _build_service(graph_store, request)


def _caller_id(request: Request) -> str | None:
    """Return the authenticated caller's id, or None when auth is off.

    The auth middleware stores this under ``contributor_id`` in the request's
    scope state (see ``authz.py`` and ``routers/admin.py`` for the same
    read).
    """
    state: dict = request.scope.get("state", {})
    return state.get("contributor_id")


@router.get(
    "/sessions/{session_id}/summary",
    dependencies=[Depends(require_read)],
)
async def get_session_summary(
    session_id: str,
    service: DeletionService = Depends(read_deletion_service),
) -> dict[str, Any]:
    """Report what deleting this session's data would do. Deletes nothing.

    Returns 404 when ``session_id`` does not match any known session, and
    409 when ``session_id`` is somehow found in more than one workspace
    (see ``AmbiguousSessionError`` -- this should not happen in practice).
    """
    try:
        preview = await service.preview(session_id)
    except AmbiguousSessionError as exc:
        raise HTTPException(status_code=409, detail=_AMBIGUOUS_SESSION_DETAIL) from exc
    if preview is None:
        raise HTTPException(status_code=404, detail=f"session {session_id!r} not found")
    return _preview_to_dict(preview)


@router.delete(
    "/sessions/{session_id}",
    dependencies=[Depends(require_write)],
)
async def delete_session(
    session_id: str,
    request: Request,
    service: DeletionService = Depends(delete_route_service),
) -> dict[str, Any]:
    """Delete a session's data. This always deletes -- to preview what would
    be removed first, without deleting anything, call
    ``GET /sessions/{session_id}/summary``.

    Returns 404 when ``session_id`` does not match any known session, and 409
    in two distinct, machine-distinguishable cases:

    - The session (or a related session in the same graph) still has undrained
      queue records -- ``reason: "sessions_pending"``. This is transient and
      **retryable**: the response carries a ``Retry-After`` header and a body
      ``retry_after_seconds`` + ``pending_sessions`` so the caller can back off
      and retry once the drain finishes.
    - The session id is somehow found in more than one workspace
      (``AmbiguousSessionError``) -- not retryable; no ``Retry-After``.
    """
    try:
        result = await service.apply(session_id, requested_by=_caller_id(request))
    except SessionsPendingError as exc:
        retry_after = get_settings().delete_retry_after_seconds
        raise HTTPException(
            status_code=409,
            detail={
                "reason": "sessions_pending",
                "message": str(exc),
                "pending_sessions": exc.pending_sessions,
                "retry_after_seconds": retry_after,
            },
            headers={"Retry-After": str(retry_after)},
        ) from exc
    except AmbiguousSessionError as exc:
        raise HTTPException(status_code=409, detail=_AMBIGUOUS_SESSION_DETAIL) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    if result is None:
        raise HTTPException(status_code=404, detail=f"session {session_id!r} not found")
    return _result_to_dict(result)
