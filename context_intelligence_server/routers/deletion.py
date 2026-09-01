"""Routes for deleting a session's stored data.

These routes are a thin layer over ``DeletionService``. Each route reads the
request, builds the graph store, blob store, and queue manager for one
workspace, creates a ``DeletionService`` with them, calls it, and turns the
answer into JSON. The routes do not talk to Neo4j or the file system on their
own -- ``DeletionService`` already knows how to do that.

There are two routes:

- ``GET /sessions/{session_id}/summary`` -- read-only. Reports what deleting
  this session's data would do, without deleting anything.
- ``DELETE /sessions/{session_id}`` -- with ``apply=false`` (the default)
  this is the same dry run as the GET route. With ``apply=true`` it actually
  deletes the data.
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
)
from context_intelligence_server.neo4j_store import Neo4jGraphStore

logger = logging.getLogger(__name__)

router = APIRouter()


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


async def read_deletion_service(request: Request, workspace: str) -> DeletionService:
    """Build a DeletionService that only reads, through the read-only Neo4j
    connection (``app.state.neo4j_query_driver``).

    Used by the summary route and by the delete route's dry run. Both only
    read, so both use the read-only connection -- a read never goes through
    the admin connection.
    """
    graph_store = Neo4jGraphStore(
        uri="",
        workspace=workspace,
        driver=request.app.state.neo4j_query_driver,
    )
    return _build_service(graph_store, request)


async def delete_route_service(
    request: Request, workspace: str, apply: bool = False
) -> DeletionService:
    """Build the DeletionService the delete route uses, choosing the Neo4j
    connection by what the route is about to do.

    A dry run (``apply`` is false) only reads, so it uses the read-only
    connection (``app.state.neo4j_query_driver``). A real delete (``apply`` is
    true) changes stored data, so it uses the admin connection
    (``app.state.neo4j_driver``). A read never goes through the admin
    connection, and a change always does.
    """
    driver = (
        request.app.state.neo4j_driver
        if apply
        else request.app.state.neo4j_query_driver
    )
    graph_store = Neo4jGraphStore(uri="", workspace=workspace, driver=driver)
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

    Returns 404 when ``session_id`` does not match any known session.
    """
    preview = await service.preview(session_id)
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
    apply: bool = False,
    service: DeletionService = Depends(delete_route_service),
) -> dict[str, Any]:
    """Delete a session's data, or preview what deleting it would do.

    With ``apply=false`` (the default) nothing is deleted: the response is
    the same preview the GET summary route returns, and it only reads
    (through the read-only connection).

    With ``apply=true`` the data is actually deleted through the admin
    connection, and the response reports what was removed.

    Returns 404 when ``session_id`` does not match any known session, and
    409 when the session (or a related session in the same graph) is still
    receiving data and has not finished being written yet.
    """
    if not apply:
        preview = await service.preview(session_id)
        if preview is None:
            raise HTTPException(
                status_code=404, detail=f"session {session_id!r} not found"
            )
        return _preview_to_dict(preview)

    try:
        result = await service.apply(session_id, requested_by=_caller_id(request))
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    if result is None:
        raise HTTPException(status_code=404, detail=f"session {session_id!r} not found")
    return _result_to_dict(result)
