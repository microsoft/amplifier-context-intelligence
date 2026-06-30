"""Queue inspection endpoints — dead-letter aggregation.

These routes are authenticated by default: ``/queues/*`` is intentionally NOT
added to the BearerTokenMiddleware exempt set, so they require a valid bearer
token when an API key is configured.
"""

from __future__ import annotations

import base64
import json
import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException  # noqa: F401  (HTTPException per spec)
from fastapi.requests import Request

from context_intelligence_server.authz import require_read, require_write

logger = logging.getLogger(__name__)

router = APIRouter()


def _decode_payload(record: dict[str, Any]) -> bytes:
    """Return the original payload bytes from a dead-letter record.

    A record stores its payload either as a UTF-8 string under ``payload`` or,
    for non-UTF-8 data, base64-encoded under ``payload_b64``. Raises
    ``ValueError`` when neither field is present.
    """
    if "payload" in record:
        return str(record["payload"]).encode("utf-8")
    if "payload_b64" in record:
        return base64.b64decode(record["payload_b64"])
    raise ValueError("dead-letter record missing both 'payload' and 'payload_b64'")


@router.get("/queues/dead-letter", dependencies=[Depends(require_read)])
async def list_dead_letters(request: Request) -> dict[str, Any]:
    """List dead-letter queues with per-worker record counts and last error.

    Aggregates one entry per worker key that has dead-letter records. Worker
    keys with an empty dead-letter file are skipped.
    """
    registry = request.app.state.registry
    qm = registry.queue_manager

    entries: list[dict[str, Any]] = []
    for worker_key in await qm.dead_letter_keys():
        records = await qm.read_dead_letters(worker_key)
        if not records:
            continue
        last = records[-1]
        entries.append(
            {
                "worker_key": worker_key,
                "item_count": len(records),
                "last_error": last.get("error", ""),
                "last_ts": last.get("ts"),
            }
        )
    return {"dead_letters": entries}


@router.post(
    "/queues/dead-letter/{worker_key:path}/purge",
    dependencies=[Depends(require_write)],
)
async def purge_dead_letters(worker_key: str, request: Request) -> dict[str, Any]:
    """Purge all dead-letter records for ``worker_key``.

    Routes deletion exclusively through ``QueueManager.purge_dead_letters`` (no
    raw filesystem access). Returns the worker key and the number of records
    purged (0 when none exist). An unsafe worker key yields a 400.
    """
    registry = request.app.state.registry
    try:
        purged = await registry.queue_manager.purge_dead_letters(worker_key)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"worker_key": worker_key, "purged": purged}


@router.post(
    "/queues/dead-letter/{worker_key:path}/replay",
    dependencies=[Depends(require_write)],
)
async def replay_dead_letters(worker_key: str, request: Request) -> dict[str, Any]:
    """Re-enqueue every dead-letter record for ``worker_key`` then purge them.

    Each record's original payload is decoded and appended back onto the
    worker's durable log (re-enqueued), ensuring its owning worker exists via
    ``get_or_create``. ALL records are appended BEFORE the dead-letter file is
    purged, so a mid-loop failure can never lose a record (a re-appended
    duplicate is a harmless MERGE no-op downstream).

    Conservation: replayed records were already counted as ``accepted`` at the
    original ingest, so ``record_accepted`` is intentionally NOT called here —
    replay only moves a line from dead -> in_queue. Only ``record_replayed`` is
    advanced. An unsafe worker key yields a 400.
    """
    registry = request.app.state.registry
    qm = registry.queue_manager
    try:
        records = await qm.read_dead_letters(worker_key)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not records:
        return {"worker_key": worker_key, "replayed": 0}

    replayed = 0
    for record in records:
        raw = _decode_payload(record)
        obj = json.loads(raw)
        workspace = obj.get("workspace", "")
        registry.get_or_create(worker_key, workspace)
        await qm.append(worker_key, raw)
        replayed += 1

    await qm.purge_dead_letters(worker_key)
    registry.record_replayed(replayed)
    return {"worker_key": worker_key, "replayed": replayed}
