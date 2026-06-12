"""Queue inspection endpoints — dead-letter aggregation.

These routes are authenticated by default: ``/queues/*`` is intentionally NOT
added to the BearerTokenMiddleware exempt set, so they require a valid bearer
token when an API key is configured.
"""

from __future__ import annotations

import base64
import json  # noqa: F401  (imported per spec for payload decoding parity)
import logging
from typing import Any

from fastapi import APIRouter, HTTPException  # noqa: F401  (HTTPException per spec)
from fastapi.requests import Request

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


@router.get("/queues/dead-letter")
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
