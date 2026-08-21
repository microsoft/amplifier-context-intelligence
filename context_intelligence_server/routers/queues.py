"""Queue inspection endpoints — dead-letter aggregation, and operator GC.

These routes are authenticated by default: ``/queues/*`` is intentionally NOT
added to the BearerTokenMiddleware exempt set, so they require a valid bearer
token when an API key is configured.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import time
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from fastapi.requests import Request

from context_intelligence_server.authz import require_read, require_write
from context_intelligence_server.config import get_settings
from context_intelligence_server.models import GcApplyRequest
from context_intelligence_server.status import boot_state

logger = logging.getLogger(__name__)

router = APIRouter()

# Closed set of exclusion tokens the scan can report. Always
# present in the response, zero-filled, so the shape never varies with which
# exclusions happened to fire this pass.
_EXCLUDED_KEYS = (
    "live_worker",
    "undrained_tail",
    "no_offset",
    "torn_tail",
    "too_young",
    "log_present",
    "unreadable",
)


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
    registry.record_purged(purged)
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


async def _gc_report(
    request: Request,
    *,
    apply: bool,
    max_delete: int | None = None,
) -> dict[str, Any]:
    """Shared implementation for GET /queues/gc (preview) and POST /queues/gc/apply.

    ``apply=False`` (preview) NEVER reaches a deleter: it calls only
    ``scan_gc_candidates`` (stat/read only, per that method's own contract)
    and reports every candidate with ``action="preview"``. This is the
    mechanical form of "preview deletes NOTHING" -- there is no deleter in
    reach from this branch, not a flag that could be misread.

    ``apply=True`` re-verifies each candidate immediately before deleting,
    via the existing gate-verified primitives (``delete_drained`` /
    ``purge_dead_letters(require_logless=True)``) -- never a second,
    hand-rolled deletion path.
    """
    registry = request.app.state.registry
    qm = registry.queue_manager
    settings = get_settings()
    now = time.time()

    is_owned = frozenset(registry.active_sessions()).__contains__
    candidates, excluded_counts, scanned_keys = await qm.scan_gc_candidates(
        now,
        settings.gc_queue_ttl_seconds,
        settings.dead_letter_retention_seconds,
        is_owned,
    )

    excluded = {key: excluded_counts.get(key, 0) for key in _EXCLUDED_KEYS}

    queue_log_candidates = [c for c in candidates if c.kind == "queue_log"]
    dead_letter_candidates = [c for c in candidates if c.kind == "dead_letter"]
    totals = {
        "queue_log": {
            "count": len(queue_log_candidates),
            "bytes": sum(c.bytes for c in queue_log_candidates),
        },
        "dead_letter": {
            "count": len(dead_letter_candidates),
            "bytes": sum(c.bytes for c in dead_letter_candidates),
        },
        "total_bytes": sum(c.bytes for c in candidates),
    }

    # Largest-first within each class, deterministic tie-break by key (§4.2):
    # maximises bytes reclaimed per bounded pass.
    ordered = sorted(queue_log_candidates, key=lambda c: (-c.bytes, c.key)) + sorted(
        dead_letter_candidates, key=lambda c: (-c.bytes, c.key)
    )

    # A request body may only LOWER the configured ceiling, never raise it.
    bound = settings.gc_max_delete_per_pass
    if apply and max_delete is not None:
        bound = min(max_delete, settings.gc_max_delete_per_pass)

    deleted = 0
    skipped = 0
    failed = 0
    bytes_reclaimed_estimate = 0
    bounded_by_max_delete = False
    result_candidates: list[dict[str, Any]] = []

    for c in ordered:
        entry: dict[str, Any] = {
            "key": c.key,
            "class": c.kind,
            "path": c.path,
            "bytes": c.bytes,
            "age_seconds": c.age_seconds,
            "reason": c.reason,
            "records": c.records,
            "action": "preview",
            "skip_reason": None,
        }

        if not apply:
            result_candidates.append(entry)
            continue

        if deleted + skipped + failed >= bound:
            bounded_by_max_delete = True
            result_candidates.append(entry)
            continue

        if c.kind == "queue_log":
            if registry.has_worker(c.key):
                entry["action"] = "skipped"
                entry["skip_reason"] = "live_worker"
                skipped += 1
                result_candidates.append(entry)
                continue
            try:
                mtime = (await asyncio.to_thread(os.stat, c.path)).st_mtime
                age = now - mtime
            except OSError:
                age = None
            if age is not None and not (age > settings.gc_queue_ttl_seconds):
                entry["action"] = "skipped"
                entry["skip_reason"] = "too_young"
                skipped += 1
                result_candidates.append(entry)
                continue
            # Log BEFORE the unlink -- load-bearing (house rule): a crash
            # mid-unlink still leaves a record of the intent.
            logger.info(
                "gc_delete key=%s class=queue_log bytes=%d age=%.0f reason=%s",
                c.key,
                c.bytes,
                c.age_seconds,
                c.reason,
            )
            try:
                ok = await qm.delete_drained(c.key)
            except OSError:
                entry["action"] = "failed"
                failed += 1
                logger.exception("gc_delete_failed key=%s class=queue_log", c.key)
                result_candidates.append(entry)
                continue
            if ok:
                entry["action"] = "deleted"
                deleted += 1
                bytes_reclaimed_estimate += c.bytes
            else:
                # delete_drained's own in-lock re-verify refused -- the log
                # grew (an uncommitted append landed) since this scan.
                entry["action"] = "skipped"
                entry["skip_reason"] = "changed"
                skipped += 1
            result_candidates.append(entry)
        else:  # dead_letter
            try:
                mtime = (await asyncio.to_thread(os.stat, c.path)).st_mtime
                age = now - mtime
            except OSError:
                age = None
            if age is not None and not (age > settings.dead_letter_retention_seconds):
                entry["action"] = "skipped"
                entry["skip_reason"] = "too_young"
                skipped += 1
                result_candidates.append(entry)
                continue
            logger.info(
                "gc_delete key=%s class=dead_letter bytes=%d records=%d reason=%s",
                c.key,
                c.bytes,
                c.records,
                c.reason,
            )
            try:
                n = await qm.purge_dead_letters(c.key, require_logless=True)
            except (OSError, ValueError):
                entry["action"] = "failed"
                failed += 1
                logger.exception("gc_delete_failed key=%s class=dead_letter", c.key)
                result_candidates.append(entry)
                continue
            if n == -1:
                # In-lock refusal: a `.log` reappeared for this key (e.g. a
                # replay) since this scan.
                entry["action"] = "skipped"
                entry["skip_reason"] = "log_present"
                skipped += 1
            else:
                entry["action"] = "deleted"
                deleted += 1
                bytes_reclaimed_estimate += c.bytes
                registry.record_purged(n)
            result_candidates.append(entry)

    applied = {
        "deleted": deleted,
        "skipped": skipped,
        "failed": failed,
        # R7: a scan-time ESTIMATE, not a byte-exact accounting of what was
        # actually unlinked (delete_drained/purge_dead_letters can return a
        # success for an already-absent file) -- labelled honestly rather
        # than silently over-reporting.
        "bytes_reclaimed_estimate": bytes_reclaimed_estimate if apply else 0,
        "max_delete": bound,
        "bounded_by_max_delete": bounded_by_max_delete,
    }

    return {
        "mode": "apply" if apply else "preview",
        "scanned_keys": scanned_keys,
        "candidates": result_candidates,
        "totals": totals,
        "excluded": excluded,
        "applied": applied,
    }


@router.get("/queues/gc", dependencies=[Depends(require_read)])
async def preview_gc(request: Request) -> dict[str, Any]:
    """PREVIEW safe-to-delete queue logs and dead-letters. Never writes.

    Calls exactly one queue method (``scan_gc_candidates``), whose own
    contract is stat()-and-read only -- there is no code path from this
    route to an unlink. UNGATED on boot phase (R1): an operator reaching a
    spool-full box during a slow boot is exactly when this is most wanted.
    """
    return await _gc_report(request, apply=False)


@router.post("/queues/gc/apply", dependencies=[Depends(require_write)])
async def apply_gc(
    request: Request, body: GcApplyRequest | None = None
) -> dict[str, Any]:
    """APPLY: delete exactly the previewed-safe artifacts.

    Refused with 409 unless the server has finished booting
    (``boot_state.phase in ("ready", "failed")`` -- R1, BLOCKER): applying
    during the boot window can race the no-lock reconcile/seed passes and
    (for dead-letters) land before ``recovery_seed_counts`` has seeded the
    conservation counters for a key this pass just purged.

    ``body`` is OPTIONAL (R6): a bodyless POST with a valid token must
    succeed, never 422.
    """
    if boot_state.phase not in ("ready", "failed"):
        raise HTTPException(
            status_code=409,
            detail=(
                "GC apply refused: server is still booting "
                f"(phase={boot_state.phase!r}). Preview (GET /queues/gc) "
                "remains available; retry apply once boot completes."
            ),
        )
    max_delete = body.max_delete if body is not None else None
    return await _gc_report(request, apply=True, max_delete=max_delete)
