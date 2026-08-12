"""Admin API — CRUD over the live identity stores.

Endpoints manage the entra-identities and api-keys stores at runtime with no
server restart. Mutations flow through ``IdentityStore.put`` / ``delete``, which
use the ROB-F2 commit order (write-file-then-swap-memory), so the persistent
file and the in-process dict are always in sync.

**Store access pattern**

Each endpoint reads its store from ``request.app.state``
(``api_key_store`` or ``entra_identity_store``), populated by
``create_asgi_app()`` in ``main.py``. This avoids a circular import between
the router and the main module. If the relevant store is ``None`` for the
current ``auth_mode``, the endpoint returns **503**.

**Auth seam — T4 placeholder**

``require_admin`` is a NO-OP dependency applied to the whole ``/admin`` router
via ``APIRouter(dependencies=[Depends(require_admin)])``. T5 replaces the
function body with real enforcement (static: admin_api_key; entra: IdentityAdmin
App Role) — the routes and tests do not change. Tests override it via::

    app.dependency_overrides[require_admin] = lambda: None

**Guards (T6)**

Path-param and body validation follow the same rules as ``config.py``
(reuse ``_GUID_RE``, ``_ALL_ZEROS_GUID``, and the 64-hex pattern).  Invalid
inputs raise **422**.  The admin key hash is un-deletable and un-shadowable via
the data store (**409**).  Every successful mutation emits ONE structured audit
log line to stdout → Log Analytics; raw keys are NEVER logged.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from neo4j import READ_ACCESS, WRITE_ACCESS
from pydantic import BaseModel, field_validator

from context_intelligence_server.config import _ALL_ZEROS_GUID, _GUID_RE, get_settings
from context_intelligence_server.identity_store import IdentityStore

# ---------------------------------------------------------------------------
# Validation constants
# ---------------------------------------------------------------------------

# Mirrors the 64-hex check in config._validate_api_keys (config.py:172-179).
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")

# Maximum contributor id length (TB-12).  Matches the cap implied by config
# (non-empty, non-whitespace, sane upper bound for an identifier string).
_MAX_CONTRIBUTOR_LEN = 256

# ---------------------------------------------------------------------------
# Blob-reclaim constants (see docs/plans/2026-08-12-blob-reclaim-endpoint-spec.md,
# "Council amendment -- AUTHORITATIVE" section for the governing design)
# ---------------------------------------------------------------------------

# B3 (council amendment): hard mtime-floor safety net, defense-in-depth behind
# the durable undrained-queue gate. min_age_minutes below this is rejected
# (422) rather than silently raised -- a caller passing 0 must not be able to
# disable the age gate entirely.
_MIN_AGE_FLOOR_MINUTES = 15

# "sample" is bounded to keep the response small; totals (orphans_found,
# reclaimable_bytes) remain authoritative even when the sample is truncated.
_MAX_SAMPLE = 50

# ---------------------------------------------------------------------------
# Module-level audit logger
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Auth seam — real enforcement (T5)
# ---------------------------------------------------------------------------


def require_admin(request: Request) -> None:
    """Router-level admin guard — enforces admin authority on all /admin/* requests.

    Applied router-wide via ``APIRouter(dependencies=[Depends(require_admin)])``.

    Security model (design §6, T5):
    - The middleware (BearerTokenMiddleware) ALWAYS enforces authentication on
      /admin/* paths (they are never in an exempt set — TB-07 startup assertion).
      A missing/invalid token → 401 before this function is ever reached.
    - This dependency enforces *authorization* (not authentication): the request
      has already been authenticated; here we check whether the authenticated
      principal has admin authority.

    Static mode logic:
    - ``is_admin=True`` on scope state → allow.  This flag is set by the
      middleware when the bearer token's sha256 matches ``admin_api_key_digest``
      (ROB F1 — admin key recognized before data keystore lookup).
    - ``is_admin=False`` → 403 "use the admin key to call /admin/*".
    - ``admin_api_key_configured=False`` on app.state → 503 "admin API disabled".

    Entra mode logic:
    - ``IdentityAdmin`` (or the configured ``entra_admin_role``) in the token's
      ``roles`` claim (stored in scope state by the middleware) → allow.
    - Role not present → 403 naming the required role.
    - ``entra_admin_role`` empty/unconfigured → 503 "admin API disabled".

    503 signals "capability not configured" (distinct from 403 "you are denied").

    Notes:
    - Tests override this with ``app.dependency_overrides[require_admin] = lambda: None``
      to bypass enforcement in T4 route tests (this is the standard FastAPI override
      mechanism; the lambda's signature must satisfy ITS OWN declared parameters —
      FastAPI injects based on the override's signature, not the original's).
    - The ``roles`` check reads ONLY the ``roles`` claim — never ``groups``.
      Group membership in the token cannot grant admin access (TB-09).
    """
    auth_mode: str = getattr(request.app.state, "auth_mode", "static")
    # Read auth metadata from scope state (set by BearerTokenMiddleware).
    scope_state: dict = request.scope.get("state", {})

    if auth_mode == "static":
        # 503 when admin key is not configured (capability off, not forbidden).
        admin_configured: bool = getattr(
            request.app.state, "admin_api_key_configured", False
        )
        if not admin_configured:
            raise HTTPException(
                status_code=503,
                detail=(
                    "Admin API disabled: admin_api_key is not configured. "
                    "Set admin_api_key in the YAML config or via the "
                    "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_ADMIN_API_KEY env var."
                ),
            )

        # 403 when the request was authenticated with a data key (not the admin key).
        is_admin: bool = scope_state.get("is_admin", False)
        if not is_admin:
            raise HTTPException(
                status_code=403,
                detail=(
                    "Forbidden: the admin API key is required to call /admin/* "
                    "endpoints. Data keys authenticate to the data API only."
                ),
            )

    else:  # entra mode
        # 503 when admin role is not configured (capability off, not forbidden).
        entra_admin_role: str = getattr(request.app.state, "entra_admin_role", "")
        if not entra_admin_role:
            raise HTTPException(
                status_code=503,
                detail=(
                    "Admin API disabled: entra_admin_role is not configured. "
                    "Set entra_admin_role in the YAML config or via the "
                    "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_ENTRA_ADMIN_ROLE env var."
                ),
            )

        # 403 when the token's `roles` claim does not contain the required role.
        # ONLY the `roles` claim is checked — `groups` is intentionally excluded
        # so group membership cannot grant admin access (TB-09).
        roles: list[str] = scope_state.get("roles", [])
        if entra_admin_role not in roles:
            raise HTTPException(
                status_code=403,
                detail=(
                    f"Forbidden: the App Role '{entra_admin_role}' is required "
                    f"to call /admin/* endpoints. Assign the role in the Entra "
                    f"App Registration and ensure the token's 'roles' claim "
                    f"(not 'groups') contains it."
                ),
            )


# ---------------------------------------------------------------------------
# Request body models
# ---------------------------------------------------------------------------


class IdentityBody(BaseModel):
    """Body for PUT /admin/identities/{oid}."""

    id: str
    display_name: str | None = None

    @field_validator("id")
    @classmethod
    def id_must_be_valid(cls, v: str) -> str:
        """Validate contributor id: non-empty, non-whitespace, bounded, no null bytes (TB-12)."""
        if not v.strip():
            raise ValueError("id must be a non-empty, non-whitespace string")
        if len(v) > _MAX_CONTRIBUTOR_LEN:
            raise ValueError(
                f"id must be at most {_MAX_CONTRIBUTOR_LEN} characters (got {len(v)})"
            )
        if "\x00" in v:
            raise ValueError("id must not contain null bytes")
        return v


class KeyBody(BaseModel):
    """Body for PUT /admin/keys/{sha256hash}."""

    id: str

    @field_validator("id")
    @classmethod
    def id_must_be_valid(cls, v: str) -> str:
        """Validate contributor id: non-empty, non-whitespace, bounded, no null bytes (TB-12)."""
        if not v.strip():
            raise ValueError("id must be a non-empty, non-whitespace string")
        if len(v) > _MAX_CONTRIBUTOR_LEN:
            raise ValueError(
                f"id must be at most {_MAX_CONTRIBUTOR_LEN} characters (got {len(v)})"
            )
        if "\x00" in v:
            raise ValueError("id must not contain null bytes")
        return v


# ---------------------------------------------------------------------------
# Path-param validation helpers (TB-10)
# ---------------------------------------------------------------------------


def _validate_oid(oid: str) -> None:
    """Raise HTTPException(422) when *oid* fails GUID format or all-zeros check.

    Reuses ``_GUID_RE`` (config.py:42) and ``_ALL_ZEROS_GUID`` (config.py:45)
    exactly as the config-level validator does — no regex duplication.
    """
    if not _GUID_RE.fullmatch(oid):
        raise HTTPException(
            status_code=422,
            detail=(
                f"invalid OID {oid!r}: must be a GUID in lowercase hex format "
                f"(xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)"
            ),
        )
    if oid == _ALL_ZEROS_GUID:
        raise HTTPException(
            status_code=422,
            detail=(
                "invalid OID: the all-zeros GUID is not permitted "
                "(use the real oid from 'az ad signed-in-user show --query id -o tsv')"
            ),
        )


def _validate_hash(sha256hash: str) -> None:
    """Raise HTTPException(422) when *sha256hash* is not exactly 64 lowercase hex chars.

    Mirrors the 64-hex check in ``config._validate_api_keys`` (config.py:172-179).
    """
    if not _HASH_RE.fullmatch(sha256hash):
        raise HTTPException(
            status_code=422,
            detail=(
                f"invalid hash {sha256hash!r}: must be exactly 64 lowercase hex "
                f"characters (SHA-256 digest). Got {len(sha256hash)} chars."
            ),
        )


# ---------------------------------------------------------------------------
# Audit helpers
# ---------------------------------------------------------------------------


def _admin_who(request: Request) -> str:
    """Return the calling admin's identity from scope state for audit logging.

    The middleware (auth.py) stores ``contributor_id`` in scope state:
    - Static admin key → ``"admin"``
    - Entra token → the contributor id mapped from the token's ``oid``
    """
    state: dict = request.scope.get("state", {})
    return state.get("contributor_id", "unknown")


def _audit_put(
    request: Request,
    *,
    target: str,
    contributor: str,
    old_contributor: str | None,
) -> None:
    """Emit one structured audit log line for a PUT (upsert) mutation.

    When *old_contributor* is set and differs from *contributor*, the entry
    records an OVERWRITE event with old → new (TB-11).  Otherwise it records
    a normal insert/same-contributor upsert.

    NEVER logs raw keys — only the *target* (oid or hash) is recorded.
    """
    who = _admin_who(request)
    if old_contributor is not None and old_contributor != contributor:
        # Overwrite: different contributor (TB-11 explicit old→new audit).
        logger.info(
            "admin.audit action=put target=%s old_contributor=%r new_contributor=%r who=%s",
            target,
            old_contributor,
            contributor,
            who,
        )
    else:
        logger.info(
            "admin.audit action=put target=%s contributor=%r who=%s",
            target,
            contributor,
            who,
        )


def _audit_delete(request: Request, *, target: str) -> None:
    """Emit one structured audit log line for a successful DELETE mutation."""
    logger.info(
        "admin.audit action=delete target=%s who=%s",
        target,
        _admin_who(request),
    )


def _audit_blob_reclaim_delete(request: Request, *, uri: str) -> None:
    """Emit one structured audit log line per successfully-deleted blob.

    NEVER logs blob contents -- only the ``ci-blob://`` URI is recorded.
    """
    logger.info(
        "admin.audit action=blob_reclaim target=%s who=%s",
        uri,
        _admin_who(request),
    )


# ---------------------------------------------------------------------------
# Blob reclaim -- orphaned-blob GC (design: docs/plans/2026-08-12-blob-
# reclaim-endpoint-spec.md, "Council amendment -- AUTHORITATIVE" section)
# ---------------------------------------------------------------------------


def _access_mode_const(mode: str) -> str:
    """Map the configured query access-mode string ("READ"/"WRITE") to the
    driver's access-mode constant.

    Deliberately duplicated (not imported) from ``main._neo4j_access_const``:
    importing from ``main`` here would create a circular import (``main``
    already imports ``routers.admin`` at module load time). Two lines of
    duplication is cheaper than that coupling.
    """
    return READ_ACCESS if mode == "READ" else WRITE_ACCESS


@dataclass(frozen=True)
class _OnDiskBlob:
    """One blob file discovered on disk during the reclaim scan."""

    session_id: str
    key: str
    uri: str
    path: Path
    mtime: float
    size: int


def _scan_disk_blobs(blob_root: Path) -> list[_OnDiskBlob]:
    """Enumerate on-disk blobs under *blob_root* (step 1 of the design).

    Walks ``<blob_root>/*/blobs/*.json`` -- the exact layout
    ``AsyncDiskBlobStore`` writes to (``blob_store.py``). ``*.tmp`` residue
    from an in-progress write (``tempfile.mkstemp(..., suffix=".tmp")``) is
    skipped defensively, though the ``*.json`` glob already excludes it.
    A file that vanishes between the glob listing and ``stat()`` (e.g. a
    concurrent delete) is silently skipped rather than raising.
    """
    blobs: list[_OnDiskBlob] = []
    for p in blob_root.glob("*/blobs/*.json"):
        if p.name.endswith(".tmp"):
            continue
        session_id = p.parent.parent.name
        key = p.stem
        try:
            st = p.stat()
        except FileNotFoundError:
            continue
        blobs.append(
            _OnDiskBlob(
                session_id=session_id,
                key=key,
                uri=f"ci-blob://{session_id}/{key}",
                path=p,
                mtime=st.st_mtime,
                size=st.st_size,
            )
        )
    return blobs


def _collect_blob_refs(obj: Any, out: set[str]) -> None:
    """Recursively walk a decoded JSON value collecting ``$blob_ref`` URIs.

    B2 (council amendment, highest severity): this is STRUCTURAL extraction
    over the parsed object, never a regex over the serialized string. A
    regex anchored on ``ci-blob://`` truncates at the first unescaped
    special character (e.g. a literal ``"`` in a session_id, which
    ``queue_manager._validate_session_id`` explicitly permits -- it only
    rejects ``/ \\ \\0``), silently misclassifying a genuinely-referenced
    blob as orphan. ``json.loads`` has already resolved all escaping by the
    time this function runs, so any character in a URI (quotes, non-ASCII)
    is handled correctly -- there is no APOC path and no regex path.
    """
    if isinstance(obj, dict):
        ref = obj.get("$blob_ref")
        if isinstance(ref, str):
            out.add(ref)
        for v in obj.values():
            _collect_blob_refs(v, out)
    elif isinstance(obj, list):
        for item in obj:
            _collect_blob_refs(item, out)


async def _scan_referenced_uris(request: Request) -> set[str]:
    """Enumerate every ``ci-blob://`` URI referenced anywhere in the graph.

    Step 2 of the design -- deliberately GLOBAL, never workspace-filtered
    (hazard #1): blobs are session_id-scoped while nodes are
    (node_id, workspace)-scoped, so a per-workspace scan could delete another
    workspace's live data.

    B1 (council amendment): the scan enumerates ``:Event.data`` only. This is
    the complete reference carrier by pipeline ordering, not by contract --
    ``DefaultHandler`` (handlers/data_layer_1/default.py) fires on EVERY
    unclaimed event and stores the full ``data`` (including ``tool_input``
    and any ``$blob_ref`` already written by ``blob_processor.py``) as
    ``json.dumps(data)`` on ``:Event.data`` BEFORE any field-lifter runs and
    strips/promotes individual fields. See
    ``tests/neo4j/test_blob_reclaim.py::test_b1_event_data_carries_every_blob_ref``
    for the regression test that pins this invariant.

    B2 (council amendment): APOC is NOT used here (removed entirely, no
    dual-path). Each matching row's ``data`` is ``json.loads``-ed and walked
    structurally by :func:`_collect_blob_refs`. A malformed/non-JSON ``data``
    value is logged and skipped conservatively -- it can never positively
    assert an orphan, only fail to positively assert a reference.

    Streams rows via the async driver iterator rather than materializing all
    ``data`` strings at once (only the resulting, much smaller, URI set is
    retained), per the design's "bound its own work" cost note.
    """
    driver = request.app.state.neo4j_query_driver
    access_mode = _access_mode_const(request.app.state.neo4j_query_access_mode)
    referenced: set[str] = set()
    async with driver.session(default_access_mode=access_mode) as session:
        # B1: :Event.data is scanned because DefaultHandler persists the full
        # event data (all $blob_ref values, pre-field-lifting) on every
        # unclaimed event -- see the docstring above.
        result = await session.run(
            "MATCH (n:Event) WHERE n.data CONTAINS 'ci-blob://' RETURN n.data AS data"
        )
        async for record in result:
            data_str = record["data"]
            try:
                obj = json.loads(data_str)
            except (TypeError, ValueError):
                logger.warning(
                    "blob_reclaim: unparseable Event.data encountered during "
                    "reference scan -- skipped conservatively (never causes "
                    "a delete, may only miss a reference)"
                )
                continue
            _collect_blob_refs(obj, referenced)
    return referenced


async def _select_orphans(request: Request, *, min_age_minutes: int) -> dict[str, Any]:
    """The ONE selection path shared by dry-run and apply (I5b hard rule).

    Returns a dict with every response field EXCEPT ``dry_run``/``sample``/
    ``rescanned``/``deleted``/``deleted_bytes`` (the caller fills those in),
    plus a ``candidates`` key (list[_OnDiskBlob], sorted by uri for
    deterministic sampling/capping) that the caller pops before returning the
    response and uses to actually delete in apply mode.

    Safety gates applied to every on-disk blob not in the referenced set
    (step 3 of the design):
      1. Undrained-queue gate (primary, durable -- B3): skipped when the
         session has a live worker (``registry.active_sessions()``) OR its
         queue is not fully drained (``QueueManager.is_fully_drained``,
         durable across restarts). Counted as ``skipped_pending_session``.
      2. mtime floor (defense-in-depth -- B3): skipped when younger than
         ``min_age_minutes`` (already clamped >= ``_MIN_AGE_FLOOR_MINUTES`` by
         the request body validator). Counted as ``skipped_recent``.
    """
    settings = get_settings()
    blob_root = Path(settings.blob_path)
    disk_blobs = _scan_disk_blobs(blob_root)

    referenced = await _scan_referenced_uris(request)

    registry = request.app.state.registry
    queue_manager = registry.queue_manager
    live_workers = set(registry.active_sessions())

    now = time.time()
    age_cutoff_seconds = min_age_minutes * 60

    candidates: list[_OnDiskBlob] = []
    skipped_recent = 0
    skipped_pending_session = 0
    reclaimable_bytes = 0

    for blob in disk_blobs:
        if blob.uri in referenced:
            continue
        if blob.session_id in live_workers or not await queue_manager.is_fully_drained(
            blob.session_id
        ):
            skipped_pending_session += 1
            continue
        if now - blob.mtime < age_cutoff_seconds:
            skipped_recent += 1
            continue
        candidates.append(blob)
        reclaimable_bytes += blob.size

    candidates.sort(key=lambda b: b.uri)

    return {
        "scanned_disk_blobs": len(disk_blobs),
        "referenced_uris": len(referenced),
        "orphans_found": len(candidates),
        "reclaimable_bytes": reclaimable_bytes,
        "skipped_recent": skipped_recent,
        "skipped_pending_session": skipped_pending_session,
        "candidates": candidates,
    }


class BlobReclaimBody(BaseModel):
    """Body for POST /admin/blobs/reclaim."""

    dry_run: bool = True
    min_age_minutes: int = 60
    max_delete: int | None = None

    @field_validator("min_age_minutes")
    @classmethod
    def _min_age_at_least_floor(cls, v: int) -> int:
        """B3 (council amendment): reject (422) below the hard safety floor
        rather than silently raising -- a caller passing 0 must not be able
        to disable the age gate.
        """
        if v < _MIN_AGE_FLOOR_MINUTES:
            raise ValueError(
                f"min_age_minutes must be >= {_MIN_AGE_FLOOR_MINUTES} "
                f"(hard safety floor); got {v}"
            )
        return v


# ---------------------------------------------------------------------------
# Per-request store dependencies (via app.state — no circular import)
# ---------------------------------------------------------------------------


def _require_entra_store(request: Request) -> IdentityStore:
    """Dependency: resolve the entra-identity store or raise 503.

    The store is ``None`` when ``auth_mode != "entra"``.
    """
    store: IdentityStore | None = getattr(
        request.app.state, "entra_identity_store", None
    )
    if store is None:
        raise HTTPException(
            status_code=503,
            detail="identity store not active in this auth mode",
        )
    return store


def _require_key_store(request: Request) -> IdentityStore:
    """Dependency: resolve the api-key store or raise 503.

    The store is ``None`` when ``auth_mode != "static"``.
    """
    store: IdentityStore | None = getattr(request.app.state, "api_key_store", None)
    if store is None:
        raise HTTPException(
            status_code=503,
            detail="key store not active in this auth mode",
        )
    return store


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

# include_in_schema=False keeps the /admin/* routes out of the OpenAPI schema
# (/openapi.json) and Swagger UI (/docs).  Those two doc surfaces are
# intentionally unauthenticated (see auth._EXEMPT_PATHS); the data API is
# published there on purpose for interoperability, but the admin/identity
# surface is operator-only and is not an interop surface, so it is not disclosed
# to unauthenticated callers.  This is a schema-visibility choice only -- routing
# and auth are unaffected (admin remains bearer-token + IdentityAdmin gated).
router = APIRouter(
    prefix="/admin",
    dependencies=[Depends(require_admin)],
    include_in_schema=False,
)


# --- Entra identities -------------------------------------------------------


@router.put("/identities/{oid}", status_code=200)
def put_identity(
    oid: str,
    body: IdentityBody,
    request: Request,
    store: IdentityStore = Depends(_require_entra_store),
) -> dict[str, str]:
    """Upsert an entra identity (OID → contributor).

    Path-param guard: ``oid`` must be a valid GUID in lowercase hex and must
    not be the all-zeros sentinel → **422** on violation (TB-10).

    Write-through via ``IdentityStore.put``: the persistent file is updated
    atomically and the in-process map (shared with ``EntraResolver``) is
    updated immediately — no server restart required.

    An overwrite (existing oid with a different contributor) emits an explicit
    audit line recording old → new contributor (TB-11).

    Returns the stored record as ``{oid, id[, display_name]}``.
    """
    _validate_oid(oid)

    existing = store.get(oid)
    old_contributor: str | None = existing.get("id") if existing is not None else None

    record: dict[str, str] = {"id": body.id}
    if body.display_name is not None:
        record["display_name"] = body.display_name

    _audit_put(
        request,
        target=oid,
        contributor=body.id,
        old_contributor=old_contributor,
    )
    store.put(oid, record)
    return {"oid": oid, **record}


@router.delete("/identities/{oid}", status_code=200)
def delete_identity(
    oid: str,
    request: Request,
    store: IdentityStore = Depends(_require_entra_store),
) -> dict[str, str | bool]:
    """Delete an entra identity.

    Path-param guard: ``oid`` must be a valid GUID → **422** on violation.

    Returns 200 on success, 404 when the OID is not present.
    Deletion is write-through and immediately visible to the resolver.
    """
    _validate_oid(oid)
    if store.get(oid) is None:
        raise HTTPException(status_code=404, detail=f"identity {oid!r} not found")
    _audit_delete(request, target=oid)
    store.delete(oid)
    return {"oid": oid, "deleted": True}


@router.get("/identities", status_code=200)
def list_identities(
    store: IdentityStore = Depends(_require_entra_store),
) -> dict[str, list[dict[str, str]]]:
    """List all entra identities as ``{oid, id[, display_name]}``."""
    return {"identities": [{"oid": oid, **record} for oid, record in store.items()]}


# --- Static API keys --------------------------------------------------------


@router.put("/keys/{sha256hash}", status_code=200)
def put_key(
    sha256hash: str,
    body: KeyBody,
    request: Request,
    store: IdentityStore = Depends(_require_key_store),
) -> dict[str, str]:
    """Upsert a static API-key entry (sha256 hex → contributor).

    Path-param guard: ``sha256hash`` must be exactly 64 lowercase hex chars
    → **422** on violation (TB-10).

    Admin-key guard: the hash of the configured ``admin_api_key`` cannot be
    shadow-bound via this endpoint → **409** (TB-05). The admin key lives in
    config, not in the data keystore; rebinding its hash here would be
    confusing and is explicitly rejected.

    The path parameter is the **sha256 hash** of an externally-generated key.
    Operators hash the raw key out-of-band and register only the hash here;
    the server never sees or stores the raw key.

    Write-through: visible to ``StaticKeyResolver`` immediately, no restart.

    Returns ``{hash, id}`` — NEVER the raw key.
    """
    _validate_hash(sha256hash)

    # Admin-key un-shadowable guard (TB-05): reject PUT targeting the admin
    # key's hash.  The admin key is a config credential, not a data store
    # entry; allowing it to be shadowed here would silently rebind it.
    admin_digest: str | None = getattr(request.app.state, "admin_api_key_digest", None)
    if admin_digest is not None and sha256hash == admin_digest:
        raise HTTPException(
            status_code=409,
            detail=(
                "Cannot shadow or rebind the admin key via the data store. "
                "The admin key is a config-level credential (admin_api_key) "
                "and cannot be registered in the api-keys map."
            ),
        )

    existing = store.get(sha256hash)
    old_contributor: str | None = existing.get("id") if existing is not None else None

    record: dict[str, str] = {"id": body.id}
    _audit_put(
        request,
        target=sha256hash,
        contributor=body.id,
        old_contributor=old_contributor,
    )
    store.put(sha256hash, record)
    return {"hash": sha256hash, **record}


@router.delete("/keys/{sha256hash}", status_code=200)
def delete_key(
    sha256hash: str,
    request: Request,
    store: IdentityStore = Depends(_require_key_store),
) -> dict[str, str | bool]:
    """Delete a static API-key entry.

    Path-param guard: ``sha256hash`` must be exactly 64 lowercase hex chars
    → **422** on violation (TB-10).

    Admin-key guard: the hash of the configured ``admin_api_key`` cannot be
    deleted via this endpoint → **409** (TB-05). The admin key is the
    bootstrap floor — deleting it via the API must not be possible.

    Returns 200 on success, 404 when the hash is not present.
    Deletion is write-through and immediately visible to the resolver.
    """
    _validate_hash(sha256hash)

    # Admin-key un-deletable guard (TB-05): reject DELETE targeting the admin
    # key's hash.  The admin key is the emergency-bootstrap credential; it must
    # never be deletable via the API (operators would lock themselves out).
    admin_digest: str | None = getattr(request.app.state, "admin_api_key_digest", None)
    if admin_digest is not None and sha256hash == admin_digest:
        raise HTTPException(
            status_code=409,
            detail=(
                "Cannot delete the admin key via the API. "
                "The admin key (admin_api_key) is protected as the bootstrap "
                "credential. To rotate it, update the config and restart."
            ),
        )

    if store.get(sha256hash) is None:
        raise HTTPException(status_code=404, detail=f"key {sha256hash!r} not found")
    _audit_delete(request, target=sha256hash)
    store.delete(sha256hash)
    return {"hash": sha256hash, "deleted": True}


@router.get("/keys", status_code=200)
def list_keys(
    store: IdentityStore = Depends(_require_key_store),
) -> dict[str, list[dict[str, str]]]:
    """List all static API-key entries as ``{hash, id}``.

    Raw keys are NEVER returned — only the sha256 hash and contributor id.
    """
    return {
        "keys": [{"hash": h, "id": record.get("id", "")} for h, record in store.items()]
    }


# --- Blob reclaim (orphaned-blob GC) ----------------------------------------


@router.post("/blobs/reclaim", status_code=200)
async def reclaim_blobs(body: BlobReclaimBody, request: Request) -> dict[str, Any]:
    """Preview (dry-run) or apply reclamation of orphaned blob files.

    An orphan is an on-disk blob (``<blob_path>/<session_id>/blobs/<key>.json``)
    whose ``ci-blob://`` URI is referenced by NO ``:Event.data`` anywhere in
    the graph (scanned globally, across ALL workspaces), and whose session is
    both fully drained (durable ``QueueManager`` state, not in-memory worker
    liveness) and older than ``min_age_minutes``. See
    ``docs/plans/2026-08-12-blob-reclaim-endpoint-spec.md`` for the full
    design and its council-mandated safety amendments (B1-B3).

    ``dry_run=true`` (default) computes and reports the candidate set without
    deleting anything. ``dry_run=false`` requires ``max_delete`` (422
    otherwise -- a conscious blast-radius opt-in for an irreversible,
    cross-workspace delete) and performs its OWN fresh, authoritative
    ``_select_orphans`` scan at delete time (``rescanned: true`` in the
    response) -- it never deletes a URI that is referenced or pending at the
    moment of deletion, independent of any earlier dry-run preview.

    Deletion is a direct ``os.unlink`` on the blob path (idempotent -- a file
    already gone is a no-op) capped at ``max_delete``; ``orphans_found`` and
    ``reclaimable_bytes`` always reflect the FULL candidate set even when
    ``max_delete`` caps how many are actually removed. One structured audit
    log line is emitted per successful delete; blob CONTENTS are never
    logged, only the ``ci-blob://`` URI.
    """
    if not body.dry_run and body.max_delete is None:
        raise HTTPException(
            status_code=422,
            detail=(
                "max_delete is required when dry_run=false -- a conscious "
                "blast-radius opt-in for an irreversible, cross-workspace "
                "delete. Omit dry_run (or set it true) to preview first."
            ),
        )

    # I5b: the ONE selection path. In apply mode this call itself IS the
    # "own authoritative fresh scan at delete time" the design requires --
    # there is no earlier cached scan in this request to grow stale.
    selection = await _select_orphans(request, min_age_minutes=body.min_age_minutes)
    candidates: list[_OnDiskBlob] = selection.pop("candidates")

    response: dict[str, Any] = {
        "dry_run": body.dry_run,
        **selection,
        "sample": [b.uri for b in candidates[:_MAX_SAMPLE]],
        "rescanned": not body.dry_run,
        "deleted": 0,
        "deleted_bytes": 0,
    }

    if body.dry_run:
        return response

    assert body.max_delete is not None  # guaranteed by the 422 guard above
    deleted = 0
    deleted_bytes = 0
    # TOCTOU note: low severity (an unlink of an already-gone file is a
    # no-op below); this loop runs immediately after the fresh scan above,
    # well within the age floor, so the window is negligible -- stated, not
    # assumed (design "RISKS folded in").
    for blob in candidates[: body.max_delete]:
        try:
            os.unlink(blob.path)
        except FileNotFoundError:
            continue  # already gone -- idempotent, not an error
        deleted += 1
        deleted_bytes += blob.size
        _audit_blob_reclaim_delete(request, uri=blob.uri)

    response["deleted"] = deleted
    response["deleted_bytes"] = deleted_bytes
    return response
