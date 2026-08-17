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

import asyncio
import json
import logging
import re
import time
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from neo4j import READ_ACCESS, WRITE_ACCESS
from pydantic import BaseModel, Field, field_validator

from context_intelligence_server.blob_processor import (
    BLOB_REF_CARRIER_PROPERTIES as _BLOB_REF_CARRIER_PROPERTIES,
)
from context_intelligence_server.blob_store import BlobReference
from context_intelligence_server.config import _ALL_ZEROS_GUID, _GUID_RE, get_settings
from context_intelligence_server.identity_store import IdentityStore
from context_intelligence_server.maintenance import coordinator
from context_intelligence_server.maintenance_ops import run_maintenance_operation

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


# Reference-scan hardening (docs/plans/2026-08-12-blob-reclaim-reference-scan-
# hardening.md): the known node properties that can carry a "ci-blob://"
# token anywhere in the graph, keyed to the property name Cypher matches on.
#
# ASSUMPTION -- make it explicit + greppable: the reference scan is scoped to
# EXACTLY these properties, not an all-property-all-node walk, for
# performance (a per-property UNION lets Neo4j evaluate ONE property per row
# instead of toString()-ing every key of every node -- this codebase has
# scar tissue from a 1.3M-node AllNodesScan stall). Adding a NEW ci-blob
# carrier property in the future (a new field-lifter, a new enricher
# property, etc.) REQUIRES adding it to this tuple -- otherwise the reclaim
# GC will not see refs stored there and could misclassify a live blob as an
# orphan. Covers every carrier in the hardening doc's table:
#   data        -- *Event.data (all 14 event types)
#   tool_input  -- ToolPreEvent/ToolPostEvent.tool_input (L1), ToolCall.tool_input (L2)
#   prompt      -- PromptSubmitEvent/PromptCompleteEvent.prompt (L1), Prompt.prompt (L2)
#   response    -- OrchestratorRun.response (L2)
# NOTE: the canonical allowlist lives in blob_processor as BLOB_REF_CARRIER_PROPERTIES
# and is imported (aliased to _BLOB_REF_CARRIER_PROPERTIES) at the top of this module --
# a single source of truth shared by the mint site (blob_processor) and this reclaim
# scan. Do NOT re-declare it here.

# Fallback extraction for carrier values that are NOT valid JSON (a bare
# string property, e.g. a plain-string tool_input/prompt that itself
# contains a ci-blob:// URI rather than the {"$blob_ref": "..."} wrapper).
# Safe here -- unlike a regex over a JSON-*serialized* string (see the B2
# docstring above) -- because these values are already fully-decoded Neo4j
# property strings with no JSON escaping left to trip over.
_BARE_BLOB_URI_RE = re.compile(r'ci-blob://[^"\s]+')


def _extract_blob_refs_from_value(val: str, out: set[str]) -> None:
    """Extract every ``ci-blob://`` URI referenced by one carrier property value.

    Two extraction paths, unioned into *out*:

    1. **Structural (preferred):** ``json.loads(val)`` then recurse with
       :func:`_collect_blob_refs`. Handles the ``{"$blob_ref": "..."}``
       JSON-string carriers -- ``neo4j_store._sanitize_properties``
       JSON-serializes any dict/list property value on write, so
       ``Event.data``, ``ToolCall.tool_input``, ``Prompt.prompt``, and
       ``OrchestratorRun.response`` all round-trip through this path when
       they hold a dict/list.
    2. **Regex fallback (only on JSON-parse failure):** a plain-string
       carrier is written through VERBATIM (``_sanitize_properties`` only
       JSON-serializes dict/list values -- a bare string is stored as-is),
       so it is never valid JSON and always lands here. Extracts every bare
       ``ci-blob://[^"\\s]+`` token directly from the decoded string --
       covers a lifted ``*.tool_input``/``*.prompt`` property that is a
       plain string mentioning a blob URI.

    A value that is neither valid JSON nor contains a bare token contributes
    nothing -- this can only ever fail to positively assert a reference,
    never falsely assert one, matching the conservative-skip contract of
    :func:`_scan_referenced_uris`.
    """
    try:
        obj = json.loads(val)
    except (TypeError, ValueError):
        out.update(_BARE_BLOB_URI_RE.findall(val))
        return
    _collect_blob_refs(obj, out)


def _carrier_scan_clause(prop: str) -> str:
    """Build one ``UNION ALL`` branch of the reclaim reference-scan query
    for carrier property *prop*.

    ``data`` is special-cased: ``Event.data`` is always a JSON string
    (``DefaultHandler`` writes ``json.dumps(data)`` -- see
    ``handlers/data_layer_1/default.py``) and the scan for it is scoped to
    ``:Event``, matching this carrier's scope in the pre-hardening scan. Every
    other registered carrier is an unrestricted-label match with
    ``toString()``, since the property may be lifted onto any node type as a
    dict, list, or bare string.
    """
    if prop == "data":
        return "MATCH (n:Event) WHERE n.data CONTAINS 'ci-blob://' RETURN n.data AS val"
    return (
        f"MATCH (n) WHERE n.{prop} IS NOT NULL "
        f"AND toString(n.{prop}) CONTAINS 'ci-blob://' "
        f"RETURN toString(n.{prop}) AS val"
    )


# Generated FROM _BLOB_REF_CARRIER_PROPERTIES (imported from
# blob_processor.BLOB_REF_CARRIER_PROPERTIES) -- not hand-duplicated -- so the
# query text can never drift from the allowlist. See
# tests/test_blob_processor.py for the regression test that locks this
# agreement structurally (extracts every `n.<prop>` reference out of the
# built query and diffs it against the allowlist tuple).
_BLOB_REF_SCAN_QUERY = " UNION ALL ".join(
    _carrier_scan_clause(prop) for prop in _BLOB_REF_CARRIER_PROPERTIES
)


async def _scan_referenced_uris(request: Request) -> set[str]:
    """Enumerate every ``ci-blob://`` URI referenced anywhere in the graph.

    Step 2 of the design -- deliberately GLOBAL, never workspace-filtered
    (hazard #1): blobs are session_id-scoped while nodes are
    (node_id, workspace)-scoped, so a per-workspace scan could delete another
    workspace's live data.

    Reference-scan hardening (docs/plans/2026-08-12-blob-reclaim-reference-
    scan-hardening.md): the referenced set is computed GRAPH-WIDE over the
    known ``ci-blob://`` carrier properties (:data:`_BLOB_REF_CARRIER_PROPERTIES`
    -- ``data``, ``tool_input``, ``prompt``, ``response``), not ``Event.data``
    only. This makes the scan correct BY CONSTRUCTION -- a strict superset of
    the prior ``Event.data``-only scan -- rather than resting on the
    (empirically true today, but unenforced) pipeline-ordering invariant that
    ``DefaultHandler`` always persists every ref onto ``Event.data`` before any
    field-lifter/enricher can strip or promote it elsewhere. Widening the scan
    can only ever *protect* more blobs, never delete more: any URI the old
    scan found is still found here (still scanned via the ``data`` branch),
    plus any URI that lives ONLY on ``tool_input``/``prompt``/``response`` is
    now ALSO found. See
    ``tests/neo4j/test_blob_reclaim.py::test_b1_event_data_carries_every_blob_ref``
    for the regression test that continues to pin the pipeline-ordering
    invariant (belt-and-suspenders now, not the sole safety net), and the
    ``test_*_only_referenced_via_*`` tests alongside it that pin the new
    per-property carriers directly.

    Query shape (performance-critical -- see :data:`_BLOB_REF_CARRIER_PROPERTIES`):
    a ``UNION ALL`` of four single-property predicates, each touching exactly
    ONE property per row (``data`` restricted to ``:Event``, matching the
    prior scan's scope for that carrier; the other three unrestricted across
    labels since ToolCall/Prompt/OrchestratorRun are ordinary nodes). This
    avoids a pathological ``MATCH (n) ... [k IN keys(n) WHERE toString(n[k])
    ...]`` all-property-all-node walk, which would toString() every key of
    every node in the graph -- this codebase has scar tissue from exactly
    that shape of full scan (a 1.3M-node AllNodesScan stall). ``UNION ALL``
    (not plain ``UNION``) is deliberate: plain ``UNION``'s implicit DISTINCT
    would force Neo4j to materialize and dedupe every row before returning
    the first one, defeating streaming; the Python ``set`` below already
    dedupes, so ``UNION ALL`` costs nothing and preserves the stream.

    B2 (council amendment, retained): no APOC. Each returned value is
    extracted via :func:`_extract_blob_refs_from_value` (structural
    ``json.loads`` + recursive walk, falling back to a bare-token regex only
    on JSON-parse failure -- see that function's docstring for why the regex
    path is safe here and was NOT safe for the original ``Event.data`` scan).
    A malformed/unparseable value is skipped conservatively -- it can never
    positively assert an orphan, only fail to positively assert a reference.

    Streams rows via the async driver iterator rather than materializing all
    carrier-property strings at once (only the resulting, much smaller, URI
    set is retained), per the design's "bound its own work" cost note.
    """
    driver = request.app.state.neo4j_query_driver
    access_mode = _access_mode_const(request.app.state.neo4j_query_access_mode)
    referenced: set[str] = set()
    async with driver.session(default_access_mode=access_mode) as session:
        # Graph-wide, per-property UNION ALL -- see the docstring above for
        # why this shape (not an all-property walk, not plain UNION). The
        # query text is generated from _BLOB_REF_CARRIER_PROPERTIES
        # (_BLOB_REF_SCAN_QUERY, module level) -- not hand-duplicated here --
        # so it can never drift from the allowlist.
        result = await session.run(_BLOB_REF_SCAN_QUERY)
        async for record in result:
            val = record["val"]
            if not isinstance(val, str):
                # toString() on a non-null value is always a str; this guards
                # conservatively against an unexpected driver type mapping.
                continue
            _extract_blob_refs_from_value(val, referenced)
    return referenced


async def _select_orphans(request: Request, *, min_age_minutes: int) -> dict[str, Any]:
    """The ONE selection path shared by dry-run and apply (I5b hard rule).

    Returns a dict with every response field EXCEPT ``dry_run``/``sample``/
    ``rescanned``/``deleted``/``deleted_bytes`` (the caller fills those in),
    plus a ``candidates`` key (list[BlobReference], sorted by uri for
    deterministic sampling/capping) that the caller pops before returning the
    response and uses to actually delete in apply mode.

    Pure storage-API consumer (docs/blob-store-abstraction.md, "How
    routers/admin.py consumes it"): orphan enumeration is
    ``registry.blob_store.scan()`` -- a stream of :class:`BlobReference`
    (``uri``/``session_id``/``size``/``last_modified``). References only --
    the scan never reads a blob's payload, so no JSON is deserialized across
    the whole store. No ``Path(settings.blob_path)``, no ``glob``, no
    ``stat`` -- ever, here.

    Safety gates applied to every scanned blob not in the referenced set
    (step 3 of the design):
      1. Undrained-queue gate (primary, durable -- B3): skipped when the
         session has a live worker (``registry.active_sessions()``) OR its
         queue is not fully drained (``QueueManager.is_fully_drained``,
         durable across restarts). Counted as ``skipped_pending_session``.
      2. mtime floor (defense-in-depth -- B3): skipped when younger than
         ``min_age_minutes`` (already clamped >= ``_MIN_AGE_FLOOR_MINUTES`` by
         the request body validator). Counted as ``skipped_recent``.
    """
    registry = request.app.state.registry
    disk_blobs = [ref async for ref in registry.blob_store.scan()]

    referenced = await _scan_referenced_uris(request)

    queue_manager = registry.queue_manager
    live_workers = set(registry.active_sessions())

    now = time.time()
    age_cutoff_seconds = min_age_minutes * 60

    candidates: list[BlobReference] = []
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
        if now - blob.last_modified < age_cutoff_seconds:
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
    max_delete: int | None = Field(default=None, ge=1)

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
# Blob-reclaim apply single-flight (item -17v)
#
# Serializes APPLY-vs-APPLY only (``dry_run=false``): two concurrent applies
# must never each run their own independent scan+delete pass over the store
# at once. This is a small, purpose-built CAS for reclaim specifically -- NOT
# ``maintenance.coordinator`` above, which tracks actual maintenance-mode
# operations (schema repair) with its own gate/window semantics; reusing that
# singleton here would incorrectly couple blob reclaim into the
# maintenance-mode state machine (e.g. blocking real maintenance repairs
# while a reclaim runs, or vice versa, for no reason related to either).
#
# Mirrors ``MaintenanceCoordinator.try_begin_op``'s contract: a synchronous
# compare-and-swap with NO ``await`` between check and set, which is atomic
# under asyncio's single-threaded event loop (no separate lock needed).
#
# This guard does NOT handle the scan-vs-ingest race (a blob rewritten
# between this request's own scan and its own delete) -- that is handled
# per-blob by the fenced ``blob_store.delete(uri, if_unmodified=ref)`` call
# in the apply loop below, independent of this lock.
# ---------------------------------------------------------------------------

_reclaim_apply_running = False


def _try_begin_reclaim_apply() -> bool:
    """Synchronous CAS: True iff this call is the one that started the run."""
    global _reclaim_apply_running
    if _reclaim_apply_running:
        return False
    _reclaim_apply_running = True
    return True


def _finish_reclaim_apply() -> None:
    """Release the single-flight slot. Always called from a ``finally``."""
    global _reclaim_apply_running
    _reclaim_apply_running = False


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


@router.post("/blobs/reclaim", status_code=200, response_model=None)
async def reclaim_blobs(
    body: BlobReclaimBody, request: Request
) -> dict[str, Any] | JSONResponse:
    """Preview (dry-run) or apply reclamation of orphaned blobs.

    An orphan is a stored blob (``ci-blob://<session_id>/<key>``) whose URI is
    referenced by NO ``:Event.data`` (or other carrier property) anywhere in
    the graph (scanned globally, across ALL workspaces), and whose session is
    both fully drained (durable ``QueueManager`` state, not in-memory worker
    liveness) and older than ``min_age_minutes``. See
    ``docs/plans/2026-08-12-blob-reclaim-endpoint-spec.md`` for the full
    design and its council-mandated safety amendments (B1-B3), and
    ``docs/blob-store-abstraction.md`` ("How routers/admin.py consumes it")
    for the storage-isolation contract this endpoint honors.

    ``dry_run=true`` (default) computes and reports the candidate set without
    deleting anything. ``dry_run=false`` requires ``max_delete`` (422
    otherwise -- a conscious blast-radius opt-in for an irreversible,
    cross-workspace delete) and performs its OWN fresh, authoritative
    ``_select_orphans`` scan at delete time (``rescanned: true`` in the
    response) -- it never deletes a URI that is referenced or pending at the
    moment of deletion, independent of any earlier dry-run preview.

    Deletion is a **pure storage-API consumer**: every apply request is a
    fenced ``await blob_store.delete(uri, if_unmodified=ref)`` call -- the
    store itself performs the compare-and-delete (re-checking the blob's
    current metadata against the ``BlobReference`` observed by the scan
    above), never an ``os.unlink``/``.path`` reach-around. A blob rewritten
    since the scan simply fails the fence (``delete()`` returns ``False``)
    and is skipped, not force-deleted -- no data loss. Capped at
    ``max_delete``; ``orphans_found`` and ``reclaimable_bytes`` always
    reflect the FULL candidate set even when ``max_delete`` caps how many are
    actually removed. One structured audit log line is emitted per
    successful delete; blob CONTENTS are never logged, only the
    ``ci-blob://`` URI.

    Single-flight (item -17v): the APPLY path (``dry_run=false``) is CAS
    single-flighted (see :func:`_try_begin_reclaim_apply`) so two concurrent
    applies can never run their own independent, unsynchronized scan+delete
    passes over the same store at once. A second concurrent apply gets
    **409** rather than starting a second run. This guard only serializes
    scan-vs-scan; the scan-vs-ingest race (a blob rewritten between this
    request's own scan and its own delete) is handled per-blob by the fenced
    ``delete(if_unmodified=ref)`` call above, not by this lock.
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

    if body.dry_run:
        # I5b: the ONE selection path (shared with apply, below).
        selection = await _select_orphans(request, min_age_minutes=body.min_age_minutes)
        candidates: list[BlobReference] = selection.pop("candidates")
        return {
            "dry_run": True,
            **selection,
            "sample": [b.uri for b in candidates[:_MAX_SAMPLE]],
            "rescanned": False,
            "deleted": 0,
            "deleted_bytes": 0,
        }

    if not _try_begin_reclaim_apply():
        return JSONResponse(
            status_code=409,
            content={"detail": "blob reclaim apply already running"},
        )
    try:
        # I5b: the ONE selection path. In apply mode this call itself IS the
        # "own authoritative fresh scan at delete time" the design requires --
        # there is no earlier cached scan in this request to grow stale.
        selection = await _select_orphans(request, min_age_minutes=body.min_age_minutes)
        candidates = selection.pop("candidates")

        response: dict[str, Any] = {
            "dry_run": False,
            **selection,
            "sample": [b.uri for b in candidates[:_MAX_SAMPLE]],
            "rescanned": True,
            "deleted": 0,
            "deleted_bytes": 0,
        }

        assert body.max_delete is not None  # guaranteed by the 422 guard above
        registry = request.app.state.registry
        deleted = 0
        deleted_bytes = 0
        # Fenced delete, not TOCTOU-vulnerable: blob_store.delete(if_unmodified=)
        # re-checks the blob's current metadata against the BlobReference
        # observed by the scan above and refuses (returns False) if it changed
        # since -- the store does the compare-and-delete, never admin.py.
        for blob in candidates[: body.max_delete]:
            deleted_ok = await registry.blob_store.delete(blob.uri, if_unmodified=blob)
            if not deleted_ok:
                continue  # changed since scan (or already gone) -- skipped, not an error
            deleted += 1
            deleted_bytes += blob.size
            _audit_blob_reclaim_delete(request, uri=blob.uri)

        response["deleted"] = deleted
        response["deleted_bytes"] = deleted_bytes
        return response
    finally:
        _finish_reclaim_apply()


# --- Maintenance operation (WS-3c; seam + gate built in WS-3a) --------------
#
# See docs/plans/2026-08-13-ws3-implementation-spec.md sec 6 for the full
# design. This endpoint is on ``maintenance.MAINTENANCE_ALLOW_LIST`` (enforced
# structurally by ``main._assert_maintenance_endpoint_allow_listed`` at
# startup) so it stays reachable even while the gate is closed -- otherwise
# it would 503 at exactly the moment it exists to unblock.


@router.post("/maintenance", status_code=202)
async def post_maintenance(request: Request) -> JSONResponse:
    """Trigger the maintenance repair operation (dedup -> :Node backfill ->
    schema DDL), reusing the existing ``neo4j_store.run_repair`` -- see
    ``maintenance_ops.run_maintenance_operation``.

    Single-flight (council MUST-FIX #3): ``coordinator.try_begin_op()`` is a
    synchronous compare-and-swap with no ``await`` between check and set, so
    two concurrent POSTs can never both start an op. A second POST while one
    is running gets **409** with the in-progress ``run_id`` -- it never
    starts a second op.

    Returns promptly (council MUST-FIX): the operation runs as a background
    task; this handler returns **202** immediately rather than blocking for
    the op's full duration (which includes the quiesce sleep + an
    O(graph-size) dedup pass).

    Idempotent honest re-scan (D-H): POST against an already-clean graph
    still performs a genuine ``run_repair`` call and returns a **fresh**
    ``run_id``/``completed_at`` with ``records_affected: 0`` -- it never
    short-circuits, which would make ``0`` indistinguishable from "did not
    run" and defeat the freshness marker.
    """
    run_id = coordinator.try_begin_op()
    if run_id is None:
        current = coordinator.current_op()
        return JSONResponse(
            status_code=409,
            content={
                "detail": "maintenance operation already running",
                "run_id": current.run_id,
                "state": current.state,
            },
        )

    settings = get_settings()
    # Defensive getattr (matches _check_driver_connected's convention above):
    # lets this route degrade gracefully rather than 500 if ever hit before
    # lifespan has bound a driver -- run_repair() will raise on a None
    # driver, which finish_op() records as a normal `failed` outcome.
    driver = getattr(request.app.state, "neo4j_driver", None)
    task = asyncio.create_task(
        run_maintenance_operation(
            driver,
            run_id,
            quiesce_seconds=settings.maintenance_quiesce_seconds,
        )
    )
    coordinator.retain_task(
        task
    )  # strong ref -- see MaintenanceCoordinator.retain_task

    op = coordinator.current_op()
    return JSONResponse(
        status_code=202,
        content={"run_id": run_id, "state": op.state, "started_at": op.started_at},
    )


@router.get("/maintenance", status_code=200)
async def get_maintenance() -> dict[str, Any]:
    """Report maintenance-operation progress.

    Separate, admin-authenticated route -- council: required, and never
    folded into ``/status`` (``/status`` is unauthenticated; folding these
    fields in would be an auth-boundary leak). ``state`` initializes to
    ``"unknown"`` on boot so "never ran" is distinguishable from "ran, record
    lost to a crash" (council D4).
    """
    st = await coordinator.status()
    op = st.op
    return {
        "mode": st.mode,
        "state": op.state,
        "run_id": op.run_id,
        "started_at": op.started_at,
        "completed_at": op.completed_at,
        "elapsed_seconds": st.elapsed_seconds,
        "records_affected": op.records_affected,
        "error": op.error,
    }
