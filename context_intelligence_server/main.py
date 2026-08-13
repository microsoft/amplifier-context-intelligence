"""FastAPI application entrypoint for the Context Intelligence Server."""

import argparse
import asyncio
import json
import logging
import os
import re
import sys
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from neo4j import READ_ACCESS, WRITE_ACCESS, AsyncGraphDatabase

from context_intelligence_server import __version__
from context_intelligence_server.auth import (
    _EXEMPT_PATHS,
    BearerTokenMiddleware,
    EntraResolver,
    StaticKeyResolver,
)
from context_intelligence_server.authz import (  # noqa: F401 — re-exported for tests/routes
    _is_write_capable,
    require_read,
    require_write,
)
from context_intelligence_server.blob_store import AsyncDiskBlobStore
from context_intelligence_server.config import Neo4jClientConfig, Settings, get_settings
from context_intelligence_server.idempotency import EventIdempotencyCache
from context_intelligence_server.identity_store import IdentityStore
from context_intelligence_server.logging_config import setup_logging
from context_intelligence_server.maintenance import (
    MAINTENANCE_ALLOW_LIST,
    coordinator,
    maintenance_gate_middleware,
)
from context_intelligence_server.models import (
    CypherRequest,
    EventRequest,
    EventResponse,
)
from context_intelligence_server.neo4j_store import (
    count_untagged_nodes,
    ensure_neo4j_schema,
    ensure_schema_version_baseline,
    read_graph_schema_version,
)
from context_intelligence_server.registry import SessionRegistry
from context_intelligence_server.routers.admin import router as admin_router
from context_intelligence_server.routers.queues import router as queues_router
from context_intelligence_server.routers.version import router as version_router
from context_intelligence_server.status import SCHEMA_VERSION, build_status_response

_settings = get_settings()

logger = logging.getLogger("context_intelligence_server")


def _neo4j_access_const(mode: str) -> str:
    """Map our config string ("READ"/"WRITE") to the driver's access-mode constant."""
    return READ_ACCESS if mode == "READ" else WRITE_ACCESS


def build_neo4j_driver(config: Neo4jClientConfig) -> Any:
    """Construct an AsyncGraphDatabase driver from a resolved Neo4j client config.

    Shared by ``lifespan()`` (the admin driver, on every server boot) and
    ``doctor.run_doctor()`` (the CLI), so the two entry points can never
    construct the connection differently.
    """
    return AsyncGraphDatabase.driver(config.url, auth=config.auth)


# ---------------------------------------------------------------------------
# Module-level live identity-map stores (T3)
#
# Set by create_asgi_app() so the future /admin router can mutate the active
# store without needing to carry a reference through the middleware chain.
# Exactly ONE of these is non-None at any time — whichever mode is active.
# The other is always reset to None so accessors return an unambiguous result.
# ---------------------------------------------------------------------------
_api_key_store: IdentityStore | None = None
_entra_identity_store: IdentityStore | None = None


def get_api_key_store() -> IdentityStore | None:
    """Return the live API-key IdentityStore, or None when entra mode is active.

    The /admin router calls this to mutate the keystore (PUT/DELETE entries).
    The returned store's flat_dict is the SAME object used by StaticKeyResolver,
    so any put() or delete() is visible to the resolver immediately.
    """
    return _api_key_store


def get_entra_identity_store() -> IdentityStore | None:
    """Return the live Entra-identity IdentityStore, or None when static mode is active.

    The /admin router calls this to mutate the identity map (PUT/DELETE entries).
    The returned store's flat_dict is the SAME object used by EntraResolver,
    so any put() or delete() is visible to the resolver immediately.
    """
    return _entra_identity_store


def _recover_one_session(
    sid: str,
    first_line: str | bytes,
    get_or_create: Any,
) -> bool:
    """Parse the first queued line for *sid* and respawn a drainer when valid.

    Extracted from the lifespan startup recovery loop so tests can exercise the
    real parsing/dispatch logic rather than reimplementing it inline.

    The queue-read step is handled by the caller (the lifespan loop or the test)
    so this function is pure — no I/O, fully synchronous.

    Args:
        sid:            Session id being recovered.
        first_line:     The first raw log line (bytes from QueueManager or str
                        from tests).  ``json.loads`` accepts both.
        get_or_create:  The registry callable — ``registry.get_or_create`` in
                        production or a spy in tests.

    Returns:
        True  – drainer was (re)spawned via *get_or_create*.
        False – session skipped (empty/torn workspace, or malformed JSON line).
    """
    try:
        obj = json.loads(first_line)
        workspace: str = obj.get("workspace", "")
        created_by: str | None = obj.get("created_by")
    except (ValueError, KeyError):
        workspace = ""
        created_by = None
    if not workspace:
        logger.warning(
            "recovery_skipped session=%s: torn or empty workspace in first line",
            sid,
        )
        return False
    get_or_create(sid, workspace, created_by=created_by)
    return True


def _record_schema_health(
    app: FastAPI,
    constraint_established: bool,
    untagged: int | None,
) -> None:
    """Compute and stash the tri-state schema-health signal on app.state.

    Council amendment B3/B7 (deploy-safe boot, 2026-08-12): health is a
    tri-state enum, never coerced to a false "healthy":

    - ``"healthy"``  -- the :Node uniqueness constraint is established AND
      the untagged-node probe ran and found 0.
    - ``"degraded"`` -- the constraint is absent (a data conflict, logged
      loudly by ``ensure_neo4j_schema``/B4) OR the probe found untagged
      nodes. Bounded/repairable, but never silent.
    - ``"unknown"``  -- *untagged* is None, meaning the probe itself could
      not run (Neo4j unreachable, credential rejection, etc). A probe that
      cannot answer must say so, not report green (B3).

    Written prohibition (B7): this is a **data-migration** signal, computed
    once at boot. It MUST NOT be wired to a Kubernetes/ACA liveness or
    readiness probe -- doing so would recreate the exact crash-loop this fix
    removes, one layer up. See docs/azure-deployment.md.
    """
    app.state.schema_untagged_nodes = untagged
    app.state.schema_checked_at = datetime.now(UTC).isoformat()

    if untagged is None:
        app.state.schema_health = "unknown"
        app.state.schema_degraded_reason = (
            "untagged-node probe failed -- graph may be unreachable or "
            "credentials rejected; schema state could not be determined"
        )
        return

    reasons: list[str] = []
    if not constraint_established:
        reasons.append(":Node uniqueness constraint absent (data conflict)")
    if untagged > 0:
        reasons.append(f"{untagged} node(s) lacking the :Node label")

    if reasons:
        app.state.schema_health = "degraded"
        app.state.schema_degraded_reason = "; ".join(reasons)
        logger.error("schema_degraded: %s", app.state.schema_degraded_reason)
    else:
        app.state.schema_health = "healthy"
        app.state.schema_degraded_reason = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Manage application lifespan: configure logging and create shared Neo4j driver.

    Deploy-safe boot (council amendment, 2026-08-12): the server MUST boot
    and serve regardless of graph migration/reachability state -- a deploy
    (restart) must never crash-loop. See
    docs/plans/2026-08-12-deploy-safe-boot-spec.md for the full incident and
    rationale. Concretely:

    - Schema DDL no longer fails closed on graph *data* state
      (``ensure_neo4j_schema(..., fail_on_data_conflict=False)``); a genuine
      :Node constraint data conflict is logged loudly and degrades
      ``schema_health`` instead of raising.
    - The untagged-node probe no longer raises on a positive count; it feeds
      the same tri-state health signal (see ``_record_schema_health``).
    - B1: the ENTIRE startup body -- from the FIRST statement
      (setup_logging), through driver construction and the app.state
      assignments, to schema DDL, the untagged probe, the SchemaMeta
      baseline, and queue/recovery/reconcile -- is wrapped in ONE
      try/except boundary. This is a structural invariant, not a per-site
      patch list: whichever startup step fails (an unwritable /data log
      dir before the volume is mounted, Neo4j unreachable, a TransientError
      during the ACA cold-start race, credential rotation, a corrupt queue
      file, ...), the exception is logged LOUDLY and boot proceeds. Nothing
      may sit before the boundary and prevent the ASGI app from reaching
      `yield` and serving requests. (A live real-process boot test caught
      the earlier version where setup_logging + driver construction sat
      BEFORE the try and a PermissionError on `/data` crash-looped the
      worker with no Neo4j involvement at all.)
    - B6: crash-recovery iterates sessions defensively -- a corrupt
      per-session offset/dead-letter quarantines THAT session (logged,
      skipped) rather than sinking the whole boot; the B1 boundary is the
      backstop for anything the per-session guard doesn't catch.
    """
    # These MUST be set BEFORE the try so they exist on app.state even if the
    # very first statement inside the boundary raises. The shutdown `finally`
    # and the /status handler both read them via getattr with defaults, but
    # seeding them here keeps the tri-state honest from the first instant.
    #
    # B3/B7: tri-state schema-health defaults -- "unknown" until the startup
    # sequence below proves otherwise. A boundary failure leaves these at
    # "unknown" (never coerced to healthy) -- see _record_schema_health.
    app.state.schema_health = "unknown"
    app.state.schema_untagged_nodes = None
    app.state.schema_checked_at = None
    app.state.schema_degraded_reason = None
    # W-2: queue-recovery health is a SEPARATE signal from schema_health --
    # a queue fault is not a schema fault (see the inner try/except around
    # the crash-recovery block below). Defaults "healthy"; only the
    # queue-recovery block itself may downgrade it to "degraded".
    app.state.queue_health = "healthy"
    # Driver slots default to None so the shutdown finally can close them
    # safely even if construction below never ran (B1: construction is now
    # INSIDE the boundary, so it can fail without these ever being assigned).
    app.state.neo4j_driver = None
    app.state.neo4j_query_driver = None
    app.state.neo4j_query_access_mode = None

    # B1: ONE loud try/except boundary around the ENTIRE startup body --
    # setup_logging FIRST, then driver construction, then schema/probe/
    # recovery. Boot NEVER raises regardless of which step fails.
    try:
        # setup_logging() is itself resilient (never raises -- console
        # logging is always configured, file logging is best-effort; see
        # logging_config.setup_logging), but it lives INSIDE the boundary
        # anyway so the invariant holds structurally rather than depending on
        # that guarantee. The except handler below uses the module-level
        # `logger`, which works regardless of whether setup_logging attached
        # any handlers (Python's lastResort emits to stderr as a floor).
        setup_logging()

        _admin = _settings.resolve_neo4j_admin()
        _query = _settings.resolve_neo4j_query()
        logger.info(
            "lifespan_startup: creating Neo4j drivers admin_url=%s query_url=%s "
            "query_access_mode=%s",
            _admin.url,
            _query.url,
            _query.access_mode,
        )
        # Admin (read/write): schema init + all mutation paths. Keep the
        # existing app.state.neo4j_driver NAME so nothing that reads it
        # silently breaks. build_neo4j_driver() is the SAME helper
        # doctor.run_doctor() uses, so the server and the doctor CLI can
        # never construct this connection differently. Driver construction is
        # a non-blocking local object build (no network call) -- but it lives
        # inside the boundary anyway so a misconfigured URL/auth (e.g. a
        # malformed bolt scheme) can never crash-loop boot.
        app.state.neo4j_driver = build_neo4j_driver(_admin)
        # Cypher-query (read-intent): /cypher + dashboard reads.
        app.state.neo4j_query_driver = AsyncGraphDatabase.driver(
            _query.url, auth=_query.auth
        )
        # Stash the resolved query access_mode so /cypher opens READ sessions
        # without re-resolving settings on every request.
        app.state.neo4j_query_access_mode = _query.access_mode

        # Initialize schema (indexes + uniqueness constraints) BEFORE the
        # server starts accepting requests. This ensures the Session
        # uniqueness constraint is active before any concurrent flush()
        # transactions execute MERGE, which prevents the duplicate-Session-
        # node race condition observed under concurrent upload load.
        logger.info(
            "lifespan_startup: initializing Neo4j schema (indexes + uniqueness constraints)"
        )
        # fail_on_data_conflict=False (deploy-safe boot): a genuine :Node
        # constraint data conflict is logged loudly by ensure_neo4j_schema
        # (B2/B4: it also establishes a fallback idx_node_universal so the
        # write path keeps a NodeIndexSeek) and reported via the return
        # value instead of raising -- boot must never fail closed on graph
        # *data* state. Only run_repair/`doctor --fix` still opts into
        # fail_on_data_conflict=True (see ensure_neo4j_schema's docstring).
        constraint_established = await ensure_neo4j_schema(
            app.state.neo4j_driver, fail_on_data_conflict=False
        )
        logger.info("lifespan_startup: Neo4j schema initialized")

        # Migration-health probe: duplicate nodes are already caught above by
        # the :Node constraint; this catches the OTHER un-migrated shape the
        # constraint can't see on its own -- nodes that simply lack the
        # :Node label altogether, which violate no constraint and so raise
        # nothing by themselves. O(1) via the counts store (see
        # count_untagged_nodes) -- this must never regress into the
        # AllNodesScan stall PR #67 removed from the write path.
        #
        # B3: a probe failure is NOT the same as "confirmed clean" -- it
        # means graph state could not be determined. untagged stays None so
        # _record_schema_health reports "unknown", never "healthy".
        try:
            untagged: int | None = await count_untagged_nodes(app.state.neo4j_driver)
        except Exception as exc:  # noqa: BLE001 - connectivity probe, not confirmed bad state
            logger.warning(
                "startup migration-health probe failed (graph unreachable? "
                "credentials rejected?): %s",
                exc,
            )
            untagged = None

        _record_schema_health(app, constraint_established, untagged)

        # SchemaMeta baseline singleton (§10.2 of the cursor-durability spec).
        # Deliberately called ONLY here -- startup, single-writer -- NOT from
        # ensure_neo4j_schema (which also runs on every Neo4jGraphStore's first
        # flush and from doctor --fix; see ensure_schema_version_baseline's
        # docstring for why that would be redundant/concurrent instead of
        # single-writer). Must run AFTER ensure_neo4j_schema above so the rest of
        # the schema (indexes/constraints) is already established. Non-fatal by
        # design (see docstring): never raises, so it cannot block server boot.
        await ensure_schema_version_baseline(app.state.neo4j_driver)

        # Crash recovery (decisions #5/#6): on startup, respawn one drainer per
        # session that still has an undrained, complete line. The workspace is
        # parsed from that session's FIRST log line so the respawned worker is
        # bound to the same workspace it was originally created with.
        #
        # Conservation-counter recovery runs FIRST, and its two steps are
        # order-load-bearing: reconcile MUST precede seed. recovery_reconcile_dead
        # advances committed offsets past already-dead pending lines so the
        # dead-letter counts are settled; only then does recovery_seed_counts read
        # disk to reconstruct the accepted/written baseline. Seeding before
        # reconciling would leave a residual==1 false DEGRADED. Both run before the
        # respawn loop so the respawned drainers start from a conserved baseline.
        # W-2: this recovery block is wrapped in its OWN inner try/except,
        # separate from the outer B1 boundary. A failure here is a QUEUE
        # fault, not a schema fault -- conflating the two (the pre-W-2
        # behavior) made a queue-recovery exception masquerade as
        # schema_health="unknown" with reason "startup sequence failed",
        # which is operator-misleading. Re-raises nothing: the outer B1
        # boundary is still the backstop for anything this doesn't catch.
        try:
            await registry.queue_manager.recovery_reconcile_dead()
            _accepted_seed, _written_seed = (
                await registry.queue_manager.recovery_seed_counts()
            )
            registry.seed_counters(_accepted_seed, _written_seed)
            recovered = await registry.queue_manager.recover()
            respawned = 0
            for sid in recovered:
                # B6: iterate defensively -- a corrupt per-session .offset/
                # dead-letter quarantines THAT session (logged, skipped)
                # rather than sinking the whole boot via an unhandled
                # exception here.
                try:
                    batch = await registry.queue_manager.read_batch(sid, max_items=1)
                    if not batch.lines:
                        continue
                    if _recover_one_session(
                        sid, batch.lines[0], registry.get_or_create
                    ):
                        respawned += 1
                except Exception:  # quarantine this session, don't sink boot
                    logger.exception("recovery_session_quarantined session=%s", sid)
            logger.info(
                "lifespan_startup: crash recovery respawned %d/%d drainers",
                respawned,
                len(recovered),
            )
        except Exception:  # W-2: a queue fault, not a schema fault
            logger.exception(
                "queue_recovery_degraded: queue-recovery sequence failed but "
                "boot continues (deploy-safe boot invariant) -- this is a "
                "queue-health signal, distinct from schema_health"
            )
            app.state.queue_health = "degraded"

        # WS-3a: wire the live admin driver into the maintenance coordinator
        # so the gate/status probe can run. Placed after schema-health is
        # recorded (not inside the W-2 block above) so a queue-recovery
        # failure never prevents the coordinator from being bound -- the
        # maintenance gate must reflect graph/schema state, not queue state.
        coordinator.bind_driver(
            app.state.neo4j_driver,
            untagged=untagged,
            probe_ttl_seconds=_settings.maintenance_probe_ttl_seconds,
        )
    except Exception as exc:  # B1: deploy-safe boot -- NEVER crash-loop
        logger.exception(
            "startup_degraded: lifespan startup sequence failed but boot "
            "continues (deploy-safe boot invariant -- the server must never "
            "crash-loop on graph/data state)"
        )
        app.state.schema_health = "unknown"
        app.state.schema_degraded_reason = f"startup sequence failed: {exc}"
        app.state.schema_checked_at = datetime.now(UTC).isoformat()

    try:
        yield
    finally:
        # Defensive shutdown: driver construction is INSIDE the B1 boundary
        # now, so either driver may be None (construction failed or never
        # ran). Close only what exists, and never let a close() error escape
        # shutdown -- it is not a boot concern and must not mask the reason
        # the app is shutting down.
        logger.info("lifespan_shutdown: closing Neo4j drivers")
        for _attr in ("neo4j_driver", "neo4j_query_driver"):
            _driver = getattr(app.state, _attr, None)
            if _driver is None:
                continue
            try:
                await _driver.close()
            except Exception:  # shutdown best-effort -- never raise here
                logger.exception("lifespan_shutdown: error closing %s", _attr)


app = FastAPI(
    title="Context Intelligence Server",
    version=__version__,
    lifespan=lifespan,
    # Headless server: no browser-facing UI. The OpenAPI contract + Swagger UI
    # are the developer surface and are always registered; ReDoc is a redundant
    # second doc UI and is intentionally left off (docs_url=None equivalent).
    docs_url="/docs",
    redoc_url=None,
    openapi_url="/openapi.json",
)
app.include_router(admin_router)
app.include_router(version_router)
app.include_router(queues_router)
# WS-3a: registered on `app` itself, NOT on the auth-wrapped `asgi_app` --
# so it cannot be bypassed by the bare `main:app` entrypoint (the same class
# of bug the auth-fold fix below addresses one layer over). BearerTokenMiddleware
# wraps `app`, so auth still runs FIRST; an unauthenticated request 401s
# before it ever reaches this gate.
app.middleware("http")(maintenance_gate_middleware)
_start_time = time.time()
registry = SessionRegistry()
# Expose the registry singleton on app.state so routers can read it via
# request.app.state.registry instead of importing the module-level name
# (avoids a circular import between main and the routers package).
app.state.registry = registry
idempotency_cache = EventIdempotencyCache()

# Session-less events are keyed by a per-workspace sentinel stem so that events
# from distinct workspaces never collide in one durable log (decision #10).
_NO_SESSION_PREFIX = "_no_session__"


def _workspace_slug(workspace: str) -> str:
    """Return a filesystem-safe slug for a workspace (session-less log stem)."""
    slug = re.sub(r"[^a-z0-9]+", "-", (workspace or "").lower()).strip("-")
    return slug or "default"


def _validate_data_timestamp(data: dict[str, Any]) -> None:
    """Raise HTTPException(400) if data['timestamp'] is missing, empty, or not ISO-8601.

    This is the ingest boundary check (Option A). Real Amplifier clients always
    supply data.timestamp (verified: 224,530 events on disk, 0 missing). This
    guard rejects only malformed/hand-rolled payloads with a clear 400, instead
    of accepting them silently and dead-lettering them later when the graph
    drainer calls make_node_id() on an empty string.
    """
    value = data.get("timestamp")
    if value is None or not isinstance(value, str) or not value.strip():
        raise HTTPException(
            status_code=400,
            detail="data.timestamp is required and must be a non-empty ISO-8601 string",
        )
    try:
        datetime.fromisoformat(value)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"data.timestamp must be a valid ISO-8601 string; got {value!r}",
        )


def _assert_admin_not_exempt() -> None:
    """Startup assertion (TB-07): /admin/* must NEVER be in any exempt set.

    Called by ``create_asgi_app`` before constructing the middleware.
    Raises ``RuntimeError`` if any ``/admin`` path or prefix appears in
    ``_EXEMPT_PATHS`` or ``_EXEMPT_PREFIXES``, because that would make the
    admin API accessible without authentication.

    This is a defence-in-depth structural check: it is impossible to
    accidentally ship an unauthenticated admin surface.
    """
    import context_intelligence_server.auth as _auth_module

    # Check the exact-path exempt set.
    for path in _auth_module._EXEMPT_PATHS:
        if path == "/admin" or path.startswith("/admin/"):
            raise RuntimeError(
                f"Security invariant violated: /admin path {path!r} found in "
                f"auth._EXEMPT_PATHS.  The /admin surface MUST be "
                f"authenticated — remove it from the exempt set immediately."
            )

    # Check prefix exempt tuple.
    for prefix in _auth_module._EXEMPT_PREFIXES:
        if prefix == "/admin" or prefix.startswith("/admin/") or prefix == "/admin":
            raise RuntimeError(
                f"Security invariant violated: /admin prefix {prefix!r} found in "
                f"auth._EXEMPT_PREFIXES.  The /admin surface MUST be authenticated "
                f"— remove it from the exempt prefix list immediately."
            )


def _assert_maintenance_endpoint_allow_listed() -> None:
    """Startup assertion (WS-3a MUST-FIX #1): the maintenance-mode allow-list
    must always contain ``/admin/maintenance``, ``/status``, and ``/version``.

    Called by ``create_asgi_app`` before constructing the middleware, mirroring
    ``_assert_admin_not_exempt`` above. Without this, ``/admin/maintenance``
    could accidentally be gated by its own allow-list -- 503ing at precisely
    the moment it exists to unblock, recreating the ACA deadlock this
    endpoint was built to close.
    """
    required = {"/admin/maintenance", "/status", "/version"}
    missing = required - MAINTENANCE_ALLOW_LIST
    if missing:
        raise RuntimeError(
            f"Availability invariant violated: {sorted(missing)!r} missing from "
            f"maintenance.MAINTENANCE_ALLOW_LIST -- these paths must never be "
            f"gated by maintenance mode."
        )


def _assert_neo4j_clients_explicit(settings: Settings) -> None:
    """Startup assertion (doc 11 gap #12): the deployed profile MUST declare the
    structured neo4j.admin / neo4j.cypher_query clients explicitly.

    When settings.neo4j_require_explicit_clients is True, refuse to boot if the
    server silently fell back to the legacy flat neo4j_* fields (settings.neo4j is
    None). Back-compat fallback is allowed ONLY when the flag is False (dev / test /
    transition). This makes a silent partial-config fallback impossible in the
    deployed profile.
    """
    if settings.neo4j_require_explicit_clients and settings.neo4j is None:
        raise RuntimeError(
            "Neo4j config invariant violated: neo4j_require_explicit_clients=True but "
            "the structured `neo4j` block (admin + cypher_query) is absent — the server "
            "would silently fall back to legacy neo4j_* fields. The deployed profile MUST "
            "declare both clients explicitly (doc 11 §Backward-compatibility). Set the "
            "`neo4j` block in amplifier-online.yaml / server-config.yaml, or unset "
            "neo4j_require_explicit_clients for a dev/transition deploy."
        )


def create_asgi_app(
    settings: Settings | None = None,
    *,
    _jwks_client: Any = None,
) -> BearerTokenMiddleware:
    """Return the ASGI app wrapped with auth middleware.

    This is the single strategy-selection point.  *settings* defaults to
    the module-level ``_settings`` (the cached production config).  Pass an
    explicit :class:`~context_intelligence_server.config.Settings` instance
    from tests to exercise specific configurations without touching the live
    cached settings.

    *_jwks_client* is an injectable JWKS client used **only** when
    ``auth_mode="entra"`` — intended for tests that need to construct an
    :class:`~context_intelligence_server.auth.EntraResolver` without making
    real network calls.  Production deployments leave it as ``None``; the
    resolver builds a real ``PyJWKClient`` internally.

    Startup behavior on an EMPTY store:
        An empty keystore (static) or empty identity map (entra) NO LONGER
        raises — it is a supported bootstrap state. The server BOOTS
        fail-CLOSED and logs a loud startup WARNING; every request 401/403s
        until the store is populated at runtime via the /admin API. Wide-open
        pass-through is reachable ONLY via the explicit
        ``settings.allow_unauthenticated=True`` opt-out combined with no
        credentials configured, which additionally logs a "WIDE OPEN" warning.

    Raises:
        RuntimeError: (TB-07) When any ``/admin`` path or prefix appears in an
            auth-exempt set.  The admin API surface must never be unguarded.
    """
    global _api_key_store, _entra_identity_store

    # TB-07 structural assertion: /admin must not be in any exempt set.
    # This runs before any middleware construction so the failure is loud and
    # immediate — no request ever reaches an unauthenticated /admin endpoint.
    _assert_admin_not_exempt()
    # WS-3a MUST-FIX #1: /admin/maintenance (and /status, /version) must never
    # be gated by maintenance mode -- same defence-in-depth pattern as above.
    _assert_maintenance_endpoint_allow_listed()

    s = settings if settings is not None else _settings
    _assert_neo4j_clients_explicit(s)

    # Reset both stores; the active mode sets exactly one of them below.
    # app.state.* mirrors the module-level globals so the /admin router can
    # access the live stores via request.app.state without importing from main
    # (which would create a circular import).
    _api_key_store = None
    _entra_identity_store = None
    app.state.api_key_store = None
    app.state.entra_identity_store = None

    # T5: store auth/admin config on app.state so the require_admin dependency
    # can read it without importing from main (avoids circular import) and so
    # test-specific settings (passed via create_asgi_app(settings=...)) take
    # effect without relying on the module-level cached get_settings().
    app.state.auth_mode = s.auth_mode
    app.state.admin_api_key_configured = s.resolve_admin_api_key_digest() is not None
    app.state.entra_admin_role = s.entra_admin_role
    # M2: service capability role names for require_write / require_read deps.
    app.state.service_data_role = s.service_data_role
    app.state.reader_role = s.reader_role

    # Compute the admin-key digest for the middleware (static mode only).
    # The middleware checks the bearer token's sha256 against this digest BEFORE
    # calling the resolver, so the admin key can authenticate even though it is
    # not in the data keystore (ROB F1).
    #
    # Storage-at-rest is resolved by Settings: the RECOMMENDED admin_api_key_sha256
    # (digest at rest) is used verbatim; the legacy raw admin_api_key (DEPRECATED,
    # plaintext at rest) is hashed by the resolver.  Surface the deprecation and
    # precedence as one-time startup warnings so operators can migrate.
    admin_api_key_digest: str | None = s.resolve_admin_api_key_digest()
    if s.admin_api_key is not None and s.admin_api_key_sha256 is not None:
        logger.warning(
            "Both admin_api_key and admin_api_key_sha256 are configured; using "
            "admin_api_key_sha256 (digest at rest) and IGNORING the raw "
            "admin_api_key. Remove the raw admin_api_key from your config."
        )
    elif s.admin_api_key is not None:
        logger.warning(
            "admin_api_key is configured as a RAW token, which stores the secret "
            "in plaintext at rest. This is DEPRECATED. Store its SHA-256 digest in "
            "admin_api_key_sha256 instead (see docs/managing-api-keys.md): "
            'python3 -c "import hashlib,sys;print(hashlib.sha256('
            'sys.argv[1].encode()).hexdigest())" "<token>"'
        )

    if s.auth_mode == "entra":
        # Build and load the entra identity store.
        entra_store = IdentityStore(Path(s.entra_identities_store_path))
        entra_store.load()
        if not entra_store.path.exists():
            # First boot: seed in-process map from config.  Converts the flat
            # {oid -> contributor_id} from build_identity_map() to the rich
            # {oid -> {"id": contributor_id}} format that IdentityStore expects.
            config_map = s.build_identity_map()
            if config_map:
                rich_seed = {oid: {"id": cid} for oid, cid in config_map.items()}
                entra_store.seed(rich_seed)
        _entra_identity_store = entra_store
        app.state.entra_identity_store = entra_store

        # Bootstrap visibility: announce an EMPTY identity map loudly at startup.
        # This is a SUPPORTED state, not an error — the server is up and serving.
        # Delegated (human) tokens will 403 until an IdentityAdmin role-holder
        # binds the first oid via PUT /admin/identities/{oid}. Without this line
        # an empty map would be silent and look like a misconfiguration.
        if not entra_store.flat_dict:
            logger.warning(
                "entra identity map is EMPTY at startup (0 bound oids) — server "
                "is UP and serving, but every delegated (human) token will "
                "receive 403 until identities are onboarded. Bind the first user "
                "with an IdentityAdmin-role token via PUT /admin/identities/{oid} "
                "(store=%s). This is expected on a fresh /data volume.",
                s.entra_identities_store_path,
            )

        # B4: boot disjointness invariant — each oid must belong to exactly one
        # identity source.  Building the service map here (not inline in the
        # EntraResolver call) lets us check the overlap BEFORE construction so
        # the server fails loudly at startup rather than silently misbehaving.
        # This is cheap hygiene: B1 already keeps app tokens off the human map
        # at request time; this prevents a same-oid-in-both misconfiguration.
        _service_id_map = s.build_service_identity_map()
        _entra_oids = set(entra_store.flat_dict.keys())
        _service_oids = set(_service_id_map.keys())
        _overlap = _entra_oids & _service_oids
        if _overlap:
            raise RuntimeError(
                f"Boot invariant violated (B4): oid(s) {sorted(_overlap)!r} appear "
                f"in both entra_identities and service_identities. Each oid must "
                f"belong to exactly one identity source. Fix the config to remove "
                f"the overlap before restarting."
            )

        # EntraResolver raises RuntimeError at construction if the JWKS
        # prefetch fails (eager fail-closed guard from §8b / crusty gate).
        # Pass entra_store.flat_dict (the LIVE dict) so the resolver sees
        # any put()/delete() made by /admin immediately, no restart required.
        resolver: StaticKeyResolver | EntraResolver = EntraResolver(
            s.azure_client_id,  # type: ignore[arg-type]  — validated non-None by config
            s.azure_tenant_id,  # type: ignore[arg-type]  — validated non-None by config
            entra_store.flat_dict,  # live reference — mutations visible immediately
            service_identity_map=_service_id_map,  # B4: pre-built, disjointness verified
            service_data_role=s.service_data_role,  # M2: role gate
            reader_role=s.reader_role,  # M2: role gate
            entra_admin_role=s.entra_admin_role,  # M2: role gate
            jwks_client=_jwks_client,
        )
        # Entra mode does not use admin_api_key_digest (admin via roles claim).
        admin_api_key_digest = None
    else:
        # Build and load the API-key store.
        key_store = IdentityStore(Path(s.api_keys_store_path))
        key_store.load()
        if not key_store.path.exists():
            # First boot: seed from config.  Converts the flat
            # {sha256_hex -> contributor_id} from build_keystore() to the
            # rich {sha256_hex -> {"id": contributor_id}} format.
            config_ks = s.build_keystore()
            if config_ks:
                rich_seed = {digest: {"id": cid} for digest, cid in config_ks.items()}
                key_store.seed(rich_seed)
        _api_key_store = key_store
        app.state.api_key_store = key_store

        # Bootstrap visibility: announce an EMPTY keystore loudly at startup.
        # This is a SUPPORTED state (fail-CLOSED, not fail-open) — the server
        # is up and serving, but every request 401s until keys are onboarded.
        if not key_store.flat_dict:
            if s.resolve_admin_api_key_digest() is not None:
                logger.warning(
                    "static keystore is EMPTY at startup (0 bound keys) — server "
                    "is UP but fail-CLOSED; every request will 401 until keys are "
                    "onboarded. Add the first key with the admin token via "
                    "PUT /admin/keys/{sha256hash} (store=%s). Expected on a fresh "
                    "/data volume.",
                    s.api_keys_store_path,
                )
            else:
                logger.warning(
                    "static keystore is EMPTY at startup (0 bound keys) AND no "
                    "admin_api_key/admin_api_key_sha256 is configured — server is "
                    "UP but fail-CLOSED and CANNOT be bootstrapped at runtime (the "
                    "/admin API is unreachable without an admin key: every token "
                    "401s at the middleware before require_admin runs). Set "
                    "admin_api_key/admin_api_key_sha256 to enable runtime "
                    "onboarding, or add api_keys in config and restart. (store=%s)",
                    s.api_keys_store_path,
                )

        # Pass key_store.flat_dict (the LIVE dict) so the resolver sees any
        # put()/delete() made by /admin immediately, no restart required.
        resolver = StaticKeyResolver(key_store.flat_dict)

    # Wide-open warning: fires ONLY on the explicit allow_unauthenticated
    # opt-out combined with no credentials configured. An empty keystore/map
    # ALONE no longer triggers this (and no longer refuses to start) — it now
    # boots fail-closed instead (see the empty-map/keystore warnings above).
    if s.allow_unauthenticated and not resolver.auth_enabled:
        logger.warning(
            "allow_unauthenticated=True AND no credentials configured — the "
            "server is WIDE OPEN: EVERY request is admitted UNAUTHENTICATED. "
            "This opt-out is for TEST/DEV ONLY and must NEVER be set in "
            "production. Configure api_key/api_keys (static) or entra_identities "
            "(entra) and unset allow_unauthenticated to enforce authentication."
        )

    # Log admin capability status for operator visibility (E: status surfacing).
    if s.auth_mode == "static":
        _admin_status = (
            "enabled"
            if s.resolve_admin_api_key_digest() is not None
            else "disabled (admin_api_key/admin_api_key_sha256 not set)"
        )
    else:
        _admin_status = (
            f"enabled (role={s.entra_admin_role!r})"
            if s.entra_admin_role
            else "disabled (entra_admin_role not configured)"
        )
    logger.info(
        "create_asgi_app: auth_mode=%s admin_api=%s",
        s.auth_mode,
        _admin_status,
    )

    # T6: store the admin-key digest on app.state so the /admin router handlers
    # can read it without importing from main (no circular import) and so that
    # test-specific settings are honoured.  In entra mode admin_api_key_digest
    # has already been set to None above (line ~385); in static mode it is the
    # sha256 of admin_api_key (or None when admin_api_key is not configured).
    app.state.admin_api_key_digest = admin_api_key_digest

    return BearerTokenMiddleware(
        app,
        resolver=resolver,
        exempt_paths=_EXEMPT_PATHS,
        admin_api_key_digest=admin_api_key_digest,
        allow_unauthenticated=s.allow_unauthenticated,
    )


# Module-level ASGI app used by Gunicorn: context_intelligence_server.main:asgi_app
# The raw `app` is kept for internal use and testing against un-authed routes.
asgi_app: BearerTokenMiddleware = create_asgi_app()


# ---------------------------------------------------------------------------
# M2 — service capability dependencies (moved to authz.py to avoid circular import)
#
# require_write, require_read, _is_write_capable are imported from
# context_intelligence_server.authz at the top of this file (re-exported here
# so tests and existing imports from main still work).
# ---------------------------------------------------------------------------


@app.get("/status")
async def get_status(request: Request) -> dict[str, Any]:
    response = build_status_response(registry, _start_time)
    response["neo4j_connected"] = await _check_driver_connected(
        request.app, "neo4j_driver"
    )
    # Additive (Concern B, council review): surface the query (read-intent)
    # driver's connectivity too, so a misconfigured cypher_query client shows
    # up here instead of on the first /cypher call.
    response["neo4j_query_connected"] = await _check_driver_connected(
        request.app, "neo4j_query_driver"
    )
    response["neo4j_url"] = _settings.resolve_neo4j_admin().url
    response["neo4j_browser_url"] = _settings.neo4j_browser_url
    # Additive, aggregate-only conservation metrics (D3). /status is
    # unauthenticated, so this block must NOT carry the per-key table or the
    # dead-letter listing — both are authenticated-only.
    response["metrics"] = await registry.pipeline_metrics()
    # T5 (E): surface auth mode and admin-API capability so operators can
    # confirm admin is enabled without tailing startup logs.  /status is
    # unauthenticated — only config-level boolean flags are exposed here
    # (no credential values, no key hashes, no token details).
    _auth_mode = getattr(request.app.state, "auth_mode", _settings.auth_mode)
    _admin_key_set = getattr(
        request.app.state,
        "admin_api_key_configured",
        _settings.resolve_admin_api_key_digest() is not None,
    )
    _entra_admin_role = getattr(
        request.app.state, "entra_admin_role", _settings.entra_admin_role
    )
    response["auth"] = {
        "mode": _auth_mode,
        "admin_api_enabled": (
            _admin_key_set if _auth_mode == "static" else bool(_entra_admin_role)
        ),
        # Surface the role names (not secrets) so operators can confirm which
        # roles are configured without exposing credential values.  Additive:
        # existing fields (mode, admin_api_enabled, entra_admin_role) are
        # unchanged; reader_role and service_data_role are new in M2.
        **(
            {
                "entra_admin_role": _entra_admin_role,
                "reader_role": getattr(
                    request.app.state, "reader_role", _settings.reader_role
                ),
                "service_data_role": getattr(
                    request.app.state, "service_data_role", _settings.service_data_role
                ),
            }
            if _auth_mode == "entra"
            else {}
        ),
    }
    # WS-3a: mode/schema_health are now DE-LATCHED -- sourced from the live,
    # TTL-cached MaintenanceCoordinator probe instead of the boot-only
    # snapshot. This self-clears after an out-of-band repair (`doctor --fix`
    # or `POST /admin/maintenance`) within the probe's TTL, with NO restart
    # required -- the exact latch this replaces.
    #
    #   *** schema_health/mode MUST NOT be wired to a Kubernetes/ACA        ***
    #   *** liveness or readiness probe. Doing so would recreate the exact ***
    #   *** crash-loop the deploy-safe-boot fix removes, one layer up      ***
    #   *** (see docs/azure-deployment.md).                                ***
    _maint = await coordinator.status()
    response["mode"] = _maint.mode
    response["maintenance_started_at"] = _maint.started_at
    response["maintenance_elapsed_seconds"] = _maint.elapsed_seconds
    if _maint.constraint_present is None:
        response["schema_health"] = "unknown"
    elif _maint.constraint_present is False:
        response["schema_health"] = "degraded"
    elif (getattr(request.app.state, "schema_untagged_nodes", None) or 0) > 0:
        response["schema_health"] = "degraded"
    else:
        response["schema_health"] = "healthy"
    # untagged_nodes stays the BOOT-time value (documented as such, not a
    # gate input -- the live gate/mode signal is the constraint probe above).
    response["untagged_nodes"] = getattr(
        request.app.state, "schema_untagged_nodes", None
    )
    # Live probe timestamp (bounded staleness <= the probe TTL), replacing
    # the old boot-only snapshot timestamp.
    response["schema_checked_at"] = datetime.now(UTC).isoformat()
    # degraded_reason is sourced from the SAME live coordinator probe that
    # drives mode/schema_health above (_maint.reason), NOT the boot-time
    # app.state snapshot. The snapshot version goes stale after an
    # out-of-band repair (POST /admin/maintenance or `doctor --fix`): mode
    # correctly de-latches to "healthy" but the boot-time reason string kept
    # asserting a constraint-absent condition that was no longer true. This
    # is the same reason MaintenanceCoordinator.status() already produces
    # for the 503 body (maintenance_response), so /status and the 503 stay
    # consistent -- and it naturally clears to None once the live probe
    # confirms the constraint is present and no maintenance op is running.
    response["degraded_reason"] = _maint.reason
    # W-2: queue-recovery health is reported SEPARATELY from schema_health --
    # a queue-recovery fault at boot is not a schema fault (see lifespan).
    response["queue_health"] = getattr(request.app.state, "queue_health", "healthy")
    # W-4: ADVISORY drift signal ONLY, NOT a guard. Surface the STORED
    # :SchemaMeta.schema_version (read fresh from the graph) next to the
    # server's compiled-in SCHEMA_VERSION so a server/graph mismatch is
    # DETECTABLE by automation -- no gating, no migration, no behavior
    # change of any kind results from this. `read_graph_schema_version` is a
    # separate read-only helper (neo4j_store.py); it is intentionally NOT
    # wired into `ensure_schema_version_baseline`'s write path or into
    # `GET /version` (both stay exactly as they were -- see the read/write
    # separation documented on `ensure_schema_version_baseline`). Full
    # mismatch handling/migration is deferred (tracked separately).
    _schema_driver = getattr(request.app.state, "neo4j_driver", None)
    _graph_schema_version = (
        await read_graph_schema_version(_schema_driver)
        if _schema_driver is not None
        else None
    )
    response["graph_schema_version"] = _graph_schema_version
    response["schema_version_current"] = (
        None
        if _graph_schema_version is None
        else _graph_schema_version == SCHEMA_VERSION
    )
    return response


async def _check_driver_connected(app_instance: FastAPI, attr_name: str) -> bool:
    """Check a Neo4j driver's connectivity via verify_connectivity().

    *attr_name* names the app.state attribute holding the driver -- either
    "neo4j_driver" (admin) or "neo4j_query_driver" (cypher_query). Defensive:
    returns False (never raises, never 500s /status) when the driver is
    absent or verify_connectivity() raises for any reason.
    """
    driver = getattr(app_instance.state, attr_name, None)
    if driver is None:
        return False
    try:
        await driver.verify_connectivity()
        return True
    except Exception:
        return False


@app.post(
    "/events",
    status_code=202,
    response_model=EventResponse,
    dependencies=[Depends(require_write)],
)
async def post_events(
    request: EventRequest, http_request: Request, replay: bool = False
) -> EventResponse:
    # Read contributor_id injected by auth middleware (None when auth not configured).
    contributor_id: str | None = http_request.scope.get("state", {}).get(
        "contributor_id"
    )
    session_id = request.data.get("session_id", "")
    # Validate data.timestamp at the ingest boundary (fail loud, not silent dead-letter).
    # Real Amplifier clients always supply this field; 400 only hits malformed payloads.
    _validate_data_timestamp(request.data)
    # Idempotency-cache check + replay stay BEFORE the durable append so a
    # duplicate is rejected without persisting a second log line.
    if request.idempotency_key and not replay:
        is_new = idempotency_cache.check_and_store(request.idempotency_key)
        if not is_new:
            logger.info(
                "event_duplicate_skipped: event=%s session_id=%s",
                request.event,
                session_id,
            )
            return EventResponse(status="duplicate", session_id=session_id or None)
    # Empty session_id maps to a per-workspace sentinel stem so session-less
    # events from distinct workspaces never collide in one log (decision #10).
    worker_key = session_id or (_NO_SESSION_PREFIX + _workspace_slug(request.workspace))
    # Spawn (or reuse) the sticky drainer keyed by worker_key.
    registry.get_or_create(worker_key, request.workspace, created_by=contributor_id)
    # Re-parse the raw validated body bytes, stamp created_by (server-assigned,
    # unconditional overwrite — kills any client-supplied spoofed value), then
    # re-serialize compact JSON before persisting to the durable queue.
    # IMPORTANT: re-parse raw bytes (not the pydantic model) so client extra
    # fields are preserved. body() is cached by Starlette after the first read.
    body = await http_request.body()
    body_obj = json.loads(body)
    body_obj["created_by"] = contributor_id  # overwrite, never setdefault
    # I1: lift the optional top-level working_dir envelope field into body_obj["data"]
    # so it rides the existing data pipeline (registry's _parse_line extracts "data"
    # wholesale) and reaches ensure_session_node. Forward-only: only set when the client
    # supplied it; absent/empty leaves Session.working_dir null.
    if request.working_dir and isinstance(body_obj.get("data"), dict):
        body_obj["data"]["working_dir"] = request.working_dir
    body = json.dumps(body_obj, separators=(",", ":")).encode()
    await registry.queue_manager.append(worker_key, body)
    registry.record_accepted()  # count the durably-accepted event
    return EventResponse(status="queued", session_id=session_id or None)


@app.get("/blobs/{session_id}", dependencies=[Depends(require_read)])
async def list_blobs(session_id: str) -> JSONResponse:
    blob_store = AsyncDiskBlobStore(root=_settings.blob_path)
    uris = await blob_store.list(session_id)
    return JSONResponse(content={"session_id": session_id, "blobs": uris})


@app.get("/blobs/{session_id}/{key}", dependencies=[Depends(require_read)])
async def get_blob(session_id: str, key: str) -> JSONResponse:
    blob_store = AsyncDiskBlobStore(root=_settings.blob_path)
    uri = f"ci-blob://{session_id}/{key}"
    try:
        content = await blob_store.read(uri)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"Blob not found: {uri}")
    return JSONResponse(content=content)


@app.post("/cypher", dependencies=[Depends(require_read)])
async def post_cypher(body: CypherRequest, request: Request) -> Response:
    """Proxy a Cypher query to Neo4j and return the results as JSON."""
    driver = request.app.state.neo4j_query_driver
    access_mode = request.app.state.neo4j_query_access_mode
    params = dict(body.params)
    if body.workspace is not None and body.workspace != "*":
        params["workspace"] = body.workspace
    rows: list[dict] = []
    try:
        async with driver.session(
            default_access_mode=_neo4j_access_const(access_mode)
        ) as session:
            result = await session.run(body.query, params)
            async for record in result:
                rows.append(dict(record))
        serialized = json.dumps({"results": rows}, default=str)
        return Response(content=serialized, media_type="application/json")
    except Exception as exc:  # catch all Neo4j and serialization errors
        raise HTTPException(status_code=500, detail=str(exc))


def main(argv: list[str] | None = None) -> None:
    """CLI entrypoint.

    INVARIANT: no subcommand (or the explicit ``serve`` subcommand) starts the
    ingestion server. This MUST hold because the systemd unit (and the
    macOS launchd agent) invoke the bare console script
    ``context-intelligence-server`` with NO arguments -- that call dispatches
    to ``serve`` unchanged.

    ``doctor [--fix]`` diagnoses (and, with ``--fix``, repairs) Neo4j graph
    health -- the two O(graph-size) migration scans (dedup + :Node backfill)
    that used to run unconditionally at cold start now live ONLY here, never
    on server boot. See ``context_intelligence_server.doctor``.
    """
    parser = argparse.ArgumentParser(prog="context-intelligence-server")
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("serve", help="Start the ingestion server (default).")
    doctor_parser = subparsers.add_parser(
        "doctor", help="Diagnose (and optionally repair) Neo4j graph health."
    )
    doctor_parser.add_argument(
        "--fix",
        action="store_true",
        help=(
            "Repair detected issues (duplicate-node dedup + :Node label "
            "backfill) instead of reporting only."
        ),
    )

    args = parser.parse_args(argv)

    if args.command in (None, "serve"):
        run()
        return

    # Deferred import: doctor.py imports build_neo4j_driver back from this
    # module, so importing it at module load time (rather than here, inside
    # main()) would be a circular import at import time. By the time main()
    # runs, this module has already finished executing top-to-bottom.
    from context_intelligence_server import doctor as _doctor

    sys.exit(asyncio.run(_doctor.run_doctor(fix=args.fix)))


def _effective_worker_count() -> int:
    """Return the worker count gunicorn will actually honor from WEB_CONCURRENCY.

    WEB_CONCURRENCY is gunicorn's own env override for the worker count, so it
    is the single source of truth for how many worker processes will run. Unset
    means 1; a non-integer value is treated as 1 (with a warning) rather than
    crashing on a malformed env var.
    """
    raw = os.environ.get("WEB_CONCURRENCY")
    if raw is None:
        return 1
    try:
        return int(raw)
    except ValueError:
        logger.warning(
            "WEB_CONCURRENCY=%r is not an integer; treating effective workers as 1",
            raw,
        )
        return 1


def _validate_single_worker(workers: int | None = None) -> int:
    """Fail loud unless exactly one worker will run; return that worker count.

    The durable drainer assumes exactly one drainer per session per process, so
    more than one worker process would split a session's drainer across
    processes and reintroduce the loss this design eliminates. When ``workers``
    is None the effective count is read from WEB_CONCURRENCY (the value gunicorn
    honors) so the guard and the live config can never diverge.
    """
    effective = workers if workers is not None else _effective_worker_count()
    if effective != 1:
        raise RuntimeError(
            f"context-intelligence-server requires exactly one worker, got {effective}. "
            "The durable drainer assumes one drainer per session per process; unset "
            "WEB_CONCURRENCY or set WEB_CONCURRENCY=1. Multi-process operation needs a "
            "distributed backend (Open Q7)."
        )
    return effective


def run() -> None:
    """Start the server using gunicorn + uvicorn worker for graceful SIGTERM shutdown."""
    from gunicorn.app.base import BaseApplication

    # Read WEB_CONCURRENCY and fail loud if it would run != 1 worker. The same
    # value is fed into gunicorn below so the guard and the live config are one
    # source of truth (they can never diverge).
    workers = _validate_single_worker()

    class _App(BaseApplication):
        def load_config(self) -> None:
            for key, value in {
                "bind": f"{_settings.server_host}:{_settings.server_port}",
                "workers": workers,
                "worker_class": "uvicorn.workers.UvicornWorker",
                "timeout": 30,
                "graceful_timeout": 10,
                "loglevel": _settings.log_level.lower(),
            }.items():
                self.cfg.set(key, value)

        def load(self) -> Any:
            return asgi_app

    _App().run()
