"""FastAPI application entrypoint for the Context Intelligence Server."""

import asyncio
import json
import logging
import os
import re
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

import aiofiles
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import (
    FileResponse,
    HTMLResponse,
    JSONResponse,
    Response,
    StreamingResponse,
)
from fastapi.staticfiles import StaticFiles
from neo4j import READ_ACCESS, WRITE_ACCESS, AsyncGraphDatabase

from context_intelligence_server import __version__
from context_intelligence_server.auth import (
    BearerTokenMiddleware,
    EntraResolver,
    StaticKeyResolver,
    _EXEMPT_PATHS,
    _EXEMPT_PATHS_API_ONLY,
)
from context_intelligence_server.authz import (  # noqa: F401 — re-exported for tests/routes
    _is_write_capable,
    require_read,
    require_write,
)
from context_intelligence_server.blob_store import AsyncDiskBlobStore
from context_intelligence_server.config import Settings, get_settings
from context_intelligence_server.identity_store import IdentityStore
from context_intelligence_server.dashboard import build_status_response
from context_intelligence_server.routers.admin import router as admin_router
from context_intelligence_server.routers.queues import router as queues_router
from context_intelligence_server.routers.version import router as version_router
from context_intelligence_server.idempotency import EventIdempotencyCache
from context_intelligence_server.logging_config import setup_logging
from context_intelligence_server.models import (
    CypherRequest,
    EventRequest,
    EventResponse,
)
from context_intelligence_server.neo4j_store import ensure_neo4j_schema
from context_intelligence_server.registry import SessionRegistry

_settings = get_settings()

logger = logging.getLogger("context_intelligence_server")


def _neo4j_access_const(mode: str) -> str:
    """Map our config string ("READ"/"WRITE") to the driver's access-mode constant."""
    return READ_ACCESS if mode == "READ" else WRITE_ACCESS


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


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Manage application lifespan: configure logging and create shared Neo4j driver."""
    setup_logging()
    _admin = _settings.resolve_neo4j_admin()
    _query = _settings.resolve_neo4j_query()
    logger.info(
        "lifespan_startup: creating Neo4j drivers admin_url=%s query_url=%s query_access_mode=%s",
        _admin.url,
        _query.url,
        _query.access_mode,
    )
    # Admin (read/write): schema init + all mutation paths. Keep the existing
    # app.state.neo4j_driver NAME so nothing that reads it silently breaks.
    app.state.neo4j_driver = AsyncGraphDatabase.driver(_admin.url, auth=_admin.auth)
    # Cypher-query (read-intent): /cypher + dashboard reads.
    app.state.neo4j_query_driver = AsyncGraphDatabase.driver(
        _query.url, auth=_query.auth
    )
    # Stash the resolved query access_mode so /cypher opens READ sessions without
    # re-resolving settings on every request.
    app.state.neo4j_query_access_mode = _query.access_mode
    # Initialize schema (indexes + uniqueness constraints) BEFORE the server starts
    # accepting requests.  This ensures the Session uniqueness constraint is active
    # before any concurrent flush() transactions execute MERGE, which prevents the
    # duplicate-Session-node race condition observed under concurrent upload load.
    logger.info(
        "lifespan_startup: initializing Neo4j schema (indexes + uniqueness constraints)"
    )
    await ensure_neo4j_schema(app.state.neo4j_driver)
    logger.info("lifespan_startup: Neo4j schema initialized")
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
    await registry.queue_manager.recovery_reconcile_dead()
    _accepted_seed, _written_seed = await registry.queue_manager.recovery_seed_counts()
    registry.seed_counters(_accepted_seed, _written_seed)
    recovered = await registry.queue_manager.recover()
    respawned = 0
    for sid in recovered:
        batch = await registry.queue_manager.read_batch(sid, max_items=1)
        if not batch.lines:
            continue
        if _recover_one_session(sid, batch.lines[0], registry.get_or_create):
            respawned += 1
    logger.info(
        "lifespan_startup: crash recovery respawned %d/%d drainers",
        respawned,
        len(recovered),
    )
    try:
        yield
    finally:
        logger.info("lifespan_shutdown: closing Neo4j drivers")
        await app.state.neo4j_driver.close()
        await app.state.neo4j_query_driver.close()


app = FastAPI(
    title="Context Intelligence Server",
    version=__version__,
    lifespan=lifespan,
    # web_ui_enabled=False locks down to API-only: no OpenAPI schema, no Swagger UI.
    # Must be set at construction time — FastAPI does not support changing these after init.
    docs_url="/docs" if _settings.web_ui_enabled else None,
    redoc_url="/redoc" if _settings.web_ui_enabled else None,
    openapi_url="/openapi.json" if _settings.web_ui_enabled else None,
)
app.include_router(admin_router)
app.include_router(version_router)
app.include_router(queues_router)
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


_WEB_DIR = Path(__file__).parent / "web"


def _assert_admin_not_exempt() -> None:
    """Startup assertion (TB-07): /admin/* must NEVER be in any exempt set.

    Called by ``create_asgi_app`` before constructing the middleware.
    Raises ``RuntimeError`` if any ``/admin`` path or prefix appears in
    ``_EXEMPT_PATHS``, ``_EXEMPT_PATHS_API_ONLY``, or ``_EXEMPT_PREFIXES``,
    because that would make the admin API accessible without authentication.

    This is a defence-in-depth structural check: it is impossible to
    accidentally ship an unauthenticated admin surface.
    """
    import context_intelligence_server.auth as _auth_module  # noqa: PLC0415

    # Check exact-path exempt sets.
    for exempt_set_name, exempt_set in (
        ("_EXEMPT_PATHS", _auth_module._EXEMPT_PATHS),
        ("_EXEMPT_PATHS_API_ONLY", _auth_module._EXEMPT_PATHS_API_ONLY),
    ):
        for path in exempt_set:
            if path == "/admin" or path.startswith("/admin/"):
                raise RuntimeError(
                    f"Security invariant violated: /admin path {path!r} found in "
                    f"auth.{exempt_set_name}.  The /admin surface MUST be "
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

    Raises:
        RuntimeError: When the chosen resolver has ``auth_enabled=False``
            (no credentials configured for the selected mode) AND
            ``settings.allow_unauthenticated`` is ``False`` (default).
            This is the fail-closed startup gate — the server must never boot
            silently unauthenticated in production.
        RuntimeError: (TB-07) When any ``/admin`` path or prefix appears in an
            auth-exempt set.  The admin API surface must never be unguarded.
    """
    global _api_key_store, _entra_identity_store

    # TB-07 structural assertion: /admin must not be in any exempt set.
    # This runs before any middleware construction so the failure is loud and
    # immediate — no request ever reaches an unauthenticated /admin endpoint.
    _assert_admin_not_exempt()

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

        # Pass key_store.flat_dict (the LIVE dict) so the resolver sees any
        # put()/delete() made by /admin immediately, no restart required.
        resolver = StaticKeyResolver(key_store.flat_dict)

    # Fail-closed gate: refuse to start if authentication is not configured.
    # The only valid exception is allow_unauthenticated=True, an explicit opt-out
    # for test harnesses and local dev environments — never set in production.
    if not resolver.auth_enabled and not s.allow_unauthenticated:
        raise RuntimeError(
            "No authentication configured — the server refuses to start. "
            "For auth_mode='static': set api_key or api_keys in the config. "
            "For auth_mode='entra': set azure_client_id, azure_tenant_id, and "
            "entra_identities. "
            "To allow unauthenticated access (TEST/DEV ONLY) set "
            "allow_unauthenticated: true in the config."
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

    # Select the auth-exempt path set based on web_ui_enabled.
    # web_ui_enabled=False (api-only): use the smaller set that excludes /logs/stream
    # and other web-UI paths so they cannot be reached unauthenticated.
    # web_ui_enabled=True (default): use the full set including web-UI paths.
    exempt = _EXEMPT_PATHS if s.web_ui_enabled else _EXEMPT_PATHS_API_ONLY
    return BearerTokenMiddleware(
        app,
        resolver=resolver,
        exempt_paths=exempt,
        admin_api_key_digest=admin_api_key_digest,
    )


# Module-level ASGI app used by Gunicorn: context_intelligence_server.main:asgi_app
# The raw `app` is kept for internal use and testing against un-authed routes.
asgi_app: BearerTokenMiddleware = create_asgi_app()


if _settings.web_ui_enabled:
    # static mount moved into this block so there is ONE conditional for all web-UI
    # registrations that appear before the API routes.  (The /logs/stream route has
    # its own block below because it lives after the /blobs routes in the file.)
    app.mount("/static", StaticFiles(directory=_WEB_DIR / "static"), name="static")

    @app.get("/", response_class=HTMLResponse)
    async def index() -> FileResponse:
        return FileResponse(_WEB_DIR / "index.html")

    @app.get("/dashboard", response_class=HTMLResponse)
    async def dashboard() -> FileResponse:
        return FileResponse(_WEB_DIR / "dashboard.html")


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


if _settings.web_ui_enabled:
    # /logs/stream is web-UI only — the dashboard consumes it.  When web_ui_enabled=False
    # this route is absent (→ 404) and the path is not in _EXEMPT_PATHS_API_ONLY, so any
    # unauthenticated request is gated by the middleware (→ 401).
    @app.get("/logs/stream")
    async def stream_logs(request: Request) -> StreamingResponse:
        """Stream server log lines as Server-Sent Events."""
        log_path = Path(_settings.log_path)

        async def event_generator() -> AsyncGenerator[str, None]:
            # Backfill last 200 lines (skip if log file does not yet exist)
            if log_path.exists():
                for line in log_path.read_text().splitlines()[-200:]:
                    yield f"data: {line}\n\n"

            # Tail new lines (return early if log file still does not exist)
            if not log_path.exists():
                return
            async with aiofiles.open(log_path, mode="r") as f:
                await f.seek(0, 2)
                while True:
                    if await request.is_disconnected():
                        break
                    line = await f.readline()
                    if not line:
                        await asyncio.sleep(0.2)
                    else:
                        yield f"data: {line.rstrip()}\n\n"

        return StreamingResponse(
            event_generator(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            },
        )


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


def main() -> None:
    """CLI entrypoint."""
    run()


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
