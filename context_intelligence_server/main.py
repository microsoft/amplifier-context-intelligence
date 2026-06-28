"""FastAPI application entrypoint for the Context Intelligence Server."""

import asyncio
import json
import logging
import os
import re
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import aiofiles
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import (
    FileResponse,
    HTMLResponse,
    JSONResponse,
    Response,
    StreamingResponse,
)
from fastapi.staticfiles import StaticFiles
from neo4j import AsyncGraphDatabase

from context_intelligence_server import __version__
from context_intelligence_server.auth import (
    BearerTokenMiddleware,
    EntraResolver,
    StaticKeyResolver,
    _EXEMPT_PATHS,
    _EXEMPT_PATHS_API_ONLY,
)
from context_intelligence_server.blob_store import AsyncDiskBlobStore
from context_intelligence_server.config import Settings, get_settings
from context_intelligence_server.dashboard import build_status_response
from context_intelligence_server.routers.queues import router as queues_router
from context_intelligence_server.routers.skills import SkillRegistry
from context_intelligence_server.routers.skills import router as skills_router
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
    logger.info("lifespan_startup: creating Neo4j driver url=%s", _settings.neo4j_url)
    app.state.neo4j_driver = AsyncGraphDatabase.driver(
        _settings.neo4j_url,
        auth=(_settings.neo4j_user, _settings.neo4j_password),
    )
    # Initialize schema (indexes + uniqueness constraints) BEFORE the server starts
    # accepting requests.  This ensures the Session uniqueness constraint is active
    # before any concurrent flush() transactions execute MERGE, which prevents the
    # duplicate-Session-node race condition observed under concurrent upload load.
    logger.info(
        "lifespan_startup: initializing Neo4j schema (indexes + uniqueness constraints)"
    )
    await ensure_neo4j_schema(app.state.neo4j_driver)
    logger.info("lifespan_startup: Neo4j schema initialized")
    _skills_dir = Path(__file__).parent / "skills"
    app.state.skill_registry = SkillRegistry()
    if _skills_dir.exists():
        app.state.skill_registry.load_from_dir(_skills_dir)
        logger.info(
            "lifespan_startup: skill_registry populated count=%d",
            len(app.state.skill_registry.skill_names),
        )
    else:
        logger.warning(
            "lifespan_startup: skills directory not found at %s; skill_registry will be empty",
            _skills_dir,
        )
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
        logger.info("lifespan_shutdown: closing Neo4j driver")
        await app.state.neo4j_driver.close()


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
app.include_router(skills_router)
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


_WEB_DIR = Path(__file__).parent / "web"


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
    """
    s = settings if settings is not None else _settings

    if s.auth_mode == "entra":
        # EntraResolver raises RuntimeError at construction if the JWKS
        # prefetch fails (eager fail-closed guard from §8b / crusty gate).
        resolver: StaticKeyResolver | EntraResolver = EntraResolver(
            s.azure_client_id,  # type: ignore[arg-type]  — validated non-None by config
            s.azure_tenant_id,  # type: ignore[arg-type]  — validated non-None by config
            s.build_identity_map(),
            jwks_client=_jwks_client,
        )
    else:
        resolver = StaticKeyResolver(s.build_keystore())

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

    # Select the auth-exempt path set based on web_ui_enabled.
    # web_ui_enabled=False (api-only): use the smaller set that excludes /logs/stream
    # and other web-UI paths so they cannot be reached unauthenticated.
    # web_ui_enabled=True (default): use the full set including web-UI paths.
    exempt = _EXEMPT_PATHS if s.web_ui_enabled else _EXEMPT_PATHS_API_ONLY
    return BearerTokenMiddleware(app, resolver=resolver, exempt_paths=exempt)


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


@app.get("/status")
async def get_status(request: Request) -> dict[str, Any]:
    response = build_status_response(registry, _start_time)
    response["neo4j_connected"] = await _check_neo4j_connected(request.app)
    response["neo4j_url"] = _settings.neo4j_url
    response["neo4j_browser_url"] = _settings.neo4j_browser_url
    # Additive, aggregate-only conservation metrics (D3). /status is
    # unauthenticated, so this block must NOT carry the per-key table or the
    # dead-letter listing — both are authenticated-only.
    response["metrics"] = await registry.pipeline_metrics()
    return response


async def _check_neo4j_connected(app_instance: FastAPI) -> bool:
    """Check Neo4j connectivity via the driver's verify_connectivity method."""
    driver = getattr(app_instance.state, "neo4j_driver", None)
    if driver is None:
        return False
    try:
        await driver.verify_connectivity()
        return True
    except Exception:
        return False


@app.post("/events", status_code=202, response_model=EventResponse)
async def post_events(
    request: EventRequest, http_request: Request, replay: bool = False
) -> EventResponse:
    # Read contributor_id injected by auth middleware (None when auth not configured).
    contributor_id: str | None = http_request.scope.get("state", {}).get(
        "contributor_id"
    )
    session_id = request.data.get("session_id", "")
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


@app.get("/blobs/{session_id}")
async def list_blobs(session_id: str) -> JSONResponse:
    blob_store = AsyncDiskBlobStore(root=_settings.blob_path)
    uris = await blob_store.list(session_id)
    return JSONResponse(content={"session_id": session_id, "blobs": uris})


@app.get("/blobs/{session_id}/{key}")
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


@app.post("/cypher")
async def post_cypher(body: CypherRequest, request: Request) -> Response:
    """Proxy a Cypher query to Neo4j and return the results as JSON."""
    driver = request.app.state.neo4j_driver
    params = dict(body.params)
    if body.workspace is not None and body.workspace != "*":
        params["workspace"] = body.workspace
    rows: list[dict] = []
    try:
        async with driver.session() as session:
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
