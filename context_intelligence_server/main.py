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
from context_intelligence_server.auth import BearerTokenMiddleware
from context_intelligence_server.blob_store import AsyncDiskBlobStore
from context_intelligence_server.config import get_settings
from context_intelligence_server.dashboard import build_status_response
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
    recovered = await registry.queue_manager.recover()
    respawned = 0
    for sid in recovered:
        batch = await registry.queue_manager.read_batch(sid, max_items=1)
        if not batch.lines:
            continue
        try:
            obj = json.loads(batch.lines[0])
            workspace = obj.get("workspace", "")
        except (ValueError, KeyError):
            workspace = ""
        if not workspace:
            # Panel finding #10: never spawn a workspace='' worker — it would
            # violate the EventRequest non-empty-workspace invariant (422). A
            # torn or empty-workspace first line is skipped, not recovered.
            logger.warning(
                "recovery_skipped session=%s: torn or empty workspace in first line",
                sid,
            )
            continue
        registry.get_or_create(sid, workspace)
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
    title="Context Intelligence Server", version=__version__, lifespan=lifespan
)
app.include_router(skills_router)
app.include_router(version_router)
_start_time = time.time()
registry = SessionRegistry()
idempotency_cache = EventIdempotencyCache()

# Session-less events are keyed by a per-workspace sentinel stem so that events
# from distinct workspaces never collide in one durable log (decision #10).
_NO_SESSION_PREFIX = "_no_session__"


def _workspace_slug(workspace: str) -> str:
    """Return a filesystem-safe slug for a workspace (session-less log stem)."""
    slug = re.sub(r"[^a-z0-9]+", "-", (workspace or "").lower()).strip("-")
    return slug or "default"


_WEB_DIR = Path(__file__).parent / "web"
app.mount("/static", StaticFiles(directory=_WEB_DIR / "static"), name="static")


def create_asgi_app() -> BearerTokenMiddleware:
    """Return the ASGI app wrapped with auth middleware."""
    return BearerTokenMiddleware(app, api_key=_settings.api_key)


# Module-level ASGI app used by Gunicorn: context_intelligence_server.main:asgi_app
# The raw `app` is kept for internal use and testing against un-authed routes.
asgi_app: BearerTokenMiddleware = create_asgi_app()


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
    registry.get_or_create(worker_key, request.workspace)
    # Persist the EXACT request body bytes (the raw EventRequest JSON, which
    # preserves idempotency_key) BEFORE returning 202 — this is the
    # zero-silent-loss window. The body is compact JSON with no literal newline
    # bytes, so the newline-delimited log framing is safe.
    body = await http_request.body()
    await registry.queue_manager.append(worker_key, body)
    logger.info("event_enqueued: event=%s session_id=%s", request.event, session_id)
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
    """CLI entrypoint.

    context-intelligence-server           → start the server (gunicorn)
    context-intelligence-server init ...  → run first-run configuration
    """
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "init":
        # Strip "init" from argv so init_command's argparse sees only its own flags
        sys.argv = [sys.argv[0]] + sys.argv[2:]
        from context_intelligence_server.init_command import main as _init_main

        _init_main()
    else:
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
