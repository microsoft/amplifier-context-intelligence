"""FastAPI application entrypoint for the Context Intelligence Server."""

import asyncio
import json
import logging
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
async def post_events(request: EventRequest, replay: bool = False) -> EventResponse:
    session_id = request.data.get("session_id", "")
    if request.idempotency_key and not replay:
        is_new = idempotency_cache.check_and_store(request.idempotency_key)
        if not is_new:
            logger.info(
                "event_duplicate_skipped: event=%s session_id=%s",
                request.event,
                session_id,
            )
            return EventResponse(status="duplicate", session_id=session_id or None)
    worker = registry.get_or_create(session_id, request.workspace)
    await worker.queue.put((request.event, request.workspace, request.data))
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


def run() -> None:
    """Start the server using gunicorn + uvicorn worker for graceful SIGTERM shutdown."""
    from gunicorn.app.base import BaseApplication

    class _App(BaseApplication):
        def load_config(self) -> None:
            for key, value in {
                "bind": f"{_settings.server_host}:{_settings.server_port}",
                "workers": 1,
                "worker_class": "uvicorn.workers.UvicornWorker",
                "timeout": 30,
                "graceful_timeout": 10,
                "loglevel": _settings.log_level.lower(),
            }.items():
                self.cfg.set(key, value)

        def load(self) -> Any:
            return asgi_app

    _App().run()
