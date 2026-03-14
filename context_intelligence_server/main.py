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
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse
from neo4j import AsyncGraphDatabase

from context_intelligence_server.blob_store import AsyncDiskBlobStore
from context_intelligence_server.config import get_settings
from context_intelligence_server.dashboard import build_status_response
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
    try:
        yield
    finally:
        logger.info("lifespan_shutdown: closing Neo4j driver")
        await app.state.neo4j_driver.close()


app = FastAPI(title="Context Intelligence Server", lifespan=lifespan)
_start_time = time.time()
registry = SessionRegistry()


_DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Context Intelligence Server</title>
  <style>
    body {
      background: #1a1a2e;
      color: #e0e0e0;
      font-family: monospace;
      margin: 0;
      padding: 20px;
    }
    h1 { color: #a0c4ff; }
    h2 { color: #9fb3c8; margin-top: 24px; }
    .metrics { display: flex; gap: 32px; margin: 16px 0; }
    .metric { background: #16213e; padding: 12px 20px; border-radius: 6px; }
    .metric-label { font-size: 0.8em; color: #888; }
    .metric-value { font-size: 1.4em; color: #a0c4ff; }
    table { width: 100%; border-collapse: collapse; margin-top: 8px; }
    th { background: #16213e; padding: 8px 12px; text-align: left; color: #9fb3c8; }
    td { padding: 6px 12px; border-bottom: 1px solid #2a2a4a; }
    tr:hover td { background: #1e2a3a; }
  </style>
</head>
<body>
  <h1>Context Intelligence Server</h1>
  <div class="metrics">
    <div class="metric">
      <div class="metric-label">Uptime (s)</div>
      <div class="metric-value"><span id="uptime">-</span></div>
    </div>
    <div class="metric">
      <div class="metric-label">Active Sessions</div>
      <div class="metric-value"><span id="active_sessions">-</span></div>
    </div>
  </div>

  <h2>Sessions</h2>
  <table>
    <thead>
      <tr>
        <th>Session</th>
        <th>Workspace</th>
        <th>Queue</th>
        <th>Last Event</th>
        <th>Processed</th>
      </tr>
    </thead>
    <tbody id="sessions-body"></tbody>
  </table>

  <h2>Recent Events</h2>
  <table>
    <thead>
      <tr>
        <th>Time</th>
        <th>Event</th>
        <th>Session</th>
        <th>Workspace</th>
        <th>Result</th>
      </tr>
    </thead>
    <tbody id="events-body"></tbody>
  </table>

  <script>
    function refresh() {
      fetch('/status')
        .then(r => r.json())
        .then(data => {
          document.getElementById('uptime').textContent = data.uptime_seconds.toFixed(1);
          document.getElementById('active_sessions').textContent = data.active_sessions;

          const sb = document.getElementById('sessions-body');
          sb.innerHTML = (data.sessions || []).map(s =>
            '<tr><td>' + s.session_id + '</td><td>' + s.workspace + '</td><td>' +
            s.queue_depth + '</td><td>' + (s.last_event || '-') + '</td><td>' +
            s.events_processed + '</td></tr>'
          ).join('');

          const eb = document.getElementById('events-body');
          eb.innerHTML = (data.recent_events || []).map(e => {
            const t = new Date(e.timestamp * 1000).toISOString();
            return '<tr><td>' + t + '</td><td>' + e.event + '</td><td>' +
              e.session_id + '</td><td>' + e.workspace + '</td><td>' + e.result + '</td></tr>';
          }).join('');
        });
    }
    refresh();
    setInterval(refresh, 3000);
  </script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
async def dashboard() -> HTMLResponse:
    return HTMLResponse(content=_DASHBOARD_HTML)


@app.get("/status")
async def get_status() -> dict[str, Any]:
    return build_status_response(registry, _start_time)


@app.post("/events", status_code=202, response_model=EventResponse)
async def post_events(request: EventRequest) -> EventResponse:
    session_id = request.data.get("session_id", "")
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
        # Backfill last 200 lines
        lines = log_path.read_text().splitlines()[-200:]
        for line in lines:
            yield f"data: {line}\n\n"

        # Tail new lines
        async with aiofiles.open(log_path, mode="r") as f:
            await f.seek(0, 2)
            while True:
                if await request.is_disconnected():
                    break
                line = await f.readline()
                if not line:
                    await asyncio.sleep(0.2)
                else:
                    yield f"data: {line}\n\n"

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
