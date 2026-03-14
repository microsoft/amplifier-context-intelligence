"""FastAPI application entrypoint for the Context Intelligence Server."""

import logging
import time

from fastapi import FastAPI

from context_intelligence_server.config import get_settings
from context_intelligence_server.models import (
    EventRequest,
    EventResponse,
    StatusResponse,
)
from context_intelligence_server.registry import SessionRegistry

_settings = get_settings()

_LOG_FORMAT = '{"time": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}'

logging.basicConfig(level=_settings.log_level, format=_LOG_FORMAT)

logger = logging.getLogger("context_intelligence_server")

app = FastAPI(title="Context Intelligence Server")
_start_time = time.time()
registry = SessionRegistry()


@app.get("/status", response_model=StatusResponse)
async def get_status() -> StatusResponse:
    return StatusResponse(
        status="ok",
        uptime_seconds=time.time() - _start_time,
        active_sessions=registry.active_count(),
    )


@app.post("/events", status_code=202, response_model=EventResponse)
async def post_events(request: EventRequest) -> EventResponse:
    session_id = request.data.get("session_id", "")
    worker = registry.get_or_create(session_id, request.workspace)
    await worker.queue.put((request.event, request.workspace, request.data))
    logger.info("event_enqueued: event=%s session_id=%s", request.event, session_id)
    return EventResponse(status="queued", session_id=session_id or None)
