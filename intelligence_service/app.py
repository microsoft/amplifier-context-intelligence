"""Intelligence Service FastAPI application."""

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from intelligence_service.a2ui_bridge import (
    format_action_ack,
    format_error,
    format_response,
    format_session_created,
    parse_incoming,
)
from intelligence_service.config import get_settings
from intelligence_service.drain import DrainManager
from intelligence_service.session_manager import SessionManager, StubSessionManager

logger = logging.getLogger(__name__)

_settings = get_settings()


@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncGenerator[None, None]:
    """Manage application startup and shutdown lifecycle."""
    logger.info("Intelligence Service starting up")
    application.state.drain = DrainManager(
        timeout_seconds=_settings.drain_timeout_seconds
    )
    application.state.session_manager = StubSessionManager()
    yield
    logger.info("Intelligence Service shutting down")


app = FastAPI(title="Intelligence Service", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    """Return service health status."""
    return {"status": "ok"}


@app.get("/admin/reload-bundle")
async def reload_bundle() -> dict[str, str]:
    """Stub endpoint for bundle reload (not yet implemented)."""
    return {
        "status": "reload_not_implemented",
        "message": "Bundle reload will be available when agent integration is complete.",
    }


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    """WebSocket endpoint wiring together session manager, A2UI bridge, and drain manager."""
    drain: DrainManager = websocket.app.state.drain
    session_manager: SessionManager = websocket.app.state.session_manager

    if not drain.accepting:
        await websocket.close(code=1013)
        return

    await websocket.accept()

    session_id = await session_manager.create_session()
    drain.register(session_id)

    try:
        await websocket.send_json(format_session_created(session_id))

        while True:
            data = await websocket.receive_json()
            msg = parse_incoming(data)

            if msg.msg_type == "new_session":
                session_id = await session_manager.reset_session(session_id)
                await websocket.send_json(format_session_created(session_id))

            elif msg.msg_type == "message":
                text = msg.payload.get("text", "")
                await websocket.send_json(format_response(session_id, text))

            elif msg.msg_type == "action":
                component_id = msg.payload.get("componentId", "")
                await websocket.send_json(format_action_ack(session_id, component_id))

            else:
                await websocket.send_json(
                    format_error(
                        session_id,
                        f"Unknown message type: {msg.msg_type}",
                    )
                )

    except WebSocketDisconnect:
        logger.info("WebSocket disconnected for session %s", session_id)
    finally:
        drain.unregister(session_id)
        await session_manager.destroy_session(session_id)
