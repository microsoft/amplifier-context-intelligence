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


@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncGenerator[None, None]:
    """Manage application startup and shutdown lifecycle."""
    settings = get_settings()
    logger.info("Intelligence Service starting up")
    application.state.drain = DrainManager(
        timeout_seconds=settings.drain_timeout_seconds
    )

    if settings.bundle_path:
        # Lazy imports — only needed when bundle_path is configured so that
        # dev/test mode never requires amplifier packages to be installed.
        from intelligence_service.amplifier_app import AmplifierApp  # noqa: PLC0415
        from intelligence_service.amplifier_session_manager import (  # noqa: PLC0415
            AmplifierSessionManager,
        )

        amplifier_app = AmplifierApp(
            bundle_path=settings.bundle_path,
            routing_matrix=settings.routing_matrix,
            amplifier_home=settings.amplifier_home,
        )
        await amplifier_app.startup()
        application.state.amplifier_app = amplifier_app
        application.state.session_manager = AmplifierSessionManager(
            amplifier_app=amplifier_app,
            workspace=settings.workspace,
            amplifier_home=settings.amplifier_home,
        )
    else:
        application.state.amplifier_app = None
        application.state.session_manager = StubSessionManager()

    yield

    logger.info("Intelligence Service shutting down")
    drain: DrainManager = application.state.drain
    await drain.start_drain()

    session_manager = application.state.session_manager
    close_all = getattr(session_manager, "close_all", None)
    if close_all is not None:
        close_all()

    amplifier_app = application.state.amplifier_app
    if amplifier_app is not None:
        await amplifier_app.close()


app = FastAPI(title="Intelligence Service", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    """Return service health status."""
    return {"status": "ok"}


@app.post("/admin/reload-bundle")
async def reload_bundle() -> dict[str, str]:
    """Reload the Amplifier bundle, or return skipped in stub mode."""
    amplifier_app = app.state.amplifier_app
    if amplifier_app is None:
        return {"status": "skipped"}
    try:
        await amplifier_app.reload()
        return {"status": "reloaded"}
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "message": str(exc)}


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
                old_session_id = session_id
                session_id = await session_manager.reset_session(old_session_id)
                drain.unregister(old_session_id)
                drain.register(session_id)
                await websocket.send_json(format_session_created(session_id))

            elif msg.msg_type == "message":
                text = msg.payload.get("text", "")
                try:
                    result = await session_manager.execute(session_id, text)  # type: ignore[attr-defined]
                    await websocket.send_json(
                        format_response(session_id, result["text"])
                    )
                    for a2ui_msg in result.get("a2ui", []):
                        await websocket.send_json(a2ui_msg)
                except Exception as exc:  # noqa: BLE001
                    logger.exception(
                        "execute failed for session %s", session_id
                    )
                    await websocket.send_json(
                        format_error(session_id, str(exc))
                    )

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
