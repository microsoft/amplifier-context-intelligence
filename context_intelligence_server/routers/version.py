"""Lightweight version endpoint — returns the running server version."""

from __future__ import annotations

from fastapi import APIRouter, Request

from context_intelligence_server.status import SERVER_VERSION

router = APIRouter()


@router.get("/version")
async def get_version(request: Request) -> dict[str, object]:
    """Return the running server version.

    This endpoint is intentionally unauthenticated so clients can check
    server compatibility without credentials.

    Returns:
        JSON object with a single ``version`` key, e.g. ``{"version": "2.0.0"}``.
    """
    recovery_enabled = bool(getattr(request.app.state, "recovery_enabled", False))
    return {
        "version": SERVER_VERSION,
        "capabilities": ["native-recovery"] if recovery_enabled else [],
    }
