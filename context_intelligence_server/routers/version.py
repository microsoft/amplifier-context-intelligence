"""Lightweight version endpoint — returns the running server version."""

from __future__ import annotations

from fastapi import APIRouter

from context_intelligence_server.dashboard import SERVER_VERSION

router = APIRouter()


@router.get("/version")
async def get_version() -> dict[str, str]:
    """Return the running server version.

    This endpoint is intentionally unauthenticated so clients can check
    server compatibility without credentials.

    Returns:
        JSON object with a single ``version`` key, e.g. ``{"version": "2.0.0"}``.
    """
    return {"version": SERVER_VERSION}
