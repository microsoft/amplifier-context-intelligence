"""Lightweight version endpoint — returns the running server version."""

from __future__ import annotations

import os

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
    permit_enabled = bool(getattr(request.app.state, "recovery_permit_required", False))
    capabilities: list[str] = []
    if permit_enabled:
        capabilities.append("native-recovery-source-v1")
    elif recovery_enabled:
        capabilities.append("native-recovery-lease-compatibility")
    return {
        "version": SERVER_VERSION,
        "capabilities": capabilities,
    }


@router.get("/health")
async def get_health() -> dict[str, str]:
    """Liveness for the hosting platform, with the deployed commit.

    Unauthenticated and cheap (no Neo4j or /data access): Amplifier Online's
    deploy checks it, and the commit tells the new code from the old.
    """
    return {"status": "ok", "commit": os.environ.get("GIT_COMMIT", "unknown")}
