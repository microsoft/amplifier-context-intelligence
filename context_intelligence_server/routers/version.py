"""Lightweight version endpoint — returns the running server version."""

from __future__ import annotations

from fastapi import APIRouter

from context_intelligence_server.status import SCHEMA_VERSION, SERVER_VERSION

router = APIRouter()


@router.get("/version")
async def get_version() -> dict[str, str | int]:
    """Return the running server version and expected graph schema version.

    This endpoint is intentionally unauthenticated so clients can check
    server compatibility without credentials.

    ``schema_version`` is a read-only baseline data point (no comparison or
    upgrade logic lives here — see ``SCHEMA_VERSION`` in ``status.py``).

    Returns:
        JSON object with ``version`` and ``schema_version`` keys, e.g.
        ``{"version": "2.0.0", "schema_version": 1}``.
    """
    return {"version": SERVER_VERSION, "schema_version": SCHEMA_VERSION}
