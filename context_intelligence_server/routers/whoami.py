"""Route for the authenticated caller's own identity.

``GET /whoami`` answers "who am I" for the calling credential. It exists so a
client -- in particular the deletion bundle's agent -- can compare its own
identity against a session's ``created_by`` before acting on it (for example,
warning before deleting a session someone else created), without
re-implementing the server's auth extraction itself.

This route does not add a new identity source. It reads the exact same
``contributor_id`` the auth middleware (``auth.py``) already writes to
``request.scope["state"]`` for every authenticated request -- the same value
``routers/deletion.py``'s ``_caller_id`` reads to stamp a delete's
``requested_by``, and ``routers/admin.py``'s ``_admin_who`` reads for audit
logging.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Request

from context_intelligence_server.authz import require_read

router = APIRouter()


def _caller_id(request: Request) -> str | None:
    """Return the authenticated caller's id, or None when auth is off.

    Mirrors ``routers/deletion.py``'s ``_caller_id`` and ``routers/admin.py``'s
    ``_admin_who`` -- both read the identical ``contributor_id`` key the auth
    middleware (``auth.py``) stores in the request's scope state. Kept as a
    small local copy rather than a cross-router import, matching how those two
    routers each already keep their own copy of this one-line read.
    """
    state: dict = request.scope.get("state", {})
    return state.get("contributor_id")


@router.get(
    "/whoami",
    dependencies=[Depends(require_read)],
)
async def get_whoami(request: Request) -> dict[str, Any]:
    """Report the authenticated caller's identity.

    Returns the same ``contributor_id`` value the server stamps onto
    ``created_by`` (on session data) and ``requested_by`` (on a delete) --
    this is what lets a client check "am I the one who created this session"
    before acting on it.

    When auth is disabled (``allow_unauthenticated=True``, no credential
    required) there is no caller identity to report, so ``contributor_id`` is
    ``null`` rather than a 500 -- the shape of the response never changes,
    only the value.
    """
    return {"contributor_id": _caller_id(request)}
