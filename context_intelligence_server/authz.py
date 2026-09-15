"""Authorization capability dependencies for the Context Intelligence Server.

Extracted from main.py into a standalone module so both main.py and sub-routers
(e.g. routers/queues.py) can import these dependencies without creating a circular
import.  main.py imports the routers at module load time; routers therefore cannot
import from main.py without triggering a cycle.  This module has no such dependency:
it imports only from fastapi — a leaf with no knowledge of the application graph.

Public interface
----------------
_is_write_capable — predicate consumed by require_write and require_read
require_write     — FastAPI dependency: gate write-only routes
require_read      — FastAPI dependency: gate read-and-above routes
"""

from __future__ import annotations

from fastapi import HTTPException, Request


def _is_write_capable(request: Request) -> bool:
    """True for any human/static principal; for a service iff it holds Contributor.

    When ``is_service`` is absent from scope state the principal defaults to
    False (human-like), making it write-capable.  This default is ONLY
    reachable in two safe situations:

    1. ``allow_unauthenticated=True`` (dev/test mode, no credential required).
    2. Auth-exempt paths (/status, /version) — none of which carry
       a capability gate, so this function is never called for them.

    In auth-enabled production mode BearerTokenMiddleware ALWAYS sets
    ``is_service`` on scope state before any route handler or dependency runs,
    so the default is never exercised in that path.
    """
    if getattr(request.app.state, "access_control_mode", "compatibility") == "scoped":
        if _is_operator(request):
            return True
        return _has_capability(request, "live:write")
    state: dict = request.scope.get("state", {})
    if not state.get("is_service", False):
        return True  # human / static — always write-capable, unchanged
    roles: list[str] = state.get("roles", [])
    role: str = getattr(request.app.state, "service_data_role", "")
    return bool(role) and role in roles


def require_write(request: Request) -> None:
    """Gate write routes: human/static always pass; service iff service_data_role.

    Add as a route-level dependency for any endpoint that performs a
    destructive or mutating operation::

        @router.post("/path", dependencies=[Depends(require_write)])

    Raises HTTPException(403) when a service token does not hold the
    configured ``service_data_role`` (e.g. "Contributor").
    """
    if _is_write_capable(request):
        return
    role: str = getattr(request.app.state, "service_data_role", "")
    raise HTTPException(
        status_code=403,
        detail=(
            f"Forbidden: write access requires the App Role {role!r}. "
            f"This service token is read-only or unprivileged."
        ),
    )


def require_read(request: Request) -> None:
    """Gate read routes: write-capable OR a service with reader_role.

    Any principal that passes ``require_write`` also passes this dependency.
    Additionally, a service token that holds only ``reader_role`` (e.g.
    "Reader") is granted read-only access.

    Raises HTTPException(403) when neither condition is met.
    """
    if getattr(request.app.state, "access_control_mode", "compatibility") == "scoped":
        if _is_operator(request) or _has_capability(request, "data:read"):
            return
        raise HTTPException(status_code=403, detail="Forbidden")
    if _is_write_capable(request):
        return
    state: dict = request.scope.get("state", {})
    roles: list[str] = state.get("roles", [])
    reader: str = getattr(request.app.state, "reader_role", "")
    if bool(reader) and reader in roles:
        return
    raise HTTPException(
        status_code=403,
        detail=(
            f"Forbidden: read access requires App Role {reader!r} (read-only) "
            f"or {getattr(request.app.state, 'service_data_role', '')!r} (write)."
        ),
    )


def _has_capability(request: Request, capability: str) -> bool:
    """Check an explicit scoped grant for the resolved contributor."""
    state: dict = request.scope.get("state", {})
    contributor_id = state.get("contributor_id")
    grants = getattr(request.app.state, "contributor_grants", {})
    grant = grants.get(contributor_id) if contributor_id else None
    return bool(grant and capability in grant.capabilities)


def _is_operator(request: Request) -> bool:
    """Return whether the request has the server's existing admin authority.

    Scoped contributor grants govern data-plane principals. The separately
    configured operator credential/role remains able to use the existing
    operational routes, including queue repair. This does not turn an admin
    credential into a contributor identity: it still cannot originate an
    attributed event because the event routes require a contributor id before
    persistence in scoped mode.
    """
    state: dict = request.scope.get("state", {})
    if state.get("is_admin", False):
        return True
    role = getattr(request.app.state, "entra_admin_role", "")
    return bool(role) and role in state.get("roles", [])


def require_recovery_write(request: Request) -> None:
    if getattr(request.app.state, "access_control_mode", "compatibility") != "scoped":
        require_write(request)
        return
    if not _has_capability(request, "recovery:write"):
        raise HTTPException(status_code=403, detail="Forbidden")


def require_workspace_access(request: Request, workspace: str, capability: str) -> None:
    """Enforce a scoped capability and workspace allow-list without disclosure."""
    if getattr(request.app.state, "access_control_mode", "compatibility") != "scoped":
        return
    state: dict = request.scope.get("state", {})
    grant = getattr(request.app.state, "contributor_grants", {}).get(
        state.get("contributor_id")
    )
    if not grant or capability not in grant.capabilities:
        raise HTTPException(status_code=403, detail="Forbidden")
    if not grant.all_workspaces and workspace not in (grant.workspaces or []):
        raise HTTPException(status_code=403, detail="Forbidden")


def require_arbitrary_data_read(request: Request) -> None:
    """Raw Cypher cannot be safely constrained for a workspace-limited grant."""
    if getattr(request.app.state, "access_control_mode", "compatibility") != "scoped":
        return
    state: dict = request.scope.get("state", {})
    grant = getattr(request.app.state, "contributor_grants", {}).get(
        state.get("contributor_id")
    )
    if not grant or "data:read" not in grant.capabilities or not grant.all_workspaces:
        raise HTTPException(status_code=403, detail="Forbidden")


async def require_claimed_session_access(
    request: Request, session_id: str, capability: str
) -> None:
    """Authorize a session resource from server-owned claims, or fail closed."""
    if getattr(request.app.state, "access_control_mode", "compatibility") != "scoped":
        return
    claims = getattr(request.app.state, "session_claims", None)
    workspace = await claims.get_workspace(session_id) if claims is not None else None
    if workspace is None:
        raise HTTPException(status_code=403, detail="Forbidden")
    require_workspace_access(request, workspace, capability)
