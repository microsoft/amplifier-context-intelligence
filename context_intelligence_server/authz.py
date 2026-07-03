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

    Fail-closed on unpopulated scope state (Step 3, doc 16 W1): when
    ``is_service`` is absent from scope state entirely, the request never
    passed through BearerTokenMiddleware's identity-setting path. That is
    only a safe situation when the server is explicitly in the
    ``allow_unauthenticated=True`` dev/test opt-out (read from
    ``app.state.allow_unauthenticated``) — otherwise this denies.

    In auth-enabled production mode BearerTokenMiddleware ALWAYS sets
    ``is_service`` on scope state before any route handler or dependency runs
    (auth.py), and the boot guard (``_assert_capability_routes_not_exempt``,
    main.py) keeps every capability-gated route non-exempt, so the
    absent-``is_service`` branch is unreachable in production.

    Once ``is_service`` IS present, ``False`` (human / static / easyauth)
    remains always write-capable — unchanged.
    """
    state: dict = request.scope.get("state", {})
    if "is_service" not in state:
        # Unpopulated scope state → fail CLOSED (Step 3, doc 16 W1).
        # In auth-enabled mode this branch is unreachable: middleware always
        # sets is_service (auth.py) and the boot guard keeps capability
        # routes non-exempt (main.py). It is reachable ONLY in the explicit
        # allow_unauthenticated dev/test opt-out, where the middleware
        # short-circuits without populating state. Honour that opt-out;
        # otherwise deny.
        return bool(getattr(request.app.state, "allow_unauthenticated", False))
    if not state["is_service"]:
        return True  # human / static / easyauth — write-capable, unchanged
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
