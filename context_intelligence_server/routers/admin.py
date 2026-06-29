"""Admin API — CRUD over the live identity stores.

Endpoints manage the entra-identities and api-keys stores at runtime with no
server restart. Mutations flow through ``IdentityStore.put`` / ``delete``, which
use the ROB-F2 commit order (write-file-then-swap-memory), so the persistent
file and the in-process dict are always in sync.

**Store access pattern**

Each endpoint reads its store from ``request.app.state``
(``api_key_store`` or ``entra_identity_store``), populated by
``create_asgi_app()`` in ``main.py``. This avoids a circular import between
the router and the main module. If the relevant store is ``None`` for the
current ``auth_mode``, the endpoint returns **503**.

**Auth seam — T4 placeholder**

``require_admin`` is a NO-OP dependency applied to the whole ``/admin`` router
via ``APIRouter(dependencies=[Depends(require_admin)])``. T5 replaces the
function body with real enforcement (static: admin_api_key; entra: IdentityAdmin
App Role) — the routes and tests do not change. Tests override it via::

    app.dependency_overrides[require_admin] = lambda: None
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, field_validator

from context_intelligence_server.identity_store import IdentityStore


# ---------------------------------------------------------------------------
# Auth seam — real enforcement (T5)
# ---------------------------------------------------------------------------


def require_admin(request: Request) -> None:
    """Router-level admin guard — enforces admin authority on all /admin/* requests.

    Applied router-wide via ``APIRouter(dependencies=[Depends(require_admin)])``.

    Security model (design §6, T5):
    - The middleware (BearerTokenMiddleware) ALWAYS enforces authentication on
      /admin/* paths (they are never in an exempt set — TB-07 startup assertion).
      A missing/invalid token → 401 before this function is ever reached.
    - This dependency enforces *authorization* (not authentication): the request
      has already been authenticated; here we check whether the authenticated
      principal has admin authority.

    Static mode logic:
    - ``is_admin=True`` on scope state → allow.  This flag is set by the
      middleware when the bearer token's sha256 matches ``admin_api_key_digest``
      (ROB F1 — admin key recognized before data keystore lookup).
    - ``is_admin=False`` → 403 "use the admin key to call /admin/*".
    - ``admin_api_key_configured=False`` on app.state → 503 "admin API disabled".

    Entra mode logic:
    - ``IdentityAdmin`` (or the configured ``entra_admin_role``) in the token's
      ``roles`` claim (stored in scope state by the middleware) → allow.
    - Role not present → 403 naming the required role.
    - ``entra_admin_role`` empty/unconfigured → 503 "admin API disabled".

    503 signals "capability not configured" (distinct from 403 "you are denied").

    Notes:
    - Tests override this with ``app.dependency_overrides[require_admin] = lambda: None``
      to bypass enforcement in T4 route tests (this is the standard FastAPI override
      mechanism; the lambda's signature must satisfy ITS OWN declared parameters —
      FastAPI injects based on the override's signature, not the original's).
    - The ``roles`` check reads ONLY the ``roles`` claim — never ``groups``.
      Group membership in the token cannot grant admin access (TB-09).
    """
    auth_mode: str = getattr(request.app.state, "auth_mode", "static")
    # Read auth metadata from scope state (set by BearerTokenMiddleware).
    scope_state: dict = request.scope.get("state", {})

    if auth_mode == "static":
        # 503 when admin key is not configured (capability off, not forbidden).
        admin_configured: bool = getattr(
            request.app.state, "admin_api_key_configured", False
        )
        if not admin_configured:
            raise HTTPException(
                status_code=503,
                detail=(
                    "Admin API disabled: admin_api_key is not configured. "
                    "Set admin_api_key in the YAML config or via the "
                    "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_ADMIN_API_KEY env var."
                ),
            )

        # 403 when the request was authenticated with a data key (not the admin key).
        is_admin: bool = scope_state.get("is_admin", False)
        if not is_admin:
            raise HTTPException(
                status_code=403,
                detail=(
                    "Forbidden: the admin API key is required to call /admin/* "
                    "endpoints. Data keys authenticate to the data API only."
                ),
            )

    else:  # entra mode
        # 503 when admin role is not configured (capability off, not forbidden).
        entra_admin_role: str = getattr(request.app.state, "entra_admin_role", "")
        if not entra_admin_role:
            raise HTTPException(
                status_code=503,
                detail=(
                    "Admin API disabled: entra_admin_role is not configured. "
                    "Set entra_admin_role in the YAML config or via the "
                    "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_ENTRA_ADMIN_ROLE env var."
                ),
            )

        # 403 when the token's `roles` claim does not contain the required role.
        # ONLY the `roles` claim is checked — `groups` is intentionally excluded
        # so group membership cannot grant admin access (TB-09).
        roles: list[str] = scope_state.get("roles", [])
        if entra_admin_role not in roles:
            raise HTTPException(
                status_code=403,
                detail=(
                    f"Forbidden: the App Role '{entra_admin_role}' is required "
                    f"to call /admin/* endpoints. Assign the role in the Entra "
                    f"App Registration and ensure the token's 'roles' claim "
                    f"(not 'groups') contains it."
                ),
            )


# ---------------------------------------------------------------------------
# Request body models
# ---------------------------------------------------------------------------


class IdentityBody(BaseModel):
    """Body for PUT /admin/identities/{oid}."""

    id: str
    display_name: str | None = None

    @field_validator("id")
    @classmethod
    def id_must_be_non_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("id must be a non-empty string")
        return v


class KeyBody(BaseModel):
    """Body for PUT /admin/keys/{sha256hash}."""

    id: str

    @field_validator("id")
    @classmethod
    def id_must_be_non_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("id must be a non-empty string")
        return v


# ---------------------------------------------------------------------------
# Per-request store dependencies (via app.state — no circular import)
# ---------------------------------------------------------------------------


def _require_entra_store(request: Request) -> IdentityStore:
    """Dependency: resolve the entra-identity store or raise 503.

    The store is ``None`` when ``auth_mode != "entra"``.
    """
    store: IdentityStore | None = getattr(
        request.app.state, "entra_identity_store", None
    )
    if store is None:
        raise HTTPException(
            status_code=503,
            detail="identity store not active in this auth mode",
        )
    return store


def _require_key_store(request: Request) -> IdentityStore:
    """Dependency: resolve the api-key store or raise 503.

    The store is ``None`` when ``auth_mode != "static"``.
    """
    store: IdentityStore | None = getattr(request.app.state, "api_key_store", None)
    if store is None:
        raise HTTPException(
            status_code=503,
            detail="key store not active in this auth mode",
        )
    return store


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/admin", dependencies=[Depends(require_admin)])


# --- Entra identities -------------------------------------------------------


@router.put("/identities/{oid}", status_code=200)
def put_identity(
    oid: str,
    body: IdentityBody,
    store: IdentityStore = Depends(_require_entra_store),
) -> dict[str, str]:
    """Upsert an entra identity (OID → contributor).

    Write-through via ``IdentityStore.put``: the persistent file is updated
    atomically and the in-process map (shared with ``EntraResolver``) is
    updated immediately — no server restart required.

    Returns the stored record as ``{oid, id[, display_name]}``.
    """
    record: dict[str, str] = {"id": body.id}
    if body.display_name is not None:
        record["display_name"] = body.display_name
    store.put(oid, record)
    return {"oid": oid, **record}


@router.delete("/identities/{oid}", status_code=200)
def delete_identity(
    oid: str,
    store: IdentityStore = Depends(_require_entra_store),
) -> dict[str, str | bool]:
    """Delete an entra identity.

    Returns 200 on success, 404 when the OID is not present.
    Deletion is write-through and immediately visible to the resolver.
    """
    if store.get(oid) is None:
        raise HTTPException(status_code=404, detail=f"identity {oid!r} not found")
    store.delete(oid)
    return {"oid": oid, "deleted": True}


@router.get("/identities", status_code=200)
def list_identities(
    store: IdentityStore = Depends(_require_entra_store),
) -> dict[str, list[dict[str, str]]]:
    """List all entra identities as ``{oid, id[, display_name]}``."""
    return {"identities": [{"oid": oid, **record} for oid, record in store.items()]}


# --- Static API keys --------------------------------------------------------


@router.put("/keys/{sha256hash}", status_code=200)
def put_key(
    sha256hash: str,
    body: KeyBody,
    store: IdentityStore = Depends(_require_key_store),
) -> dict[str, str]:
    """Upsert a static API-key entry (sha256 hex → contributor).

    The path parameter is the **sha256 hash** of an externally-generated key.
    Operators hash the raw key out-of-band and register only the hash here;
    the server never sees or stores the raw key.

    Write-through: visible to ``StaticKeyResolver`` immediately, no restart.

    Returns ``{hash, id}`` — NEVER the raw key.
    """
    record: dict[str, str] = {"id": body.id}
    store.put(sha256hash, record)
    return {"hash": sha256hash, **record}


@router.delete("/keys/{sha256hash}", status_code=200)
def delete_key(
    sha256hash: str,
    store: IdentityStore = Depends(_require_key_store),
) -> dict[str, str | bool]:
    """Delete a static API-key entry.

    Returns 200 on success, 404 when the hash is not present.
    Deletion is write-through and immediately visible to the resolver.
    """
    if store.get(sha256hash) is None:
        raise HTTPException(status_code=404, detail=f"key {sha256hash!r} not found")
    store.delete(sha256hash)
    return {"hash": sha256hash, "deleted": True}


@router.get("/keys", status_code=200)
def list_keys(
    store: IdentityStore = Depends(_require_key_store),
) -> dict[str, list[dict[str, str]]]:
    """List all static API-key entries as ``{hash, id}``.

    Raw keys are NEVER returned — only the sha256 hash and contributor id.
    """
    return {
        "keys": [{"hash": h, "id": record.get("id", "")} for h, record in store.items()]
    }
