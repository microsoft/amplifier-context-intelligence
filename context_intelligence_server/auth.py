"""Bearer token authentication middleware for the Context Intelligence Server."""

import hashlib
import json
import logging
from collections.abc import Callable, MutableMapping
from typing import Any

import jwt  # pyjwt[crypto] — used by EntraResolver
from jwt import PyJWKClient
from typing_extensions import Protocol

_log = logging.getLogger(__name__)

# JWKS signing-key cache TTL passed to PyJWKClient (matches its own default;
# kept explicit so the contract is visible in code). PyJWKClient handles
# per-kid caching and refresh natively -- no custom dedup lock needed.
JWKS_CACHE_LIFESPAN_SECONDS: int = 300

# Paths that are exempt from authentication: health checks, version info, and
# the developer-facing OpenAPI/Swagger surface (/docs, /openapi.json). This is
# a headless, API-only server -- there is no browser dashboard, so there is
# only ONE exempt-path set.
_EXEMPT_PATHS: frozenset[str] = frozenset(
    {
        "/status",
        "/version",
        "/docs",
        "/openapi.json",
    }
)

# Path prefixes that are exempt from authentication. Empty: the server no
# longer serves any static assets (the dashboard's /static/ mount was removed).
_EXEMPT_PREFIXES: tuple[str, ...] = ()

# The admin-key fast-path is scoped to these paths only: the admin key is an
# administration credential, not a data-ingestion identity, so it must never
# short-circuit auth on data routes like POST /events.
_ADMIN_ROUTE_PREFIX: str = "/admin"


def _is_admin_route(path: str) -> bool:
    """True for the admin router's own paths (``/admin`` and ``/admin/...``)."""
    return path == _ADMIN_ROUTE_PREFIX or path.startswith(_ADMIN_ROUTE_PREFIX + "/")


# ---------------------------------------------------------------------------
# Auth error — carries a specific HTTP status code
# ---------------------------------------------------------------------------


class AuthError(Exception):
    """Authentication/authorisation failure with a specific HTTP status code.

    401: token missing/malformed/expired/wrong-aud-or-iss/wrong-tenant/bad-sig
    or missing required claims. 403: token is cryptographically valid but the
    ``oid`` is unmapped, or a service token has no qualifying App Role -- the
    ``reason`` must name the unbound ``oid`` so operators can diagnose it.
    """

    def __init__(self, status_code: int, reason: str) -> None:
        super().__init__(reason)
        self.status_code = status_code
        self.reason = reason


def _first_nonblank(*values: Any) -> str | None:
    """Return the first value that is a non-empty, non-whitespace str, else None.

    Used to chain service created_by candidates with truthiness semantics:
    empty/whitespace/non-string candidates fall through (B6/B8).
    """
    for v in values:
        if isinstance(v, str) and v.strip():
            return v
    return None


def _resolve_token(token: str, keystore: dict[str, str]) -> str | None:
    """Return the contributor id for *token*, or ``None`` if not found.

    Hashes the bearer token (UTF-8 bytes, sha256) and does a plain dict lookup
    against *keystore* (which stores ``{sha256_hex -> contributor_id}``).  Returns
    ``None`` — never ``"unknown"`` — on a miss so callers fail-closed on absence.
    """
    digest = hashlib.sha256(token.encode()).hexdigest()
    return keystore.get(digest)


class PrincipalResolver(Protocol):
    """Resolves a raw bearer token string to a contributor id.

    Returns ``(contributor_id, roles, is_service)`` on success, or ``None``
    when the token is not recognised (caller should respond 401).
    Implementations may also raise :class:`AuthError` for a specific status.

    ``roles`` carries the token's App-Role claim (entra) or ``[]`` (static),
    stored on ``scope["state"]["roles"]``. ``is_service`` is ``True`` for
    app/service tokens, stored on ``scope["state"]["is_service"]`` so route
    capability deps can gate service principals without re-parsing.
    """

    @property
    def auth_enabled(self) -> bool:
        """True when authentication is active (at least one credential configured).

        ``False`` only for a :class:`StaticKeyResolver` built with an empty
        keystore — the explicit ``allow_unauthenticated=True`` opt-out path.
        ``EntraResolver`` always returns ``True``.
        """
        ...

    def resolve(
        self, token: str, *, admin_path: bool = False
    ) -> tuple[str, list[str], bool] | None:
        """Return ``(contributor_id, roles, is_service)`` or ``None`` if token not recognised.

        Raises :class:`AuthError` (with ``status_code`` 401 or 403) when the
        token is present but invalid or maps to an unauthorised identity.

        ``roles`` is a list of App-Role strings (from the Entra ``roles``
        claim).  ``StaticKeyResolver`` always returns ``[]``.

        ``is_service`` is ``True`` for app/service tokens (no ``scp`` claim,
        routed through the service branch); ``False`` for delegated user tokens
        and static-key tokens.

        ``admin_path`` (keyword-only, default ``False``) signals a request
        targeting ``/admin/*``. When ``True``, ``EntraResolver`` relaxes only
        the identity-map membership check (never token authenticity), so an
        IdentityAdmin role-holder can bootstrap the map. ``StaticKeyResolver``
        ignores this parameter -- its admin authorization is a separate
        admin-key fast-path, not map membership.
        """
        ...


class EntraResolver:
    """Resolves Entra RS256 bearer tokens to contributor ids.

    PyJWKClient → ``jwt.decode`` with ``algorithms=["RS256"]``, dual audience,
    explicit ``tid`` + ``scp`` + ``oid`` checks, then ``oid → contributor_id``
    lookup via *identity_map*. A second branch handles app/service tokens (no
    ``scp``), authorized by App-Role alone, with `created_by` derived from
    stable claims.

    Raises:
        :class:`AuthError` (401): token invalid/expired/wrong-aud-or-iss/
            wrong-tenant, an anomalous scp+idtyp=app combo, or missing/invalid
            identity claim.
        :class:`AuthError` (403): token valid but ``oid`` unmapped (user
            branch), or no qualifying App Role (service branch).
        RuntimeError: at construction if eager JWKS prefetch fails -- the
            server must refuse to start rather than fail lazily.

    Args:
        client_id:            Azure App Registration client ID (GUID).
        tenant_id:            Azure AD tenant ID (GUID).
        identity_map:         ``{oid_lower -> contributor_id}``. May be empty
                              at construction (supported bootstrap state,
                              populated at runtime via /admin/identities). A
                              live reference so runtime mutation is visible
                              immediately; the map-miss exemption is scoped
                              to ``admin_path=True`` only.
        service_identity_map: ``{oid_lower -> contributor_id}`` for service
                              principals. Optional; ``{}`` = no service map.
        service_data_role:    App Role name granting write access. ``""`` disables.
        reader_role:          App Role name granting read-only access. ``""`` disables.
        entra_admin_role:     App Role name granting admin access. ``""`` disables.
        jwks_client:          Injectable JWKS client for tests.
    """

    def __init__(
        self,
        client_id: str,
        tenant_id: str,
        identity_map: dict[str, str],
        *,
        service_identity_map: dict[str, str] | None = None,
        service_data_role: str = "",
        reader_role: str = "",
        entra_admin_role: str = "",
        jwks_client: Any = None,
    ) -> None:
        self._client_id = client_id
        self._tenant_id = tenant_id
        self._identity_map = identity_map
        # Fail-closed defaults: empty string disables each role.
        self._service_identity_map: dict[str, str] = service_identity_map or {}
        self._service_data_role = service_data_role
        self._reader_role = reader_role
        self._entra_admin_role = entra_admin_role
        # Accept both the bare client GUID (ID-token aud) and the api:// form
        # (access-token aud when access_as_user scope is exposed).
        self._expected_aud = [client_id, f"api://{client_id}"]
        self._expected_issuer = f"https://login.microsoftonline.com/{tenant_id}/v2.0"

        if jwks_client is None:
            jwks_uri = (
                f"https://login.microsoftonline.com/{tenant_id}/discovery/v2.0/keys"
            )
            jwks_client = PyJWKClient(jwks_uri, lifespan=JWKS_CACHE_LIFESPAN_SECONDS)

        # Eager prefetch, fail-closed at startup -- runs regardless of whether
        # the client was injected, so tests can verify the fail-closed guarantee.
        try:
            jwks_client.fetch_data()
        except Exception as exc:
            raise RuntimeError(
                f"EntraResolver: JWKS prefetch failed for tenant "
                f"{tenant_id!r} — server cannot start without a reachable "
                f"JWKS endpoint.  Cause: {exc}"
            ) from exc

        # Guard: a reachable-but-empty JWKS would let construction succeed
        # but then 401 every request lazily -- detect and refuse to start.
        try:
            jwk_set = jwks_client.get_jwk_set()
        except AttributeError:
            pass  # stub without get_jwk_set() — skip the check
        else:
            if not jwk_set.keys:
                raise RuntimeError(
                    f"EntraResolver: JWKS endpoint returned zero signing keys "
                    f"for tenant {tenant_id!r} — server cannot start without "
                    f"signing keys."
                )

        self._jwks_client = jwks_client

    @property
    def auth_enabled(self) -> bool:
        """Always True — EntraResolver is always active (identity map is non-empty by construction)."""
        return True

    def resolve(
        self, token: str, *, admin_path: bool = False
    ) -> tuple[str, list[str], bool]:
        """Validate Entra JWT; return (contributor_id, roles, is_service).

        Tokens with ``scp`` present → user/delegated branch. Tokens without
        ``scp`` → service/app branch. A token carrying both ``scp`` and
        ``idtyp=app`` is anomalous and rejected (401) before either branch
        can claim it.

        Raises:
            AuthError(401): JWT validation failure, wrong tenant, the
                scp+idtyp anomaly, missing/invalid ``oid``, or no resolvable
                service identity.
            AuthError(403): valid user token with unmapped ``oid``, or valid
                service token with no qualifying App Role.
        """
        # ---- Shared validation (both paths) ----
        try:
            key = self._jwks_client.get_signing_key_from_jwt(token).key
            claims = jwt.decode(
                token,
                key,
                algorithms=["RS256"],  # pin RS256, reject alg=none / HS256
                audience=self._expected_aud,
                issuer=self._expected_issuer,
                options={"require": ["exp", "iss", "aud"]},
            )
        except jwt.PyJWTError as exc:
            raise AuthError(401, f"Invalid bearer token: {exc}") from exc

        # Explicit tid check, defense-in-depth alongside the issuer pin.
        if claims.get("tid") != self._tenant_id:
            raise AuthError(401, "Token from wrong tenant")

        _scp_raw = claims.get("scp")
        scp: str = _scp_raw if isinstance(_scp_raw, str) else ""
        has_scp: bool = bool(scp.split())

        _idtyp_raw = claims.get("idtyp")
        idtyp: str = _idtyp_raw.strip().lower() if isinstance(_idtyp_raw, str) else ""

        # Branches must be mutually exclusive: a token with both a delegated
        # scope and idtyp=="app" is anomalous and rejected before either
        # branch can claim it.
        if has_scp and idtyp == "app":
            raise AuthError(
                401,
                "Ambiguous token: carries both delegated 'scp' and idtyp='app'; "
                "refusing to classify as either user or service",
            )

        if has_scp:
            # User / delegated branch.
            if "access_as_user" not in scp.split():
                raise AuthError(
                    401,
                    f"Token missing required scope 'access_as_user' (got scp={scp!r})",
                )
            oid = claims.get("oid")
            if not isinstance(oid, str) or not oid.strip():
                raise AuthError(401, "Token missing or invalid 'oid' claim")
            # Both sides lowercased: config validator lowercases keys at build time.
            oid_lower = oid.lower()
            contributor_id = self._identity_map.get(oid_lower)
            if contributor_id is None:
                # Bootstrap exemption, /admin/* only: an unbound-but-valid oid
                # is admitted so an IdentityAdmin role-holder can populate the
                # map; require_admin still enforces the roles claim downstream.
                if not admin_path:
                    raise AuthError(
                        403,
                        f"Identity not authorized: oid {oid_lower!r} is not in the "
                        f"identity map; contact the server administrator to add this "
                        f"identity (tenant {self._tenant_id!r})",
                    )
                # Provisional contributor id = the oid itself, for audit trail.
                contributor_id = oid_lower
            # Only `roles`, never `groups`.
            _roles_raw = claims.get("roles")
            roles: list[str] = (
                [r for r in _roles_raw if isinstance(r, str)]
                if isinstance(_roles_raw, list)
                else []
            )
            return (contributor_id, roles, False)

        # Service / app branch (scp absent).
        _roles_raw = claims.get("roles")
        roles = (
            [r for r in _roles_raw if isinstance(r, str)]
            if isinstance(_roles_raw, list)
            else []
        )

        # Authorization = role alone; empty name disables that role.
        authorized = (
            (self._service_data_role and self._service_data_role in roles)
            or (self._reader_role and self._reader_role in roles)
            or (self._entra_admin_role and self._entra_admin_role in roles)
        )
        if not authorized:
            # Name the rejected principal, not the raw roles claim.
            _appid_raw = claims.get("appid")
            _oid_raw_msg = claims.get("oid")
            _principal = (
                _appid_raw
                if isinstance(_appid_raw, str) and _appid_raw.strip()
                else (
                    _oid_raw_msg
                    if isinstance(_oid_raw_msg, str) and _oid_raw_msg.strip()
                    else "(unknown)"
                )
            )
            raise AuthError(
                403,
                f"Service principal {_principal!r} is not authorized: "
                f"no qualifying App Role. "
                f"Required App Roles: "
                f"write={self._service_data_role!r}, "
                f"read={self._reader_role!r}, "
                f"admin={self._entra_admin_role!r}. "
                f"Assign one as an Application App Role on the service principal "
                f"in Azure Entra, then re-request a token.",
            )

        # created_by derivation: stable claims only, never app_displayname
        # (spoofable in Entra). Order: service_map[oid] > appid > azp > oid.
        _oid_raw = claims.get("oid")
        oid_str = _oid_raw if isinstance(_oid_raw, str) and _oid_raw.strip() else ""
        oid_lower = oid_str.lower()
        mapped = self._service_identity_map.get(oid_lower) if oid_lower else None

        created_by = _first_nonblank(
            mapped,  # 1. operator-assigned contributor id
            claims.get("appid"),  # 2. app client id (v1.0 token)
            claims.get("azp"),  # 3. authorized party (v2.0 token)
            oid_str,  # 4. SP object id (always present; last resort)
        )
        if created_by is None:
            # Unreachable in practice (oid always present); fail-loud, never null.
            raise AuthError(
                401,
                "Service token has no resolvable identity claim "
                "(service map miss and appid/azp/oid all blank)",
            )

        return (created_by, roles, True)


class StaticKeyResolver:
    """Resolves tokens via a pre-built ``{sha256_hex(token) -> contributor_id}`` keystore.

    Built by :meth:`~context_intelligence_server.config.Settings.build_keystore`.
    Raw tokens are never stored here.
    """

    def __init__(self, keystore: dict[str, str]) -> None:
        self._keystore = keystore

    @property
    def auth_enabled(self) -> bool:
        """True when at least one key is configured (authentication is active).

        An empty keystore boots fail-closed (401 until onboarded via
        /admin/keys); the only unauthenticated path is the explicit
        ``allow_unauthenticated=True`` opt-out combined with this returning
        ``False``.
        """
        return bool(self._keystore)

    @property
    def is_empty(self) -> bool:
        """True when no keys are configured. Prefer ``auth_enabled`` (inverse) for new code."""
        return not self._keystore

    def resolve(
        self, token: str, *, admin_path: bool = False
    ) -> tuple[str, list[str], bool] | None:
        """Return ``(contributor_id, [], False)`` for *token*, or ``None`` on a miss.

        Roles is always empty and is_service always False for static-key
        auth. ``admin_path`` is accepted for Protocol compatibility but
        unused: static-mode admin goes through a separate key fast-path.
        """
        _ = admin_path  # unused: static-mode admin uses the admin-key fast-path
        contributor_id = _resolve_token(token, self._keystore)
        if contributor_id is None:
            return None
        return (contributor_id, [], False)


class BearerTokenMiddleware:
    """ASGI middleware that validates ``Authorization: Bearer <token>`` headers.

    Accepts a :class:`PrincipalResolver` via *resolver* (preferred), or a raw
    *keystore* dict for tests that construct the middleware directly.

    Fail-open pass-through happens only when the middleware was constructed
    with ``allow_unauthenticated=True`` AND the resolver's ``auth_enabled`` is
    ``False`` (an empty static keystore). Entra mode can never fire this
    branch (``EntraResolver.auth_enabled`` is always ``True``).

    On a match, injects into ``scope["state"]``: ``contributor_id``,
    ``is_admin`` (True only for the static-mode admin key), ``roles`` (Entra
    App Role assignments, ``[]`` for static mode), and ``is_service``.

    The admin key digest is checked against the bearer token's sha256 before
    delegating to the resolver, authenticating as ``contributor_id="admin"``,
    ``is_admin=True`` directly.

    ``_EXEMPT_PATHS`` bypasses auth for health checks and public-facing pages.
    """

    def __init__(
        self,
        app: Callable[..., Any],
        keystore: dict[str, str] | None = None,
        *,
        resolver: PrincipalResolver | None = None,
        exempt_paths: frozenset[str] | None = None,
        admin_api_key_digest: str | None = None,
        allow_unauthenticated: bool = False,
    ) -> None:
        self.app = app
        # Test/dev opt-out only: see the fail-open check in __call__.
        self._allow_unauthenticated: bool = allow_unauthenticated
        if resolver is not None:
            self.resolver: PrincipalResolver = resolver
        else:
            # Backward-compat: construct a StaticKeyResolver from the keystore.
            ks: dict[str, str] = keystore if keystore is not None else {}
            self.resolver = StaticKeyResolver(ks)
        self._exempt_paths: frozenset[str] = (
            exempt_paths if exempt_paths is not None else _EXEMPT_PATHS
        )
        # sha256 hex of admin_api_key; None when not configured or irrelevant
        # (entra mode -- admin comes via the roles claim instead).
        self._admin_api_key_digest: str | None = admin_api_key_digest

    async def __call__(
        self, scope: MutableMapping[str, Any], receive: Any, send: Any
    ) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        # Fail-open only when the operator explicitly opted out AND the
        # resolver has no credentials configured. Entra mode can never fire
        # this (EntraResolver.auth_enabled is always True).
        if self._allow_unauthenticated and not self.resolver.auth_enabled:
            await self.app(scope, receive, send)
            return

        path: str = scope.get("path", "")
        # Used by both the admin-key fast-path and the entra bootstrap
        # exemption; scoping to admin paths keeps data routes hard-gated.
        is_admin_path: bool = _is_admin_route(path)
        if path in self._exempt_paths or any(
            path.startswith(p) for p in _EXEMPT_PREFIXES
        ):
            await self.app(scope, receive, send)
            return

        # Extract bearer token from headers.
        token = _extract_bearer_token(scope.get("headers", []))
        if token is None:
            await _send_401(send)
            return

        # Admin key gates /admin/* only -- it is an administration credential,
        # not a data-ingestion identity. On a data route it must fall through
        # to the resolver instead, so a registered data key still resolves to
        # its real contributor id rather than a synthetic "admin".
        if self._admin_api_key_digest is not None and is_admin_path:
            token_digest = hashlib.sha256(token.encode()).hexdigest()
            if token_digest == self._admin_api_key_digest:
                state = scope.setdefault("state", {})
                state["contributor_id"] = "admin"
                state["is_admin"] = True
                state["roles"] = []
                state["is_service"] = False
                await self.app(scope, receive, send)
                return

        try:
            result = self.resolver.resolve(token, admin_path=is_admin_path)
        except AuthError as exc:
            _log.info(
                "auth_event=auth_denied: %s (status=%d)",
                exc.reason,
                exc.status_code,
            )
            await _send_error(send, exc.status_code, exc.reason)
            return
        except Exception:  # noqa: BLE001 -- defense-in-depth catch-all
            # Any unexpected resolver exception must not propagate as a 500 --
            # respond fail-closed and log loudly. Raw token is never logged.
            _log.error(
                "auth_event=resolver_unexpected_exception: unexpected error in "
                "resolver.resolve() — denying request fail-closed "
                "(investigate exc_info below)",
                exc_info=True,
            )
            await _send_401(send)
            return

        if result is None:
            # Raw token is never logged; a short sha256 fingerprint lets
            # operators correlate the rejected credential without exposing it.
            _log.info(
                "auth_event=auth_denied: static key not recognized (status=401) "
                "token_sha256=%s",
                hashlib.sha256(token.encode()).hexdigest()[:12],
            )
            await _send_401(send)
            return

        contributor_id, roles, is_service = result

        # is_admin is False here for entra tokens too -- admin authority for
        # entra is signalled via the roles list instead.
        state = scope.setdefault("state", {})
        state["contributor_id"] = contributor_id
        state["is_admin"] = False
        state["roles"] = roles
        state["is_service"] = is_service

        await self.app(scope, receive, send)


def _extract_bearer_token(headers: list[tuple[bytes, bytes]]) -> str | None:
    """Extract the bearer token from ASGI headers."""
    for name, value in headers:
        if name.lower() == b"authorization":
            decoded = value.decode("latin-1")
            if decoded.startswith("Bearer "):
                return decoded[7:]
    return None


async def _send_error(send: Any, status_code: int, detail: str) -> None:
    """Send an HTTP error response with *status_code* and a JSON body."""
    body = json.dumps({"detail": detail}).encode()
    await send(
        {
            "type": "http.response.start",
            "status": status_code,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(body)).encode()),
            ],
        }
    )
    await send({"type": "http.response.body", "body": body})


async def _send_401(send: Any) -> None:
    """Send a 401 Unauthorized JSON response."""
    await _send_error(send, 401, "Missing or invalid bearer token")
