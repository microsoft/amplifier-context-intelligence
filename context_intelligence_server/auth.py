"""Bearer token authentication middleware for the Context Intelligence Server."""

import hashlib
import json
import logging
from collections.abc import Callable, MutableMapping
from typing import Any

import jwt  # pyjwt[crypto] — added in T1; used by EntraResolver
from jwt import PyJWKClient
from typing_extensions import Protocol

_log = logging.getLogger(__name__)

# JWKS signing-key cache TTL passed to PyJWKClient.
#
# PyJWKClient handles per-kid caching and lifespan-bounded refresh natively;
# making the value explicit here keeps the contract visible in code even
# though 300 s matches the library default.  No custom per-kid dedup lock
# or global refresh cap is built for the pilot (council/cranky: pilot-scale
# does not justify that complexity — revisit at scale if stampede behaviour
# is observed in production metrics).
JWKS_CACHE_LIFESPAN_SECONDS: int = 300

# Paths that are exempt from authentication (health checks, monitoring, dashboard pages).
# Used when web_ui_enabled=True (the default full-web mode).
_EXEMPT_PATHS: frozenset[str] = frozenset(
    {
        "/status",
        "/version",
        "/logs/stream",
        "/",
        "/dashboard",
        "/docs",
        "/openapi.json",
    }
)

# Paths exempt from authentication in API-only mode (web_ui_enabled=False).
# Web-UI-only paths (/logs/stream, /, /dashboard, /docs, /openapi.json) are intentionally
# absent: those routes are not registered in api-only mode and /logs/stream must not
# remain an unauthenticated log drain.
_EXEMPT_PATHS_API_ONLY: frozenset[str] = frozenset(
    {
        "/status",
        "/version",
    }
)

# Path prefixes that are exempt from authentication (static assets).
_EXEMPT_PREFIXES: tuple[str, ...] = ("/static/", "/skills/")


# ---------------------------------------------------------------------------
# Auth error — carries a specific HTTP status code
# ---------------------------------------------------------------------------


class AuthError(Exception):
    """Authentication/authorisation failure with a specific HTTP status code.

    Raised by :class:`EntraResolver` (and may be raised by future resolvers)
    to communicate *why* a request was rejected, not just *that* it was.

    ``status_code``:
        401 — token is missing, malformed, expired, has wrong audience/issuer/
              tenant, fails signature verification, uses a disallowed algorithm,
              or is missing required claims (``oid``, ``scp``).
        403 — token is cryptographically valid but the ``oid`` is not in the
              identity map (``bearer_identity_unbound``).

    ``reason``:
        Short human-readable message for logging / response bodies.  The 403
        reason MUST name the unbound ``oid`` so operators can diagnose and add
        the missing entry.
    """

    def __init__(self, status_code: int, reason: str) -> None:
        super().__init__(reason)
        self.status_code = status_code
        self.reason = reason


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

    Returns ``(contributor_id, roles)`` on success, or ``None`` when the token
    is not recognised (caller should respond 401).  Implementations may also
    raise :class:`AuthError` to signal specific auth failures (401 or 403);
    the middleware dispatches the exact ``status_code`` from the exception.

    The ``roles`` element (a list of strings) carries the token's App-Role
    claim for the entra resolver, or an empty list for the static resolver.
    The middleware stores these on ``scope["state"]["roles"]`` so downstream
    dependencies (e.g. ``require_admin``) can read them without re-parsing.

    Only one concrete implementation exists today: :class:`StaticKeyResolver`.
    The ``EntraResolver`` (JWT via JWKS) is added in T4.  Do NOT add a third
    resolver without a separate design review.

    T5 protocol change: ``resolve()`` previously returned ``str | None``.
    Changed to ``tuple[str, list[str]] | None`` to carry the ``roles`` claim
    from the Entra JWT so the middleware can set ``is_admin``/``roles`` on
    scope state without a second token parse.  ``StaticKeyResolver`` always
    returns an empty roles list.
    """

    @property
    def auth_enabled(self) -> bool:
        """True when authentication is active (at least one credential configured).

        ``False`` only for a :class:`StaticKeyResolver` built with an empty
        keystore — the explicit ``allow_unauthenticated=True`` opt-out path.
        ``EntraResolver`` always returns ``True``.
        """
        ...

    def resolve(self, token: str) -> tuple[str, list[str]] | None:
        """Return ``(contributor_id, roles)`` or ``None`` if token not recognised.

        Raises :class:`AuthError` (with ``status_code`` 401 or 403) when the
        token is present but invalid or maps to an unauthorised identity.

        ``roles`` is a list of App-Role strings (from the Entra ``roles``
        claim).  ``StaticKeyResolver`` always returns ``[]``.
        """
        ...


class EntraResolver:
    """Resolves Entra RS256 bearer tokens to contributor ids.

    Mirrors ``validate_entra_token()`` from Team Pulse
    (``amplifier-app-team-pulse`` / ``team_pulse/identity/extractors.py``):
    PyJWKClient → ``jwt.decode`` with ``algorithms=["RS256"]``, dual audience,
    explicit ``tid`` + ``scp`` + ``oid`` checks, then ``oid → contributor_id``
    lookup via *identity_map*.

    V1 scope: delegated USER tokens from the ``az`` CLI client.
    ``scp`` must contain ``access_as_user``; ``oid`` is extracted and mapped.
    Service-to-service is NOT built; the seam (step 2 ``extract_principal``)
    accommodates it later without touching validation logic.

    Raises:
        :class:`AuthError` (401): Token is missing/malformed/expired/wrong-aud/
            wrong-iss/wrong-tid/fails-sig-verification/wrong-alg, or is missing
            required claims (``oid``, ``scp``).
        :class:`AuthError` (403): Token is cryptographically valid but the
            lowercased ``oid`` is not in *identity_map*
            (``bearer_identity_unbound``).
        RuntimeError: At construction if eager JWKS prefetch fails — the server
            must refuse to start rather than lazily fail at first request (§8b).

    Args:
        client_id:    Azure App Registration client ID (GUID).
        tenant_id:    Azure AD tenant ID (GUID).
        identity_map: ``{oid_lower -> contributor_id}`` — built by
                      :meth:`~context_intelligence_server.config.Settings.build_identity_map`.
                      MUST be non-empty (config validator ensures this).
        jwks_client:  Injectable JWKS client for tests.  Must expose
                      ``fetch_data() -> None`` and
                      ``get_signing_key_from_jwt(token) -> obj`` where
                      ``obj.key`` is the signing key.  When ``None`` the default
                      builds a ``PyJWKClient`` pointed at
                      ``https://login.microsoftonline.com/{tenant_id}/discovery/v2.0/keys``
                      and calls ``fetch_data()`` eagerly.

    Note — JWKS caching (T5):
        Per-``kid`` caching and lifespan-bounded refresh are handled by
        ``PyJWKClient`` (``lifespan=JWKS_CACHE_LIFESPAN_SECONDS``).  No custom
        per-``kid`` dedup lock or global refresh cap is built for the pilot;
        revisit at scale if stampede behaviour appears in production metrics.
    """

    def __init__(
        self,
        client_id: str,
        tenant_id: str,
        identity_map: dict[str, str],
        *,
        jwks_client: Any = None,
    ) -> None:
        self._client_id = client_id
        self._tenant_id = tenant_id
        self._identity_map = identity_map
        # Accept both the bare client GUID (ID-token aud) and the api:// form
        # (access-token aud when access_as_user scope is exposed).  Matches
        # the Team Pulse mirror and the Q-AUD confirmation from §2b.
        self._expected_aud = [client_id, f"api://{client_id}"]
        self._expected_issuer = f"https://login.microsoftonline.com/{tenant_id}/v2.0"

        if jwks_client is None:
            jwks_uri = (
                f"https://login.microsoftonline.com/{tenant_id}/discovery/v2.0/keys"
            )
            jwks_client = PyJWKClient(jwks_uri, lifespan=JWKS_CACHE_LIFESPAN_SECONDS)

        # Eager prefetch — fail-closed at startup (§8b / crusty gate).
        # Called regardless of whether the client was injected or built by
        # default so that tests can inject a _FailingJWKSClient and verify
        # the fail-closed guarantee.
        # Per-kid caching and lifespan-bounded refresh are handled by
        # PyJWKClient (lifespan=JWKS_CACHE_LIFESPAN_SECONDS).  No custom
        # per-kid dedup lock or global refresh cap is built for the pilot.
        try:
            jwks_client.fetch_data()
        except Exception as exc:  # noqa: BLE001 — any failure is fatal here
            raise RuntimeError(
                f"EntraResolver: JWKS prefetch failed for tenant "
                f"{tenant_id!r} — server cannot start without a reachable "
                f"JWKS endpoint.  Cause: {exc}"
            ) from exc

        # Guard: a reachable-but-empty JWKS ({"keys": []}) would let
        # construction succeed but then 401 every request lazily.  Detect it
        # here so the server refuses to start rather than silently degrading.
        # Uses get_jwk_set() if available; stubs that pre-date this check
        # (AttributeError) are tolerated — all production PyJWKClient
        # instances expose the method.
        try:
            jwk_set = jwks_client.get_jwk_set()
        except AttributeError:
            pass  # Pre-existing stub without get_jwk_set() — skip the check
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

    def resolve(self, token: str) -> tuple[str, list[str]]:
        """Validate Entra JWT and return the mapped contributor id and roles.

        Args:
            token: Raw bearer token string (``Authorization: Bearer`` prefix
                   already stripped by the middleware).

        Returns:
            A ``(contributor_id, roles)`` tuple where *contributor_id* is
            mapped from the token's ``oid`` claim and *roles* is the token's
            ``roles`` claim (App Role assignments) as a list of strings, or
            ``[]`` when the claim is absent or not a list.

            The ``roles`` list is stored by the middleware on
            ``scope["state"]["roles"]`` so ``require_admin`` can check it
            without re-parsing the token.  Only the ``roles`` claim is
            returned — ``groups`` is intentionally excluded (TB-09).

        Raises:
            AuthError(401): Any JWT validation failure (signature, audience,
                issuer, expiry, algorithm, missing/wrong ``tid``/``scp``/``oid``).
            AuthError(403): Valid token whose ``oid`` is not in *identity_map*.
        """
        try:
            key = self._jwks_client.get_signing_key_from_jwt(token).key
            claims = jwt.decode(
                token,
                key,
                algorithms=["RS256"],  # H1: pin RS256, reject alg=none / HS256
                audience=self._expected_aud,
                issuer=self._expected_issuer,
                options={"require": ["exp", "iss", "aud"]},
            )
        except jwt.PyJWTError as exc:
            # Covers: InvalidSignatureError, ExpiredSignatureError,
            # InvalidAudienceError, InvalidIssuerError, InvalidAlgorithmError,
            # MissingRequiredClaimError, PyJWKClientError, ImmatureSignatureError
            # (nbf), and all other PyJWT validation failures.
            raise AuthError(401, f"Invalid bearer token: {exc}") from exc

        # Explicit tid check — defense-in-depth alongside the issuer pin.
        # A v2 Entra token's iss already encodes the tenant, but the explicit
        # check makes the tenant binding self-documenting and mirrors TP.
        if claims.get("tid") != self._tenant_id:
            raise AuthError(401, "Token from wrong tenant")

        # scp must contain access_as_user (delegated user flow only — V1).
        # Space-split to avoid substring false-positives (e.g. "not_access_as_user").
        # Guard: a non-string scp (e.g. list from a malformed token) must be treated
        # as missing rather than crashing on .split() — FAIL-2 fix.
        _scp_raw = claims.get("scp")
        scp: str = _scp_raw if isinstance(_scp_raw, str) else ""
        if "access_as_user" not in scp.split():
            raise AuthError(
                401,
                f"Token missing required scope 'access_as_user' (got scp={scp!r})",
            )

        # oid is required.  Missing/non-string/whitespace oid is a broken/
        # service token → 401, NOT 403 (AC12 — TP omits this; we add it).
        # isinstance guard prevents AttributeError on non-string oid values
        # (e.g. int 42, list) that would crash on .lower() — FAIL-1/FAIL-3 fix.
        oid = claims.get("oid")
        if not isinstance(oid, str) or not oid.strip():
            raise AuthError(401, "Token missing or invalid 'oid' claim")

        # Map oid → contributor — unrecognised oid is a 403 (identity_unbound).
        # Both sides are lowercased: config validator lowercases keys at build
        # time; we lowercase the claim for robustness (AC12).
        oid_lower = oid.lower()
        contributor_id = self._identity_map.get(oid_lower)
        if contributor_id is None:
            raise AuthError(
                403,
                f"Identity not authorized: oid {oid_lower!r} is not in the "
                f"identity map; contact the server administrator to add this "
                f"identity (tenant {self._tenant_id!r})",
            )

        # Extract roles claim (T5: admin authorization for /admin/* endpoints).
        # roles must be a list of strings; any other type (missing claim, int,
        # string) is normalised to [] so the resolver is fail-closed on bad
        # token shapes.  Only `roles` is returned — `groups` is intentionally
        # excluded so group membership can NEVER grant admin access (TB-09).
        _roles_raw = claims.get("roles")
        roles: list[str] = (
            [r for r in _roles_raw if isinstance(r, str)]
            if isinstance(_roles_raw, list)
            else []
        )

        return (contributor_id, roles)


class StaticKeyResolver:
    """Resolves tokens via a pre-built ``{sha256_hex(token) -> contributor_id}`` keystore.

    This is a pure extraction of the inline logic that previously lived in
    :class:`BearerTokenMiddleware.__call__`.  Behaviour is byte-for-byte
    identical to the previous implementation.

    The keystore is built by :meth:`~context_intelligence_server.config.Settings.build_keystore`
    and maps the SHA-256 hex digest of each raw bearer token to the owner's
    contributor id string.  Raw tokens are never stored here.
    """

    def __init__(self, keystore: dict[str, str]) -> None:
        self._keystore = keystore

    @property
    def auth_enabled(self) -> bool:
        """True when at least one key is configured (authentication is active).

        ``False`` means the keystore is empty — the server is in
        unauthenticated mode (explicit ``allow_unauthenticated`` opt-out only;
        :func:`~context_intelligence_server.main.create_asgi_app` refuses to
        start with ``auth_enabled=False`` unless that flag is set).
        """
        return bool(self._keystore)

    @property
    def is_empty(self) -> bool:
        """True when no keys are configured.

        Kept for backward compatibility with existing tests.  Prefer
        ``auth_enabled`` (its logical inverse) for new code.
        """
        return not self._keystore

    def resolve(self, token: str) -> tuple[str, list[str]] | None:
        """Return ``(contributor_id, [])`` for *token*, or ``None`` on a miss.

        The roles list is always empty for static-key auth — admin authority is
        signalled via ``scope["state"]["is_admin"]`` by the middleware (which
        recognises the admin key before calling this resolver).
        """
        contributor_id = _resolve_token(token, self._keystore)
        if contributor_id is None:
            return None
        return (contributor_id, [])


class BearerTokenMiddleware:
    """ASGI middleware that validates ``Authorization: Bearer <token>`` headers.

    Accepts a :class:`PrincipalResolver` via the *resolver* keyword argument
    (preferred — used by :func:`~context_intelligence_server.main.create_asgi_app`),
    or a raw *keystore* dict for backward compatibility with tests that
    construct the middleware directly.

    When the resolver's ``auth_enabled`` property is ``False`` (i.e. a
    :class:`StaticKeyResolver` built with an empty keystore), all requests
    pass through without authentication — the explicit ``allow_unauthenticated``
    opt-out path.  :func:`~context_intelligence_server.main.create_asgi_app`
    prevents booting in this state at the application level.

    On a successful match the following keys are injected into
    ``scope["state"]`` so downstream handlers can read authenticated identity
    without re-resolving:

    * ``contributor_id`` (str): the resolved contributor.
    * ``is_admin`` (bool): ``True`` only when the static-mode admin key was
      used.  Always ``False`` for regular data keys and for entra tokens (where
      admin authority is carried in ``roles`` instead).
    * ``roles`` (list[str]): App Role assignments from the Entra ``roles``
      claim, or ``[]`` for static-mode tokens.  The ``require_admin``
      dependency checks this list for the ``IdentityAdmin`` role.

    T5 — static-mode admin key (ROB F1):

    The admin key (``admin_api_key_digest``) is not in the data keystore, so
    it would normally fail the resolver and produce a 401.  Instead, the
    middleware checks the bearer token's sha256 against the admin-key digest
    BEFORE delegating to the resolver.  A match authenticates the request
    directly with ``contributor_id="admin"`` and ``is_admin=True``.  The
    token still reaches data-API endpoints (it is a valid principal) but
    ``require_admin`` passes only for admin-key bearers.

    Several paths are always exempt (see ``_EXEMPT_PATHS``) so health checks,
    monitoring tools, and public-facing pages continue working without
    credentials.
    """

    def __init__(
        self,
        app: Callable[..., Any],
        keystore: dict[str, str] | None = None,
        *,
        resolver: PrincipalResolver | None = None,
        exempt_paths: frozenset[str] | None = None,
        admin_api_key_digest: str | None = None,
    ) -> None:
        self.app = app
        if resolver is not None:
            # Preferred path: caller explicitly constructed and wired the resolver.
            self.resolver: PrincipalResolver = resolver
        else:
            # Backward-compat path: construct a StaticKeyResolver from the
            # provided (or defaulted-to-empty) keystore dict.
            ks: dict[str, str] = keystore if keystore is not None else {}
            self.resolver = StaticKeyResolver(ks)
        # Exempt paths: which exact paths bypass auth entirely.  Defaults to the
        # full web-UI set (_EXEMPT_PATHS); pass _EXEMPT_PATHS_API_ONLY when
        # web_ui_enabled=False to prevent /logs/stream from being an
        # unauthenticated log drain.  Path prefixes (_EXEMPT_PREFIXES) are
        # always applied regardless of this setting.
        self._exempt_paths: frozenset[str] = (
            exempt_paths if exempt_paths is not None else _EXEMPT_PATHS
        )
        # Admin key digest (T5 / ROB F1): sha256 hex of the raw admin_api_key.
        # When set, a bearer token whose sha256 matches this digest is
        # authenticated as the "admin" principal with is_admin=True and bypasses
        # the data keystore.  None means admin key is not configured (static
        # mode) or is irrelevant (entra mode — admin is via roles claim).
        self._admin_api_key_digest: str | None = admin_api_key_digest

    async def __call__(
        self, scope: MutableMapping[str, Any], receive: Any, send: Any
    ) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        # Fail-open when auth is disabled (resolver.auth_enabled is False).
        # Uses the protocol property rather than isinstance(resolver, StaticKeyResolver)
        # so the check works for any resolver that opts out of authentication.
        # In practice this path is only reached by the allow_unauthenticated opt-out;
        # create_asgi_app() refuses to start with auth_enabled=False in production.
        if not self.resolver.auth_enabled:
            await self.app(scope, receive, send)
            return

        path: str = scope.get("path", "")
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

        # T5 / ROB F1 — static-mode admin key check.
        #
        # The admin key is NOT in the data keystore, so it would normally fail
        # the resolver and produce a 401.  Check the bearer token's sha256
        # against the admin-key digest BEFORE calling the resolver.  A match
        # authenticates the request directly with contributor_id="admin" and
        # is_admin=True — the resolver is bypassed entirely.
        #
        # Note: entra mode does not use admin_api_key_digest (it is always
        # None in entra mode); admin authority comes from the roles claim.
        if self._admin_api_key_digest is not None:
            token_digest = hashlib.sha256(token.encode()).hexdigest()
            if token_digest == self._admin_api_key_digest:
                state = scope.setdefault("state", {})
                state["contributor_id"] = "admin"
                state["is_admin"] = True
                state["roles"] = []
                await self.app(scope, receive, send)
                return

        try:
            result = self.resolver.resolve(token)
        except AuthError as exc:
            # EntraResolver (and future resolvers) raise AuthError to communicate
            # 401 vs 403.  Dispatch the status code directly.
            # Log at INFO — distinguishable from unexpected errors (ERROR).
            # auth_event=auth_denied is a greppable marker for "bad token rejected".
            _log.info(
                "auth_event=auth_denied: %s (status=%d)",
                exc.reason,
                exc.status_code,
            )
            await _send_error(send, exc.status_code, exc.reason)
            return
        except Exception:
            # Defense-in-depth catch-all: any unexpected exception from the
            # resolver (e.g. a transient library bug) must not propagate as a
            # 500 — respond fail-closed (401) and log loudly for operators.
            # auth_event=resolver_unexpected_exception distinguishes this from
            # a normal auth denial so operators can grep specifically for it.
            # The raw token is intentionally NOT logged (credential hygiene).
            _log.error(
                "auth_event=resolver_unexpected_exception: unexpected error in "
                "resolver.resolve() — denying request fail-closed "
                "(investigate exc_info below)",
                exc_info=True,
            )
            await _send_401(send)
            return

        if result is None:
            # Backward-compat path: StaticKeyResolver returns None on a miss.
            await _send_401(send)
            return

        contributor_id, roles = result

        # Inject authenticated identity and auth metadata into scope state.
        # is_admin is False for regular data keys and for entra tokens (admin
        # authority for entra is signalled via the roles list, not this flag).
        state = scope.setdefault("state", {})
        state["contributor_id"] = contributor_id
        state["is_admin"] = False
        state["roles"] = roles

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
