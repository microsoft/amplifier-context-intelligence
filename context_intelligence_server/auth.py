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

# Route prefix of the admin router (mirrors routers/admin.py:
# ``APIRouter(prefix="/admin", ...)``).  The static-mode admin-key fast-path is
# scoped to these paths: the admin key is an administration credential, NOT a
# data-ingestion identity, so it must only short-circuit auth on /admin/* — never
# on data routes like POST /events (where it would otherwise stamp a synthetic
# ``created_by="admin"`` and let a bare admin key post events).
_ADMIN_ROUTE_PREFIX: str = "/admin"


def _is_admin_route(path: str) -> bool:
    """True for the admin router's own paths (``/admin`` and ``/admin/...``)."""
    return path == _ADMIN_ROUTE_PREFIX or path.startswith(_ADMIN_ROUTE_PREFIX + "/")


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
              identity map (``bearer_identity_unbound``), or a service token
              with no qualifying App Role.

    ``reason``:
        Short human-readable message for logging / response bodies.  The 403
        reason MUST name the unbound ``oid`` so operators can diagnose and add
        the missing entry.
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

    Returns ``(contributor_id, roles, is_service)`` on success, or ``None`` when
    the token is not recognised (caller should respond 401).  Implementations may
    also raise :class:`AuthError` to signal specific auth failures (401 or 403);
    the middleware dispatches the exact ``status_code`` from the exception.

    The ``roles`` element (a list of strings) carries the token's App-Role
    claim for the entra resolver, or an empty list for the static resolver.
    The middleware stores these on ``scope["state"]["roles"]`` so downstream
    dependencies (e.g. ``require_admin``) can read them without re-parsing.

    ``is_service`` is ``True`` for app/service tokens resolved by the service
    branch, ``False`` for delegated user tokens and static-key tokens.  The
    middleware writes this onto ``scope["state"]["is_service"]`` so route
    capability deps (``require_write`` / ``require_read``) can gate service
    principals without re-parsing the token.

    Only one concrete implementation exists today: :class:`StaticKeyResolver`.
    The ``EntraResolver`` (JWT via JWKS) is added in T4.  Do NOT add a third
    resolver without a separate design review.

    M2 protocol change: ``resolve()`` previously returned
    ``tuple[str, list[str]] | None``.  Changed to
    ``tuple[str, list[str], bool] | None`` to carry the ``is_service`` flag
    so the middleware can set capability state without re-parsing the token.
    ``StaticKeyResolver`` always returns ``is_service=False``.
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

        ``admin_path`` (keyword-only, default ``False``) signals that the
        request targets an ``/admin/*`` route. When ``True``, ``EntraResolver``
        relaxes ONLY the identity-map membership check (an unbound-but-valid
        oid is admitted so an IdentityAdmin role-holder can bootstrap the map);
        NO token-authenticity check (signature/issuer/audience/expiry/tenant/
        scope/oid-presence) is ever relaxed. ``StaticKeyResolver`` ignores this
        parameter entirely — its admin authorization is via a separate
        admin-key fast-path, not map membership.
        """
        ...


class EntraResolver:
    """Resolves Entra RS256 bearer tokens to contributor ids.

    Mirrors ``validate_entra_token()`` from Team Pulse
    (``amplifier-app-team-pulse`` / ``team_pulse/identity/extractors.py``):
    PyJWKClient → ``jwt.decode`` with ``algorithms=["RS256"]``, dual audience,
    explicit ``tid`` + ``scp`` + ``oid`` checks, then ``oid → contributor_id``
    lookup via *identity_map*.

    M2: adds a second branch for app/service tokens (no ``scp``) selected by
    a ``scp``-presence discriminator, authorized by App-Role alone, with a
    fail-loud ``created_by`` derived from stable claims.

    Raises:
        :class:`AuthError` (401): Token is missing/malformed/expired/wrong-aud/
            wrong-iss/wrong-tid/fails-sig-verification/wrong-alg, [B1] anomaly,
            or missing/invalid identity claim.
        :class:`AuthError` (403): Token is cryptographically valid but the
            lowercased ``oid`` is not in *identity_map* (user branch), or no
            qualifying App Role is present (service branch).
        RuntimeError: At construction if eager JWKS prefetch fails — the server
            must refuse to start rather than lazily fail at first request (§8b).

    Args:
        client_id:            Azure App Registration client ID (GUID).
        tenant_id:            Azure AD tenant ID (GUID).
        identity_map:         ``{oid_lower -> contributor_id}`` — built by
                              :meth:`~context_intelligence_server.config.Settings.build_identity_map`.
                              MAY be empty at construction: an empty map is a
                              supported bootstrap state (the server boots
                              fail-closed and is populated at runtime via the
                              IdentityAdmin-gated /admin/identities API). A live
                              reference is passed so runtime PUT/DELETE are
                              visible immediately. On a data route an unmapped
                              oid still 403s; the map-miss is exempted ONLY for
                              /admin/* paths (``admin_path=True``) so a role-
                              holder can bootstrap the first identity.
        service_identity_map: ``{oid_lower -> contributor_id}`` for service
                              principals.  Optional; ``{}`` = no service map.
        service_data_role:    App Role name granting write access.  ``""`` disables.
        reader_role:          App Role name granting read-only access.  ``""`` disables.
        entra_admin_role:     App Role name granting admin access.  ``""`` disables.
        jwks_client:          Injectable JWKS client for tests.

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
        service_identity_map: dict[str, str] | None = None,  # NEW (M2)
        service_data_role: str = "",  # NEW (M2)
        reader_role: str = "",  # NEW (M2)
        entra_admin_role: str = "",  # NEW (M2)
        jwks_client: Any = None,
    ) -> None:
        self._client_id = client_id
        self._tenant_id = tenant_id
        self._identity_map = identity_map
        # M2 service-path config — fail-closed defaults (empty disables each role).
        self._service_identity_map: dict[str, str] = service_identity_map or {}
        self._service_data_role = service_data_role
        self._reader_role = reader_role
        self._entra_admin_role = entra_admin_role
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

    def resolve(
        self, token: str, *, admin_path: bool = False
    ) -> tuple[str, list[str], bool]:
        """Validate Entra JWT; return (contributor_id, roles, is_service).

        Implements the dual-path discriminator (M2):
        - Tokens with ``scp`` present → USER / delegated branch (unchanged from V1).
        - Tokens without ``scp`` → SERVICE / app branch (new in M2).
        The ``[B1]`` anomaly check fires first when both ``scp`` and
        ``idtyp=app`` are present — neither branch can claim such a token
        (fail-closed, 401).

        Args:
            token: Raw bearer token string (``Authorization: Bearer`` prefix
                   already stripped by the middleware).

        Returns:
            A ``(contributor_id, roles, is_service)`` 3-tuple.
            *contributor_id* is mapped from ``oid`` (user branch) or derived
            via ``service_identity_map``/``appid``/``azp``/``oid`` (service
            branch).  *roles* is the token's App Role assignments as a list of
            strings.  *is_service* is ``True`` for app/service tokens,
            ``False`` for delegated user tokens.

        Raises:
            AuthError(401): JWT validation failure, wrong tenant, [B1] anomaly,
                missing/invalid ``oid`` (user branch), or no resolvable identity
                (service branch).
            AuthError(403): Valid user token with unmapped ``oid``, or valid
                service token with no qualifying App Role [D7].
        """
        # ---- SHARED VALIDATION (both paths) — UNCHANGED from V1 ----
        try:
            key = self._jwks_client.get_signing_key_from_jwt(token).key
            claims = jwt.decode(
                token,
                key,
                algorithms=["RS256"],  # H1: pin RS256, reject alg=none / HS256
                audience=self._expected_aud,  # B7: aud enforced here
                issuer=self._expected_issuer,  # B7: iss enforced here
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

        # ---- DISCRIMINATOR (M2) — scp PRIMARY, idtyp CONFIRMATION ----
        # scp normalization is IDENTICAL to V1 (non-string -> "").
        _scp_raw = claims.get("scp")
        scp: str = _scp_raw if isinstance(_scp_raw, str) else ""
        has_scp: bool = bool(
            scp.split()
        )  # any whitespace-delimited scope token present

        # idtyp normalization [B2]: non-string -> "", then strip().lower().
        _idtyp_raw = claims.get("idtyp")
        idtyp: str = _idtyp_raw.strip().lower() if isinstance(_idtyp_raw, str) else ""

        # [B1] Branches MUST be mutually exclusive.  A token bearing BOTH a
        # delegated scope AND idtyp=="app" is anomalous (no legitimate Entra
        # token does this) -> fail closed.  Checked FIRST so neither branch
        # can claim it.
        if has_scp and idtyp == "app":
            raise AuthError(
                401,
                "Ambiguous token: carries both delegated 'scp' and idtyp='app'; "
                "refusing to classify as either user or service",
            )

        if has_scp:
            # =========================================================
            # USER / DELEGATED BRANCH — BYTE-FOR-BYTE auth.py V1 logic
            # (only the return arity changes: append is_service=False)
            # =========================================================
            if "access_as_user" not in scp.split():
                raise AuthError(
                    401,
                    f"Token missing required scope 'access_as_user' (got scp={scp!r})",
                )
            # oid is required.  Missing/non-string/whitespace oid -> 401, NOT 403
            # (AC12).  isinstance guard prevents AttributeError on non-string oid
            # (e.g. int 42, list) — FAIL-1/FAIL-3 fix.
            oid = claims.get("oid")
            if not isinstance(oid, str) or not oid.strip():
                raise AuthError(401, "Token missing or invalid 'oid' claim")
            # Map oid -> contributor — unrecognised oid is a 403 (identity_unbound).
            # Both sides lowercased: config validator lowercases keys at build time.
            oid_lower = oid.lower()
            contributor_id = self._identity_map.get(oid_lower)
            if contributor_id is None:
                # BOOTSTRAP EXEMPTION — /admin/* paths ONLY (admin_path=True):
                # a cryptographically-valid delegated token whose oid is not yet
                # bound is admitted to routing so an IdentityAdmin role-holder can
                # populate the map on a fresh deployment. Authorization is still
                # enforced downstream by require_admin on the `roles` claim; a
                # non-admin unbound token reaches /admin and is 403'd there.
                #
                # SECURITY: this relaxes ONLY the oid->id map-membership lookup.
                # All JWT authenticity checks (signature, issuer, audience,
                # expiry, tenant, access_as_user scope, oid presence) have already
                # passed above and are NOT affected. On every non-admin path
                # admin_path is False, so an unmapped oid is still a hard 403.
                if not admin_path:
                    raise AuthError(
                        403,
                        f"Identity not authorized: oid {oid_lower!r} is not in the "
                        f"identity map; contact the server administrator to add this "
                        f"identity (tenant {self._tenant_id!r})",
                    )
                # Provisional contributor id = the oid itself, so the admin audit
                # log records who performed the bootstrap mutation even though
                # they are not yet a mapped contributor.
                contributor_id = oid_lower
            # Roles: list[str] normalization — only `roles`, never `groups` (TB-09).
            _roles_raw = claims.get("roles")
            roles: list[str] = (
                [r for r in _roles_raw if isinstance(r, str)]
                if isinstance(_roles_raw, list)
                else []
            )
            return (
                contributor_id,
                roles,
                False,
            )  # <-- only delta from V1: third element

        # =========================================================
        # SERVICE / APP BRANCH (NEW, M2) — scp ABSENT
        # =========================================================
        # Roles normalization is identical to the user branch.
        _roles_raw = claims.get("roles")
        roles = (
            [r for r in _roles_raw if isinstance(r, str)]
            if isinstance(_roles_raw, list)
            else []
        )

        # --- Authorization = ROLE ALONE [D7].  Admit iff a qualifying configured
        #     role is present.  Empty name disables that role (config.py:504-513).
        authorized = (
            (self._service_data_role and self._service_data_role in roles)
            or (self._reader_role and self._reader_role in roles)
            or (self._entra_admin_role and self._entra_admin_role in roles)
        )
        if not authorized:
            # 403 message: name the rejected principal (appid preferred over oid)
            # and the required App Roles.  Do NOT echo roles=[...] in the response
            # body — that is an internal claim value, not operator guidance (R1).
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

        # --- created_by derivation [B6/B8]: stable claims, truthiness chaining,
        #     NEVER app_displayname (spoofable in Entra — B8), fail-loud.
        #     Order: service_map[oid] > appid > azp > oid.
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

        ``False`` means the keystore is empty. This alone NO LONGER makes the
        server pass requests through: an empty keystore now boots fail-CLOSED
        (a supported bootstrap state) and every request 401s until keys are
        onboarded via the /admin/keys API. The ONLY way requests pass through
        unauthenticated is the explicit ``allow_unauthenticated=True`` opt-out
        combined with this returning ``False`` — see
        :class:`BearerTokenMiddleware` and
        :func:`~context_intelligence_server.main.create_asgi_app`.
        """
        return bool(self._keystore)

    @property
    def is_empty(self) -> bool:
        """True when no keys are configured.

        Kept for backward compatibility with existing tests.  Prefer
        ``auth_enabled`` (its logical inverse) for new code.
        """
        return not self._keystore

    def resolve(
        self, token: str, *, admin_path: bool = False
    ) -> tuple[str, list[str], bool] | None:
        """Return ``(contributor_id, [], False)`` for *token*, or ``None`` on a miss.

        The roles list is always empty for static-key auth — admin authority is
        signalled via ``scope["state"]["is_admin"]`` by the middleware (which
        recognises the admin key before calling this resolver).

        ``is_service`` is always ``False`` for static-key tokens — they behave
        like humans (always write-capable), preserving static-mode behavior.

        ``admin_path`` is accepted for Protocol compatibility with
        :class:`EntraResolver` but is unused here: static-mode admin
        authorization goes through the admin-key fast-path (matched before
        this resolver is ever called), not identity-map membership.
        """
        _ = admin_path  # unused: static-mode admin uses the admin-key fast-path
        contributor_id = _resolve_token(token, self._keystore)
        if contributor_id is None:
            return None
        return (contributor_id, [], False)


class BearerTokenMiddleware:
    """ASGI middleware that validates ``Authorization: Bearer <token>`` headers.

    Accepts a :class:`PrincipalResolver` via the *resolver* keyword argument
    (preferred — used by :func:`~context_intelligence_server.main.create_asgi_app`),
    or a raw *keystore* dict for backward compatibility with tests that
    construct the middleware directly.

    Fail-open pass-through happens ONLY when BOTH conditions hold: the
    middleware was constructed with ``allow_unauthenticated=True`` (the explicit
    test/dev opt-out) AND the resolver's ``auth_enabled`` property is ``False``
    (i.e. a :class:`StaticKeyResolver` built with an empty keystore). An empty
    keystore ALONE no longer passes requests through — with the production
    default (``allow_unauthenticated=False``) an empty keystore fail-CLOSES:
    the request falls through to token extraction and the resolver returns
    ``None`` → 401. Entra mode is unaffected: ``EntraResolver.auth_enabled`` is
    always ``True``, so this fail-open branch can never fire there.

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
    * ``is_service`` (bool): ``True`` for app/service tokens resolved by the
      service branch; ``False`` for human/delegated and static-key tokens.
      Used by ``require_write`` / ``require_read`` in ``main.py`` to gate
      service principals without re-parsing the token.

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
        allow_unauthenticated: bool = False,
    ) -> None:
        self.app = app
        # Explicit opt-out (test/dev ONLY): when True AND the resolver has no
        # credentials configured (auth_enabled is False), ALL requests pass
        # through unauthenticated. An empty keystore ALONE no longer fails
        # open — see the fail-open check in __call__ for the full rationale.
        self._allow_unauthenticated: bool = allow_unauthenticated
        if resolver is not None:
            # Preferred path: caller explicitly constructed and wired the resolver.
            self.resolver: PrincipalResolver = resolver
        else:
            # Backward-compat path: construct a StaticKeyResolver from the
            # provided (or defaulted-to-empty) keystore dict.
            ks: dict[str, str] = keystore if keystore is not None else {}
            self.resolver = StaticKeyResolver(ks)
        # Exempt paths: which exact paths bypass auth entirely.  Defaults to
        # the single API-only set (_EXEMPT_PATHS: /status, /version, /docs,
        # /openapi.json).  Path prefixes (_EXEMPT_PREFIXES) are always applied
        # in addition to this set.
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

        # Fail-open pass-through ONLY when the operator EXPLICITLY opted out via
        # allow_unauthenticated=True (test/dev) AND the resolver has no
        # credentials configured. An empty keystore ALONE no longer fails open:
        # with allow_unauthenticated=False (production default) an empty static
        # keystore fail-CLOSES — the request falls through to token extraction
        # and the resolver returns None -> 401. This is the change that makes an
        # empty-keystore boot a SAFE bootstrap state instead of a wide-open one.
        #
        # SECURITY: entra mode is unaffected — EntraResolver.auth_enabled is
        # always True, so `not self.resolver.auth_enabled` is always False and
        # this branch can never fire in entra mode regardless of the flag.
        if self._allow_unauthenticated and not self.resolver.auth_enabled:
            await self.app(scope, receive, send)
            return

        path: str = scope.get("path", "")
        # Compute once: is this an /admin/* route? Used by BOTH the static-mode
        # admin-key fast-path below AND the entra bootstrap exemption passed into
        # resolver.resolve(admin_path=...). Scoping the map-membership exemption
        # to admin paths is what keeps every data route hard-gated.
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

        # T5 / ROB F1 — static-mode admin key check, SCOPED to /admin/* routes.
        #
        # The admin key gates the /admin/* endpoints ONLY — it is an
        # administration credential, not a data-ingestion identity.  It is NOT in
        # the data keystore, so on an admin route it would fail the resolver and
        # produce a 401; the fast-path below checks the bearer token's sha256
        # against the admin-key digest BEFORE the resolver so an admin-key bearer
        # authenticates with is_admin=True (which require_admin needs).
        #
        # Restricting this to admin routes is the fix for the identity-conflation
        # bug: on a data route (e.g. POST /events) the admin key MUST NOT
        # short-circuit auth. Instead it falls through to the resolver, so:
        #   - a token that is ALSO a registered data key resolves to its real
        #     contributor id (created_by reflects that id, never "admin"); and
        #   - a bare admin key that is not a data key is correctly rejected (401)
        #     rather than posting events attributed to a synthetic "admin".
        # is_admin is only ever read on /admin/* (require_admin), so scoping the
        # fast-path there loses no authorization capability.
        #
        # Note: entra mode does not use admin_api_key_digest (it is always
        # None in entra mode); admin authority comes from the roles claim.
        if self._admin_api_key_digest is not None and is_admin_path:
            token_digest = hashlib.sha256(token.encode()).hexdigest()
            if token_digest == self._admin_api_key_digest:
                state = scope.setdefault("state", {})
                state["contributor_id"] = "admin"
                state["is_admin"] = True
                state["roles"] = []
                state["is_service"] = False  # admin key behaves like a human (M2)
                await self.app(scope, receive, send)
                return

        try:
            # admin_path=True relaxes ONLY the oid->id map-membership lookup for
            # /admin/* (bootstrap): an unbound-but-valid delegated token reaches
            # require_admin, which then authorizes on the role claim. On every
            # non-admin path admin_path=False, so an unmapped oid is still 403.
            result = self.resolver.resolve(token, admin_path=is_admin_path)
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
            # Log it with the SAME greppable auth_event=auth_denied marker as the
            # AuthError branch above so a static-key rejection is not invisible in
            # server.jsonl. Without this line a genuine 401 leaves zero trace,
            # which made a real "Bearer [REDACTED]" rejection look impossible to
            # diagnose. The raw token is intentionally NOT logged (credential
            # hygiene); a short sha256 fingerprint is emitted so operators can
            # correlate the rejected credential (e.g. the redaction sentinel
            # "[REDACTED]" has a recognisable digest) without exposing a secret.
            _log.info(
                "auth_event=auth_denied: static key not recognized (status=401) "
                "token_sha256=%s",
                hashlib.sha256(token.encode()).hexdigest()[:12],
            )
            await _send_401(send)
            return

        contributor_id, roles, is_service = result  # M2: unpack 3-tuple

        # Inject authenticated identity and auth metadata into scope state.
        # is_admin is False for regular data keys and for entra tokens (admin
        # authority for entra is signalled via the roles list, not this flag).
        state = scope.setdefault("state", {})
        state["contributor_id"] = contributor_id
        state["is_admin"] = False
        state["roles"] = roles
        state["is_service"] = is_service  # M2: capability signal for route deps

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
