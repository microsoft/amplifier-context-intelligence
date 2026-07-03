"""Bearer token authentication middleware for the Context Intelligence Server."""

import hashlib
import json
import logging
from collections.abc import Callable, MutableMapping
from typing import Any

import jwt  # pyjwt[crypto] — added in T1; used by EntraResolver
from jwt import PyJWKClient
from typing_extensions import Protocol

from context_intelligence_server.config import ALL_ZEROS_GUID, GUID_RE

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
        "/version",
    }
)

# Path prefixes that are exempt from authentication (static assets).
_EXEMPT_PREFIXES: tuple[str, ...] = ("/static/", "/skills/")

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

    def resolve(self, token: str) -> tuple[str, list[str], bool] | None:
        """Return ``(contributor_id, roles, is_service)`` or ``None`` if token not recognised.

        Raises :class:`AuthError` (with ``status_code`` 401 or 403) when the
        token is present but invalid or maps to an unauthorised identity.

        ``roles`` is a list of App-Role strings (from the Entra ``roles``
        claim).  ``StaticKeyResolver`` always returns ``[]``.

        ``is_service`` is ``True`` for app/service tokens (no ``scp`` claim,
        routed through the service branch); ``False`` for delegated user tokens
        and static-key tokens.
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
                              MUST be non-empty (config validator ensures this).
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

    def resolve(self, token: str) -> tuple[str, list[str], bool]:
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
                raise AuthError(
                    403,
                    f"Identity not authorized: oid {oid_lower!r} is not in the "
                    f"identity map; contact the server administrator to add this "
                    f"identity (tenant {self._tenant_id!r})",
                )
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

    def resolve_principal_id(self, oid: str) -> tuple[str, list[str], bool]:
        """Resolve an EasyAuth-injected browser oid to a contributor id.

        doc 14 (EasyAuth browser-identity spec) §3.3: this is the browser-path
        counterpart to :meth:`resolve` — it runs the SAME map lookup and 401/403
        semantics as the JWT user branch (see ``resolve()`` above, "oid is
        required" / "Map oid -> contributor") but with NO JWT to verify, since
        the oid arrives as a bare, untrusted header value rather than inside a
        cryptographically-signed token. It therefore re-validates GUID shape and
        the all-zeros sentinel that the JWT path takes for granted (the JWT path
        only ever sees oids that already survived the config-time GUID/sentinel
        gate baked into the identity map).

        Trust in the oid's authenticity rests entirely OUTSIDE this method — on
        sole-ingress topology (EasyAuth is the only thing that can reach this
        server) — see the middleware caller and the module-level design note in
        doc 14 §4 row 7. This method's job is only: is the credential well-formed,
        and is it bound to a contributor?

        Args:
            oid: The raw value of the ``X-MS-CLIENT-PRINCIPAL-ID`` header, or
                 ``""`` if the header was present but empty, or ``None``-like
                 (never actually ``None`` — callers pass a ``str``).

        Returns:
            ``(contributor_id, roles, is_service)`` — ``roles`` is always ``[]``
            and ``is_service`` is always ``False`` in this spike (Step 2).
            Browser-path admin authority (reading the additive
            ``entra_identities[oid]["admin"]`` key) is Step 3 (doc 14 §2.3/C5) —
            NOT wired here.

        Raises:
            AuthError(401): oid missing/empty/whitespace-only, not a valid GUID,
                or the all-zeros sentinel (doc 14 §4 rows 1-4; row 4 per C4/R2).
            AuthError(403): oid is a valid, non-sentinel GUID but not in the
                identity map (doc 14 §4 row 5, "identity_unbound" — names the
                oid for onboarding, mirrors the JWT branch's 403 message shape).
        """
        # rows 1-2: missing / empty / whitespace-only.
        if not isinstance(oid, str) or not oid.strip():
            raise AuthError(401, "EasyAuth principal id missing or empty")
        candidate = oid.strip().lower()
        # row 3: malformed / non-GUID.
        if not GUID_RE.fullmatch(candidate):
            raise AuthError(401, f"EasyAuth principal id {oid!r} is not a valid GUID")
        # row 4: all-zeros sentinel (C4/R2: 401, treated as a bad credential —
        # matches the identity map's build-time ban on this placeholder).
        if candidate == ALL_ZEROS_GUID:
            raise AuthError(401, "EasyAuth principal id is the all-zeros sentinel")
        # row 5: valid but unbound -> 403 identity_unbound (names the oid for
        # onboarding). self._identity_map is the SAME live entra_store.flat_dict
        # the JWT user branch reads, so an /admin put()/delete() is visible to
        # the browser path immediately -- same guarantee the Bearer path enjoys.
        contributor_id = self._identity_map.get(candidate)
        if contributor_id is None:
            raise AuthError(
                403,
                f"identity_unbound: oid {candidate!r} (provider=aad) is not in "
                f"the identity map; contact the server administrator to add "
                f"this identity (tenant {self._tenant_id!r})",
            )
        # row 6: accept. roles=[] in the spike (browser admin is Step 3);
        # is_service=False (browser = human-like).
        return (contributor_id, [], False)


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

    def resolve(self, token: str) -> tuple[str, list[str], bool] | None:
        """Return ``(contributor_id, [], False)`` for *token*, or ``None`` on a miss.

        The roles list is always empty for static-key auth — admin authority is
        signalled via ``scope["state"]["is_admin"]`` by the middleware (which
        recognises the admin key before calling this resolver).

        ``is_service`` is always ``False`` for static-key tokens — they behave
        like humans (always write-capable), preserving static-mode behavior.
        """
        contributor_id = _resolve_token(token, self._keystore)
        if contributor_id is None:
            return None
        return (contributor_id, [], False)


class EasyAuthResolveFn(Protocol):
    """Callable contract for resolving an EasyAuth browser oid (doc 14 §3.1/C3).

    C3 (BINDING, supersedes the bare ``Callable[[str], tuple[str, list[str],
    bool]]`` in the pre-council draft): a NAMED Protocol instead of a bare
    positional-tuple callable type, so Step 3 (browser-admin, richer return
    shape) can widen what this returns without a signature-shaped churn at
    every call site.

    ``None`` wired into :class:`BearerTokenMiddleware` means EasyAuth trust is
    OFF; a bound :meth:`EntraResolver.resolve_principal_id` means it is ON.
    The middleware only ever sees this Protocol — it stays ignorant of
    ``EntraResolver``'s concrete type (R5, doc 14 §9).
    """

    def __call__(self, oid: str) -> tuple[str, list[str], bool]:
        """Resolve *oid* to ``(contributor_id, roles, is_service)``.

        Raises :class:`AuthError` (401 or 403) on an invalid/unbound oid —
        see :meth:`EntraResolver.resolve_principal_id` for the concrete
        implementation and the full 401/403 row mapping.
        """
        ...


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
        easyauth_resolve: EasyAuthResolveFn | None = None,
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
        # EasyAuth browser-identity trust (doc 14 §3.1/R5): trust is expressed
        # by WIRING A CALLABLE, not a bool the middleware interprets. `None` =
        # off (static mode, or entra mode with trust_easyauth_principal=False);
        # a bound `EntraResolver.resolve_principal_id` = on. This keeps the
        # middleware ignorant of EntraResolver's concrete type and makes "off"
        # structurally unrepresentable outside entra mode (main.py only wires
        # this in the entra branch).
        self._easyauth_resolve: EasyAuthResolveFn | None = easyauth_resolve

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
        headers = scope.get("headers", [])
        token = _extract_bearer_token(headers)
        if token is None:
            # No Bearer token. Fall through to the EasyAuth browser-identity
            # source (doc 14 §3.1) if it has been wired ON (entra mode +
            # trust_easyauth_principal=True, see main.py wiring). `None` means
            # off — original behaviour (401) is unchanged.
            if self._easyauth_resolve is not None:
                try:
                    oid = _extract_easyauth_oid(headers)
                except AuthError as exc:
                    # TB-2: 2+ X-MS-CLIENT-PRINCIPAL-ID headers (smuggling anomaly).
                    _log.info(
                        "auth_event=auth_denied: %s (status=%d)",
                        exc.reason,
                        exc.status_code,
                    )
                    await _send_error(send, exc.status_code, exc.reason)
                    return
                if oid is None:
                    # row 1: no EasyAuth header at all — no credential presented.
                    # F5: log it (unlike the other 401s here, this path names no
                    # oid) so an operator can distinguish "EasyAuth stripped/ate
                    # the header" from "trust off / unrelated 401".
                    _log.info(
                        "auth_event=auth_denied: no bearer and no EasyAuth "
                        "principal header (trust on)"
                    )
                    await _send_401(send)
                    return
                try:
                    contributor_id, roles, is_service = self._easyauth_resolve(oid)
                except AuthError as exc:
                    # rows 2-5: empty / malformed / all-zeros / unbound.
                    _log.info(
                        "auth_event=auth_denied: %s (status=%d)",
                        exc.reason,
                        exc.status_code,
                    )
                    await _send_error(send, exc.status_code, exc.reason)
                    return
                # row 6: accept.
                state = scope.setdefault("state", {})
                state["contributor_id"] = contributor_id
                state["is_admin"] = False  # browser-path admin is Step 3
                state["roles"] = roles  # [] in the spike
                state["is_service"] = is_service  # False (browser = human-like)
                await self.app(scope, receive, send)
                return
            # Neither Bearer nor EasyAuth trust.
            await _send_401(send)
            return

        # C6 (BINDING) — co-presence anomaly log. If BOTH a Bearer token AND an
        # X-MS-CLIENT-PRINCIPAL-ID header are present, Bearer wins (below,
        # UNCHANGED existing logic) but the co-presence is logged as a
        # structured, greppable event — the fingerprint of a client injecting
        # the header directly rather than EasyAuth injecting it. This is a
        # tolerated benign double-send (not a hard rejection, R4/C6), but it
        # must be surfaced, not silently accepted.
        if self._easyauth_resolve is not None and easyauth_id_header_present(headers):
            # Non-raising presence probe: bearer already won, so a duplicate
            # EasyAuth header (TB-2) is moot here — do NOT raise, just surface
            # the co-presence anomaly.
            _log.warning("auth_event=easyauth_header_with_bearer")

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
        if self._admin_api_key_digest is not None and _is_admin_route(path):
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


def _header_value(headers: list[tuple[bytes, bytes]], name: bytes) -> str | None:
    """Case-insensitive ASGI header lookup, decoded latin-1 (mirrors ``_extract_bearer_token``).

    doc 14 §3.2 / C7: on a DUPLICATE header (the same header name sent more than
    once), this returns the FIRST match, matching iteration order of the ASGI
    ``headers`` list. This behaviour is deliberately defined (not left as an
    accident of dict-building) so a client sending two
    ``X-MS-CLIENT-PRINCIPAL-ID`` headers has a predictable, tested outcome.

    Returns ``None`` when the header is absent entirely; returns ``""`` when the
    header is present but empty (callers distinguish "absent" from "empty").
    """
    target = name.lower()
    for header_name, value in headers:
        if header_name.lower() == target:
            return value.decode("latin-1")
    return None


_EASYAUTH_ID_HEADER = b"x-ms-client-principal-id"


def _count_header(headers: list[tuple[bytes, bytes]], name: bytes) -> int:
    """Count case-insensitive occurrences of header *name* in the ASGI list."""
    target = name.lower()
    return sum(1 for header_name, _ in headers if header_name.lower() == target)


def easyauth_id_header_present(headers: list[tuple[bytes, bytes]]) -> bool:
    """True if at least one ``X-MS-CLIENT-PRINCIPAL-ID`` header is present.

    Non-raising presence probe used by the C6 co-presence anomaly log (where a
    Bearer token is present and wins regardless): it must NOT raise on a
    duplicate header, since bearer-wins short-circuits the EasyAuth path before
    the TB-2 duplicate check would ever matter.
    """
    return _count_header(headers, _EASYAUTH_ID_HEADER) >= 1


def _extract_easyauth_oid(headers: list[tuple[bytes, bytes]]) -> str | None:
    """Extract the EasyAuth-injected browser oid from ASGI headers.

    doc 14 §3.2 as amended by C2 (BINDING, supersedes the original base64
    ``X-MS-CLIENT-PRINCIPAL`` blob fallback): SCALAR-ONLY. Reads
    ``X-MS-CLIENT-PRINCIPAL-ID`` (the oid EasyAuth injects as a plain header)
    and returns it verbatim, or ``None`` if the header is absent entirely.

    **TB-2 hardening (adversarial review).** A legitimate EasyAuth edge injects
    EXACTLY ONE ``X-MS-CLIENT-PRINCIPAL-ID`` header and strips any inbound
    copies. Two or more occurrences is therefore an anomaly — the fingerprint of
    a caller smuggling a forged value ahead of (or behind) the real one. Rather
    than silently pick first-or-last (which would let the attacker choose which
    wins by ordering), this raises :class:`AuthError` (401) so the middleware
    rejects the whole request. This is scoped to the EasyAuth header ONLY;
    ``_header_value`` / ``_extract_bearer_token`` keep their first-match
    behaviour for all other headers.

    The base64 ``X-MS-CLIENT-PRINCIPAL`` claims-blob fallback described in the
    pre-council draft of this spec is intentionally NOT implemented here — C2
    deleted it (untested accept-path, duplicate-``typ``-key ambiguity, no size
    bound, and EasyAuth always sends the scalar header anyway). A
    ``X-MS-CLIENT-PRINCIPAL`` blob with no scalar ``-ID`` header present is
    therefore treated as "no EasyAuth identity" (returns ``None`` -> 401 at the
    caller), matching doc 14 §10 C2's redefinition of Test A case A7.

    Returns:
        The single header value (may be ``""`` —
        ``EntraResolver.resolve_principal_id`` rejects an empty string), or
        ``None`` if no EasyAuth identity is present at all.

    Raises:
        AuthError(401): two or more ``X-MS-CLIENT-PRINCIPAL-ID`` headers (TB-2).
    """
    count = _count_header(headers, _EASYAUTH_ID_HEADER)
    if count == 0:
        return None
    if count >= 2:
        raise AuthError(
            401,
            "Multiple X-MS-CLIENT-PRINCIPAL-ID headers present; the EasyAuth "
            "edge injects exactly one, so this is an anomaly (possible header "
            "smuggling) — refusing to pick one",
        )
    return _header_value(headers, _EASYAUTH_ID_HEADER)


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
