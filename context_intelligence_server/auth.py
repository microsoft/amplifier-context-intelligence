"""Bearer token authentication middleware for the Context Intelligence Server."""

import hashlib
import json
from collections.abc import Callable, MutableMapping
from typing import Any, runtime_checkable

from typing_extensions import Protocol

# Paths that are exempt from authentication (health checks, monitoring, dashboard pages).
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

# Path prefixes that are exempt from authentication (static assets).
_EXEMPT_PREFIXES: tuple[str, ...] = ("/static/", "/skills/")


def _resolve_token(token: str, keystore: dict[str, str]) -> str | None:
    """Return the contributor id for *token*, or ``None`` if not found.

    Hashes the bearer token (UTF-8 bytes, sha256) and does a plain dict lookup
    against *keystore* (which stores ``{sha256_hex -> contributor_id}``).  Returns
    ``None`` — never ``"unknown"`` — on a miss so callers fail-closed on absence.
    """
    digest = hashlib.sha256(token.encode()).hexdigest()
    return keystore.get(digest)


@runtime_checkable
class PrincipalResolver(Protocol):
    """Resolves a raw bearer token string to a contributor id.

    Returns the contributor id string on success, or ``None`` when the token is
    not recognised (caller should respond 401).  Implementations may also raise
    to signal specific auth failures (e.g. an expired JWT); the middleware
    catches those and maps them to 401 responses.

    Only one concrete implementation exists today: :class:`StaticKeyResolver`.
    The ``EntraResolver`` (JWT via JWKS) is added in T4.  Do NOT add a third
    resolver without a separate design review.
    """

    def resolve(self, token: str) -> str | None:
        """Return contributor id or ``None`` if the token is not recognised."""
        ...


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
    def is_empty(self) -> bool:
        """True when no keys are configured (auth is effectively disabled)."""
        return not self._keystore

    def resolve(self, token: str) -> str | None:
        """Return contributor id for *token*, or ``None`` on a miss."""
        return _resolve_token(token, self._keystore)


class BearerTokenMiddleware:
    """ASGI middleware that validates ``Authorization: Bearer <token>`` headers.

    Accepts a :class:`PrincipalResolver` via the *resolver* keyword argument
    (preferred — used by :func:`~context_intelligence_server.main.create_asgi_app`),
    or a raw *keystore* dict for backward compatibility with tests that
    construct the middleware directly.

    When built with a :class:`StaticKeyResolver` whose keystore is empty (no
    keys configured), all requests pass through without authentication —
    backward-compatible behaviour for un-authed local dev setups.

    On a successful match, the resolved contributor id is injected into the
    ASGI scope under ``scope["state"]["contributor_id"]`` so downstream
    handlers can read authenticated identity without re-resolving.

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

    async def __call__(
        self, scope: MutableMapping[str, Any], receive: Any, send: Any
    ) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        # Backward-compat fail-open: a StaticKeyResolver with no keys means
        # auth is disabled — every request passes through unauthenticated.
        # This preserves the pre-T2 behaviour of ``not self.keystore``.
        if isinstance(self.resolver, StaticKeyResolver) and self.resolver.is_empty:
            await self.app(scope, receive, send)
            return

        path: str = scope.get("path", "")
        if path in _EXEMPT_PATHS or any(path.startswith(p) for p in _EXEMPT_PREFIXES):
            await self.app(scope, receive, send)
            return

        # Extract bearer token from headers.
        token = _extract_bearer_token(scope.get("headers", []))
        if token is None:
            await _send_401(send)
            return

        contributor_id = self.resolver.resolve(token)
        if contributor_id is None:
            await _send_401(send)
            return

        # Inject authenticated identity into scope state so post_events can read it.
        scope.setdefault("state", {})["contributor_id"] = contributor_id

        await self.app(scope, receive, send)


def _extract_bearer_token(headers: list[tuple[bytes, bytes]]) -> str | None:
    """Extract the bearer token from ASGI headers."""
    for name, value in headers:
        if name.lower() == b"authorization":
            decoded = value.decode("latin-1")
            if decoded.startswith("Bearer "):
                return decoded[7:]
    return None


async def _send_401(send: Any) -> None:
    """Send a 401 Unauthorized JSON response."""
    body = json.dumps({"detail": "Missing or invalid bearer token"}).encode()
    await send(
        {
            "type": "http.response.start",
            "status": 401,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(body)).encode()),
            ],
        }
    )
    await send(
        {
            "type": "http.response.body",
            "body": body,
        }
    )
