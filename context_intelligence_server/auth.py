"""Bearer token authentication middleware for the Context Intelligence Server."""

import hashlib
import json
from collections.abc import Callable, MutableMapping
from typing import Any

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


class BearerTokenMiddleware:
    """ASGI middleware that validates ``Authorization: Bearer <token>`` headers.

    If *keystore* is empty (no keys configured), all requests pass through without
    authentication (backward compatibility for un-authed local dev setups).

    On a successful match, the resolved contributor id is injected into the ASGI
    scope under ``scope["state"]["contributor_id"]`` so downstream handlers can
    read authenticated identity without re-hashing.

    Several paths are always exempt (see ``_EXEMPT_PATHS``) so health checks,
    monitoring tools, and public-facing pages continue working without credentials.
    """

    def __init__(
        self, app: Callable[..., Any], keystore: dict[str, str] | None = None
    ) -> None:
        self.app = app
        self.keystore: dict[str, str] = keystore if keystore is not None else {}

    async def __call__(
        self, scope: MutableMapping[str, Any], receive: Any, send: Any
    ) -> None:
        if scope.get("type") != "http" or not self.keystore:
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

        contributor_id = _resolve_token(token, self.keystore)
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
