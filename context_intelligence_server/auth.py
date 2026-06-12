"""Bearer token authentication middleware for the Context Intelligence Server."""

import hmac
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
        "/queues",
        "/docs",
        "/openapi.json",
    }
)

# Path prefixes that are exempt from authentication (static assets).
_EXEMPT_PREFIXES: tuple[str, ...] = ("/static/", "/skills/")


class BearerTokenMiddleware:
    """ASGI middleware that validates ``Authorization: Bearer <token>`` headers.

    If *api_key* is ``None``, all requests pass through without authentication
    (backward compatibility for un-authed local dev setups).

    Several paths are always exempt (see ``_EXEMPT_PATHS``) so health checks,
    monitoring tools, and public-facing pages continue working without credentials.
    """

    def __init__(self, app: Callable[..., Any], api_key: str | None = None) -> None:
        self.app = app
        self.api_key = api_key

    async def __call__(
        self, scope: MutableMapping[str, Any], receive: Any, send: Any
    ) -> None:
        if scope.get("type") != "http" or self.api_key is None:
            await self.app(scope, receive, send)
            return

        path: str = scope.get("path", "")
        if path in _EXEMPT_PATHS or any(path.startswith(p) for p in _EXEMPT_PREFIXES):
            await self.app(scope, receive, send)
            return

        # Extract bearer token from headers — use constant-time comparison to
        # prevent timing attacks on the credential value.
        token = _extract_bearer_token(scope.get("headers", []))
        if token is None or not hmac.compare_digest(token, self.api_key):
            await _send_401(send)
            return

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
