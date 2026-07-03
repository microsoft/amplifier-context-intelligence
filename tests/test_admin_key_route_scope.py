"""The admin-key fast-path is scoped to /admin/* routes only.

The admin key is an ADMINISTRATION credential (it gates /admin/*), not a
data-ingestion identity. On data routes (e.g. POST /events) it must NOT
short-circuit auth:
  - a token that is ALSO a registered data key resolves to its real contributor
    id (created_by reflects that id, never a synthetic "admin"); and
  - a bare admin key that is not a data key is rejected (401) rather than
    posting events attributed to "admin".
On /admin/* the fast-path still applies so an admin-key bearer is is_admin=True.

These drive the ASGI middleware directly (mirrors tests/test_auth.py).
"""

import hashlib
from unittest.mock import AsyncMock

from context_intelligence_server.auth import BearerTokenMiddleware

ADMIN_TOKEN = "scope-test-admin-key"
ADMIN_DIGEST = hashlib.sha256(ADMIN_TOKEN.encode()).hexdigest()

SHARED_TOKEN = "scope-test-shared-key"  # both admin key AND a registered data key
SHARED_DIGEST = hashlib.sha256(SHARED_TOKEN.encode()).hexdigest()


def _scope(token: str, path: str, method: str = "POST") -> dict:
    return {
        "type": "http",
        "path": path,
        "method": method,
        "headers": [(b"authorization", f"Bearer {token}".encode())],
    }


def _keystore(token: str, contributor_id: str) -> dict[str, str]:
    return {hashlib.sha256(token.encode()).hexdigest(): contributor_id}


async def test_bare_admin_key_is_rejected_on_data_route() -> None:
    """A bare admin key (not in the keystore) gets 401 on POST /events — it
    cannot post events attributed to a synthetic "admin"."""
    app = AsyncMock()
    middleware = BearerTokenMiddleware(
        app,
        keystore=_keystore("some-other-data-key", "colombod"),
        admin_api_key_digest=ADMIN_DIGEST,
    )
    send = AsyncMock()

    await middleware(_scope(ADMIN_TOKEN, "/events"), AsyncMock(), send)

    app.assert_not_called()
    assert send.call_args_list[0][0][0]["status"] == 401


async def test_admin_key_authenticates_on_admin_route() -> None:
    """On /admin/* the fast-path applies: admin-key bearer is authenticated
    with is_admin=True (so require_admin passes)."""
    app = AsyncMock()
    middleware = BearerTokenMiddleware(
        app,
        keystore=_keystore("some-other-data-key", "colombod"),
        admin_api_key_digest=ADMIN_DIGEST,
    )

    scope = _scope(ADMIN_TOKEN, "/admin/keys", method="PUT")
    await middleware(scope, AsyncMock(), AsyncMock())

    app.assert_called_once()
    assert scope["state"]["is_admin"] is True
    assert scope["state"]["contributor_id"] == "admin"


async def test_dual_role_key_attributes_to_keystore_id_on_data_route() -> None:
    """When ONE secret is both the admin key and a registered data key, a data
    POST resolves to the real keystore id (colombod), NOT "admin"."""
    app = AsyncMock()
    middleware = BearerTokenMiddleware(
        app,
        keystore=_keystore(SHARED_TOKEN, "colombod"),
        admin_api_key_digest=SHARED_DIGEST,
    )

    scope = _scope(SHARED_TOKEN, "/events")
    await middleware(scope, AsyncMock(), AsyncMock())

    app.assert_called_once()
    assert scope["state"]["contributor_id"] == "colombod"
    assert scope["state"]["is_admin"] is False


async def test_dual_role_key_is_admin_on_admin_route() -> None:
    """The same dual-role secret still gets is_admin=True on /admin/* (the
    fast-path wins there, which is what require_admin needs)."""
    app = AsyncMock()
    middleware = BearerTokenMiddleware(
        app,
        keystore=_keystore(SHARED_TOKEN, "colombod"),
        admin_api_key_digest=SHARED_DIGEST,
    )

    scope = _scope(SHARED_TOKEN, "/admin/keys", method="PUT")
    await middleware(scope, AsyncMock(), AsyncMock())

    app.assert_called_once()
    assert scope["state"]["is_admin"] is True
    assert scope["state"]["contributor_id"] == "admin"


async def test_admin_route_prefix_not_confused_by_lookalike_path() -> None:
    """A data path that merely starts with the letters 'admin' but is not the
    /admin router (e.g. /administrate) is NOT treated as an admin route."""
    app = AsyncMock()
    middleware = BearerTokenMiddleware(
        app,
        keystore=_keystore("some-other-data-key", "colombod"),
        admin_api_key_digest=ADMIN_DIGEST,
    )
    send = AsyncMock()

    # Bare admin key on a non-admin lookalike path -> falls through -> 401.
    await middleware(_scope(ADMIN_TOKEN, "/administrate"), AsyncMock(), send)

    app.assert_not_called()
    assert send.call_args_list[0][0][0]["status"] == 401
