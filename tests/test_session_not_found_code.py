"""A 404 must say WHICH kind of 404 it is.

The delete flow attests absence -- it tells a user "your data is not on this
server". A bare 404 cannot support that claim: Starlette answers an unmatched
route with ``{"detail": "Not Found"}``, and any proxy or gateway in front of the
server can answer 404 for reasons of its own. A request that never reached the
handler is indistinguishable from one that did and found nothing.

So the handlers emit a machine-readable ``session_not_found`` code, and these
tests pin BOTH sides of the distinction: the semantic 404 carries the code, and
a router 404 must never look like it does.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from context_intelligence_server.routers.deletion import SESSION_NOT_FOUND_CODE

pytestmark = pytest.mark.anyio


class TestSemanticNotFoundIsDistinguishable:
    async def test_summary_missing_session_carries_the_code(self, client: Any) -> None:
        from context_intelligence_server.main import app
        from context_intelligence_server.routers.deletion import read_deletion_service

        svc = AsyncMock()
        svc.preview = AsyncMock(return_value=None)  # looked, not here
        app.dependency_overrides[read_deletion_service] = lambda: svc
        try:
            resp = await client.get("/sessions/missing-id/summary")
        finally:
            app.dependency_overrides.clear()

        assert resp.status_code == 404
        detail = resp.json()["detail"]
        assert isinstance(detail, dict), (
            "semantic 404 must be structured, not a bare string"
        )
        assert detail["code"] == SESSION_NOT_FOUND_CODE
        assert detail["session_id"] == "missing-id"

    async def test_delete_missing_session_carries_the_code(self, client: Any) -> None:
        from context_intelligence_server.main import app
        from context_intelligence_server.routers.deletion import delete_route_service

        svc = AsyncMock()
        svc.apply = AsyncMock(return_value=None)  # looked, not here
        app.dependency_overrides[delete_route_service] = lambda: svc
        try:
            resp = await client.delete("/sessions/missing-id")
        finally:
            app.dependency_overrides.clear()

        assert resp.status_code == 404
        detail = resp.json()["detail"]
        assert isinstance(detail, dict)
        assert detail["code"] == SESSION_NOT_FOUND_CODE

    async def test_router_404_does_NOT_look_like_semantic_absence(
        self, client: Any
    ) -> None:
        """The load-bearing negative: an unmatched route must stay distinguishable.

        This is the case that made the old behaviour unsafe -- a server without
        the deletion routes answers here, and the caller must not read it as
        "your data is gone".
        """
        resp = await client.get("/sessions/some-id/this-route-does-not-exist")
        assert resp.status_code == 404
        detail = resp.json()["detail"]
        # A plain string, not a dict -- and crucially no session_not_found code.
        assert not (
            isinstance(detail, dict) and detail.get("code") == SESSION_NOT_FOUND_CODE
        )

    async def test_a_server_without_the_deletion_routes_cannot_forge_the_code(
        self, client: Any
    ) -> None:
        """Same shape as an OLD server: the path simply is not mounted."""
        resp = await client.get("/sessions/some-id/summary/not-a-real-suffix")
        assert resp.status_code == 404
        detail = resp.json()["detail"]
        assert not (
            isinstance(detail, dict) and detail.get("code") == SESSION_NOT_FOUND_CODE
        )
