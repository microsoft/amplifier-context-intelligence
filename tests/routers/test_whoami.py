"""Tests for the GET /whoami endpoint.

Mirrors the style of ``tests/routers/test_deletion.py``: the ``auth_client``
fixture (auth middleware applied, a test static key mapped to contributor id
``"owner"``) proves the authenticated case, and the plain ``client`` fixture
(no auth middleware, ``allow_unauthenticated`` in effect for the test suite)
proves the anonymous/no-auth case. See ``tests/conftest.py`` for both
fixtures.
"""

from __future__ import annotations

import httpx
import pytest


class TestGetWhoamiAuthenticated:
    """GET /whoami returns the caller's contributor id for a bearer-token request."""

    @pytest.mark.anyio
    async def test_returns_200(self, auth_client: httpx.AsyncClient) -> None:
        response = await auth_client.get(
            "/whoami", headers={"Authorization": "Bearer test-secret"}
        )
        assert response.status_code == 200

    @pytest.mark.anyio
    async def test_returns_the_authenticated_contributor_id(
        self, auth_client: httpx.AsyncClient
    ) -> None:
        response = await auth_client.get(
            "/whoami", headers={"Authorization": "Bearer test-secret"}
        )
        assert response.json() == {"contributor_id": "owner"}

    @pytest.mark.anyio
    async def test_rejects_missing_bearer_token(
        self, auth_client: httpx.AsyncClient
    ) -> None:
        """Auth is enabled on this client -- no Authorization header is a 401,
        not an anonymous whoami."""
        response = await auth_client.get("/whoami")
        assert response.status_code == 401


class TestGetWhoamiNoAuth:
    """GET /whoami when auth is disabled (allow_unauthenticated) reports a
    null/anonymous identity rather than erroring."""

    @pytest.mark.anyio
    async def test_returns_200(self, client: httpx.AsyncClient) -> None:
        response = await client.get("/whoami")
        assert response.status_code == 200

    @pytest.mark.anyio
    async def test_returns_null_contributor_id(self, client: httpx.AsyncClient) -> None:
        response = await client.get("/whoami")
        assert response.json() == {"contributor_id": None}
