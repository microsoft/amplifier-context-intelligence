"""Tests for the GET /version endpoint."""

from __future__ import annotations

import httpx
import pytest

from context_intelligence_server.dashboard import SERVER_VERSION


class TestGetVersion200:
    """GET /version returns 200 with a version payload."""

    @pytest.mark.anyio
    async def test_returns_200(self, client: httpx.AsyncClient) -> None:
        response = await client.get("/version")
        assert response.status_code == 200

    @pytest.mark.anyio
    async def test_returns_version_field(self, client: httpx.AsyncClient) -> None:
        response = await client.get("/version")
        data = response.json()
        assert "version" in data

    @pytest.mark.anyio
    async def test_version_is_nonempty_string(self, client: httpx.AsyncClient) -> None:
        response = await client.get("/version")
        data = response.json()
        assert isinstance(data["version"], str)
        assert len(data["version"]) > 0

    @pytest.mark.anyio
    async def test_version_matches_server_version_constant(
        self, client: httpx.AsyncClient
    ) -> None:
        response = await client.get("/version")
        data = response.json()
        assert data["version"] == SERVER_VERSION


class TestGetVersionNoAuth:
    """GET /version is accessible without credentials even when auth is enabled."""

    @pytest.mark.anyio
    async def test_returns_200_through_auth_middleware(
        self, auth_client: httpx.AsyncClient
    ) -> None:
        """GET /version returns 200 even when api_key is set and no Authorization is sent."""
        response = await auth_client.get("/version")
        assert response.status_code == 200


class TestVersionIs4_0_1:
    """pyproject.toml is the single source of truth and must declare version 4.0.1."""

    def test_pyproject_version_is_4_0_1(self) -> None:
        import tomllib
        from pathlib import Path

        pyproject = Path(__file__).resolve().parents[2] / "pyproject.toml"
        data = tomllib.loads(pyproject.read_text(encoding="utf-8"))
        assert data["project"]["version"] == "4.0.1"
