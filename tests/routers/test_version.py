"""Tests for the GET /version endpoint."""

from __future__ import annotations

import httpx
import pytest

import context_intelligence_server.main as main_module
from context_intelligence_server.status import SCHEMA_VERSION, SERVER_VERSION


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

    @pytest.mark.anyio
    async def test_returns_schema_version_field(
        self, client: httpx.AsyncClient
    ) -> None:
        response = await client.get("/version")
        data = response.json()
        assert "schema_version" in data

    @pytest.mark.anyio
    async def test_schema_version_matches_constant(
        self, client: httpx.AsyncClient
    ) -> None:
        response = await client.get("/version")
        data = response.json()
        assert data["schema_version"] == SCHEMA_VERSION


class TestSchemaVersionConstant:
    """SCHEMA_VERSION is a plain integer baseline data point (§10.2)."""

    def test_schema_version_is_int(self) -> None:
        assert isinstance(SCHEMA_VERSION, int)

    def test_schema_version_initial_value(self) -> None:
        assert SCHEMA_VERSION == 1


class TestGetVersionUnchangedByW4:
    """W-4 added an advisory graph_schema_version signal to GET /status only.

    GET /version must stay exactly the cheap compiled-in-constant return it
    was before -- no graph read, no new fields -- per the deliberate
    read/write separation documented on ``ensure_schema_version_baseline``
    (neo4j_store.py) and ``read_graph_schema_version``'s docstring.
    """

    @pytest.mark.anyio
    async def test_response_shape_unchanged(self, client: httpx.AsyncClient) -> None:
        response = await client.get("/version")
        data = response.json()
        assert set(data.keys()) == {"version", "schema_version"}
        assert "graph_schema_version" not in data
        assert "schema_version_current" not in data

    @pytest.mark.anyio
    async def test_works_with_no_neo4j_driver_configured(
        self, client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """/version never touches app.state.neo4j_driver -- it must return
        200 even when no driver has been configured at all."""
        if hasattr(main_module.app.state, "neo4j_driver"):
            monkeypatch.delattr(main_module.app.state, "neo4j_driver", raising=False)

        response = await client.get("/version")
        assert response.status_code == 200
        assert response.json()["schema_version"] == SCHEMA_VERSION


class TestGetVersionNoAuth:
    """GET /version is accessible without credentials even when auth is enabled."""

    @pytest.mark.anyio
    async def test_returns_200_through_auth_middleware(
        self, auth_client: httpx.AsyncClient
    ) -> None:
        """GET /version returns 200 even when api_key is set and no Authorization is sent."""
        response = await auth_client.get("/version")
        assert response.status_code == 200


class TestVersionMatchesPyproject:
    """The version the server reports must equal the version declared in pyproject.toml.

    This is a *consistency* check, deliberately NOT a hardcoded constant: bumping the
    version in pyproject.toml never requires editing this test, and a stale/forgotten
    rebuild (where the installed package metadata drifts from pyproject) is caught.
    Combined with ``test_version_matches_server_version_constant`` above, this pins the
    full chain: ``GET /version`` == ``SERVER_VERSION`` == ``pyproject.toml [project].version``.
    """

    def test_served_version_matches_pyproject(self) -> None:
        import tomllib
        from pathlib import Path

        pyproject = Path(__file__).resolve().parents[2] / "pyproject.toml"
        declared = tomllib.loads(pyproject.read_text(encoding="utf-8"))["project"][
            "version"
        ]
        assert SERVER_VERSION == declared, (
            f"Version drift: pyproject.toml declares {declared!r} but the server reports "
            f"{SERVER_VERSION!r} (from importlib.metadata). Rebuild/reinstall the package "
            f"after a version bump so the two stay in sync."
        )
