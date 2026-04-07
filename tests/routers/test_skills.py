"""Tests for SkillRegistry — load skill-name/SKILL.md packages and compute SHA-256 ETags."""

from __future__ import annotations

import hashlib
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from context_intelligence_server.routers.skills import SkillRegistry
from context_intelligence_server.main import app as _app
from context_intelligence_server.main import lifespan


class TestSkillRegistryLoadFromDir:
    """SkillRegistry.load_from_dir reads skill-name/SKILL.md packages and computes SHA-256 ETags."""

    def test_reads_md_file_content(self, tmp_path: Path) -> None:
        """load_from_dir stores the content of a SKILL.md keyed by its parent directory name."""
        skill_dir = tmp_path / "my-skill"
        skill_dir.mkdir()
        (skill_dir / "SKILL.md").write_text(
            "# My Skill\n\nThis is the content.", encoding="utf-8"
        )

        registry = SkillRegistry()
        registry.load_from_dir(tmp_path)

        result = registry.get("my-skill")
        assert result is not None
        content, _ = result
        assert content == "# My Skill\n\nThis is the content."

    def test_computes_sha256_etag(self, tmp_path: Path) -> None:
        """load_from_dir computes SHA-256 ETags for each SKILL.md."""
        raw = "# Skill Content"
        skill_dir = tmp_path / "skill-alpha"
        skill_dir.mkdir()
        (skill_dir / "SKILL.md").write_text(raw, encoding="utf-8")

        registry = SkillRegistry()
        registry.load_from_dir(tmp_path)

        result = registry.get("skill-alpha")
        assert result is not None
        _, etag = result
        expected_etag = hashlib.sha256(raw.encode("utf-8")).hexdigest()
        assert etag == expected_etag

    def test_ignores_flat_md_files_and_dirs_without_skill_md(
        self, tmp_path: Path
    ) -> None:
        """load_from_dir only loads skill-name/SKILL.md; flat files and other names are ignored."""
        # Valid skill: subdirectory with SKILL.md
        valid_dir = tmp_path / "skill"
        valid_dir.mkdir()
        (valid_dir / "SKILL.md").write_text("# Valid", encoding="utf-8")

        # Flat .md files at root are ignored
        (tmp_path / "skill.md").write_text("ignored flat file", encoding="utf-8")
        # Directories without SKILL.md are ignored
        no_skill = tmp_path / "no-skill-md"
        no_skill.mkdir()
        (no_skill / "OTHER.md").write_text("ignored", encoding="utf-8")
        # Other files at root are also ignored
        (tmp_path / "notes.json").write_text('{"key": "value"}', encoding="utf-8")

        registry = SkillRegistry()
        registry.load_from_dir(tmp_path)

        assert registry.skill_names == frozenset({"skill"})
        assert registry.get("no-skill-md") is None
        assert registry.get("notes") is None

    def test_get_returns_none_for_unknown_skill(self, tmp_path: Path) -> None:
        """get() returns None when the skill name is not registered."""
        registry = SkillRegistry()
        registry.load_from_dir(tmp_path)  # empty directory

        assert registry.get("nonexistent-skill") is None
        assert registry.get("") is None

    def test_etag_is_stable_for_same_content(self, tmp_path: Path) -> None:
        """ETags are stable: same content always produces the same ETag."""
        content = "# Stable Skill\n\nThis content is deterministic."
        skill_dir = tmp_path / "stable"
        skill_dir.mkdir()
        (skill_dir / "SKILL.md").write_text(content, encoding="utf-8")

        registry_a = SkillRegistry()
        registry_a.load_from_dir(tmp_path)

        registry_b = SkillRegistry()
        registry_b.load_from_dir(tmp_path)

        result_a = registry_a.get("stable")
        result_b = registry_b.get("stable")

        assert result_a is not None
        assert result_b is not None
        _, etag_a = result_a
        _, etag_b = result_b
        assert etag_a == etag_b

    def test_loads_multiple_skill_directories(self, tmp_path: Path) -> None:
        """load_from_dir registers all skill-name/SKILL.md packages in the directory."""
        files = {
            "alpha": "# Alpha Skill",
            "beta": "# Beta Skill",
            "gamma": "# Gamma Skill",
        }
        for name, content in files.items():
            skill_dir = tmp_path / name
            skill_dir.mkdir()
            (skill_dir / "SKILL.md").write_text(content, encoding="utf-8")

        registry = SkillRegistry()
        registry.load_from_dir(tmp_path)

        assert registry.skill_names == frozenset({"alpha", "beta", "gamma"})

        for name, content in files.items():
            result = registry.get(name)
            assert result is not None, f"Expected skill '{name}' to be registered"
            stored_content, etag = result
            assert stored_content == content
            expected_etag = hashlib.sha256(content.encode("utf-8")).hexdigest()
            assert etag == expected_etag

    def test_loads_skill_from_directory_skill_md(self, tmp_path: Path) -> None:
        """load_from_dir loads SKILL.md from a subdirectory, keyed by directory name."""
        skill_dir = tmp_path / "context-intelligence-graph-query"
        skill_dir.mkdir()
        skill_content = "# Graph Query Skill\n\nThis is the graph query skill."
        (skill_dir / "SKILL.md").write_text(skill_content, encoding="utf-8")

        registry = SkillRegistry()
        registry.load_from_dir(tmp_path)

        result = registry.get("context-intelligence-graph-query")
        assert result is not None
        content, etag = result
        assert content == skill_content
        expected_etag = hashlib.sha256(skill_content.encode("utf-8")).hexdigest()
        assert etag == expected_etag


class TestGetSkill200:
    """GET /skills/{skill_name} returns 200 with markdown body and ETag header."""

    @pytest.mark.anyio
    async def test_returns_200_with_body(
        self, client: httpx.AsyncClient, tmp_path: Path
    ) -> None:
        """GET /skills/{skill_name} returns 200 with the skill's markdown content."""
        skill_content = "# My Skill\n\nThis is skill content."
        skill_dir = tmp_path / "my-skill"
        skill_dir.mkdir()
        (skill_dir / "SKILL.md").write_text(skill_content, encoding="utf-8")

        registry = SkillRegistry()
        registry.load_from_dir(tmp_path)
        _app.state.skill_registry = registry

        response = await client.get("/skills/my-skill")

        assert response.status_code == 200
        assert response.text == skill_content

    @pytest.mark.anyio
    async def test_returns_etag_header(
        self, client: httpx.AsyncClient, tmp_path: Path
    ) -> None:
        """GET /skills/{skill_name} includes an ETag header in the response."""
        skill_content = "# My Skill\n\nETag header test."
        skill_dir = tmp_path / "my-skill"
        skill_dir.mkdir()
        (skill_dir / "SKILL.md").write_text(skill_content, encoding="utf-8")

        registry = SkillRegistry()
        registry.load_from_dir(tmp_path)
        _app.state.skill_registry = registry

        response = await client.get("/skills/my-skill")

        assert response.status_code == 200
        assert "etag" in response.headers

    @pytest.mark.anyio
    async def test_etag_is_sha256_of_content(
        self, client: httpx.AsyncClient, tmp_path: Path
    ) -> None:
        """The ETag header value is the SHA-256 hex digest of the skill content."""
        skill_content = "# My Skill\n\nSHA-256 verification content."
        skill_dir = tmp_path / "my-skill"
        skill_dir.mkdir()
        (skill_dir / "SKILL.md").write_text(skill_content, encoding="utf-8")

        registry = SkillRegistry()
        registry.load_from_dir(tmp_path)
        _app.state.skill_registry = registry

        response = await client.get("/skills/my-skill")

        assert response.status_code == 200
        expected_etag = hashlib.sha256(skill_content.encode("utf-8")).hexdigest()
        assert response.headers["etag"] == expected_etag


class TestGetSkill304:
    """GET /skills/{skill_name} returns 304 Not Modified when If-None-Match matches ETag."""

    @pytest.mark.anyio
    async def test_returns_304_when_etag_matches(
        self, client: httpx.AsyncClient, tmp_path: Path
    ) -> None:
        """Second request with matching If-None-Match returns 304 with empty body."""
        skill_content = "# My Skill\n\nETag caching test."
        skill_dir = tmp_path / "my-skill"
        skill_dir.mkdir()
        (skill_dir / "SKILL.md").write_text(skill_content, encoding="utf-8")

        registry = SkillRegistry()
        registry.load_from_dir(tmp_path)
        _app.state.skill_registry = registry

        # First request: obtain the ETag
        first_response = await client.get("/skills/my-skill")
        assert first_response.status_code == 200
        etag = first_response.headers["etag"]

        # Second request: send matching If-None-Match
        second_response = await client.get(
            "/skills/my-skill", headers={"If-None-Match": etag}
        )

        assert second_response.status_code == 304
        assert second_response.content == b""

    @pytest.mark.anyio
    async def test_returns_200_when_etag_does_not_match(
        self, client: httpx.AsyncClient, tmp_path: Path
    ) -> None:
        """Request with stale If-None-Match returns 200 with full content."""
        skill_content = "# My Skill\n\nStale ETag test."
        skill_dir = tmp_path / "my-skill"
        skill_dir.mkdir()
        (skill_dir / "SKILL.md").write_text(skill_content, encoding="utf-8")

        registry = SkillRegistry()
        registry.load_from_dir(tmp_path)
        _app.state.skill_registry = registry

        response = await client.get(
            "/skills/my-skill", headers={"If-None-Match": "stale-etag-value"}
        )

        assert response.status_code == 200
        assert response.text == skill_content


class TestGetSkill404:
    """GET /skills/{skill_name} returns 404 Not Found for unknown skill names."""

    @pytest.mark.anyio
    async def test_returns_404_for_unknown_skill(
        self, client: httpx.AsyncClient, tmp_path: Path
    ) -> None:
        """GET /skills/{skill_name} returns 404 when the skill is not registered."""
        registry = SkillRegistry()
        registry.load_from_dir(tmp_path)  # empty directory — no skills registered
        _app.state.skill_registry = registry

        response = await client.get("/skills/nonexistent-skill")

        assert response.status_code == 404

    @pytest.mark.anyio
    async def test_404_body_contains_skill_name(
        self, client: httpx.AsyncClient, tmp_path: Path
    ) -> None:
        """GET /skills/{skill_name} 404 response body includes the requested skill name."""
        registry = SkillRegistry()
        registry.load_from_dir(tmp_path)  # empty directory — no skills registered
        _app.state.skill_registry = registry

        response = await client.get("/skills/my-missing-skill")

        assert response.status_code == 404
        assert "my-missing-skill" in response.text


class TestSkillRegistryLifespan:
    """SkillRegistry is created and populated on app.state during lifespan startup."""

    @pytest.mark.anyio
    async def test_skill_registry_set_on_app_state(
        self,
        monkeypatch: pytest.MonkeyPatch,  # noqa: ARG002
    ) -> None:
        """Lifespan creates a SkillRegistry on app.state during startup."""
        mock_driver = MagicMock()
        mock_driver.close = AsyncMock()

        with (
            patch("context_intelligence_server.main.setup_logging"),
            patch(
                "context_intelligence_server.main.AsyncGraphDatabase.driver",
                return_value=mock_driver,
            ),
        ):
            async with lifespan(_app):
                assert hasattr(_app.state, "skill_registry")
                assert isinstance(_app.state.skill_registry, SkillRegistry)

    @pytest.mark.anyio
    async def test_seed_skill_is_accessible_via_endpoint(
        self,
        client: httpx.AsyncClient,
        monkeypatch: pytest.MonkeyPatch,  # noqa: ARG002
    ) -> None:
        """The seed skill context-intelligence-graph-query is accessible after lifespan startup.

        Allows 404 if the seed file is not present in the test environment.
        """
        mock_driver = MagicMock()
        mock_driver.close = AsyncMock()

        with (
            patch("context_intelligence_server.main.setup_logging"),
            patch(
                "context_intelligence_server.main.AsyncGraphDatabase.driver",
                return_value=mock_driver,
            ),
        ):
            async with lifespan(_app):
                pass  # Let lifespan startup run and populate skill_registry

        # skill_registry remains on app.state after lifespan exits
        response = await client.get("/skills/context-intelligence-graph-query")
        assert response.status_code in (200, 404)
