"""Tests for PROVIDERS dict, _get_available_providers(), ROUTING_ROLES,
_model_suffix(), _build_provider_instances(), and _write_routing_matrix() functions.

TDD: These tests were written BEFORE the implementation.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from intelligence_service.amplifier_intelligence_runtime import (
    PROVIDERS,
    ROUTING_ROLES,
    _build_matrix_dict,
    _build_provider_instances,
    _get_available_providers,
    _model_suffix,
    _write_routing_matrix,
)


class TestProvidersDict:
    """Tests for the PROVIDERS constant dict."""

    def test_has_five_providers(self) -> None:
        """PROVIDERS dict must contain exactly 5 provider entries."""
        assert len(PROVIDERS) == 5

    def test_each_provider_has_required_keys(self) -> None:
        """Every provider entry must have env_var, module, and source keys."""
        required_keys = {"env_var", "module", "source"}
        for name, info in PROVIDERS.items():
            assert required_keys == set(info.keys()), (
                f"Provider '{name}' missing required keys. "
                f"Expected {required_keys}, got {set(info.keys())}"
            )

    def test_known_providers_present(self) -> None:
        """All five expected provider short names must be present."""
        expected = {"anthropic", "gemini", "openai", "azure-openai", "github-copilot"}
        assert expected == set(PROVIDERS.keys())


class TestGetAvailableProviders:
    """Tests for the _get_available_providers() pure function."""

    def test_returns_set_of_available_providers(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Returns a set containing names of providers whose env var is set and non-empty."""
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test-key")
        # Ensure other provider env vars are absent
        for name, info in PROVIDERS.items():
            if name != "anthropic":
                monkeypatch.delenv(info["env_var"], raising=False)

        result = _get_available_providers()

        assert isinstance(result, set)
        assert "anthropic" in result
        # Others should not be present
        assert "gemini" not in result
        assert "openai" not in result

    def test_empty_string_env_var_is_not_available(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An env var set to empty string must NOT count as available."""
        monkeypatch.setenv("ANTHROPIC_API_KEY", "")
        for name, info in PROVIDERS.items():
            if name != "anthropic":
                monkeypatch.delenv(info["env_var"], raising=False)

        result = _get_available_providers()

        assert "anthropic" not in result

    def test_all_providers_available(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """When all env vars are set and non-empty, all 5 providers are returned."""
        env_map = {
            "ANTHROPIC_API_KEY": "sk-anthropic",
            "GOOGLE_API_KEY": "aig-gemini",
            "OPENAI_API_KEY": "sk-openai",
            "AZURE_OPENAI_API_KEY": "az-key",
            "GITHUB_TOKEN": "ghp-token",
        }
        for var, val in env_map.items():
            monkeypatch.setenv(var, val)

        result = _get_available_providers()

        assert result == {
            "anthropic",
            "gemini",
            "openai",
            "azure-openai",
            "github-copilot",
        }

    def test_no_providers_available(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """When no env vars are set, the result is an empty set."""
        for info in PROVIDERS.values():
            monkeypatch.delenv(info["env_var"], raising=False)

        result = _get_available_providers()

        assert result == set()


class TestRoutingRoles:
    """Tests for the ROUTING_ROLES constant dict."""

    EXPECTED_ROLES = {
        "general",
        "fast",
        "coding",
        "reasoning",
        "critique",
        "creative",
        "writing",
        "research",
        "vision",
        "image-gen",
        "critical-ops",
    }

    def test_has_expected_roles(self) -> None:
        """ROUTING_ROLES must contain exactly 11 roles with the correct names."""
        assert set(ROUTING_ROLES.keys()) == self.EXPECTED_ROLES
        assert len(ROUTING_ROLES) == 11

    def test_each_role_has_nonempty_candidate_list(self) -> None:
        """Every role must map to a non-empty list of candidate dicts."""
        for role, candidates in ROUTING_ROLES.items():
            assert isinstance(candidates, list), (
                f"Role '{role}' candidates must be a list, got {type(candidates)}"
            )
            assert len(candidates) > 0, (
                f"Role '{role}' must have at least one candidate"
            )

    def test_each_candidate_has_required_keys(self) -> None:
        """Every candidate dict must have provider, model, and default_model keys."""
        required_keys = {"provider", "model", "default_model"}
        for role, candidates in ROUTING_ROLES.items():
            for i, candidate in enumerate(candidates):
                missing = required_keys - set(candidate.keys())
                assert not missing, (
                    f"Role '{role}' candidate {i} missing keys: {missing}. "
                    f"Got keys: {set(candidate.keys())}"
                )

    def test_all_candidate_providers_exist_in_providers_dict(self) -> None:
        """Every candidate's provider must be a key in the PROVIDERS dict."""
        for role, candidates in ROUTING_ROLES.items():
            for candidate in candidates:
                provider = candidate["provider"]
                assert provider in PROVIDERS, (
                    f"Role '{role}' candidate provider '{provider}' "
                    f"not found in PROVIDERS dict. "
                    f"Known providers: {set(PROVIDERS.keys())}"
                )


class TestModelSuffix:
    """Tests for the _model_suffix() helper function."""

    @pytest.mark.parametrize(
        "model,expected",
        [
            ("claude-sonnet-4-5", "sonnet"),
            ("claude-haiku-4-5", "haiku"),
            ("gemini-2.5-flash", "flash"),
            (
                "gemini-2.0-flash-preview-image-generation",
                "flash-preview-image-generation",
            ),
            # Fallback path: no pure-alpha segment found after stripping prefix;
            # the stripped model string is returned as-is.
            ("gpt-4o", "4o"),
        ],
    )
    def test_extracts_correct_suffix(self, model: str, expected: str) -> None:
        """_model_suffix returns the expected suffix for known model names."""
        assert _model_suffix(model) == expected, (
            f"_model_suffix({model!r}) expected {expected!r}, "
            f"got {_model_suffix(model)!r}"
        )


class TestBuildProviderInstances:
    """Tests for the _build_provider_instances() function."""

    def test_anthropic_and_gemini_available(self) -> None:
        """When anthropic and gemini are available, both appear in instances."""
        available = {"anthropic", "gemini"}
        instances = _build_provider_instances(available)

        modules = {inst["module"] for inst in instances}
        assert "provider-anthropic" in modules
        assert "provider-gemini" in modules

    def test_filters_to_available_only(self) -> None:
        """Only providers in the available set appear in the returned instances."""
        available = {"anthropic"}
        instances = _build_provider_instances(available)

        assert len(instances) > 0, "Should have at least one anthropic instance"
        for inst in instances:
            assert inst["module"] == "provider-anthropic", (
                f"Expected only 'provider-anthropic' but got '{inst['module']}'"
            )

    def test_deduplicates_provider_model_pairs(self) -> None:
        """The same (provider, default_model) pair across multiple roles produces one instance."""
        available = {"anthropic", "gemini"}
        instances = _build_provider_instances(available)

        seen: set[tuple[str, str]] = set()
        for inst in instances:
            key = (inst["module"], inst["config"]["default_model"])
            assert key not in seen, (
                f"Duplicate instance found for (module={inst['module']}, "
                f"default_model={inst['config']['default_model']})"
            )
            seen.add(key)

    def test_instance_has_required_keys(self) -> None:
        """Each returned instance dict must have module, instance_id, source, and config keys."""
        available = {"anthropic"}
        instances = _build_provider_instances(available)

        assert len(instances) > 0, "Need at least one instance to test keys"
        for inst in instances:
            assert "module" in inst, f"Missing 'module' key in: {inst}"
            assert "instance_id" in inst, f"Missing 'instance_id' key in: {inst}"
            assert "source" in inst, f"Missing 'source' key in: {inst}"
            assert "config" in inst, f"Missing 'config' key in: {inst}"
            assert "default_model" in inst["config"], (
                f"Missing 'default_model' in config for: {inst}"
            )

    def test_empty_available_returns_empty_list(self) -> None:
        """An empty available set must return an empty list."""
        result = _build_provider_instances(set())
        assert result == []

    def test_instance_id_format(self) -> None:
        """instance_id must be '{provider}-{suffix}', lowercase, with no spaces."""
        available = {"anthropic", "gemini"}
        instances = _build_provider_instances(available)

        known_providers = set(PROVIDERS.keys())
        for inst in instances:
            instance_id = inst["instance_id"]
            # Must be lowercase
            assert instance_id == instance_id.lower(), (
                f"instance_id '{instance_id}' is not lowercase"
            )
            # Must have no spaces
            assert " " not in instance_id, (
                f"instance_id '{instance_id}' contains spaces"
            )
            # Must start with a known provider name followed by a dash
            starts_with_known = any(
                instance_id.startswith(p + "-") for p in known_providers
            )
            assert starts_with_known, (
                f"instance_id '{instance_id}' does not start with a known provider name. "
                f"Known providers: {known_providers}"
            )
            # Must have something after the provider prefix
            for p in known_providers:
                if instance_id.startswith(p + "-"):
                    suffix = instance_id[len(p) + 1 :]
                    assert suffix, (
                        f"instance_id '{instance_id}' has empty suffix after provider '{p}'"
                    )
                    break


class TestBuildMatrixDict:
    """Tests for the _build_matrix_dict() function."""

    def test_has_top_level_keys(self) -> None:
        """Result must have name, description, updated, and roles keys with correct values."""
        result = _build_matrix_dict({"anthropic"})
        assert set(result.keys()) == {"name", "description", "updated", "roles"}
        assert result["name"] == "intelligence-service"
        assert (
            result["description"]
            == "Auto-generated routing matrix for the intelligence service."
        )
        assert isinstance(result["updated"], str)
        assert isinstance(result["roles"], dict)

    def test_roles_filtered_to_available_providers(self) -> None:
        """Roles whose candidates are all from unavailable providers are omitted."""
        # Only gemini available — roles with only anthropic candidates must be absent
        result = _build_matrix_dict({"gemini"})
        roles = result["roles"]
        # "general" role has only anthropic → must be omitted
        assert "general" not in roles
        # "image-gen" has only gemini → must be present
        assert "image-gen" in roles

    def test_candidates_filtered_within_role(self) -> None:
        """Within a role that has multi-provider candidates, only available ones remain."""
        # Only anthropic available; "fast" has [gemini, anthropic]
        result = _build_matrix_dict({"anthropic"})
        fast_candidates = result["roles"]["fast"]["candidates"]
        assert len(fast_candidates) == 1
        assert fast_candidates[0]["provider"] == "anthropic"

    def test_candidate_has_provider_and_model(self) -> None:
        """Every candidate dict in the result must have provider and model keys."""
        result = _build_matrix_dict({"anthropic", "gemini"})
        for _role, role_data in result["roles"].items():
            for candidate in role_data["candidates"]:
                assert "provider" in candidate, (
                    f"candidate missing 'provider': {candidate}"
                )
                assert "model" in candidate, f"candidate missing 'model': {candidate}"

    def test_candidate_config_included_when_present(self) -> None:
        """Config dict is included in candidate when present in ROUTING_ROLES entry."""
        result = _build_matrix_dict({"anthropic"})
        # "reasoning" role has config: {reasoning_effort: high}
        reasoning_candidates = result["roles"]["reasoning"]["candidates"]
        assert len(reasoning_candidates) == 1
        assert "config" in reasoning_candidates[0], (
            "Expected 'config' key in reasoning candidate"
        )
        assert reasoning_candidates[0]["config"] == {"reasoning_effort": "high"}

    def test_candidate_config_absent_when_not_specified(self) -> None:
        """Config key is absent from candidate when not present in ROUTING_ROLES entry."""
        result = _build_matrix_dict({"anthropic"})
        # "general" role has no config in ROUTING_ROLES
        general_candidates = result["roles"]["general"]["candidates"]
        assert len(general_candidates) == 1
        assert "config" not in general_candidates[0], (
            f"Unexpected 'config' key in general candidate: {general_candidates[0]}"
        )

    def test_both_providers_full_matrix(self) -> None:
        """When both anthropic and gemini are available, all 11 roles appear."""
        result = _build_matrix_dict({"anthropic", "gemini"})
        roles = result["roles"]
        assert len(roles) == 11
        # "fast" role has gemini first, then anthropic — order must be preserved
        fast_candidates = roles["fast"]["candidates"]
        assert len(fast_candidates) == 2
        assert fast_candidates[0]["provider"] == "gemini"
        assert fast_candidates[1]["provider"] == "anthropic"

    def test_empty_available_returns_empty_roles(self) -> None:
        """An empty available set must return an empty roles dict."""
        result = _build_matrix_dict(set())
        assert result["roles"] == {}


class TestWriteRoutingMatrix:
    """Tests for the _write_routing_matrix() function."""

    _SAMPLE_MATRIX: dict = {
        "name": "intelligence-service",
        "description": "Test matrix.",
        "updated": "2026-01-01",
        "roles": {
            "general": {
                "candidates": [{"provider": "anthropic", "model": "claude-sonnet-*"}]
            }
        },
    }

    def test_creates_yaml_file(self, tmp_path: Path) -> None:
        """_write_routing_matrix creates {bundle_root}/routing/{name}.yaml."""
        _write_routing_matrix(self._SAMPLE_MATRIX, tmp_path, "matrix")
        expected = tmp_path / "routing" / "matrix.yaml"
        assert expected.exists(), f"Expected file not found: {expected}"

    def test_creates_routing_directory(self, tmp_path: Path) -> None:
        """routing/ directory is created if it doesn't exist."""
        routing_dir = tmp_path / "routing"
        assert not routing_dir.exists(), "routing/ should not exist before the call"
        _write_routing_matrix(self._SAMPLE_MATRIX, tmp_path, "matrix")
        assert routing_dir.is_dir(), "routing/ directory should be created"

    def test_written_file_is_valid_yaml(self, tmp_path: Path) -> None:
        """Written YAML can be parsed back and matches the input dict."""
        _write_routing_matrix(self._SAMPLE_MATRIX, tmp_path, "matrix")
        file_path = tmp_path / "routing" / "matrix.yaml"
        with file_path.open() as f:
            parsed = yaml.safe_load(f)
        assert parsed == self._SAMPLE_MATRIX

    def test_routing_dir_already_exists(self, tmp_path: Path) -> None:
        """Does not fail if routing/ directory already exists."""
        (tmp_path / "routing").mkdir()
        # Should not raise
        _write_routing_matrix(self._SAMPLE_MATRIX, tmp_path, "matrix")
        assert (tmp_path / "routing" / "matrix.yaml").exists()

    def test_uses_name_parameter_for_filename(self, tmp_path: Path) -> None:
        """The name parameter controls the output filename."""
        _write_routing_matrix(self._SAMPLE_MATRIX, tmp_path, "custom-name")
        expected = tmp_path / "routing" / "custom-name.yaml"
        assert expected.exists(), f"Expected file at: {expected}"
        unexpected = tmp_path / "routing" / "matrix.yaml"
        assert not unexpected.exists(), (
            "Should not create matrix.yaml for name='custom-name'"
        )


class TestConfigDefaults:
    """Tests for config.py default values."""

    def test_routing_matrix_default_is_intelligence_service(self) -> None:
        """The routing_matrix default must be 'intelligence-service'."""
        from intelligence_service.config import Settings

        settings = Settings()
        assert settings.routing_matrix == "intelligence-service", (
            f"Expected routing_matrix default 'intelligence-service', "
            f"got '{settings.routing_matrix}'"
        )


class TestPhase5Integration:
    """Integration test for the full Phase 5 flow (four helper functions in sequence)."""

    def test_end_to_end_produces_valid_artifacts(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Full Phase 5 flow with anthropic+gemini produces correct artifacts.

        Calls all four functions in sequence and verifies:
        - available set contains exactly anthropic and gemini
        - instance_ids include expected provider-model suffixes
        - matrix YAML file is written with correct content and role ordering
        """
        # Set up: only anthropic and gemini API keys present
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test-anthropic")
        monkeypatch.setenv("GOOGLE_API_KEY", "test-google-key")
        for name, info in PROVIDERS.items():
            if name not in ("anthropic", "gemini"):
                monkeypatch.delenv(info["env_var"], raising=False)

        # Step 5a: _get_available_providers()
        available = _get_available_providers()

        assert isinstance(available, set), "available must be a set"
        assert available == {"anthropic", "gemini"}, (
            f"Expected {{'anthropic', 'gemini'}}, got {available}"
        )

        # Step 5b: _build_provider_instances(available)
        provider_instances = _build_provider_instances(available)

        instance_ids = {inst["instance_id"] for inst in provider_instances}
        assert "anthropic-sonnet" in instance_ids, (
            f"Expected 'anthropic-sonnet' in instance_ids, got {instance_ids}"
        )
        assert "anthropic-haiku" in instance_ids, (
            f"Expected 'anthropic-haiku' in instance_ids, got {instance_ids}"
        )
        assert "gemini-flash" in instance_ids, (
            f"Expected 'gemini-flash' in instance_ids, got {instance_ids}"
        )
        assert "gemini-flash-preview-image-generation" in instance_ids, (
            f"Expected 'gemini-flash-preview-image-generation' in instance_ids, "
            f"got {instance_ids}"
        )
        # Exactly 4 unique instances
        assert len(provider_instances) == 4, (
            f"Expected 4 provider instances, got {len(provider_instances)}: {instance_ids}"
        )

        # Step 5c: _build_matrix_dict(available) then _write_routing_matrix(...)
        matrix = _build_matrix_dict(available)
        _write_routing_matrix(matrix, tmp_path, "intelligence-service")

        # Verify YAML file exists at expected path
        yaml_file = tmp_path / "routing" / "intelligence-service.yaml"
        assert yaml_file.exists(), f"Expected YAML file at {yaml_file}"

        with yaml_file.open() as f:
            written = yaml.safe_load(f)

        # Verify top-level fields
        assert written["name"] == "intelligence-service"
        assert (
            written["description"]
            == "Auto-generated routing matrix for the intelligence service."
        )
        assert "roles" in written

        # All 11 roles must be present when both providers available
        assert len(written["roles"]) == 11, (
            f"Expected 11 roles, got {len(written['roles'])}: {set(written['roles'].keys())}"
        )

        # Verify role ordering: "fast" has gemini first, then anthropic
        fast_candidates = written["roles"]["fast"]["candidates"]
        assert len(fast_candidates) == 2, (
            f"Expected 2 fast candidates, got {len(fast_candidates)}"
        )
        assert fast_candidates[0]["provider"] == "gemini", (
            f"Expected gemini first in fast, got {fast_candidates[0]['provider']}"
        )
        assert fast_candidates[1]["provider"] == "anthropic", (
            f"Expected anthropic second in fast, got {fast_candidates[1]['provider']}"
        )

        # Verify role ordering: "vision" has gemini first, then anthropic
        vision_candidates = written["roles"]["vision"]["candidates"]
        assert len(vision_candidates) == 2, (
            f"Expected 2 vision candidates, got {len(vision_candidates)}"
        )
        assert vision_candidates[0]["provider"] == "gemini", (
            f"Expected gemini first in vision, got {vision_candidates[0]['provider']}"
        )
        assert vision_candidates[1]["provider"] == "anthropic", (
            f"Expected anthropic second in vision, got {vision_candidates[1]['provider']}"
        )
