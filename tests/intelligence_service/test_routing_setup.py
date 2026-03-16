"""Tests for PROVIDERS dict and _get_available_providers() function.

TDD: These tests were written BEFORE the implementation.
"""

from __future__ import annotations

import pytest

from intelligence_service.amplifier_intelligence_runtime import (
    PROVIDERS,
    _get_available_providers,
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

    def test_returns_set_of_available_providers(self, monkeypatch: pytest.MonkeyPatch) -> None:
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

    def test_empty_string_env_var_is_not_available(self, monkeypatch: pytest.MonkeyPatch) -> None:
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

        assert result == {"anthropic", "gemini", "openai", "azure-openai", "github-copilot"}

    def test_no_providers_available(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """When no env vars are set, the result is an empty set."""
        for info in PROVIDERS.values():
            monkeypatch.delenv(info["env_var"], raising=False)

        result = _get_available_providers()

        assert result == set()
