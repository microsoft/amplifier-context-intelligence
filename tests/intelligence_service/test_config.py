"""Tests for intelligence_service configuration module."""

import pytest

from intelligence_service.config import Settings, get_settings


def test_settings_defaults() -> None:
    """Verify Settings() has correct default values."""
    settings = Settings()

    assert settings.server_host == "0.0.0.0"
    assert settings.server_port == 8100
    assert settings.ingestion_server_url == "http://context-intelligence-server:8000"
    assert settings.bundle_name == "context-intelligence-server"
    assert settings.drain_timeout_seconds == 30
    assert settings.max_sessions == 50
    assert settings.blob_path == "/data/blobs"
    assert settings.log_level == "INFO"
    assert settings.amplifier_home == "/data/context-intelligence-service"
    assert settings.bundle_path == ""
    assert settings.routing_matrix == "balanced"
    assert settings.workspace == "context-intelligence-service"


def test_settings_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify environment variables with INTEL_SERVICE_ prefix override defaults."""
    monkeypatch.setenv("INTEL_SERVICE_SERVER_PORT", "9999")
    monkeypatch.setenv("INTEL_SERVICE_LOG_LEVEL", "DEBUG")

    settings = Settings()

    assert settings.server_port == 9999
    assert settings.log_level == "DEBUG"


def test_get_settings_returns_instance() -> None:
    """Verify get_settings() returns a Settings instance with correct defaults."""
    get_settings.cache_clear()

    settings = get_settings()

    assert isinstance(settings, Settings)
    assert settings.server_port == 8100


def test_settings_amplifier_home_default() -> None:
    """Verify amplifier_home defaults to '/data/context-intelligence-service'."""
    settings = Settings()

    assert settings.amplifier_home == "/data/context-intelligence-service"


def test_settings_bundle_path_default() -> None:
    """Verify bundle_path defaults to ''."""
    settings = Settings()

    assert settings.bundle_path == ""


def test_settings_routing_matrix_default() -> None:
    """Verify routing_matrix defaults to 'balanced'."""
    settings = Settings()

    assert settings.routing_matrix == "balanced"


def test_settings_workspace_default() -> None:
    """Verify workspace defaults to 'context-intelligence-service'."""
    settings = Settings()

    assert settings.workspace == "context-intelligence-service"


def test_settings_amplifier_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify INTEL_SERVICE_ prefixed env vars override all 4 new settings."""
    monkeypatch.setenv("INTEL_SERVICE_AMPLIFIER_HOME", "/custom/home")
    monkeypatch.setenv("INTEL_SERVICE_BUNDLE_PATH", "/custom/bundle")
    monkeypatch.setenv("INTEL_SERVICE_ROUTING_MATRIX", "performance")
    monkeypatch.setenv("INTEL_SERVICE_WORKSPACE", "my-workspace")

    settings = Settings()

    assert settings.amplifier_home == "/custom/home"
    assert settings.bundle_path == "/custom/bundle"
    assert settings.routing_matrix == "performance"
    assert settings.workspace == "my-workspace"
