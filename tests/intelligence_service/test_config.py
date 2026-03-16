"""Tests for intelligence_service configuration module."""

import pytest

from intelligence_service.config import Settings, get_settings


def test_settings_defaults() -> None:
    """Verify Settings() has correct default values for all fields."""
    settings = Settings()

    assert settings.server_host == "0.0.0.0"
    assert settings.server_port == 8100
    assert settings.ingestion_url == "http://context-intelligence-server:8000"
    assert settings.bundle_name == "context-intelligence-server"
    assert settings.drain_timeout_seconds == 30
    assert settings.max_sessions == 50
    assert settings.blob_path == "/data/blobs"
    assert settings.log_level == "INFO"
    assert settings.runtime_state_path == "/data/intelligence-runtime"
    assert settings.workspace_path == "/data/intelligence-runtime/workspace"
    assert settings.routing_matrix == "balanced"


def test_settings_removed_fields_do_not_exist() -> None:
    """amplifier_home, bundle_path, and workspace must not exist as attributes."""
    settings = Settings()

    assert not hasattr(settings, "amplifier_home")
    assert not hasattr(settings, "bundle_path")
    assert not hasattr(settings, "workspace")


def test_settings_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify environment variables with AMPLIFIER_CONTEXT_INTELLIGENCE_SERVICE_ prefix override defaults."""
    monkeypatch.setenv("AMPLIFIER_CONTEXT_INTELLIGENCE_SERVICE_SERVER_PORT", "9999")
    monkeypatch.setenv("AMPLIFIER_CONTEXT_INTELLIGENCE_SERVICE_LOG_LEVEL", "DEBUG")

    settings = Settings()

    assert settings.server_port == 9999
    assert settings.log_level == "DEBUG"


def test_get_settings_returns_instance() -> None:
    """Verify get_settings() returns a Settings instance with correct defaults."""
    get_settings.cache_clear()

    settings = get_settings()

    assert isinstance(settings, Settings)
    assert settings.server_port == 8100


def test_settings_runtime_state_path_default() -> None:
    """Verify runtime_state_path defaults to '/data/intelligence-runtime'."""
    settings = Settings()

    assert settings.runtime_state_path == "/data/intelligence-runtime"


def test_settings_workspace_path_default() -> None:
    """Verify workspace_path defaults to '/data/intelligence-runtime/workspace'."""
    settings = Settings()

    assert settings.workspace_path == "/data/intelligence-runtime/workspace"


def test_settings_routing_matrix_default() -> None:
    """Verify routing_matrix defaults to 'balanced'."""
    settings = Settings()

    assert settings.routing_matrix == "balanced"


def test_settings_runtime_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify RUNTIME_STATE_PATH, WORKSPACE_PATH, and ROUTING_MATRIX can be overridden via env vars."""
    monkeypatch.setenv(
        "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVICE_RUNTIME_STATE_PATH", "/custom/runtime"
    )
    monkeypatch.setenv(
        "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVICE_WORKSPACE_PATH", "/custom/workspace"
    )
    monkeypatch.setenv(
        "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVICE_ROUTING_MATRIX", "performance"
    )

    settings = Settings()

    assert settings.runtime_state_path == "/custom/runtime"
    assert settings.workspace_path == "/custom/workspace"
    assert settings.routing_matrix == "performance"


def test_settings_ingestion_url_field_name() -> None:
    """Config field is 'ingestion_url' (maps to AMPLIFIER_CONTEXT_INTELLIGENCE_SERVICE_INGESTION_URL)."""
    settings = Settings()
    assert hasattr(settings, "ingestion_url")
    assert settings.ingestion_url == "http://context-intelligence-server:8000"
