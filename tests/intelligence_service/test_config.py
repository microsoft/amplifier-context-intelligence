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
