"""Tests for package configuration and metadata."""

from context_intelligence_server import __version__


def test_package_version():
    """Package version should be 0.1.0."""
    assert __version__ == "0.1.0"


def test_settings_defaults():
    """Settings should have correct default values."""
    from context_intelligence_server.config import Settings

    s = Settings()
    assert s.server_host == "0.0.0.0"
    assert s.server_port == 8000
    assert s.neo4j_url == "neo4j://neo4j:7687"
    assert s.neo4j_user == "neo4j"
    assert s.neo4j_password == "password"
    assert s.blob_path == "/data/blobs"
    assert s.log_level == "INFO"


def test_settings_env_override(monkeypatch):
    """Environment variables with AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_ prefix should override defaults."""
    monkeypatch.setenv("AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_SERVER_PORT", "9999")
    monkeypatch.setenv("AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_LOG_LEVEL", "DEBUG")

    from context_intelligence_server.config import Settings

    s = Settings()
    assert s.server_port == 9999
    assert s.log_level == "DEBUG"


def test_get_settings_returns_instance():
    """get_settings() should return a Settings instance."""
    from context_intelligence_server.config import Settings, get_settings

    # Clear the lru_cache so we get a fresh instance
    get_settings.cache_clear()

    settings = get_settings()
    assert isinstance(settings, Settings)
    assert settings.server_host == "0.0.0.0"


def test_settings_log_path_default():
    """Settings should have /data/logs/server.jsonl as the default log_path."""
    from context_intelligence_server.config import Settings

    s = Settings()
    assert s.log_path == "/data/logs/server.jsonl"


def test_three_tier_timeout_defaults():
    """Settings should have correct defaults for three-tier session lifecycle timeouts and cursor path."""
    from context_intelligence_server.config import Settings

    s = Settings()
    assert s.dashboard_inactive_timeout == 1800.0
    assert s.stale_session_timeout == 432000.0
    assert s.cursor_persist_ttl == 15552000.0
    assert s.cursor_path == "/data/cursors"
