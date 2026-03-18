"""Tests for package configuration and metadata."""

from pathlib import Path

import pytest

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


# ---------------------------------------------------------------------------
# YAML config file tests
# ---------------------------------------------------------------------------


def test_yaml_settings_source_loads_values(tmp_path: Path, monkeypatch):
    """YamlConfigSettingsSource should load field values from a YAML file."""
    config_file = tmp_path / "server-config.yaml"
    config_file.write_text(
        "neo4j_url: neo4j://localhost:9999\n"
        "blob_path: /tmp/test-blobs\n"
        "log_level: DEBUG\n"
    )

    monkeypatch.setenv(
        "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE", str(config_file)
    )

    from context_intelligence_server.config import Settings

    s = Settings()
    assert s.neo4j_url == "neo4j://localhost:9999"
    assert s.blob_path == "/tmp/test-blobs"
    assert s.log_level == "DEBUG"
    # Fields not present in the YAML file fall back to defaults
    assert s.server_port == 8000


def test_env_var_overrides_yaml(tmp_path: Path, monkeypatch):
    """Environment variables should take precedence over values in the YAML file."""
    config_file = tmp_path / "server-config.yaml"
    config_file.write_text("log_level: DEBUG\nserver_port: 7777\n")

    monkeypatch.setenv(
        "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE", str(config_file)
    )
    # Env var sets a different value for server_port
    monkeypatch.setenv("AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_SERVER_PORT", "9000")

    from context_intelligence_server.config import Settings

    s = Settings()
    # Env var wins over YAML
    assert s.server_port == 9000
    # YAML value is used when no env var overrides it
    assert s.log_level == "DEBUG"


def test_yaml_unknown_keys_are_ignored(tmp_path: Path, monkeypatch):
    """Unknown keys in the YAML file should not cause errors."""
    config_file = tmp_path / "server-config.yaml"
    config_file.write_text("neo4j_url: neo4j://localhost:7687\nunknown_key: surprise\n")

    monkeypatch.setenv(
        "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE", str(config_file)
    )

    from context_intelligence_server.config import Settings

    s = Settings()
    assert s.neo4j_url == "neo4j://localhost:7687"


def test_missing_yaml_file_uses_defaults(tmp_path: Path, monkeypatch):
    """When the YAML file does not exist all settings fall back to defaults."""
    monkeypatch.setenv(
        "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE",
        str(tmp_path / "nonexistent.yaml"),
    )

    from context_intelligence_server.config import Settings

    s = Settings()
    assert s.neo4j_url == "neo4j://neo4j:7687"
    assert s.blob_path == "/data/blobs"


def test_yaml_config_file_path_via_env(tmp_path: Path, monkeypatch):
    """AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE should point to any path."""
    config_file = tmp_path / "custom-name.yaml"
    config_file.write_text("server_port: 1234\n")

    monkeypatch.setenv(
        "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE", str(config_file)
    )

    from context_intelligence_server.config import Settings

    s = Settings()
    assert s.server_port == 1234


def test_yaml_settings_source_direct_path(tmp_path: Path):
    """YamlConfigSettingsSource should accept a yaml_file argument directly."""
    config_file = tmp_path / "direct.yaml"
    config_file.write_text("log_level: WARNING\ncursor_path: /tmp/cursors\n")

    from context_intelligence_server.config import Settings, YamlConfigSettingsSource

    src = YamlConfigSettingsSource(Settings, yaml_file=config_file)
    data = src()
    assert data["log_level"] == "WARNING"
    assert data["cursor_path"] == "/tmp/cursors"


def test_yaml_empty_file_uses_defaults(tmp_path: Path, monkeypatch):
    """An empty YAML file should not override any defaults."""
    config_file = tmp_path / "empty.yaml"
    config_file.write_text("")

    monkeypatch.setenv(
        "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE", str(config_file)
    )

    from context_intelligence_server.config import Settings

    s = Settings()
    assert s.neo4j_url == "neo4j://neo4j:7687"
    assert s.server_port == 8000


@pytest.mark.parametrize(
    "field,yaml_value,expected",
    [
        ("neo4j_url", "neo4j://db:7688", "neo4j://db:7688"),
        ("neo4j_user", "admin", "admin"),
        ("neo4j_password", "secret", "secret"),
        ("blob_path", "/mnt/blobs", "/mnt/blobs"),
        ("cursor_path", "/mnt/cursors", "/mnt/cursors"),
        ("log_level", "WARNING", "WARNING"),
        ("server_host", "127.0.0.1", "127.0.0.1"),
    ],
)
def test_yaml_sets_individual_fields(
    tmp_path: Path, monkeypatch, field, yaml_value, expected
):
    """Each supported field can be overridden individually via YAML."""
    config_file = tmp_path / "single.yaml"
    config_file.write_text(f"{field}: {yaml_value}\n")

    monkeypatch.setenv(
        "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE", str(config_file)
    )

    from context_intelligence_server.config import Settings

    s = Settings()
    assert getattr(s, field) == expected
