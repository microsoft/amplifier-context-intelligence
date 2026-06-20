"""Tests for package configuration and metadata."""

from pathlib import Path

import pytest


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
    assert s.neo4j_browser_url == "http://localhost:7474"


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


def test_session_timeout_defaults():
    """Settings should have correct defaults for session lifecycle timeouts."""
    from context_intelligence_server.config import Settings

    s = Settings()
    assert s.dashboard_inactive_timeout == 1800.0
    assert s.stale_session_timeout == 432000.0


def test_settings_has_durable_queue_defaults():
    """Settings should expose conservative durable-queue defaults."""
    from context_intelligence_server.config import Settings

    s = Settings()
    assert s.queues_path == "/data/queues"
    assert s.write_concurrency == 8
    assert s.max_delivery_attempts == 5


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
    config_file.write_text("log_level: WARNING\n")

    from context_intelligence_server.config import Settings, YamlConfigSettingsSource

    src = YamlConfigSettingsSource(Settings, yaml_file=config_file)
    data = src()
    assert data["log_level"] == "WARNING"


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


def test_neo4j_browser_url_env_var(monkeypatch):
    """AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J_BROWSER_URL overrides the default."""
    monkeypatch.setenv(
        "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J_BROWSER_URL",
        "http://neo4j.internal:7474",
    )
    from context_intelligence_server.config import Settings

    s = Settings()
    assert s.neo4j_browser_url == "http://neo4j.internal:7474"


def test_api_key_defaults_to_none():
    """api_key should default to None when not configured."""
    from context_intelligence_server.config import Settings

    s = Settings()
    assert s.api_key is None


def test_api_key_from_yaml(tmp_path: Path, monkeypatch):
    """api_key should be loadable from YAML config."""
    config_file = tmp_path / "server-config.yaml"
    config_file.write_text("api_key: test-secret-token-123\n")

    monkeypatch.setenv(
        "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE", str(config_file)
    )

    from context_intelligence_server.config import Settings

    s = Settings()
    assert s.api_key == "test-secret-token-123"


def test_api_key_from_env(monkeypatch):
    """api_key should be settable via environment variable."""
    monkeypatch.setenv("AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_API_KEY", "env-token-456")

    from context_intelligence_server.config import Settings

    s = Settings()
    assert s.api_key == "env-token-456"


def test_api_key_empty_string_treated_as_none():
    """api_key='' (empty string) should be normalized to None — disabling auth.

    A user copying server-config.example.yaml verbatim gets api_key: "" which
    must behave identically to omitting api_key entirely (auth disabled).
    """
    from context_intelligence_server.config import Settings

    s = Settings(api_key="")
    assert s.api_key is None


def test_api_key_empty_string_from_yaml_treated_as_none(tmp_path: Path, monkeypatch):
    """api_key: '' in a YAML config file should be normalized to None."""
    config_file = tmp_path / "server-config.yaml"
    config_file.write_text('api_key: ""\n')

    monkeypatch.setenv(
        "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE", str(config_file)
    )

    from context_intelligence_server.config import Settings

    s = Settings()
    assert s.api_key is None


@pytest.mark.parametrize(
    "field,yaml_value,expected",
    [
        ("neo4j_url", "neo4j://db:7688", "neo4j://db:7688"),
        ("neo4j_user", "admin", "admin"),
        ("neo4j_password", "secret", "secret"),
        ("blob_path", "/mnt/blobs", "/mnt/blobs"),
        ("log_level", "WARNING", "WARNING"),
        ("server_host", "127.0.0.1", "127.0.0.1"),
        (
            "neo4j_browser_url",
            "http://neo4j.internal:7474",
            "http://neo4j.internal:7474",
        ),
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


def test_neo4j_flush_chunk_rows_default():
    """neo4j_flush_chunk_rows should default to 100 (cardinality bound, #278)."""
    from context_intelligence_server.config import Settings

    s = Settings()
    assert s.neo4j_flush_chunk_rows == 100, (
        f"Expected neo4j_flush_chunk_rows == 100, got {s.neo4j_flush_chunk_rows}"
    )


def test_neo4j_flush_chunk_bytes_default():
    """neo4j_flush_chunk_bytes should default to 4_194_304 (4 MiB payload bound, #278)."""
    from context_intelligence_server.config import Settings

    s = Settings()
    assert s.neo4j_flush_chunk_bytes == 4_194_304, (
        f"Expected neo4j_flush_chunk_bytes == 4_194_304, got {s.neo4j_flush_chunk_bytes}"
    )


# ---------------------------------------------------------------------------
# T1-T5, T22: per-user API keys — _is_hex, _validate_api_keys, build_keystore
# ---------------------------------------------------------------------------


class TestIsHex:
    """T1-T3: _is_hex helper."""

    def test_valid_64_char_hex_string(self) -> None:
        """T1: _is_hex accepts a valid 64-char lowercase hex string."""
        from context_intelligence_server.config import _is_hex

        assert _is_hex("a" * 64) is True
        assert _is_hex("deadbeef" * 8) is True  # 64 hex chars

    def test_mixed_case_hex_accepted(self) -> None:
        """T1: _is_hex accepts uppercase hex chars too."""
        from context_intelligence_server.config import _is_hex

        assert _is_hex("DEADBEEF" * 8) is True

    def test_too_short_rejected(self) -> None:
        """T2: _is_hex rejects strings shorter than 32 chars."""
        from context_intelligence_server.config import _is_hex

        assert _is_hex("abc") is False
        assert _is_hex("deadbeef") is False  # only 8 chars
        assert _is_hex("") is False

    def test_non_hex_chars_rejected(self) -> None:
        """T3: _is_hex rejects strings with non-hex characters."""
        from context_intelligence_server.config import _is_hex

        assert _is_hex("z" * 64) is False
        assert _is_hex("ghijklmn" * 8) is False
        assert _is_hex("test-secret-token-not-hex-00000000000") is False


class TestValidateApiKeys:
    """T8-T12 / T22: _validate_api_keys enforces the NESTED shape, fail-closed.

    The NESTED form (design D4) maps a 64-char SHA-256 hex digest to a metadata
    dict carrying at least ``id``::

        api_keys:
          "<64-hex>":
            id: owner
    """

    def test_valid_nested_api_keys_accepted(self) -> None:
        """T8: Settings with valid <64-hex> -> {id: ...} entries succeeds."""
        from context_intelligence_server.config import Settings

        d1 = "a" * 64
        d2 = "b" * 64
        s = Settings(api_keys={d1: {"id": "owner"}, d2: {"id": "peer-test"}})
        assert s.api_keys == {d1: {"id": "owner"}, d2: {"id": "peer-test"}}

    def test_bad_length_key_raises(self) -> None:
        """T9: a key that is not 64 hex chars raises ValidationError."""
        from pydantic import ValidationError

        from context_intelligence_server.config import Settings

        with pytest.raises(ValidationError):
            Settings(api_keys={"a" * 32: {"id": "owner"}})  # 32 != 64

    def test_non_hex_key_raises(self) -> None:
        """T9: a 64-char non-hex key raises ValidationError."""
        from pydantic import ValidationError

        from context_intelligence_server.config import Settings

        with pytest.raises(ValidationError):
            Settings(api_keys={"z" * 64: {"id": "owner"}})

    def test_value_not_a_dict_raises(self) -> None:
        """T10: a non-dict value (legacy flat string token) raises ValidationError."""
        from pydantic import ValidationError

        from context_intelligence_server.config import Settings

        with pytest.raises(ValidationError):
            Settings(api_keys={"a" * 64: "owner"})  # type: ignore[dict-item]

    def test_missing_id_raises(self) -> None:
        """T11: a value dict missing 'id' raises ValidationError."""
        from pydantic import ValidationError

        from context_intelligence_server.config import Settings

        with pytest.raises(ValidationError):
            Settings(api_keys={"a" * 64: {"label": "owner"}})

    def test_empty_id_raises(self) -> None:
        """T11: a value dict with empty 'id' raises ValidationError."""
        from pydantic import ValidationError

        from context_intelligence_server.config import Settings

        with pytest.raises(ValidationError):
            Settings(api_keys={"a" * 64: {"id": ""}})

    def test_empty_api_keys_is_valid(self) -> None:
        """T12: Empty api_keys dict is valid (no auth configured)."""
        from context_intelligence_server.config import Settings

        s = Settings(api_keys={})
        assert s.api_keys == {}

    def test_none_api_keys_is_valid(self) -> None:
        """T12: api_keys=None (default) is valid (no per-contributor keys configured)."""
        from context_intelligence_server.config import Settings

        s = Settings(api_keys=None)
        assert s.api_keys is None


class TestBuildKeystore:
    """T4-T5: build_keystore maps sha256_hex(token) → contributor_id."""

    def test_legacy_api_key_folds_to_owner(self) -> None:
        """T4: build_keystore maps sha256(legacy api_key) → 'owner'."""
        import hashlib

        from context_intelligence_server.config import Settings

        s = Settings(api_key="my-secret-token")
        ks = s.build_keystore()
        expected_digest = hashlib.sha256(b"my-secret-token").hexdigest()
        assert ks[expected_digest] == "owner"

    def test_nested_entry_digest_maps_to_id(self) -> None:
        """T5: build_keystore reads id via meta['id']; the digest key is used verbatim."""
        import hashlib

        from context_intelligence_server.config import Settings

        token = "peer-test-raw-token"
        digest = hashlib.sha256(token.encode()).hexdigest()
        s = Settings(api_keys={digest: {"id": "peer-test"}})
        ks = s.build_keystore()
        # The digest key is used directly (NOT re-hashed) and maps to the id.
        assert ks[digest] == "peer-test"

    def test_empty_config_returns_empty_keystore(self) -> None:
        """T4: build_keystore returns {} when neither api_key nor api_keys is set."""
        from context_intelligence_server.config import Settings

        s = Settings(api_key=None, api_keys=None)
        assert s.build_keystore() == {}

    def test_legacy_and_nested_combined(self) -> None:
        """T5: build_keystore combines legacy api_key (→owner) with nested entries."""
        import hashlib

        from context_intelligence_server.config import Settings

        legacy_token = "legacy-token"
        peer_token = "peer-raw-token"
        peer_digest = hashlib.sha256(peer_token.encode()).hexdigest()
        s = Settings(
            api_key=legacy_token,
            api_keys={peer_digest: {"id": "peer-test"}},
        )
        ks = s.build_keystore()
        legacy_digest = hashlib.sha256(legacy_token.encode()).hexdigest()
        assert ks[legacy_digest] == "owner"
        assert ks[peer_digest] == "peer-test"
        assert ks[legacy_digest] == "owner"
