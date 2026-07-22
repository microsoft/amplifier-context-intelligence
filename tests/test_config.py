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
    assert s.status_inactive_timeout == 1800.0
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
# T1-T5, T22: per-user API keys — _validate_api_keys, build_keystore
# ---------------------------------------------------------------------------
# Note: _is_hex() was removed in T7 (cranky-old-sam cleanup) — it was dead
# code never called by the validator; the 64-hex check is inline in
# _validate_api_keys.  Tests for _is_hex have been deleted alongside it.


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


# ---------------------------------------------------------------------------
# Phase 1 auth correctness — T1.1 (fail-closed empty dict) + T1.2 (digest hygiene)
# ---------------------------------------------------------------------------


class TestPhase1FailClosedEmptyApiKeys:
    """T1.1 (updated): api_keys={} is now a SUPPORTED bootstrap state (symmetric
    with entra_identities) — the server boots fail-CLOSED with zero keys and is
    populated at runtime via PUT /admin/keys/{sha256hash}. api_keys=None
    (omitted) still disables auth entirely.
    """

    def test_api_keys_empty_dict_now_returns_empty_dict(self) -> None:
        """Settings(api_keys={}) now returns {} instead of raising.

        (Previously: raised ValidationError 'at least one entry' — fail-closed
        was implemented as a startup refusal. Now: boots fail-closed instead —
        the empty keystore alone yields a resolver with auth_enabled=False,
        and BearerTokenMiddleware fail-CLOSES on that unless the operator
        opts out via allow_unauthenticated=True. See the empty-keystore
        bootstrap tests in test_t7_wire.py / test_main.py.)
        """
        from context_intelligence_server.config import Settings

        s = Settings(api_keys={})
        assert s.api_keys == {}

    def test_api_keys_none_is_allowed(self) -> None:
        """T1.1: api_keys=None (omitted) disables auth — no ValidationError raised.

        Guards against an implementation mistake where the empty-dict rejection
        accidentally fires for None as well.
        """
        from context_intelligence_server.config import Settings

        s = Settings(api_keys=None)
        assert s.api_keys is None
        # build_keystore returns {} when neither api_key nor api_keys is set.
        assert s.build_keystore() == {}


class TestPhase1DigestHygiene:
    """T1.2: Digest normalization and contributor-id hygiene (close silent-401 traps)."""

    def test_uppercase_digest_normalized_and_authenticates(self) -> None:
        """T1.2: UPPERCASE digest is accepted and normalized to lowercase.

        hashlib.sha256(...).hexdigest() always returns lowercase hex.  An operator
        who pastes a digest in UPPERCASE must not end up with a silent 401.
        After build_keystore the keystore key must be lowercase so _resolve_token
        (which uses hexdigest) can find it.
        """
        from context_intelligence_server.config import Settings

        upper = "A" * 64
        lower = upper.lower()  # "a" * 64

        s = Settings(api_keys={upper: {"id": "owner"}})

        # Validator must have lowercased the key in the stored dict.
        assert s.api_keys is not None, "api_keys must not be None after construction"
        assert lower in s.api_keys, (
            f"Expected lowercase key in api_keys, got keys: {list(s.api_keys)!r}"
        )
        assert upper not in s.api_keys, (
            "Uppercase key must not remain in api_keys after normalization"
        )
        assert s.api_keys[lower] == {"id": "owner"}

        # build_keystore must expose the lowercase key so _resolve_token hits it.
        ks = s.build_keystore()
        assert ks.get(lower) == "owner", (
            f"keystore[{lower!r}] should be 'owner'; full keystore={ks!r}"
        )
        assert ks.get(upper) is None, "Uppercase key must not appear in keystore"

    def test_non_hex_digest_rejected(self) -> None:
        """T1.2: 64-char string containing non-hex characters raises ValidationError."""
        from pydantic import ValidationError

        from context_intelligence_server.config import Settings

        with pytest.raises(ValidationError):
            Settings(api_keys={"z" * 64: {"id": "owner"}})

    def test_wrong_length_digest_rejected(self) -> None:
        """T1.2: 63- and 65-char hex strings raise ValidationError (must be exactly 64)."""
        from pydantic import ValidationError

        from context_intelligence_server.config import Settings

        with pytest.raises(ValidationError):
            Settings(api_keys={"a" * 63: {"id": "owner"}})

        with pytest.raises(ValidationError):
            Settings(api_keys={"a" * 65: {"id": "owner"}})

    def test_whitespace_in_digest_rejected(self) -> None:
        """T1.2: A digest containing whitespace is rejected (space is not a hex char)."""
        from pydantic import ValidationError

        from context_intelligence_server.config import Settings

        # 64 chars total, but a leading space makes it invalid hex.
        digest_with_spaces = " " + "a" * 62 + " "
        assert len(digest_with_spaces) == 64

        with pytest.raises(ValidationError):
            Settings(api_keys={digest_with_spaces: {"id": "owner"}})

    def test_blank_contributor_id_rejected(self) -> None:
        """T1.2: Whitespace-only contributor id raises ValidationError.

        ``not contributor_id`` is True only for the empty string; ``\"   \"`` is
        truthy.  The validator must use ``.strip()`` to catch whitespace-only ids.
        """
        from pydantic import ValidationError

        from context_intelligence_server.config import Settings

        with pytest.raises(ValidationError):
            Settings(api_keys={"a" * 64: {"id": "   "}})


# ---------------------------------------------------------------------------
# T3: Entra auth config — new fields, validators, cross-field checks
# §8b carried build-gates mapped to each test in the docstrings.
# ---------------------------------------------------------------------------

# Fake GUIDs used throughout — NEVER real app-reg ids, oids, or tenant ids.
_FAKE_OID_1 = "11111111-1111-1111-1111-111111111111"
_FAKE_OID_2 = "22222222-2222-2222-2222-222222222222"
_FAKE_OID_UPPER = "AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE"  # uppercase hex letters
_FAKE_CLIENT_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
_FAKE_TENANT_ID = "ffffffff-0000-1111-2222-333333333333"


class TestAuthModeField:
    """T3: auth_mode field defaults and valid values."""

    def test_auth_mode_defaults_to_static(self) -> None:
        """§8b regression: auth_mode defaults to 'static'."""
        from context_intelligence_server.config import Settings

        s = Settings()
        assert s.auth_mode == "static"

    def test_auth_mode_can_be_set_to_entra(self) -> None:
        """auth_mode='entra' accepted when all required entra fields are present."""
        from context_intelligence_server.config import Settings

        s = Settings(
            auth_mode="entra",
            azure_client_id=_FAKE_CLIENT_ID,
            azure_tenant_id=_FAKE_TENANT_ID,
            entra_identities={_FAKE_OID_1: {"id": "colombod"}},
        )
        assert s.auth_mode == "entra"

    def test_auth_mode_from_yaml(self, tmp_path: Path, monkeypatch) -> None:
        """auth_mode can be read from YAML config — YAML path exercised per §0.4."""
        config_file = tmp_path / "server-config.yaml"
        config_file.write_text(
            f"auth_mode: entra\n"
            f"azure_client_id: {_FAKE_CLIENT_ID}\n"
            f"azure_tenant_id: {_FAKE_TENANT_ID}\n"
            f"entra_identities:\n"
            f"  {_FAKE_OID_1}:\n"
            f"    id: colombod\n"
        )
        monkeypatch.setenv(
            "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE", str(config_file)
        )

        from context_intelligence_server.config import Settings

        s = Settings()
        assert s.auth_mode == "entra"


class TestAzureFieldNormalization:
    """T3: azure_client_id / azure_tenant_id empty/whitespace normalization.

    §8b gate (tester-breaker): whitespace azure_client_id / azure_tenant_id →
    normalized to None at field level, then caught by the cross-field validator.
    """

    def test_azure_client_id_defaults_to_none(self) -> None:
        """azure_client_id defaults to None."""
        from context_intelligence_server.config import Settings

        assert Settings().azure_client_id is None

    def test_azure_tenant_id_defaults_to_none(self) -> None:
        """azure_tenant_id defaults to None."""
        from context_intelligence_server.config import Settings

        assert Settings().azure_tenant_id is None

    def test_azure_client_id_empty_string_to_none(self) -> None:
        """azure_client_id='' normalized to None (mirrors _normalize_api_key)."""
        from context_intelligence_server.config import Settings

        s = Settings(azure_client_id="")
        assert s.azure_client_id is None

    def test_azure_tenant_id_empty_string_to_none(self) -> None:
        """azure_tenant_id='' normalized to None."""
        from context_intelligence_server.config import Settings

        s = Settings(azure_tenant_id="")
        assert s.azure_tenant_id is None

    def test_azure_client_id_whitespace_to_none(self) -> None:
        """§8b (tester-breaker): azure_client_id='   ' normalized to None."""
        from context_intelligence_server.config import Settings

        s = Settings(azure_client_id="   ")
        assert s.azure_client_id is None

    def test_azure_tenant_id_whitespace_to_none(self) -> None:
        """§8b (tester-breaker): azure_tenant_id='   ' normalized to None."""
        from context_intelligence_server.config import Settings

        s = Settings(azure_tenant_id="   ")
        assert s.azure_tenant_id is None

    def test_azure_client_id_valid_preserved(self) -> None:
        """A non-empty azure_client_id is stored as-is."""
        from context_intelligence_server.config import Settings

        s = Settings(azure_client_id=_FAKE_CLIENT_ID)
        assert s.azure_client_id == _FAKE_CLIENT_ID

    def test_azure_tenant_id_valid_preserved(self) -> None:
        """A non-empty azure_tenant_id is stored as-is."""
        from context_intelligence_server.config import Settings

        s = Settings(azure_tenant_id=_FAKE_TENANT_ID)
        assert s.azure_tenant_id == _FAKE_TENANT_ID


class TestValidateEntraIdentities:
    """T3: _validate_entra_identities — §8b full edge matrix.

    Mirrors _validate_api_keys: None→None, empty→raise, key must be GUID,
    value must be {id: non-empty-str}, keys normalized to lowercase.
    """

    def test_valid_entry_accepted(self) -> None:
        """§8b: valid single entry accepted."""
        from context_intelligence_server.config import Settings

        s = Settings(entra_identities={_FAKE_OID_1: {"id": "colombod"}})
        assert s.entra_identities is not None
        assert s.entra_identities[_FAKE_OID_1]["id"] == "colombod"

    def test_none_is_valid(self) -> None:
        """§8b regression: entra_identities=None (omitted) is valid."""
        from context_intelligence_server.config import Settings

        s = Settings(entra_identities=None)
        assert s.entra_identities is None

    def test_empty_dict_now_returns_empty_dict(self) -> None:
        """entra_identities={} is now a SUPPORTED bootstrap state — returns {}.

        (Previously: raised ValidationError 'at least one entry'. The empty map
        is now accepted via allow_empty=True so the server boots on a fresh
        /data volume and is populated at runtime via PUT /admin/identities.
        service_identities={} still raises — see the service-identities suite.)
        """
        from context_intelligence_server.config import Settings

        s = Settings(entra_identities={})
        assert s.entra_identities == {}

    def test_non_guid_key_raises(self) -> None:
        """§8b: a key that is not a GUID raises ValidationError."""
        from pydantic import ValidationError

        from context_intelligence_server.config import Settings

        with pytest.raises(ValidationError, match="valid GUID"):
            Settings(entra_identities={"not-a-guid": {"id": "colombod"}})

    def test_sha256_hex_key_not_valid_guid_raises(self) -> None:
        """§8b: 64-char SHA-256 hex (valid api_keys key) is NOT a valid GUID — raises."""
        from pydantic import ValidationError

        from context_intelligence_server.config import Settings

        with pytest.raises(ValidationError, match="valid GUID"):
            Settings(entra_identities={"a" * 64: {"id": "colombod"}})

    def test_guid_with_braces_raises(self) -> None:
        """§8b: {xxxxxxxx-...} braced form raises ValidationError."""
        from pydantic import ValidationError

        from context_intelligence_server.config import Settings

        braced = "{" + _FAKE_OID_1 + "}"
        with pytest.raises(ValidationError, match="valid GUID"):
            Settings(entra_identities={braced: {"id": "colombod"}})

    def test_urn_uuid_prefix_raises(self) -> None:
        """§8b: urn:uuid:... prefix raises ValidationError."""
        from pydantic import ValidationError

        from context_intelligence_server.config import Settings

        with pytest.raises(ValidationError, match="valid GUID"):
            Settings(entra_identities={"urn:uuid:" + _FAKE_OID_1: {"id": "colombod"}})

    def test_trailing_junk_raises(self) -> None:
        """§8b: trailing characters after valid GUID raises ValidationError."""
        from pydantic import ValidationError

        from context_intelligence_server.config import Settings

        with pytest.raises(ValidationError, match="valid GUID"):
            Settings(entra_identities={_FAKE_OID_1 + "-extra": {"id": "colombod"}})

    def test_all_zeros_guid_raises(self) -> None:
        """§8b: the all-zeros GUID (00000000-0000-0000-0000-000000000000) is rejected."""
        from pydantic import ValidationError

        from context_intelligence_server.config import Settings

        with pytest.raises(ValidationError):
            Settings(
                entra_identities={
                    "00000000-0000-0000-0000-000000000000": {"id": "colombod"}
                }
            )

    def test_uppercase_key_normalized_to_lowercase(self) -> None:
        """§8b (tester-breaker F-02/F-03): UPPERCASE GUID normalized to lowercase."""
        from context_intelligence_server.config import Settings

        lower_oid = _FAKE_OID_UPPER.lower()  # "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

        s = Settings(entra_identities={_FAKE_OID_UPPER: {"id": "colombod"}})
        assert s.entra_identities is not None
        assert lower_oid in s.entra_identities, (
            f"Expected lowercase key in entra_identities, got: {list(s.entra_identities)!r}"
        )
        assert _FAKE_OID_UPPER not in s.entra_identities, (
            "Uppercase key must not remain in entra_identities after normalization"
        )

    def test_non_dict_value_raises(self) -> None:
        """§8b (tester-breaker): string value {oid: 'colombod'} instead of dict raises."""
        from pydantic import ValidationError

        from context_intelligence_server.config import Settings

        with pytest.raises(ValidationError):
            Settings(
                entra_identities={_FAKE_OID_1: "colombod"}  # type: ignore[dict-item]
            )

    def test_value_missing_id_raises(self) -> None:
        """§8b: value dict without 'id' key raises ValidationError."""
        from pydantic import ValidationError

        from context_intelligence_server.config import Settings

        with pytest.raises(ValidationError):
            Settings(entra_identities={_FAKE_OID_1: {"label": "some-person"}})

    def test_value_empty_id_raises(self) -> None:
        """§8b: value dict with empty 'id' raises ValidationError."""
        from pydantic import ValidationError

        from context_intelligence_server.config import Settings

        with pytest.raises(ValidationError):
            Settings(entra_identities={_FAKE_OID_1: {"id": ""}})

    def test_value_whitespace_id_raises(self) -> None:
        """§8b: value dict with whitespace-only 'id' raises ValidationError."""
        from pydantic import ValidationError

        from context_intelligence_server.config import Settings

        with pytest.raises(ValidationError):
            Settings(entra_identities={_FAKE_OID_1: {"id": "   "}})

    def test_two_oids_same_contributor_works(self) -> None:
        """§8b: two different OIDs → same contributor (many-to-one mapping) works."""
        from context_intelligence_server.config import Settings

        s = Settings(
            entra_identities={
                _FAKE_OID_1: {"id": "colombod"},
                _FAKE_OID_2: {"id": "colombod"},
            }
        )
        assert s.entra_identities is not None
        assert s.entra_identities[_FAKE_OID_1]["id"] == "colombod"
        assert s.entra_identities[_FAKE_OID_2]["id"] == "colombod"


class TestBuildIdentityMap:
    """T3: build_identity_map() — mirrors build_keystore(), returns {oid_lower: contributor_id}."""

    def test_returns_oid_to_contributor_map(self) -> None:
        """§8b: build_identity_map() returns {oid: contributor_id} for each entry."""
        from context_intelligence_server.config import Settings

        s = Settings(
            entra_identities={
                _FAKE_OID_1: {"id": "colombod"},
                _FAKE_OID_2: {"id": "other-contributor"},
            }
        )
        result = s.build_identity_map()
        assert result == {
            _FAKE_OID_1: "colombod",
            _FAKE_OID_2: "other-contributor",
        }

    def test_returns_empty_for_none_identities(self) -> None:
        """§8b: build_identity_map() returns {} when entra_identities is None."""
        from context_intelligence_server.config import Settings

        s = Settings(entra_identities=None)
        assert s.build_identity_map() == {}

    def test_uppercase_key_lowercased_in_result(self) -> None:
        """§8b: build_identity_map() keys are lowercased (belt-and-suspenders, mirrors build_keystore)."""
        from context_intelligence_server.config import Settings

        lower_oid = _FAKE_OID_UPPER.lower()
        s = Settings(entra_identities={_FAKE_OID_UPPER: {"id": "colombod"}})
        result = s.build_identity_map()
        assert lower_oid in result
        assert _FAKE_OID_UPPER not in result
        assert result[lower_oid] == "colombod"

    def test_two_oids_same_contributor_in_map(self) -> None:
        """§8b: two OIDs → same contributor both appear in the map."""
        from context_intelligence_server.config import Settings

        s = Settings(
            entra_identities={
                _FAKE_OID_1: {"id": "colombod"},
                _FAKE_OID_2: {"id": "colombod"},
            }
        )
        result = s.build_identity_map()
        assert result[_FAKE_OID_1] == "colombod"
        assert result[_FAKE_OID_2] == "colombod"


class TestCrossFieldEntraValidation:
    """T3: model_validator — auth_mode=entra requires client_id, tenant_id, identities (AC7, §8b).

    This is the startup-refusal gate: bad entra config must refuse to construct Settings.
    """

    def test_entra_mode_all_fields_set_succeeds(self) -> None:
        """AC7: auth_mode=entra with all required fields set constructs successfully."""
        from context_intelligence_server.config import Settings

        s = Settings(
            auth_mode="entra",
            azure_client_id=_FAKE_CLIENT_ID,
            azure_tenant_id=_FAKE_TENANT_ID,
            entra_identities={_FAKE_OID_1: {"id": "colombod"}},
        )
        assert s.auth_mode == "entra"
        assert s.azure_client_id == _FAKE_CLIENT_ID
        assert s.azure_tenant_id == _FAKE_TENANT_ID

    def test_entra_mode_missing_client_id_raises(self) -> None:
        """AC7: auth_mode=entra + azure_client_id=None → startup refused."""
        from pydantic import ValidationError

        from context_intelligence_server.config import Settings

        with pytest.raises(ValidationError, match="azure_client_id"):
            Settings(
                auth_mode="entra",
                azure_client_id=None,
                azure_tenant_id=_FAKE_TENANT_ID,
                entra_identities={_FAKE_OID_1: {"id": "colombod"}},
            )

    def test_entra_mode_missing_tenant_id_raises(self) -> None:
        """AC7: auth_mode=entra + azure_tenant_id=None → startup refused."""
        from pydantic import ValidationError

        from context_intelligence_server.config import Settings

        with pytest.raises(ValidationError, match="azure_tenant_id"):
            Settings(
                auth_mode="entra",
                azure_client_id=_FAKE_CLIENT_ID,
                azure_tenant_id=None,
                entra_identities={_FAKE_OID_1: {"id": "colombod"}},
            )

    def test_entra_mode_whitespace_client_id_raises(self) -> None:
        """AC7 + §8b: whitespace azure_client_id → None after normalization → startup refused."""
        from pydantic import ValidationError

        from context_intelligence_server.config import Settings

        with pytest.raises(ValidationError, match="azure_client_id"):
            Settings(
                auth_mode="entra",
                azure_client_id="   ",
                azure_tenant_id=_FAKE_TENANT_ID,
                entra_identities={_FAKE_OID_1: {"id": "colombod"}},
            )

    def test_entra_mode_whitespace_tenant_id_raises(self) -> None:
        """AC7 + §8b: whitespace azure_tenant_id → None after normalization → startup refused."""
        from pydantic import ValidationError

        from context_intelligence_server.config import Settings

        with pytest.raises(ValidationError, match="azure_tenant_id"):
            Settings(
                auth_mode="entra",
                azure_client_id=_FAKE_CLIENT_ID,
                azure_tenant_id="   ",
                entra_identities={_FAKE_OID_1: {"id": "colombod"}},
            )

    def test_entra_mode_none_identities_boots(self) -> None:
        """entra_identities=None is now a SUPPORTED bootstrap state — the server
        boots. (Previously: AC7 startup refused with a ValidationError.)
        main.create_asgi_app() logs a loud warning when the effective map is
        empty at startup; see the empty-map bootstrap tests in test_t7_wire.py
        / test_main.py.
        """
        from context_intelligence_server.config import Settings

        s = Settings(
            auth_mode="entra",
            azure_client_id=_FAKE_CLIENT_ID,
            azure_tenant_id=_FAKE_TENANT_ID,
            entra_identities=None,
        )
        assert s.entra_identities is None
        assert s.build_identity_map() == {}

    def test_static_mode_no_entra_fields_required(self) -> None:
        """§8b regression: auth_mode=static does not require azure_* or entra_identities."""
        from context_intelligence_server.config import Settings

        s = Settings(auth_mode="static")
        assert s.auth_mode == "static"
        assert s.azure_client_id is None
        assert s.azure_tenant_id is None
        assert s.entra_identities is None

    def test_default_settings_no_entra_required(self) -> None:
        """§8b regression: default Settings() still constructs (auth_mode=static default)."""
        from context_intelligence_server.config import Settings

        s = Settings()
        assert s.auth_mode == "static"


# ---------------------------------------------------------------------------
# T3 hardening (tester-breaker adversarial review):
#   - env-var path (the REAL production path — highest priority)
#   - auth_mode bad literal
#   - GUID regex edges (leading/trailing space, g-hex, fullwidth, ZWSP)
#   - extra keys tolerated (nested-dict extensibility)
#   - coexistence of api_keys + entra_identities
#   - duplicate-oid last-wins (documentation)
#   - YAML-native id coercion (int/bool/None each raise)
#   - dead-code/message documentation (pydantic dict_type fires before isinstance)
# ---------------------------------------------------------------------------


class TestT3EnvVarPath:
    """T3 hardening: the env-var path is the REAL production path — validates identically.

    tester-breaker flagged that the existing T3 tests exercise only direct
    Settings() construction.  These tests use monkeypatch.setenv with
    AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_ENTRA_IDENTITIES to confirm that
    pydantic-settings parses the JSON env var and feeds it through the same
    field validators as direct construction.
    """

    _ENTRA_KEY = "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_ENTRA_IDENTITIES"
    _AUTH_KEY = "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_AUTH_MODE"
    _CLIENT_KEY = "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_AZURE_CLIENT_ID"
    _TENANT_KEY = "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_AZURE_TENANT_ID"

    def test_env_var_path_loads_and_normalizes(self, monkeypatch) -> None:
        """Production path: ENTRA_IDENTITIES JSON env var loads, validates, and normalizes.

        Confirms that pydantic-settings parses the JSON env var value and runs the
        same field validators as direct Settings() construction.
        """
        monkeypatch.setenv(self._AUTH_KEY, "entra")
        monkeypatch.setenv(self._CLIENT_KEY, _FAKE_CLIENT_ID)
        monkeypatch.setenv(self._TENANT_KEY, _FAKE_TENANT_ID)
        monkeypatch.setenv(
            self._ENTRA_KEY,
            '{"11111111-1111-1111-1111-111111111111":{"id":"colombod"}}',
        )

        from context_intelligence_server.config import Settings

        s = Settings()
        assert s.entra_identities is not None
        assert s.entra_identities[_FAKE_OID_1]["id"] == "colombod"
        assert s.build_identity_map()[_FAKE_OID_1] == "colombod"

    def test_env_var_all_zeros_guid_raises(self, monkeypatch) -> None:
        """Env-var path: all-zeros GUID is rejected identically to direct construction."""
        from pydantic import ValidationError

        monkeypatch.setenv(
            self._ENTRA_KEY,
            '{"00000000-0000-0000-0000-000000000000":{"id":"colombod"}}',
        )

        from context_intelligence_server.config import Settings

        with pytest.raises(ValidationError):
            Settings()

    def test_env_var_non_guid_key_raises(self, monkeypatch) -> None:
        """Env-var path: non-GUID key is rejected (same validator fires as direct construction)."""
        from pydantic import ValidationError

        monkeypatch.setenv(
            self._ENTRA_KEY,
            '{"not-a-guid":{"id":"colombod"}}',
        )

        from context_intelligence_server.config import Settings

        with pytest.raises(ValidationError, match="valid GUID"):
            Settings()

    def test_env_var_empty_dict_now_boots(self, monkeypatch) -> None:
        """Env-var path: empty {} is now a SUPPORTED bootstrap state — returns {}.

        Symmetric with direct construction (test_empty_dict_now_returns_empty_dict):
        an explicitly empty entra_identities map boots the server (populate at
        runtime via PUT /admin/identities) instead of raising a startup error.
        """
        monkeypatch.setenv(self._ENTRA_KEY, "{}")

        from context_intelligence_server.config import Settings

        s = Settings()
        assert s.entra_identities == {}

    def test_env_var_string_value_raises(self, monkeypatch) -> None:
        """Env-var path: string (non-dict) value is rejected by pydantic dict_type coercion."""
        from pydantic import ValidationError

        monkeypatch.setenv(
            self._ENTRA_KEY,
            '{"11111111-1111-1111-1111-111111111111":"colombod"}',
        )

        from context_intelligence_server.config import Settings

        with pytest.raises(ValidationError):
            Settings()


class TestT3AuthModeLiteral:
    """T3 hardening: auth_mode is a Literal field — unknown values raise at construction."""

    def test_bad_literal_raises(self) -> None:
        """Settings(auth_mode='foobar') raises ValidationError — only 'static' and 'entra' are valid."""
        from pydantic import ValidationError

        from context_intelligence_server.config import Settings

        with pytest.raises(ValidationError):
            Settings(auth_mode="foobar")  # type: ignore[arg-type]


class TestT3EntraIdentitiesEdges:
    """T3 hardening: GUID regex edges, extra keys, duplicate-oid, YAML type coercion, dead-code doc.

    All tests assert EXISTING correct behavior (characterization / regression tests).
    Expected to pass immediately — a failure here is a real bug, not a test gap.
    """

    def test_extra_keys_in_value_dict_tolerated(self) -> None:
        """Extra keys beyond 'id' in the value dict are accepted and ignored in build_identity_map().

        Documents the intentional nested-dict extensibility of entra_identities:
        a future 'role' or 'label' key can be added to any entry without a breaking
        config change or validation failure.  build_identity_map() reads only 'id'.
        """
        from context_intelligence_server.config import Settings

        s = Settings(
            entra_identities={
                _FAKE_OID_1: {"id": "colombod", "email": "x@y.z", "extra": "k"}
            }
        )
        assert s.entra_identities is not None
        result = s.build_identity_map()
        assert result[_FAKE_OID_1] == "colombod"

    def test_guid_leading_space_rejected(self) -> None:
        """GUID key with a leading space is rejected — re.fullmatch anchors the entire string."""
        from pydantic import ValidationError

        from context_intelligence_server.config import Settings

        with pytest.raises(ValidationError, match="valid GUID"):
            Settings(entra_identities={" " + _FAKE_OID_1: {"id": "colombod"}})

    def test_guid_trailing_space_rejected(self) -> None:
        """GUID key with a trailing space is rejected — re.fullmatch anchors both ends."""
        from pydantic import ValidationError

        from context_intelligence_server.config import Settings

        with pytest.raises(ValidationError, match="valid GUID"):
            Settings(entra_identities={_FAKE_OID_1 + " ": {"id": "colombod"}})

    def test_guid_g_hex_chars_rejected(self) -> None:
        """GUID key whose hex segments contain 'g' (not a valid hex digit) is rejected."""
        from pydantic import ValidationError

        from context_intelligence_server.config import Settings

        with pytest.raises(ValidationError, match="valid GUID"):
            Settings(
                entra_identities={
                    "gggggggg-gggg-gggg-gggg-gggggggggggg": {"id": "colombod"}
                }
            )

    def test_guid_fullwidth_digits_rejected(self) -> None:
        """GUID key with fullwidth Unicode digits (U+FF10-FF19, lookalikes for 0-9) is rejected.

        Fullwidth digit U+FF11 is visually similar to ASCII '1' (U+0031) but is NOT
        matched by [0-9a-f].  Confirms the regex anchors on ASCII code points only.
        """
        from pydantic import ValidationError

        from context_intelligence_server.config import Settings

        fullwidth_1 = "\uff11"  # fullwidth digit 1 (U+FF11), visually '1' but NOT ASCII
        fullwidth_guid = (
            fullwidth_1 * 8
            + "-"
            + fullwidth_1 * 4
            + "-"
            + fullwidth_1 * 4
            + "-"
            + fullwidth_1 * 4
            + "-"
            + fullwidth_1 * 12
        )

        with pytest.raises(ValidationError, match="valid GUID"):
            Settings(entra_identities={fullwidth_guid: {"id": "colombod"}})

    def test_guid_with_zero_width_space_appended_rejected(self) -> None:
        """GUID key with a zero-width space (U+200B) appended is rejected.

        Invisible Unicode characters appended to an otherwise valid GUID must not
        silently bypass validation.  re.fullmatch() ensures the entire string is
        consumed — no invisible trailing junk is tolerated.
        """
        from pydantic import ValidationError

        from context_intelligence_server.config import Settings

        zwsp_guid = _FAKE_OID_1 + "\u200b"  # zero-width space (U+200B) appended

        with pytest.raises(ValidationError, match="valid GUID"):
            Settings(entra_identities={zwsp_guid: {"id": "colombod"}})

    def test_duplicate_oid_last_wins_via_python_dict(self) -> None:
        """Duplicate oid keys collapse to last-wins before the validator sees the data.

        Python dict construction and PyYAML parsing both deduplicate keys (last-wins)
        at their own level, so the validator never receives duplicates and cannot
        detect or reject them.  This test documents that expected behavior: the last
        value wins, no error is raised, and only one entry survives.

        Mirrors the 'duplicate digest keys' note in _validate_api_keys docstring.
        """
        from context_intelligence_server.config import Settings

        # Build dict programmatically to avoid SyntaxWarning for duplicate literal keys.
        # This simulates what a YAML file with duplicate keys would produce.
        d: dict[str, dict[str, str]] = {}
        d[_FAKE_OID_1] = {"id": "first-value"}
        d[_FAKE_OID_1] = {
            "id": "last-wins"
        }  # overwrite — last-wins, same as YAML behavior

        s = Settings(entra_identities=d)
        assert s.entra_identities is not None
        assert s.entra_identities[_FAKE_OID_1]["id"] == "last-wins"
        assert len(s.entra_identities) == 1

    def test_id_as_int_raises(self) -> None:
        """id as integer (e.g. YAML: 'id: 123') raises ValidationError.

        pydantic v2 dict[str, str] rejects int for a str field in lax mode
        (string_type error).  The custom isinstance(contributor_id, str) branch
        in _validate_entra_identities is therefore dead for this case — pydantic
        fires first and the mode='after' validator never runs.
        """
        from pydantic import ValidationError

        from context_intelligence_server.config import Settings

        with pytest.raises(ValidationError):
            Settings(
                entra_identities={_FAKE_OID_1: {"id": 123}}  # type: ignore[dict-item]
            )

    def test_id_as_bool_raises(self) -> None:
        """id as bool (e.g. YAML: 'id: true') raises ValidationError.

        pydantic v2 dict[str, str] rejects bool for a str field in lax mode
        (string_type error) — True/False are not coerced to 'True'/'False'.
        """
        from pydantic import ValidationError

        from context_intelligence_server.config import Settings

        with pytest.raises(ValidationError):
            Settings(
                entra_identities={_FAKE_OID_1: {"id": True}}  # type: ignore[dict-item]
            )

    def test_id_as_none_raises(self) -> None:
        """id as None (e.g. YAML: 'id: null') raises ValidationError.

        pydantic v2 dict[str, str] rejects None for a str field (string_type error).
        An explicit null in the YAML config is rejected before reaching our validator.
        """
        from pydantic import ValidationError

        from context_intelligence_server.config import Settings

        with pytest.raises(ValidationError):
            Settings(
                entra_identities={_FAKE_OID_1: {"id": None}}  # type: ignore[dict-item]
            )

    def test_non_dict_value_raises_pydantic_dict_type_not_custom_message(self) -> None:
        """Documents that pydantic dict_type coercion fires BEFORE the custom isinstance branch.

        _validate_entra_identities contains:

            if not isinstance(meta, dict):
                raise ValueError("... must be a dict with an 'id' field ...")

        This branch is DEAD CODE.  pydantic's dict[str, dict[str, str]] field type
        rejects a non-dict value (e.g. a bare string) with a 'dict_type' ValidationError
        before the mode='after' field validator is called.  The custom error message is
        therefore never surfaced to operators — they see pydantic's generic dict_type
        error instead.  The isinstance branch remains as a harmless defensive fallback.
        """
        from pydantic import ValidationError

        from context_intelligence_server.config import Settings

        with pytest.raises(ValidationError) as exc_info:
            Settings(
                entra_identities={_FAKE_OID_1: "colombod"}  # type: ignore[dict-item]
            )

        errors = exc_info.value.errors()
        error_types = {e["type"] for e in errors}
        assert "dict_type" in error_types, (
            "Expected 'dict_type' pydantic error (pydantic fires before custom isinstance branch);\n"
            f"got error types: {error_types!r}\nfull errors: {errors!r}"
        )


class TestT3Coexistence:
    """T3 hardening: auth_mode=entra with both api_keys and entra_identities coexist independently.

    Confirms there is no collision between the static-auth keystore and the Entra
    identity map: they are separate data structures with separate lookup paths.
    Neither map's entries appear in the other.
    """

    def test_api_keys_and_entra_identities_coexist(self) -> None:
        """auth_mode=entra + api_keys + entra_identities: both populate their stores independently."""
        from context_intelligence_server.config import Settings

        static_digest = "a" * 64

        s = Settings(
            auth_mode="entra",
            azure_client_id=_FAKE_CLIENT_ID,
            azure_tenant_id=_FAKE_TENANT_ID,
            api_keys={static_digest: {"id": "static-user"}},
            entra_identities={_FAKE_OID_1: {"id": "colombod"}},
        )

        ks = s.build_keystore()
        identity_map = s.build_identity_map()

        # Each resolver sees its own entries
        assert ks[static_digest] == "static-user"
        assert identity_map[_FAKE_OID_1] == "colombod"

        # No cross-contamination between the two maps
        assert static_digest not in identity_map, (
            "Static API key digest must not appear in the Entra identity map"
        )
        assert _FAKE_OID_1 not in ks, "Entra oid must not appear in the API keystore"


# ---------------------------------------------------------------------------
# M2 Phase 1: service_identities, service_data_role, reader_role
# ---------------------------------------------------------------------------

_SVC_OID_1 = "33333333-3333-3333-3333-333333333333"
_SVC_OID_2 = "44444444-4444-4444-4444-444444444444"


class TestServiceIdentitiesValidator:
    """M2-Phase1: _validate_service_identities enforces identical GUID rules to entra_identities.

    service_identities uses the shared _validate_identity_map() helper, so all rules
    from _validate_entra_identities apply identically.
    """

    def test_valid_map_accepted_and_keys_lowercased(self) -> None:
        """Valid map with uppercase GUID keys is accepted; keys normalized to lowercase.

        Uses _FAKE_OID_UPPER ("AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE") which contains
        actual uppercase hex letters so .lower() produces a different string.
        """
        from context_intelligence_server.config import Settings

        lower = _FAKE_OID_UPPER.lower()
        s = Settings(service_identities={_FAKE_OID_UPPER: {"id": "svc-agent"}})
        assert s.service_identities is not None
        assert lower in s.service_identities
        assert _FAKE_OID_UPPER not in s.service_identities
        assert s.service_identities[lower]["id"] == "svc-agent"

    def test_default_is_none(self) -> None:
        """service_identities defaults to None when not configured."""
        from context_intelligence_server.config import Settings

        s = Settings()
        assert s.service_identities is None

    def test_empty_dict_raises(self) -> None:
        """service_identities={} is a misconfiguration (fail-closed, mirrors entra_identities)."""
        from pydantic import ValidationError

        from context_intelligence_server.config import Settings

        with pytest.raises(ValidationError, match="at least one entry"):
            Settings(service_identities={})

    def test_non_guid_key_raises(self) -> None:
        """Non-GUID key raises ValidationError."""
        from pydantic import ValidationError

        from context_intelligence_server.config import Settings

        with pytest.raises(ValidationError, match="valid GUID"):
            Settings(service_identities={"not-a-guid": {"id": "svc"}})

    def test_all_zeros_guid_rejected(self) -> None:
        """All-zeros GUID is rejected (placeholder sentinel must not authorize anyone)."""
        from pydantic import ValidationError

        from context_intelligence_server.config import Settings

        with pytest.raises(ValidationError):
            Settings(
                service_identities={
                    "00000000-0000-0000-0000-000000000000": {"id": "svc"}
                }
            )

    def test_empty_id_raises(self) -> None:
        """Empty 'id' in value dict raises ValidationError."""
        from pydantic import ValidationError

        from context_intelligence_server.config import Settings

        with pytest.raises(ValidationError):
            Settings(service_identities={_SVC_OID_1: {"id": ""}})

    def test_missing_id_raises(self) -> None:
        """Missing 'id' in value dict raises ValidationError."""
        from pydantic import ValidationError

        from context_intelligence_server.config import Settings

        with pytest.raises(ValidationError):
            Settings(service_identities={_SVC_OID_1: {"label": "svc"}})

    def test_none_accepted(self) -> None:
        """service_identities=None is accepted (disables feature)."""
        from context_intelligence_server.config import Settings

        s = Settings(service_identities=None)
        assert s.service_identities is None

    def test_multiple_valid_entries(self) -> None:
        """Multiple valid entries all survive validation."""
        from context_intelligence_server.config import Settings

        s = Settings(
            service_identities={
                _SVC_OID_1: {"id": "svc-agent-1"},
                _SVC_OID_2: {"id": "svc-agent-2"},
            }
        )
        assert s.service_identities is not None
        assert s.service_identities[_SVC_OID_1]["id"] == "svc-agent-1"
        assert s.service_identities[_SVC_OID_2]["id"] == "svc-agent-2"


class TestBuildServiceIdentityMap:
    """M2-Phase1: build_service_identity_map() mirrors build_identity_map()."""

    def test_returns_oid_to_id_mapping(self) -> None:
        """build_service_identity_map() returns {oid_lower: id}."""
        from context_intelligence_server.config import Settings

        s = Settings(service_identities={_SVC_OID_1: {"id": "svc-agent"}})
        result = s.build_service_identity_map()
        assert result == {_SVC_OID_1: "svc-agent"}

    def test_none_returns_empty_dict(self) -> None:
        """None service_identities → empty dict."""
        from context_intelligence_server.config import Settings

        s = Settings(service_identities=None)
        assert s.build_service_identity_map() == {}

    def test_default_none_returns_empty_dict(self) -> None:
        """Default Settings() (service_identities=None) → empty dict."""
        from context_intelligence_server.config import Settings

        s = Settings()
        assert s.build_service_identity_map() == {}

    def test_uppercase_keys_lowercased_in_result(self) -> None:
        """Keys are lowercased in the result map (belt-and-suspenders).

        Uses _FAKE_OID_UPPER ("AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE") which contains
        actual uppercase hex letters so .lower() produces a different string.
        """
        from context_intelligence_server.config import Settings

        lower = _FAKE_OID_UPPER.lower()
        s = Settings(service_identities={_FAKE_OID_UPPER: {"id": "svc-agent"}})
        result = s.build_service_identity_map()
        assert lower in result
        assert _FAKE_OID_UPPER not in result
        assert result[lower] == "svc-agent"

    def test_multiple_entries(self) -> None:
        """Multiple entries all appear in the result map."""
        from context_intelligence_server.config import Settings

        s = Settings(
            service_identities={
                _SVC_OID_1: {"id": "svc-1"},
                _SVC_OID_2: {"id": "svc-2"},
            }
        )
        result = s.build_service_identity_map()
        assert result == {_SVC_OID_1: "svc-1", _SVC_OID_2: "svc-2"}


class TestServiceDataRole:
    """M2-Phase1: service_data_role — default 'Contributor', None→'' (disabled)."""

    def test_default_is_contributor(self) -> None:
        """service_data_role defaults to 'Contributor'."""
        from context_intelligence_server.config import Settings

        s = Settings()
        assert s.service_data_role == "Contributor"

    def test_none_normalized_to_empty_string(self) -> None:
        """service_data_role=None is normalized to '' (feature disabled)."""
        from context_intelligence_server.config import Settings

        s = Settings(service_data_role=None)  # type: ignore[arg-type]
        assert s.service_data_role == ""

    def test_custom_value_passes_through(self) -> None:
        """A custom role name passes through unchanged."""
        from context_intelligence_server.config import Settings

        s = Settings(service_data_role="DataWriter")
        assert s.service_data_role == "DataWriter"

    def test_empty_string_passes_through(self) -> None:
        """Empty string explicitly disables the role (passes through as-is)."""
        from context_intelligence_server.config import Settings

        s = Settings(service_data_role="")
        assert s.service_data_role == ""


class TestReaderRole:
    """M2-Phase1: reader_role — default 'Reader', None→'' (disabled)."""

    def test_default_is_reader(self) -> None:
        """reader_role defaults to 'Reader'."""
        from context_intelligence_server.config import Settings

        s = Settings()
        assert s.reader_role == "Reader"

    def test_none_normalized_to_empty_string(self) -> None:
        """reader_role=None is normalized to '' (feature disabled)."""
        from context_intelligence_server.config import Settings

        s = Settings(reader_role=None)  # type: ignore[arg-type]
        assert s.reader_role == ""

    def test_custom_value_passes_through(self) -> None:
        """A custom role name passes through unchanged."""
        from context_intelligence_server.config import Settings

        s = Settings(reader_role="ReadOnlyRole")
        assert s.reader_role == "ReadOnlyRole"

    def test_empty_string_passes_through(self) -> None:
        """Empty string explicitly disables the role (passes through as-is)."""
        from context_intelligence_server.config import Settings

        s = Settings(reader_role="")
        assert s.reader_role == ""


class TestM2RegressionEntraIdentities:
    """M2 regression: entra_identities + build_identity_map() unchanged after shared-helper refactor.

    These tests verify that refactoring to _validate_identity_map() /
    _build_identity_map_from() does not alter any observable behavior of the
    existing entra_identities validation and build_identity_map() paths.
    """

    def test_valid_entra_map_still_accepted(self) -> None:
        """Regression: valid entra_identities still accepted after shared-helper refactor."""
        from context_intelligence_server.config import Settings

        s = Settings(entra_identities={_FAKE_OID_1: {"id": "colombod"}})
        assert s.entra_identities is not None
        assert s.entra_identities[_FAKE_OID_1]["id"] == "colombod"

    def test_entra_empty_dict_now_returns_empty_dict(self) -> None:
        """entra_identities={} is now a SUPPORTED bootstrap state — returns {}.

        (Previously: raised ValidationError 'at least one entry'. allow_empty=True
        is passed ONLY for entra_identities; service_identities={} still raises —
        see test_service_identities_empty_dict_raises.)
        """
        from context_intelligence_server.config import Settings

        s = Settings(entra_identities={})
        assert s.entra_identities == {}

    def test_entra_invalid_guid_still_raises_valid_guid(self) -> None:
        """Regression: invalid GUID still raises 'valid GUID' after shared-helper refactor."""
        from pydantic import ValidationError

        from context_intelligence_server.config import Settings

        with pytest.raises(ValidationError, match="valid GUID"):
            Settings(entra_identities={"not-a-guid": {"id": "colombod"}})

    def test_entra_all_zeros_still_rejected(self) -> None:
        """Regression: all-zeros GUID still rejected after shared-helper refactor."""
        from pydantic import ValidationError

        from context_intelligence_server.config import Settings

        with pytest.raises(ValidationError):
            Settings(
                entra_identities={
                    "00000000-0000-0000-0000-000000000000": {"id": "colombod"}
                }
            )

    def test_entra_keys_still_lowercased(self) -> None:
        """Regression: UPPERCASE GUID keys still normalized to lowercase after refactor."""
        from context_intelligence_server.config import Settings

        lower = _FAKE_OID_UPPER.lower()
        s = Settings(entra_identities={_FAKE_OID_UPPER: {"id": "colombod"}})
        assert s.entra_identities is not None
        assert lower in s.entra_identities
        assert _FAKE_OID_UPPER not in s.entra_identities

    def test_build_identity_map_still_returns_oid_to_id(self) -> None:
        """Regression: build_identity_map() still returns {oid_lower: id} after refactor."""
        from context_intelligence_server.config import Settings

        s = Settings(entra_identities={_FAKE_OID_1: {"id": "colombod"}})
        result = s.build_identity_map()
        assert result[_FAKE_OID_1] == "colombod"

    def test_build_identity_map_none_still_returns_empty(self) -> None:
        """Regression: build_identity_map() with None entra_identities returns {} after refactor."""
        from context_intelligence_server.config import Settings

        s = Settings()
        assert s.build_identity_map() == {}


# ---------------------------------------------------------------------------
# Obsolete config-key warnings (headless refactor back-compat)
# ---------------------------------------------------------------------------


def test_obsolete_config_keys_warn_but_do_not_break_startup(
    tmp_path: Path,
    monkeypatch: "pytest.MonkeyPatch",
    caplog: "pytest.LogCaptureFixture",
) -> None:
    """A YAML carrying removed/renamed keys still boots, but warns for each.

    Simulates upgrading a live deployment whose server-config.yaml predates the
    headless refactor: web_ui_enabled and dashboard_inactive_timeout are dropped
    silently by pydantic, so config.py must log a warning naming each one.
    """
    import logging

    cfg = tmp_path / "server-config.yaml"
    cfg.write_text(
        "web_ui_enabled: false\n"
        "dashboard_inactive_timeout: 900.0\n"
        "neo4j_url: neo4j://localhost:7687\n"
        "neo4j_password: ''\n"
    )
    monkeypatch.setenv("AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE", str(cfg))

    from context_intelligence_server.config import Settings

    with caplog.at_level(logging.WARNING, logger="context_intelligence_server.config"):
        s = Settings()

    # Startup succeeded and the old override was ignored (default retained).
    assert not hasattr(s, "web_ui_enabled")
    assert s.status_inactive_timeout == 1800.0

    warnings = "\n".join(
        r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING
    )
    assert "web_ui_enabled" in warnings
    assert "dashboard_inactive_timeout" in warnings
    assert "status_inactive_timeout" in warnings  # migration hint present
