"""Tests for AmplifierIntelligenceRuntime - 6-phase composition pipeline."""

from __future__ import annotations

import logging
import os
from collections.abc import Iterator
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from intelligence_service.amplifier_intelligence_runtime import (
    WELL_KNOWN_BUNDLES,
    AmplifierIntelligenceRuntime,
    _expand_bundle_hook_configs,
    _expand_env_vars,
)

# Patch targets
PATCH_LOAD_BUNDLE = "intelligence_service.amplifier_intelligence_runtime.load_bundle"
PATCH_BUNDLE_REGISTRY = (
    "intelligence_service.amplifier_intelligence_runtime.BundleRegistry"
)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def make_runtime(
    runtime_state_path: str = "/data/runtime",
) -> AmplifierIntelligenceRuntime:
    """Return an AmplifierIntelligenceRuntime with standard test configuration."""
    return AmplifierIntelligenceRuntime(
        routing_matrix="balanced",
        runtime_state_path=runtime_state_path,
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_registry_and_load() -> Iterator[tuple]:
    """Patch BundleRegistry and load_bundle for the duration of a test."""
    with (
        patch(PATCH_BUNDLE_REGISTRY) as mock_registry_cls,
        patch(PATCH_LOAD_BUNDLE, new_callable=AsyncMock) as mock_load,
    ):
        mock_registry = MagicMock()
        mock_registry_cls.return_value = mock_registry
        yield mock_registry_cls, mock_registry, mock_load


@pytest.fixture
def mock_composition_chain(mock_registry_and_load: tuple) -> tuple:
    """Set up a full mock composition chain.

    Returns (mock_registry_cls, mock_registry, mock_load, base_bundle,
             server_bundle, telemetry_bundle, composed_with_server,
             composed_with_telemetry, composed_with_config, final_prepared).
    """
    mock_registry_cls, mock_registry, mock_load = mock_registry_and_load

    base_bundle = MagicMock()
    base_bundle.base_path = None
    server_bundle = MagicMock()
    telemetry_bundle = MagicMock()
    composed_with_server = MagicMock()
    composed_with_telemetry = MagicMock()
    composed_with_config = MagicMock()
    final_prepared = MagicMock()

    mock_load.side_effect = [base_bundle, server_bundle, telemetry_bundle]
    base_bundle.compose.return_value = composed_with_server
    composed_with_server.compose.return_value = composed_with_telemetry
    composed_with_telemetry.compose.return_value = composed_with_config
    composed_with_config.prepare = AsyncMock(return_value=final_prepared)

    return (
        mock_registry_cls,
        mock_registry,
        mock_load,
        base_bundle,
        server_bundle,
        telemetry_bundle,
        composed_with_server,
        composed_with_telemetry,
        composed_with_config,
        final_prepared,
    )


# ---------------------------------------------------------------------------
# Test 1: WELL_KNOWN_BUNDLES has exactly 4 entries
# ---------------------------------------------------------------------------


def test_well_known_bundles_has_four_entries() -> None:
    """WELL_KNOWN_BUNDLES contains exactly 4 well-known bundle entries."""
    assert len(WELL_KNOWN_BUNDLES) == 4
    assert "foundation" in WELL_KNOWN_BUNDLES
    assert "amplifier-dev" in WELL_KNOWN_BUNDLES
    assert "context-intelligence-server" in WELL_KNOWN_BUNDLES
    assert "context-intelligence" in WELL_KNOWN_BUNDLES


# ---------------------------------------------------------------------------
# Test 2: construction stores config, prepared is None
# ---------------------------------------------------------------------------


def test_construction_stores_config_and_prepared_is_none() -> None:
    """Constructor stores config fields and prepared is None before startup."""
    runtime = AmplifierIntelligenceRuntime(
        routing_matrix="balanced",
        runtime_state_path="/data/runtime",
    )

    assert runtime._routing_matrix == "balanced"
    assert runtime._runtime_state_path == "/data/runtime"
    assert runtime.prepared is None


# ---------------------------------------------------------------------------
# Test 3: startup sets AMPLIFIER_HOME env var
# ---------------------------------------------------------------------------


async def test_startup_sets_amplifier_home_env_var(
    mock_composition_chain: tuple,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """startup() sets AMPLIFIER_HOME to runtime_state_path."""
    monkeypatch.delenv("AMPLIFIER_HOME", raising=False)
    runtime = make_runtime()
    await runtime.startup()
    assert os.environ.get("AMPLIFIER_HOME") == "/data/runtime"


# ---------------------------------------------------------------------------
# Test 4: startup creates registry and registers well-known bundles
# ---------------------------------------------------------------------------


async def test_startup_creates_registry_and_registers_bundles(
    mock_composition_chain: tuple,
) -> None:
    """startup() creates a BundleRegistry and registers WELL_KNOWN_BUNDLES."""
    mock_registry_cls, mock_registry, *_ = mock_composition_chain

    runtime = make_runtime()
    await runtime.startup()

    mock_registry_cls.assert_called_once()
    mock_registry.register.assert_called_once_with(WELL_KNOWN_BUNDLES)


# ---------------------------------------------------------------------------
# Test 5: startup calls load_bundle in correct order (amplifier-dev first)
# ---------------------------------------------------------------------------


async def test_startup_calls_load_bundle_with_names_and_registry(
    mock_composition_chain: tuple,
) -> None:
    """startup() loads amplifier-dev first, context-intelligence-server second, context-intelligence third."""
    mock_registry_cls, mock_registry, mock_load, *_ = mock_composition_chain

    runtime = make_runtime()
    await runtime.startup()

    assert mock_load.call_count == 3
    calls = mock_load.call_args_list
    # amplifier-dev must be first
    assert calls[0] == call("amplifier-dev", registry=mock_registry)
    assert calls[1] == call("context-intelligence-server", registry=mock_registry)
    assert calls[2] == call("context-intelligence", registry=mock_registry)


# ---------------------------------------------------------------------------
# Test 6: startup composes server bundle onto base
# ---------------------------------------------------------------------------


async def test_startup_composes_server_bundle_onto_base(
    mock_composition_chain: tuple,
) -> None:
    """startup() composes context-intelligence-server onto the base (amplifier-dev)."""
    _, _, _, base_bundle, server_bundle, _, composed_with_server, *_ = (
        mock_composition_chain
    )

    runtime = make_runtime()
    await runtime.startup()

    base_bundle.compose.assert_called_once_with(server_bundle)


# ---------------------------------------------------------------------------
# Test 7: startup composes context-intelligence (telemetry) onto server bundle
# ---------------------------------------------------------------------------


async def test_startup_composes_telemetry_bundle(
    mock_composition_chain: tuple,
) -> None:
    """startup() composes context-intelligence (telemetry) onto the server-composed bundle."""
    _, _, _, _, _, telemetry_bundle, composed_with_server, *_ = mock_composition_chain

    runtime = make_runtime()
    await runtime.startup()

    composed_with_server.compose.assert_called_once_with(telemetry_bundle)


# ---------------------------------------------------------------------------
# Test 8: startup composes runtime-config with hooks-routing
# ---------------------------------------------------------------------------


async def test_startup_composes_runtime_config_with_hooks_routing(
    mock_composition_chain: tuple,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """startup() composes runtime-config Bundle with hooks-routing hook onto composed_with_telemetry."""
    # Clear provider env vars to prevent env leakage into provider detection
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("AZURE_OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)

    *_, composed_with_telemetry, _, _ = mock_composition_chain

    runtime = make_runtime()
    await runtime.startup()

    assert composed_with_telemetry.compose.call_count == 1
    runtime_config = composed_with_telemetry.compose.call_args[0][0]

    assert runtime_config.name == "runtime-config"
    assert len(runtime_config.hooks) == 1
    hook = runtime_config.hooks[0]
    assert hook["module"] == "hooks-routing"
    assert hook["config"]["default_matrix"] == "balanced"


# ---------------------------------------------------------------------------
# Test 9: startup calls prepare() and sets _prepared
# ---------------------------------------------------------------------------


async def test_startup_calls_prepare_and_sets_prepared(
    mock_composition_chain: tuple,
) -> None:
    """startup() calls prepare() and stores result in _prepared."""
    *_, composed_with_config, final_prepared = mock_composition_chain

    runtime = make_runtime()
    await runtime.startup()

    composed_with_config.prepare.assert_called_once()
    assert runtime.prepared is final_prepared


# ---------------------------------------------------------------------------
# Test 10: close() clears prepared
# ---------------------------------------------------------------------------


async def test_close_clears_prepared(
    mock_composition_chain: tuple,
) -> None:
    """close() sets prepared to None."""
    *_, final_prepared = mock_composition_chain

    runtime = make_runtime()
    await runtime.startup()
    assert runtime.prepared is final_prepared

    await runtime.close()
    assert runtime.prepared is None


# ---------------------------------------------------------------------------
# Test 11: close() calls prepared.close()
# ---------------------------------------------------------------------------


async def test_close_calls_prepared_close(
    mock_composition_chain: tuple,
) -> None:
    """close() calls close() on the prepared bundle."""
    *_, final_prepared = mock_composition_chain
    final_prepared.close = AsyncMock()

    runtime = make_runtime()
    await runtime.startup()
    await runtime.close()

    final_prepared.close.assert_called_once()
    assert runtime.prepared is None


# ---------------------------------------------------------------------------
# Test 12: startup failure leaves prepared as None
# ---------------------------------------------------------------------------


async def test_startup_failure_leaves_prepared_none(
    mock_registry_and_load: tuple,
) -> None:
    """When load_bundle raises during startup, prepared remains None."""
    _, _, mock_load = mock_registry_and_load
    mock_load.side_effect = RuntimeError("load failed")

    runtime = make_runtime()
    with pytest.raises(RuntimeError, match="load failed"):
        await runtime.startup()

    assert runtime.prepared is None


# ---------------------------------------------------------------------------
# Test 13: startup emits log messages
# ---------------------------------------------------------------------------


async def test_startup_emits_log_messages(
    mock_composition_chain: tuple,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """startup() emits log messages referencing registry, amplifier-dev, and prepare."""
    runtime = make_runtime()
    with caplog.at_level(logging.DEBUG, logger="intelligence_service.runtime"):
        await runtime.startup()

    messages = " ".join(caplog.messages).lower()
    assert "registry" in messages
    assert "amplifier-dev" in messages
    assert "prepare" in messages


# ---------------------------------------------------------------------------
# Test 14: startup adds FileHandler pointing at runtime_state_path/runtime.log
# ---------------------------------------------------------------------------


async def test_startup_adds_file_handler(
    mock_composition_chain: tuple,
    tmp_path: pytest.TempPathFactory,
) -> None:
    """startup() adds a FileHandler to the runtime logger at runtime.log."""
    runtime = AmplifierIntelligenceRuntime(
        routing_matrix="balanced",
        runtime_state_path=str(tmp_path),
    )
    await runtime.startup()

    logger = logging.getLogger("intelligence_service.runtime")
    file_handlers = [h for h in logger.handlers if isinstance(h, logging.FileHandler)]

    expected_log_path = str(tmp_path / "runtime.log")
    assert any(h.baseFilename == expected_log_path for h in file_handlers)

    # Clean up to avoid polluting other tests
    await runtime.close()


# ===========================================================================
# Env-var expansion tests
# ===========================================================================


# ---------------------------------------------------------------------------
# Test 15: _expand_env_vars resolves a set env var
# ---------------------------------------------------------------------------


class TestExpandEnvVars:
    """Unit tests for the _expand_env_vars helper function."""

    def test_resolves_set_env_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """${VAR} is replaced by the env var value when set."""
        monkeypatch.setenv("MY_TEST_VAR", "http://server:8000")
        result = _expand_env_vars("${MY_TEST_VAR:}")
        assert result == "http://server:8000"

    def test_uses_default_when_env_var_unset(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """${VAR:default} uses the default when the env var is not set."""
        monkeypatch.delenv("MY_TEST_VAR", raising=False)
        result = _expand_env_vars("${MY_TEST_VAR:fallback-value}")
        assert result == "fallback-value"

    def test_empty_default_when_env_var_unset(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """${VAR:} resolves to empty string when env var is not set."""
        monkeypatch.delenv("MY_TEST_VAR", raising=False)
        result = _expand_env_vars("${MY_TEST_VAR:}")
        assert result == ""

    def test_no_default_resolves_to_empty(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """${VAR} (no colon) resolves to empty string when env var is not set."""
        monkeypatch.delenv("MY_TEST_VAR", raising=False)
        result = _expand_env_vars("${MY_TEST_VAR}")
        assert result == ""

    def test_env_var_overrides_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """When env var is set, its value takes precedence over the default."""
        monkeypatch.setenv("MY_TEST_VAR", "actual-value")
        result = _expand_env_vars("${MY_TEST_VAR:ignored-default}")
        assert result == "actual-value"

    def test_recurses_into_dicts(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Expansion walks into nested dict values."""
        monkeypatch.setenv("URL_VAR", "http://host:8000")
        config = {"url": "${URL_VAR:}", "nested": {"level": "${URL_VAR:}"}}
        result = _expand_env_vars(config)
        assert result == {
            "url": "http://host:8000",
            "nested": {"level": "http://host:8000"},
        }

    def test_recurses_into_lists(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Expansion walks into list items."""
        monkeypatch.setenv("ITEM_VAR", "resolved")
        result = _expand_env_vars(["${ITEM_VAR:}", "literal"])
        assert result == ["resolved", "literal"]

    def test_leaves_non_strings_untouched(self) -> None:
        """Integers, booleans, and None pass through unchanged."""
        assert _expand_env_vars(42) == 42
        assert _expand_env_vars(True) is True
        assert _expand_env_vars(None) is None

    def test_multiple_vars_in_one_string(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Multiple ${VAR} references in a single string are all expanded."""
        monkeypatch.setenv("HOST", "localhost")
        monkeypatch.setenv("PORT", "7687")
        result = _expand_env_vars("bolt://${HOST:}:${PORT:}")
        assert result == "bolt://localhost:7687"


# ---------------------------------------------------------------------------
# Test 16: _expand_bundle_hook_configs mutates hook configs in-place
# ---------------------------------------------------------------------------


class TestExpandBundleHookConfigs:
    """Unit tests for the _expand_bundle_hook_configs helper."""

    def test_expands_hook_configs_on_bundle(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Hook config values containing ${VAR:} are expanded in-place."""
        monkeypatch.setenv(
            "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_URL",
            "http://context-intelligence-server:8000",
        )
        bundle = MagicMock()
        bundle.hooks = [
            {
                "module": "hook-context-intelligence",
                "config": {
                    "context_intelligence_server_url": "${AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_URL:}",
                    "log_level": "${AMPLIFIER_CONTEXT_INTELLIGENCE_LOG_LEVEL:INFO}",
                },
            }
        ]

        _expand_bundle_hook_configs(bundle)

        assert bundle.hooks[0]["config"]["context_intelligence_server_url"] == (
            "http://context-intelligence-server:8000"
        )
        assert bundle.hooks[0]["config"]["log_level"] == "INFO"

    def test_noop_when_bundle_has_no_hooks(self) -> None:
        """Bundles without a hooks attribute are silently skipped."""
        bundle = MagicMock(spec=[])  # no attributes at all
        _expand_bundle_hook_configs(bundle)  # should not raise

    def test_noop_when_hooks_is_empty(self) -> None:
        """Bundles with an empty hooks list are silently skipped."""
        bundle = MagicMock()
        bundle.hooks = []
        _expand_bundle_hook_configs(bundle)  # should not raise

    def test_skips_hooks_without_config_key(self) -> None:
        """Hook dicts that lack a 'config' key are left alone."""
        bundle = MagicMock()
        bundle.hooks = [{"module": "some-hook"}]
        _expand_bundle_hook_configs(bundle)
        assert bundle.hooks[0] == {"module": "some-hook"}


# ---------------------------------------------------------------------------
# Test 17: startup() expands env vars in loaded bundles
# ---------------------------------------------------------------------------


async def test_startup_expands_env_vars_in_telemetry_bundle(
    mock_composition_chain: tuple,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """startup() expands ${VAR:} templates in the telemetry bundle's hook configs."""
    monkeypatch.setenv(
        "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_URL",
        "http://context-intelligence-server:8000",
    )
    _, _, _, _, _, telemetry_bundle, *_ = mock_composition_chain

    # Give the mock telemetry bundle a hooks list with unexpanded config
    telemetry_bundle.hooks = [
        {
            "module": "hook-context-intelligence",
            "config": {
                "context_intelligence_server_url": "${AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_URL:}",
            },
        }
    ]

    runtime = make_runtime()
    await runtime.startup()

    assert telemetry_bundle.hooks[0]["config"]["context_intelligence_server_url"] == (
        "http://context-intelligence-server:8000"
    )
