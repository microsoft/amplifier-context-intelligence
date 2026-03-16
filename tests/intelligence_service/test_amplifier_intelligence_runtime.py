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
) -> None:
    """startup() composes runtime-config Bundle with hooks-routing hook onto composed_with_telemetry."""
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
