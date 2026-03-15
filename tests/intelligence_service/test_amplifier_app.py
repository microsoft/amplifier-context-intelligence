"""Tests for the AmplifierApp bundle lifecycle manager."""

import os
from collections.abc import Iterator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from intelligence_service.amplifier_app import AmplifierApp

# Patch target
PATCH_TARGET = "intelligence_service.amplifier_app.load_bundle"


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_load_bundle() -> Iterator[AsyncMock]:
    """Patch load_bundle for the duration of a test."""
    with patch(PATCH_TARGET, new_callable=AsyncMock) as m:
        yield m


@pytest.fixture
def mock_bundle_chain(mock_load_bundle: AsyncMock) -> tuple:
    """Set up a standard mock bundle chain: load → compose → prepare."""
    mock_loaded, mock_composed, mock_prepared = MagicMock(), MagicMock(), MagicMock()
    mock_load_bundle.return_value = mock_loaded
    mock_loaded.compose.return_value = mock_composed
    mock_composed.prepare = AsyncMock(return_value=mock_prepared)
    return mock_load_bundle, mock_loaded, mock_composed, mock_prepared


def make_app() -> AmplifierApp:
    """Return an AmplifierApp with standard test configuration."""
    return AmplifierApp(
        bundle_path="/path/to/bundle",
        routing_matrix="balanced",
        amplifier_home="/data/home",
    )


# ---------------------------------------------------------------------------
# Test 1: construction stores config and prepared is None before startup
# ---------------------------------------------------------------------------


def test_construction_stores_config_and_prepared_is_none() -> None:
    """Constructor stores config fields and prepared is None before startup."""
    app = AmplifierApp(
        bundle_path="/path/to/bundle",
        routing_matrix="balanced",
        amplifier_home="/data/home",
    )

    assert app._bundle_path == "/path/to/bundle"
    assert app._routing_matrix == "balanced"
    assert app._amplifier_home == "/data/home"
    assert app.prepared is None


# ---------------------------------------------------------------------------
# Test 2: startup calls load_bundle with path
# ---------------------------------------------------------------------------


async def test_startup_calls_load_bundle_with_path(
    mock_bundle_chain: tuple,
) -> None:
    """startup() calls load_bundle with the configured bundle_path."""
    mock_load_bundle, _, _, _ = mock_bundle_chain

    app = make_app()
    await app.startup()

    mock_load_bundle.assert_called_once_with("/path/to/bundle")


# ---------------------------------------------------------------------------
# Test 3: startup composes routing overlay (overlay.name == 'routing-config')
# ---------------------------------------------------------------------------


async def test_startup_composes_routing_overlay(
    mock_bundle_chain: tuple,
) -> None:
    """startup() composes a routing overlay with name == 'routing-config'."""
    _, mock_loaded, _, _ = mock_bundle_chain

    app = make_app()
    await app.startup()

    assert mock_loaded.compose.call_count == 1
    overlay = mock_loaded.compose.call_args[0][0]
    assert overlay.name == "routing-config"


# ---------------------------------------------------------------------------
# Test 4: startup calls prepare and sets prepared
# ---------------------------------------------------------------------------


async def test_startup_calls_prepare_and_sets_prepared(
    mock_bundle_chain: tuple,
) -> None:
    """startup() calls prepare() on the composed bundle and sets the prepared property."""
    _, _, mock_composed, mock_prepared = mock_bundle_chain

    app = make_app()
    await app.startup()

    mock_composed.prepare.assert_called_once()
    assert app.prepared is mock_prepared


# ---------------------------------------------------------------------------
# Test 5: reload swaps prepared bundle
# ---------------------------------------------------------------------------


async def test_reload_swaps_prepared_bundle(
    mock_bundle_chain: tuple,
) -> None:
    """reload() replaces the prepared bundle with a newly prepared one."""
    _, _, mock_composed, first_prepared = mock_bundle_chain
    second_prepared = MagicMock()
    mock_composed.prepare = AsyncMock(side_effect=[first_prepared, second_prepared])

    app = make_app()
    await app.startup()
    assert app.prepared is first_prepared

    await app.reload()

    assert app.prepared is second_prepared


# ---------------------------------------------------------------------------
# Test 6: reload keeps old prepared on failure
# ---------------------------------------------------------------------------


async def test_reload_keeps_old_prepared_on_failure(
    mock_bundle_chain: tuple,
) -> None:
    """reload() keeps the old PreparedBundle when an error occurs during reload."""
    _, _, mock_composed, first_prepared = mock_bundle_chain
    mock_composed.prepare = AsyncMock(
        side_effect=[first_prepared, RuntimeError("prepare failed")]
    )

    app = make_app()
    await app.startup()
    assert app.prepared is first_prepared

    with pytest.raises(RuntimeError, match="prepare failed"):
        await app.reload()

    assert app.prepared is first_prepared


# ---------------------------------------------------------------------------
# Test 7: close clears prepared
# ---------------------------------------------------------------------------


async def test_close_clears_prepared(
    mock_bundle_chain: tuple,
) -> None:
    """close() sets prepared to None."""
    _, _, _, mock_prepared = mock_bundle_chain

    app = make_app()
    await app.startup()
    assert app.prepared is mock_prepared

    await app.close()

    assert app.prepared is None


# ---------------------------------------------------------------------------
# Test 8: startup sets AMPLIFIER_HOME env var
# ---------------------------------------------------------------------------


async def test_startup_sets_amplifier_home_env_var(
    mock_bundle_chain: tuple,
) -> None:
    """startup() sets AMPLIFIER_HOME env var before loading the bundle."""
    app = make_app()
    await app.startup()
    assert os.environ.get("AMPLIFIER_HOME") == "/data/home"


# ---------------------------------------------------------------------------
# Test 9: close() calls prepared.close()
# ---------------------------------------------------------------------------


async def test_close_calls_prepared_close(
    mock_bundle_chain: tuple,
) -> None:
    """close() calls close() on the PreparedBundle if it has one."""
    _, _, _, mock_prepared = mock_bundle_chain
    mock_prepared.close = AsyncMock()
    app = make_app()
    await app.startup()
    await app.close()
    mock_prepared.close.assert_called_once()
    assert app.prepared is None


# ---------------------------------------------------------------------------
# Test 10: reload() closes old prepared after successful swap
# ---------------------------------------------------------------------------


async def test_reload_closes_old_prepared(
    mock_bundle_chain: tuple,
) -> None:
    """reload() closes the old PreparedBundle after successful swap."""
    _, _, mock_composed, first_prepared = mock_bundle_chain
    first_prepared.close = AsyncMock()
    second_prepared = MagicMock()
    mock_composed.prepare = AsyncMock(side_effect=[first_prepared, second_prepared])
    app = make_app()
    await app.startup()
    await app.reload()
    first_prepared.close.assert_called_once()
    assert app.prepared is second_prepared


# ---------------------------------------------------------------------------
# Test 11: routing overlay hook has correct module and config content
# ---------------------------------------------------------------------------


async def test_startup_routing_overlay_has_correct_hook_content(
    mock_bundle_chain: tuple,
) -> None:
    """Routing overlay hook has module='hooks-routing' and config with the matrix."""
    _, mock_loaded, _, _ = mock_bundle_chain
    app = make_app()
    await app.startup()
    overlay = mock_loaded.compose.call_args[0][0]
    assert len(overlay.hooks) == 1
    hook = overlay.hooks[0]
    assert hook["module"] == "hooks-routing"
    assert hook["config"]["default_matrix"] == "balanced"


# ---------------------------------------------------------------------------
# Test 12: startup failure leaves prepared as None
# ---------------------------------------------------------------------------


async def test_startup_failure_leaves_prepared_none(
    mock_load_bundle: AsyncMock,
) -> None:
    """When load_bundle raises during startup, prepared remains None."""
    mock_load_bundle.side_effect = RuntimeError("bundle not found")
    app = make_app()
    with pytest.raises(RuntimeError, match="bundle not found"):
        await app.startup()
    assert app.prepared is None
