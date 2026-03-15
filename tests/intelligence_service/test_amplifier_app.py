"""Tests for the AmplifierApp bundle lifecycle manager."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from intelligence_service.amplifier_app import AmplifierApp

# Patch target
PATCH_TARGET = "intelligence_service.amplifier_app.load_bundle"


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


@patch(PATCH_TARGET, new_callable=AsyncMock)
async def test_startup_calls_load_bundle_with_path(mock_load_bundle: AsyncMock) -> None:
    """startup() calls load_bundle with the configured bundle_path."""
    mock_loaded = MagicMock()
    mock_composed = MagicMock()
    mock_prepared = MagicMock()

    mock_load_bundle.return_value = mock_loaded
    mock_loaded.compose.return_value = mock_composed
    mock_composed.prepare = AsyncMock(return_value=mock_prepared)

    app = AmplifierApp(
        bundle_path="/path/to/bundle",
        routing_matrix="balanced",
        amplifier_home="/data/home",
    )
    await app.startup()

    mock_load_bundle.assert_called_once_with("/path/to/bundle")


# ---------------------------------------------------------------------------
# Test 3: startup composes routing overlay (overlay.name == 'routing-config')
# ---------------------------------------------------------------------------


@patch(PATCH_TARGET, new_callable=AsyncMock)
async def test_startup_composes_routing_overlay(mock_load_bundle: AsyncMock) -> None:
    """startup() composes a routing overlay with name == 'routing-config'."""
    mock_loaded = MagicMock()
    mock_composed = MagicMock()
    mock_prepared = MagicMock()

    mock_load_bundle.return_value = mock_loaded
    mock_loaded.compose.return_value = mock_composed
    mock_composed.prepare = AsyncMock(return_value=mock_prepared)

    app = AmplifierApp(
        bundle_path="/path/to/bundle",
        routing_matrix="balanced",
        amplifier_home="/data/home",
    )
    await app.startup()

    assert mock_loaded.compose.call_count == 1
    overlay = mock_loaded.compose.call_args[0][0]
    assert overlay.name == "routing-config"


# ---------------------------------------------------------------------------
# Test 4: startup calls prepare and sets prepared
# ---------------------------------------------------------------------------


@patch(PATCH_TARGET, new_callable=AsyncMock)
async def test_startup_calls_prepare_and_sets_prepared(
    mock_load_bundle: AsyncMock,
) -> None:
    """startup() calls prepare() on the composed bundle and sets the prepared property."""
    mock_loaded = MagicMock()
    mock_composed = MagicMock()
    mock_prepared = MagicMock()

    mock_load_bundle.return_value = mock_loaded
    mock_loaded.compose.return_value = mock_composed
    mock_composed.prepare = AsyncMock(return_value=mock_prepared)

    app = AmplifierApp(
        bundle_path="/path/to/bundle",
        routing_matrix="balanced",
        amplifier_home="/data/home",
    )
    await app.startup()

    mock_composed.prepare.assert_called_once()
    assert app.prepared is mock_prepared


# ---------------------------------------------------------------------------
# Test 5: reload swaps prepared bundle
# ---------------------------------------------------------------------------


@patch(PATCH_TARGET, new_callable=AsyncMock)
async def test_reload_swaps_prepared_bundle(mock_load_bundle: AsyncMock) -> None:
    """reload() replaces the prepared bundle with a newly prepared one."""
    mock_loaded = MagicMock()
    mock_composed = MagicMock()
    first_prepared = MagicMock()
    second_prepared = MagicMock()

    mock_load_bundle.return_value = mock_loaded
    mock_loaded.compose.return_value = mock_composed
    mock_composed.prepare = AsyncMock(side_effect=[first_prepared, second_prepared])

    app = AmplifierApp(
        bundle_path="/path/to/bundle",
        routing_matrix="balanced",
        amplifier_home="/data/home",
    )
    await app.startup()
    assert app.prepared is first_prepared

    await app.reload()

    assert app.prepared is second_prepared


# ---------------------------------------------------------------------------
# Test 6: reload keeps old prepared on failure
# ---------------------------------------------------------------------------


@patch(PATCH_TARGET, new_callable=AsyncMock)
async def test_reload_keeps_old_prepared_on_failure(
    mock_load_bundle: AsyncMock,
) -> None:
    """reload() keeps the old PreparedBundle when an error occurs during reload."""
    mock_loaded = MagicMock()
    mock_composed = MagicMock()
    first_prepared = MagicMock()

    mock_load_bundle.return_value = mock_loaded
    mock_loaded.compose.return_value = mock_composed
    mock_composed.prepare = AsyncMock(
        side_effect=[first_prepared, RuntimeError("prepare failed")]
    )

    app = AmplifierApp(
        bundle_path="/path/to/bundle",
        routing_matrix="balanced",
        amplifier_home="/data/home",
    )
    await app.startup()
    assert app.prepared is first_prepared

    with pytest.raises(RuntimeError, match="prepare failed"):
        await app.reload()

    assert app.prepared is first_prepared


# ---------------------------------------------------------------------------
# Test 7: close clears prepared
# ---------------------------------------------------------------------------


@patch(PATCH_TARGET, new_callable=AsyncMock)
async def test_close_clears_prepared(mock_load_bundle: AsyncMock) -> None:
    """close() sets prepared to None."""
    mock_loaded = MagicMock()
    mock_composed = MagicMock()
    mock_prepared = MagicMock()

    mock_load_bundle.return_value = mock_loaded
    mock_loaded.compose.return_value = mock_composed
    mock_composed.prepare = AsyncMock(return_value=mock_prepared)

    app = AmplifierApp(
        bundle_path="/path/to/bundle",
        routing_matrix="balanced",
        amplifier_home="/data/home",
    )
    await app.startup()
    assert app.prepared is mock_prepared

    await app.close()

    assert app.prepared is None
