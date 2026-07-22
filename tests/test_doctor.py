"""Tests for context_intelligence_server.doctor -- the `doctor` / `doctor --fix`
CLI gesture that replaced the two O(graph-size) migration scans formerly run
unconditionally at cold start.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from context_intelligence_server import doctor as doctor_module


def _fake_driver() -> MagicMock:
    driver = MagicMock()
    driver.verify_connectivity = AsyncMock(return_value=None)
    driver.close = AsyncMock(return_value=None)
    return driver


@pytest.fixture(autouse=True)
def _patch_settings_and_driver():
    """Every test patches get_settings/build_neo4j_driver so no real config
    or Neo4j connection is required."""
    with (
        patch.object(doctor_module, "get_settings") as mock_get_settings,
        patch.object(doctor_module, "build_neo4j_driver") as mock_build_driver,
    ):
        mock_get_settings.return_value.resolve_neo4j_admin.return_value = "admin-cfg"
        mock_build_driver.return_value = _fake_driver()
        yield mock_build_driver.return_value


async def test_run_doctor_healthy_returns_zero(_patch_settings_and_driver) -> None:
    driver = _patch_settings_and_driver
    with patch.object(
        doctor_module,
        "diagnose",
        AsyncMock(return_value={"untagged_nodes": 0, "duplicate_nodes": 0}),
    ):
        code = await doctor_module.run_doctor(fix=False)

    assert code == 0
    driver.close.assert_awaited_once()


async def test_run_doctor_unhealthy_report_only_returns_nonzero(
    _patch_settings_and_driver,
) -> None:
    with patch.object(
        doctor_module,
        "diagnose",
        AsyncMock(return_value={"untagged_nodes": 5, "duplicate_nodes": 0}),
    ):
        code = await doctor_module.run_doctor(fix=False)

    assert code != 0


async def test_run_doctor_fix_calls_run_repair_and_returns_zero_when_healthy_after(
    _patch_settings_and_driver,
) -> None:
    diagnose_mock = AsyncMock(
        side_effect=[
            {"untagged_nodes": 5, "duplicate_nodes": 2},  # before repair
            {"untagged_nodes": 0, "duplicate_nodes": 0},  # after repair
        ]
    )
    repair_mock = AsyncMock(return_value={"duplicates_removed": 2, "nodes_tagged": 5})
    with (
        patch.object(doctor_module, "diagnose", diagnose_mock),
        patch.object(doctor_module, "run_repair", repair_mock),
    ):
        code = await doctor_module.run_doctor(fix=True)

    assert code == 0
    repair_mock.assert_awaited_once()
    assert diagnose_mock.await_count == 2


async def test_run_doctor_fix_returns_nonzero_when_still_unhealthy_after(
    _patch_settings_and_driver,
) -> None:
    diagnose_mock = AsyncMock(
        side_effect=[
            {"untagged_nodes": 5, "duplicate_nodes": 0},
            {"untagged_nodes": 3, "duplicate_nodes": 0},  # repair left residual
        ]
    )
    repair_mock = AsyncMock(return_value={"duplicates_removed": 0, "nodes_tagged": 2})
    with (
        patch.object(doctor_module, "diagnose", diagnose_mock),
        patch.object(doctor_module, "run_repair", repair_mock),
    ):
        code = await doctor_module.run_doctor(fix=True)

    assert code != 0


async def test_run_doctor_fix_does_not_repair_already_healthy_graph(
    _patch_settings_and_driver,
) -> None:
    """fix=True on an already-healthy graph must not invoke run_repair at all."""
    diagnose_mock = AsyncMock(return_value={"untagged_nodes": 0, "duplicate_nodes": 0})
    repair_mock = AsyncMock()
    with (
        patch.object(doctor_module, "diagnose", diagnose_mock),
        patch.object(doctor_module, "run_repair", repair_mock),
    ):
        code = await doctor_module.run_doctor(fix=True)

    assert code == 0
    repair_mock.assert_not_awaited()


async def test_run_doctor_neo4j_unreachable_returns_nonzero(
    _patch_settings_and_driver,
) -> None:
    driver = _patch_settings_and_driver
    driver.verify_connectivity = AsyncMock(
        side_effect=RuntimeError("connection refused")
    )

    code = await doctor_module.run_doctor(fix=False)

    assert code != 0
    driver.close.assert_awaited_once()


async def test_run_doctor_closes_driver_even_on_unreachable(
    _patch_settings_and_driver,
) -> None:
    """The driver must be closed (finally-block) even when unreachable."""
    driver = _patch_settings_and_driver
    driver.verify_connectivity = AsyncMock(side_effect=RuntimeError("down"))

    await doctor_module.run_doctor(fix=True)

    driver.close.assert_awaited_once()
