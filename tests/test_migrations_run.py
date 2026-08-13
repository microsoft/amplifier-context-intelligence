"""Tests for migrations/run.py -- the standalone, out-of-band graph
rectification CLI. Unit-level only (mocked driver/config); real
rectification against a live Neo4j is DTU-validated separately.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from migrations import run as run_module


def _fake_driver() -> MagicMock:
    driver = MagicMock()
    driver.verify_connectivity = AsyncMock(return_value=None)
    driver.close = AsyncMock(return_value=None)
    session_cm = MagicMock()
    session_cm.__aenter__ = AsyncMock(return_value=session_cm)
    session_cm.__aexit__ = AsyncMock(return_value=False)
    driver.session = MagicMock(return_value=session_cm)
    return driver


@pytest.fixture(autouse=True)
def _patch_settings_and_driver():
    """Every test patches get_settings/build_neo4j_driver so no real config
    or Neo4j connection is required (mirrors tests/test_doctor.py)."""
    with (
        patch.object(run_module, "get_settings") as mock_get_settings,
        patch.object(run_module, "build_neo4j_driver") as mock_build_driver,
    ):
        admin_config = MagicMock()
        admin_config.model_copy.return_value = "admin-cfg"
        mock_get_settings.return_value.resolve_neo4j_admin.return_value = admin_config
        mock_build_driver.return_value = _fake_driver()
        yield mock_build_driver.return_value


def test_module_imports_cleanly() -> None:
    """The module must import without touching Neo4j (no side effects at
    import time) -- required for the DTU update script's `grep` probe."""
    import migrations.run  # noqa: F401 -- import-cleanliness check


def test_status_flag_exists_and_parses() -> None:
    args = run_module.build_parser().parse_args(["--status"])
    assert args.status is True
    assert args.apply is False


def test_apply_flag_exists_and_parses() -> None:
    args = run_module.build_parser().parse_args(["--apply"])
    assert args.apply is True
    assert args.status is False


def test_status_and_apply_are_mutually_exclusive() -> None:
    with pytest.raises(SystemExit):
        run_module.build_parser().parse_args(["--status", "--apply"])


def test_one_of_status_or_apply_is_required() -> None:
    with pytest.raises(SystemExit):
        run_module.build_parser().parse_args([])


def test_neo4j_overrides_are_optional_flags() -> None:
    args = run_module.build_parser().parse_args(
        [
            "--status",
            "--neo4j-url",
            "bolt://example:7687",
            "--neo4j-user",
            "u",
            "--neo4j-password",
            "p",
        ]
    )
    assert args.neo4j_url == "bolt://example:7687"
    assert args.neo4j_user == "u"
    assert args.neo4j_password == "p"


def test_banner_declares_from_to_and_out_of_band(
    capsys: pytest.CaptureFixture[str],
) -> None:
    run_module._print_banner()
    out = capsys.readouterr().out
    assert run_module.FROM_SERVER_VERSION in out
    assert run_module.TO_SERVER_VERSION in out
    assert str(run_module.FROM_SCHEMA_VERSION) in out
    assert str(run_module.TO_SCHEMA_VERSION) in out
    assert "OUT-OF-BAND" in out
    assert "IDEMPOTENT" in out
    assert "never runs at server startup" in out


async def test_status_mode_does_not_call_run_repair(
    _patch_settings_and_driver: MagicMock,
) -> None:
    with (
        patch.object(
            run_module,
            "diagnose",
            AsyncMock(return_value={"untagged_nodes": 0, "duplicate_nodes": 0}),
        ),
        patch.object(run_module, "run_repair", AsyncMock()) as repair_mock,
        patch.object(run_module, "_constraint_present", AsyncMock(return_value=True)),
    ):
        args = run_module.build_parser().parse_args(["--status"])
        code = await run_module._amain(args)

    assert code == 0
    repair_mock.assert_not_awaited()


async def test_apply_mode_calls_run_repair(
    _patch_settings_and_driver: MagicMock,
) -> None:
    diagnose_mock = AsyncMock(
        side_effect=[
            {"untagged_nodes": 5, "duplicate_nodes": 2},  # before
            {"untagged_nodes": 0, "duplicate_nodes": 0},  # after
        ]
    )
    repair_mock = AsyncMock(return_value={"duplicates_removed": 2, "nodes_tagged": 5})
    with (
        patch.object(run_module, "diagnose", diagnose_mock),
        patch.object(run_module, "run_repair", repair_mock),
        patch.object(run_module, "_constraint_present", AsyncMock(return_value=True)),
    ):
        args = run_module.build_parser().parse_args(["--apply"])
        code = await run_module._amain(args)

    assert code == 0
    repair_mock.assert_awaited_once()
    assert diagnose_mock.await_count == 2


async def test_apply_is_idempotent_noop_on_already_clean_graph(
    _patch_settings_and_driver: MagicMock,
) -> None:
    """--apply still calls run_repair (it is itself idempotent/no-op-safe),
    but a clean before-state must still report healthy after."""
    diagnose_mock = AsyncMock(return_value={"untagged_nodes": 0, "duplicate_nodes": 0})
    repair_mock = AsyncMock(return_value={"duplicates_removed": 0, "nodes_tagged": 0})
    with (
        patch.object(run_module, "diagnose", diagnose_mock),
        patch.object(run_module, "run_repair", repair_mock),
        patch.object(run_module, "_constraint_present", AsyncMock(return_value=True)),
    ):
        args = run_module.build_parser().parse_args(["--apply"])
        code = await run_module._amain(args)

    assert code == 0
    repair_mock.assert_awaited_once()


async def test_status_unreachable_neo4j_fails_gracefully(
    _patch_settings_and_driver: MagicMock,
) -> None:
    """Pointed at an unreachable URL, --status must not crash/traceback --
    it reports the connectivity failure and returns a non-zero exit code."""
    driver = _patch_settings_and_driver
    driver.verify_connectivity = AsyncMock(
        side_effect=RuntimeError("connection refused")
    )

    args = run_module.build_parser().parse_args(["--status"])
    code = await run_module._amain(args)

    assert code != 0
    driver.close.assert_awaited_once()


async def test_status_unreachable_neo4j_never_touches_diagnose_or_repair(
    _patch_settings_and_driver: MagicMock,
) -> None:
    driver = _patch_settings_and_driver
    driver.verify_connectivity = AsyncMock(
        side_effect=RuntimeError("connection refused")
    )
    with (
        patch.object(run_module, "diagnose", AsyncMock()) as diagnose_mock,
        patch.object(run_module, "run_repair", AsyncMock()) as repair_mock,
    ):
        args = run_module.build_parser().parse_args(["--status"])
        await run_module._amain(args)

    diagnose_mock.assert_not_awaited()
    repair_mock.assert_not_awaited()


async def test_constraint_present_returns_none_on_probe_failure(
    _patch_settings_and_driver: MagicMock,
) -> None:
    driver = _patch_settings_and_driver
    driver.session.side_effect = RuntimeError("catalog read failed")

    result = await run_module._constraint_present(driver)

    assert result is None
