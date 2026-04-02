"""Tests for DataLayer2State dataclass.

Verifies:
- Import from context_intelligence_server.handlers.data_layer_2.state
- Default field values (execution_start_ts, active_iteration_id,
  pending_tool_block_ids, last_prompt_id, last_completed_orch_run_id)
- Field isolation (pending_tool_block_ids not shared across instances)
"""

from __future__ import annotations

from context_intelligence_server.handlers.data_layer_2.state import DataLayer2State


class TestDataLayer2StateImport:
    """DataLayer2State must be importable from the state module."""

    def test_data_layer_2_state_is_importable(self) -> None:
        """DataLayer2State can be imported from the expected module path."""
        assert DataLayer2State is not None


class TestDataLayer2StateDefaults:
    """DataLayer2State fields must have the correct default values."""

    def test_execution_start_ts_defaults_to_none(self) -> None:
        """execution_start_ts defaults to None."""
        state = DataLayer2State()
        assert state.execution_start_ts is None

    def test_active_iteration_id_defaults_to_none(self) -> None:
        """active_iteration_id defaults to None."""
        state = DataLayer2State()
        assert state.active_iteration_id is None

    def test_pending_tool_block_ids_defaults_to_empty_dict(self) -> None:
        """pending_tool_block_ids defaults to an empty dict."""
        state = DataLayer2State()
        assert state.pending_tool_block_ids == {}

    def test_last_prompt_id_defaults_to_none(self) -> None:
        """last_prompt_id defaults to None."""
        state = DataLayer2State()
        assert state.last_prompt_id is None

    def test_last_completed_orch_run_id_defaults_to_none(self) -> None:
        """last_completed_orch_run_id defaults to None."""
        state = DataLayer2State()
        assert state.last_completed_orch_run_id is None


class TestDataLayer2StateFieldIsolation:
    """pending_tool_block_ids must not be shared across instances."""

    def test_pending_tool_block_ids_not_shared_across_instances(self) -> None:
        """Mutating pending_tool_block_ids on one instance must not affect another."""
        state_a = DataLayer2State()
        state_b = DataLayer2State()

        state_a.pending_tool_block_ids["tool_1"] = "block_1"

        assert "tool_1" not in state_b.pending_tool_block_ids, (
            "pending_tool_block_ids must not be shared across DataLayer2State instances"
        )
