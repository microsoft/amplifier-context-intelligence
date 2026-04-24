"""Tests for DataLayer3State dataclass.

Verifies:
- Import from context_intelligence_server.handlers.data_layer_3.state
- Default field values (active_recipe_run_stack, active_recipe_step_id)
- Field isolation (active_recipe_run_stack list not shared across instances)
"""

from __future__ import annotations

from context_intelligence_server.handlers.data_layer_3.state import DataLayer3State


class TestDataLayer3StateImport:
    """DataLayer3State must be importable from the state module."""

    def test_data_layer_3_state_is_importable(self) -> None:
        """DataLayer3State can be imported from the expected module path."""
        assert DataLayer3State is not None


class TestDataLayer3StateDefaults:
    """DataLayer3State fields must have the correct default values."""

    def test_active_recipe_run_stack_defaults_to_empty_list(self) -> None:
        """active_recipe_run_stack defaults to an empty list."""
        state = DataLayer3State()
        assert state.active_recipe_run_stack == []

    def test_active_recipe_step_id_defaults_to_none(self) -> None:
        """active_recipe_step_id defaults to None."""
        state = DataLayer3State()
        assert state.active_recipe_step_id is None


class TestDataLayer3StateFieldIsolation:
    """active_recipe_run_stack must not be shared across instances."""

    def test_active_recipe_run_stack_not_shared_across_instances(self) -> None:
        """Mutating active_recipe_run_stack on one instance must not affect another."""
        state_a = DataLayer3State()
        state_b = DataLayer3State()

        state_a.active_recipe_run_stack.append("recipe_run_1")

        assert "recipe_run_1" not in state_b.active_recipe_run_stack, (
            "active_recipe_run_stack must not be shared across DataLayer3State instances"
        )


class TestHookStateServiceHasDataLayer3:
    """HookStateService must expose a data_layer_3 attribute of type DataLayer3State."""

    def test_services_has_data_layer_3_attribute(self) -> None:
        """HookStateService(workspace='test') has a data_layer_3 attribute."""
        from context_intelligence_server.services import HookStateService

        services = HookStateService(workspace="test")
        assert hasattr(services, "data_layer_3")

    def test_data_layer_3_is_data_layer_3_state_instance(self) -> None:
        """services.data_layer_3 is an instance of DataLayer3State."""
        from context_intelligence_server.services import HookStateService

        services = HookStateService(workspace="test")
        assert isinstance(services.data_layer_3, DataLayer3State)

    def test_data_layer_3_defaults_are_clean(self) -> None:
        """services.data_layer_3 has clean default values on initialisation."""
        from context_intelligence_server.services import HookStateService

        services = HookStateService(workspace="test")
        assert services.data_layer_3.active_recipe_run_stack == []
        assert services.data_layer_3.active_recipe_step_id is None
