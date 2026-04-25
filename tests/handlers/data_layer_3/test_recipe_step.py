"""Tests for RecipeStepHandler — Phase 2 implementation (recipe:step flat steps).

Covers:
- handled_events == frozenset of 4 events:
  'recipe:step', 'recipe:approval', 'recipe:loop_iteration', 'tool:pre'
- recipe:step creates RecipeStep:SST_EVENT node at {run_id}::step::{current_step}
  with both 'RecipeStep' and 'SST_EVENT' labels, started_at=timestamp,
  current_step property
- step_name extracted from data['steps'][current_step]['name']
  (e.g. 'build' for index 0, 'deploy' for index 1)
- falls back to str(current_step) when steps array is empty or out-of-range
  (IndexError/KeyError/TypeError)
- creates E08 edge RecipeRun-[HAS_STEP {sst_semantic: CONTAINS}]->RecipeStep
- creates SOURCED_FROM edge to
  make_node_id(SESSION_ID, 'recipe:step', timestamp, str(current_step))
- sets active_recipe_step_id cursor
- clears previous cursor before setting new (after two sequential steps cursor
  points to step::1)
- recipe:step with empty active_recipe_run_stack is no-op (no nodes created,
  active_recipe_step_id stays None, returns HookResult(action='continue'))
"""

from __future__ import annotations

from context_intelligence_server.handlers.data_layer_3.recipe_step import (
    RecipeStepHandler,
)
from context_intelligence_server.services import HookStateService
from context_intelligence_server.utils import make_node_id


# ---------------------------------------------------------------------------
# Module-level constants and shared helpers
# ---------------------------------------------------------------------------

SESSION_ID = "sess-step"
TIMESTAMP = "2026-01-01T00:00:00Z"
RUN_ID = f"{SESSION_ID}::recipe_run::{TIMESTAMP}"


def _push_run(services: HookStateService, run_id: str = RUN_ID) -> None:
    """Pre-populate active_recipe_run_stack for tests that need a non-empty stack."""
    services.data_layer_3.active_recipe_run_stack.append(run_id)


# ---------------------------------------------------------------------------
# 1. TestRecipeStepHandlerHandledEvents
# ---------------------------------------------------------------------------


class TestRecipeStepHandlerHandledEvents:
    """handled_events must be a frozenset of exactly 4 events."""

    def test_handled_events_is_frozenset(self) -> None:
        """handled_events must be a frozenset."""
        assert isinstance(RecipeStepHandler.handled_events, frozenset)

    def test_recipe_step_in_handled_events(self) -> None:
        """recipe:step must be in handled_events."""
        assert "recipe:step" in RecipeStepHandler.handled_events

    def test_recipe_approval_in_handled_events(self) -> None:
        """recipe:approval must be in handled_events."""
        assert "recipe:approval" in RecipeStepHandler.handled_events

    def test_recipe_loop_iteration_in_handled_events(self) -> None:
        """recipe:loop_iteration must be in handled_events."""
        assert "recipe:loop_iteration" in RecipeStepHandler.handled_events

    def test_tool_pre_in_handled_events(self) -> None:
        """tool:pre must be in handled_events."""
        assert "tool:pre" in RecipeStepHandler.handled_events

    def test_handled_events_has_exactly_four_events(self) -> None:
        """handled_events must contain exactly 4 events."""
        assert len(RecipeStepHandler.handled_events) == 4


# ---------------------------------------------------------------------------
# 2. TestRecipeStepHandlerStepCreatesNode
# ---------------------------------------------------------------------------


class TestRecipeStepHandlerStepCreatesNode:
    """recipe:step creates RecipeStep:SST_EVENT node at {run_id}::step::{current_step}."""

    async def test_step_creates_recipe_step_node(
        self, services: HookStateService
    ) -> None:
        """recipe:step must create a RecipeStep node at '{run_id}::step::{current_step}'."""
        _push_run(services)
        handler = RecipeStepHandler(services)
        data = {
            "session_id": SESSION_ID,
            "timestamp": TIMESTAMP,
            "current_step": 0,
            "steps": [{"name": "build"}, {"name": "deploy"}],
        }
        await handler("recipe:step", data)

        step_id = f"{RUN_ID}::step::0"
        node = await services.graph.get_node(step_id)
        assert node is not None, f"RecipeStep node must exist at '{step_id}'"

    async def test_step_node_has_recipe_step_label(
        self, services: HookStateService
    ) -> None:
        """RecipeStep node must have 'RecipeStep' in labels."""
        _push_run(services)
        handler = RecipeStepHandler(services)
        data = {
            "session_id": SESSION_ID,
            "timestamp": TIMESTAMP,
            "current_step": 0,
            "steps": [{"name": "build"}],
        }
        await handler("recipe:step", data)

        step_id = f"{RUN_ID}::step::0"
        node = await services.graph.get_node(step_id)
        assert node is not None
        assert "RecipeStep" in node["labels"]

    async def test_step_node_has_sst_event_label(
        self, services: HookStateService
    ) -> None:
        """RecipeStep node must have 'SST_EVENT' in labels."""
        _push_run(services)
        handler = RecipeStepHandler(services)
        data = {
            "session_id": SESSION_ID,
            "timestamp": TIMESTAMP,
            "current_step": 0,
            "steps": [{"name": "build"}],
        }
        await handler("recipe:step", data)

        step_id = f"{RUN_ID}::step::0"
        node = await services.graph.get_node(step_id)
        assert node is not None
        assert "SST_EVENT" in node["labels"]

    async def test_step_node_has_started_at(self, services: HookStateService) -> None:
        """RecipeStep node must have 'started_at' matching event timestamp."""
        _push_run(services)
        handler = RecipeStepHandler(services)
        data = {
            "session_id": SESSION_ID,
            "timestamp": TIMESTAMP,
            "current_step": 0,
            "steps": [{"name": "build"}],
        }
        await handler("recipe:step", data)

        step_id = f"{RUN_ID}::step::0"
        node = await services.graph.get_node(step_id)
        assert node is not None
        assert node["started_at"] == TIMESTAMP

    async def test_step_node_has_current_step(self, services: HookStateService) -> None:
        """RecipeStep node must have 'current_step' property."""
        _push_run(services)
        handler = RecipeStepHandler(services)
        data = {
            "session_id": SESSION_ID,
            "timestamp": TIMESTAMP,
            "current_step": 0,
            "steps": [{"name": "build"}],
        }
        await handler("recipe:step", data)

        step_id = f"{RUN_ID}::step::0"
        node = await services.graph.get_node(step_id)
        assert node is not None
        assert node["current_step"] == 0


# ---------------------------------------------------------------------------
# 3. TestRecipeStepHandlerStepNameFromSteps
# ---------------------------------------------------------------------------


class TestRecipeStepHandlerStepNameFromSteps:
    """step_name is extracted from data['steps'][current_step]['name']."""

    async def test_step_name_from_steps_index_0(
        self, services: HookStateService
    ) -> None:
        """step_name is 'build' for steps[0]['name'] == 'build'."""
        _push_run(services)
        handler = RecipeStepHandler(services)
        data = {
            "session_id": SESSION_ID,
            "timestamp": TIMESTAMP,
            "current_step": 0,
            "steps": [{"name": "build"}, {"name": "deploy"}],
        }
        await handler("recipe:step", data)

        step_id = f"{RUN_ID}::step::0"
        node = await services.graph.get_node(step_id)
        assert node is not None
        assert node["name"] == "build"

    async def test_step_name_from_steps_index_1(
        self, services: HookStateService
    ) -> None:
        """step_name is 'deploy' for steps[1]['name'] == 'deploy'."""
        _push_run(services)
        handler = RecipeStepHandler(services)
        data = {
            "session_id": SESSION_ID,
            "timestamp": TIMESTAMP,
            "current_step": 1,
            "steps": [{"name": "build"}, {"name": "deploy"}],
        }
        await handler("recipe:step", data)

        step_id = f"{RUN_ID}::step::1"
        node = await services.graph.get_node(step_id)
        assert node is not None
        assert node["name"] == "deploy"

    async def test_step_name_fallback_when_steps_empty(
        self, services: HookStateService
    ) -> None:
        """step_name falls back to str(current_step) when steps array is empty (IndexError)."""
        _push_run(services)
        handler = RecipeStepHandler(services)
        data = {
            "session_id": SESSION_ID,
            "timestamp": TIMESTAMP,
            "current_step": 0,
            "steps": [],
        }
        await handler("recipe:step", data)

        step_id = f"{RUN_ID}::step::0"
        node = await services.graph.get_node(step_id)
        assert node is not None
        assert node["name"] == "0"

    async def test_step_name_fallback_when_index_out_of_range(
        self, services: HookStateService
    ) -> None:
        """step_name falls back to str(current_step) when index is out-of-range (IndexError)."""
        _push_run(services)
        handler = RecipeStepHandler(services)
        data = {
            "session_id": SESSION_ID,
            "timestamp": TIMESTAMP,
            "current_step": 5,
            "steps": [{"name": "build"}],
        }
        await handler("recipe:step", data)

        step_id = f"{RUN_ID}::step::5"
        node = await services.graph.get_node(step_id)
        assert node is not None
        assert node["name"] == "5"

    async def test_step_name_fallback_when_no_name_key(
        self, services: HookStateService
    ) -> None:
        """step_name falls back to str(current_step) when step has no 'name' key (KeyError)."""
        _push_run(services)
        handler = RecipeStepHandler(services)
        data = {
            "session_id": SESSION_ID,
            "timestamp": TIMESTAMP,
            "current_step": 0,
            "steps": [{"id": "step-0"}],  # no 'name' key
        }
        await handler("recipe:step", data)

        step_id = f"{RUN_ID}::step::0"
        node = await services.graph.get_node(step_id)
        assert node is not None
        assert node["name"] == "0"

    async def test_step_name_fallback_when_steps_not_a_list(
        self, services: HookStateService
    ) -> None:
        """step_name falls back to str(current_step) when steps is not subscriptable (TypeError)."""
        _push_run(services)
        handler = RecipeStepHandler(services)
        data = {
            "session_id": SESSION_ID,
            "timestamp": TIMESTAMP,
            "current_step": 0,
            "steps": "not-a-list",  # TypeError on indexing
        }
        await handler("recipe:step", data)

        step_id = f"{RUN_ID}::step::0"
        node = await services.graph.get_node(step_id)
        assert node is not None
        assert node["name"] == "0"


# ---------------------------------------------------------------------------
# 4. TestRecipeStepHandlerStepEdges
# ---------------------------------------------------------------------------


class TestRecipeStepHandlerStepEdges:
    """E08 and SOURCED_FROM edges must be created on recipe:step."""

    async def test_e08_has_step_contains_edge(self, services: HookStateService) -> None:
        """E08: RecipeRun -[HAS_STEP {sst_semantic: CONTAINS}]-> RecipeStep."""
        _push_run(services)
        handler = RecipeStepHandler(services)
        data = {
            "session_id": SESSION_ID,
            "timestamp": TIMESTAMP,
            "current_step": 0,
            "steps": [{"name": "build"}],
        }
        await handler("recipe:step", data)

        step_id = f"{RUN_ID}::step::0"
        edge = await services.graph.get_edge(RUN_ID, step_id)
        assert edge is not None, "E08 edge (RecipeRun -> RecipeStep) must exist"
        assert edge.get("type") == "HAS_STEP"
        assert edge.get("sst_semantic") == "CONTAINS"

    async def test_sourced_from_edge_on_recipe_step(
        self, services: HookStateService
    ) -> None:
        """SOURCED_FROM must link RecipeStep to
        make_node_id(SESSION_ID, 'recipe:step', timestamp, str(current_step))."""
        _push_run(services)
        handler = RecipeStepHandler(services)
        data = {
            "session_id": SESSION_ID,
            "timestamp": TIMESTAMP,
            "current_step": 0,
            "steps": [{"name": "build"}],
        }
        await handler("recipe:step", data)

        step_id = f"{RUN_ID}::step::0"
        expected_target = make_node_id(SESSION_ID, "recipe:step", TIMESTAMP, "0")
        edge = await services.graph.get_edge(step_id, expected_target)
        assert edge is not None, (
            "SOURCED_FROM edge must exist from RecipeStep to data_layer_1 node"
        )
        assert edge.get("type") == "SOURCED_FROM"


# ---------------------------------------------------------------------------
# 5. TestRecipeStepHandlerCursor
# ---------------------------------------------------------------------------


class TestRecipeStepHandlerCursor:
    """recipe:step sets and clears the active_recipe_step_id cursor."""

    async def test_step_sets_active_recipe_step_id(
        self, services: HookStateService
    ) -> None:
        """recipe:step sets active_recipe_step_id to the step node ID."""
        _push_run(services)
        handler = RecipeStepHandler(services)
        data = {
            "session_id": SESSION_ID,
            "timestamp": TIMESTAMP,
            "current_step": 0,
            "steps": [{"name": "build"}],
        }
        await handler("recipe:step", data)

        step_id = f"{RUN_ID}::step::0"
        assert services.data_layer_3.active_recipe_step_id == step_id

    async def test_step_clears_previous_cursor_before_setting_new(
        self, services: HookStateService
    ) -> None:
        """After two sequential steps cursor points to step::1 (not step::0)."""
        _push_run(services)
        handler = RecipeStepHandler(services)

        data_step_0 = {
            "session_id": SESSION_ID,
            "timestamp": TIMESTAMP,
            "current_step": 0,
            "steps": [{"name": "build"}, {"name": "deploy"}],
        }
        data_step_1 = {
            "session_id": SESSION_ID,
            "timestamp": TIMESTAMP,
            "current_step": 1,
            "steps": [{"name": "build"}, {"name": "deploy"}],
        }
        await handler("recipe:step", data_step_0)
        await handler("recipe:step", data_step_1)

        step_1_id = f"{RUN_ID}::step::1"
        assert services.data_layer_3.active_recipe_step_id == step_1_id

    async def test_step_empty_stack_is_noop(self, services: HookStateService) -> None:
        """recipe:step with empty active_recipe_run_stack is a no-op:
        no nodes created, active_recipe_step_id stays None, returns continue."""
        handler = RecipeStepHandler(services)
        assert services.data_layer_3.active_recipe_run_stack == []

        result = await handler(
            "recipe:step",
            {
                "session_id": SESSION_ID,
                "timestamp": TIMESTAMP,
                "current_step": 0,
                "steps": [{"name": "build"}],
            },
        )
        assert result.action == "continue"
        assert len(services.graph._nodes) == 0
        assert services.data_layer_3.active_recipe_step_id is None


# ---------------------------------------------------------------------------
# 6. TestRecipeStepHandlerApproval
# ---------------------------------------------------------------------------

APPROVAL_DATA = {
    "session_id": SESSION_ID,
    "timestamp": TIMESTAMP,
    "stage_name": "deploy",
    "prompt": "Deploy to production?",
    "is_approval_gate": True,
    "status": "waiting_approval",
}


class TestRecipeStepHandlerApproval:
    """recipe:approval creates RecipeStep:SST_EVENT node keyed by stage_name."""

    async def test_approval_creates_recipe_step_node(
        self, services: HookStateService
    ) -> None:
        """recipe:approval must create RecipeStep node at '{run_id}::step::deploy'."""
        _push_run(services)
        handler = RecipeStepHandler(services)
        await handler("recipe:approval", APPROVAL_DATA)

        step_id = f"{RUN_ID}::step::deploy"
        node = await services.graph.get_node(step_id)
        assert node is not None, f"RecipeStep node must exist at '{step_id}'"

    async def test_approval_node_has_recipe_step_label(
        self, services: HookStateService
    ) -> None:
        """RecipeStep node must have 'RecipeStep' in labels."""
        _push_run(services)
        handler = RecipeStepHandler(services)
        await handler("recipe:approval", APPROVAL_DATA)

        step_id = f"{RUN_ID}::step::deploy"
        node = await services.graph.get_node(step_id)
        assert node is not None
        assert "RecipeStep" in node["labels"]

    async def test_approval_node_has_sst_event_label(
        self, services: HookStateService
    ) -> None:
        """RecipeStep node must have 'SST_EVENT' in labels."""
        _push_run(services)
        handler = RecipeStepHandler(services)
        await handler("recipe:approval", APPROVAL_DATA)

        step_id = f"{RUN_ID}::step::deploy"
        node = await services.graph.get_node(step_id)
        assert node is not None
        assert "SST_EVENT" in node["labels"]

    async def test_approval_node_has_stage_name(
        self, services: HookStateService
    ) -> None:
        """RecipeStep node must have stage_name='deploy'."""
        _push_run(services)
        handler = RecipeStepHandler(services)
        await handler("recipe:approval", APPROVAL_DATA)

        step_id = f"{RUN_ID}::step::deploy"
        node = await services.graph.get_node(step_id)
        assert node is not None
        assert node["stage_name"] == "deploy"

    async def test_approval_node_has_name(self, services: HookStateService) -> None:
        """RecipeStep node must have name='deploy' (same as stage_name)."""
        _push_run(services)
        handler = RecipeStepHandler(services)
        await handler("recipe:approval", APPROVAL_DATA)

        step_id = f"{RUN_ID}::step::deploy"
        node = await services.graph.get_node(step_id)
        assert node is not None
        assert node["name"] == "deploy"

    async def test_approval_node_has_prompt(self, services: HookStateService) -> None:
        """RecipeStep node must have prompt='Deploy to production?'."""
        _push_run(services)
        handler = RecipeStepHandler(services)
        await handler("recipe:approval", APPROVAL_DATA)

        step_id = f"{RUN_ID}::step::deploy"
        node = await services.graph.get_node(step_id)
        assert node is not None
        assert node["prompt"] == "Deploy to production?"

    async def test_approval_e08_has_step_contains_edge(
        self, services: HookStateService
    ) -> None:
        """E08: RecipeRun -[HAS_STEP {sst_semantic: CONTAINS}]-> RecipeStep."""
        _push_run(services)
        handler = RecipeStepHandler(services)
        await handler("recipe:approval", APPROVAL_DATA)

        step_id = f"{RUN_ID}::step::deploy"
        edge = await services.graph.get_edge(RUN_ID, step_id)
        assert edge is not None, "E08 edge (RecipeRun -> RecipeStep) must exist"
        assert edge.get("type") == "HAS_STEP"
        assert edge.get("sst_semantic") == "CONTAINS"

    async def test_approval_sourced_from_edge(self, services: HookStateService) -> None:
        """SOURCED_FROM must link RecipeStep to
        make_node_id(SESSION_ID, 'recipe:approval', TIMESTAMP, 'deploy')."""
        _push_run(services)
        handler = RecipeStepHandler(services)
        await handler("recipe:approval", APPROVAL_DATA)

        step_id = f"{RUN_ID}::step::deploy"
        expected_target = make_node_id(
            SESSION_ID, "recipe:approval", TIMESTAMP, "deploy"
        )
        edge = await services.graph.get_edge(step_id, expected_target)
        assert edge is not None, (
            "SOURCED_FROM edge must exist from RecipeStep to data_layer_1 node"
        )
        assert edge.get("type") == "SOURCED_FROM"

    async def test_approval_sets_active_recipe_step_id(
        self, services: HookStateService
    ) -> None:
        """recipe:approval sets active_recipe_step_id to '{run_id}::step::deploy'."""
        _push_run(services)
        handler = RecipeStepHandler(services)
        await handler("recipe:approval", APPROVAL_DATA)

        step_id = f"{RUN_ID}::step::deploy"
        assert services.data_layer_3.active_recipe_step_id == step_id

    async def test_approval_clears_cursor_even_on_empty_stack(
        self, services: HookStateService
    ) -> None:
        """Previous cursor is cleared even when empty-stack guard fires.

        Set active_recipe_step_id='old-run::step::5' without _push_run,
        call handler, assert cursor is None.
        """
        # Set cursor without pushing a run onto the stack
        services.data_layer_3.active_recipe_step_id = "old-run::step::5"
        # Stack is empty — guard will fire
        assert services.data_layer_3.active_recipe_run_stack == []

        handler = RecipeStepHandler(services)
        await handler("recipe:approval", APPROVAL_DATA)

        # Cursor must be cleared (None) even though the stack was empty
        assert services.data_layer_3.active_recipe_step_id is None


# ---------------------------------------------------------------------------
# 7. TestRecipeStepHandlerLoopIteration
# ---------------------------------------------------------------------------

LOOP_DATA = {
    "session_id": SESSION_ID,
    "timestamp": TIMESTAMP,
    "step_id": "while-loop-step",
    "iteration": 2,
    "max_iterations": 5,
    "context_snapshot": {"counter": 2},
}

LOOP_NODE_ID = f"{RUN_ID}::step::while-loop-step::loop::2"


class TestRecipeStepHandlerLoopIteration:
    """recipe:loop_iteration creates RecipeStep:SST_EVENT node at compound loop ID."""

    async def test_loop_iteration_creates_recipe_step_node(
        self, services: HookStateService
    ) -> None:
        """recipe:loop_iteration must create RecipeStep node at compound loop ID."""
        _push_run(services)
        handler = RecipeStepHandler(services)
        await handler("recipe:loop_iteration", LOOP_DATA)

        node = await services.graph.get_node(LOOP_NODE_ID)
        assert node is not None, f"RecipeStep node must exist at '{LOOP_NODE_ID}'"

    async def test_loop_iteration_node_has_recipe_step_label(
        self, services: HookStateService
    ) -> None:
        """RecipeStep node must have 'RecipeStep' in labels."""
        _push_run(services)
        handler = RecipeStepHandler(services)
        await handler("recipe:loop_iteration", LOOP_DATA)

        node = await services.graph.get_node(LOOP_NODE_ID)
        assert node is not None
        assert "RecipeStep" in node["labels"]

    async def test_loop_iteration_node_has_sst_event_label(
        self, services: HookStateService
    ) -> None:
        """RecipeStep node must have 'SST_EVENT' in labels."""
        _push_run(services)
        handler = RecipeStepHandler(services)
        await handler("recipe:loop_iteration", LOOP_DATA)

        node = await services.graph.get_node(LOOP_NODE_ID)
        assert node is not None
        assert "SST_EVENT" in node["labels"]

    async def test_loop_iteration_node_has_name(
        self, services: HookStateService
    ) -> None:
        """RecipeStep node must have name='loop iteration 2'."""
        _push_run(services)
        handler = RecipeStepHandler(services)
        await handler("recipe:loop_iteration", LOOP_DATA)

        node = await services.graph.get_node(LOOP_NODE_ID)
        assert node is not None
        assert node["name"] == "loop iteration 2"

    async def test_loop_iteration_node_has_step_id(
        self, services: HookStateService
    ) -> None:
        """RecipeStep node must have step_id='while-loop-step'."""
        _push_run(services)
        handler = RecipeStepHandler(services)
        await handler("recipe:loop_iteration", LOOP_DATA)

        node = await services.graph.get_node(LOOP_NODE_ID)
        assert node is not None
        assert node["step_id"] == "while-loop-step"

    async def test_loop_iteration_node_has_iteration(
        self, services: HookStateService
    ) -> None:
        """RecipeStep node must have iteration=2."""
        _push_run(services)
        handler = RecipeStepHandler(services)
        await handler("recipe:loop_iteration", LOOP_DATA)

        node = await services.graph.get_node(LOOP_NODE_ID)
        assert node is not None
        assert node["iteration"] == 2

    async def test_loop_iteration_node_has_max_iterations(
        self, services: HookStateService
    ) -> None:
        """RecipeStep node must have max_iterations=5."""
        _push_run(services)
        handler = RecipeStepHandler(services)
        await handler("recipe:loop_iteration", LOOP_DATA)

        node = await services.graph.get_node(LOOP_NODE_ID)
        assert node is not None
        assert node["max_iterations"] == 5

    async def test_loop_iteration_e08_has_step_contains_edge(
        self, services: HookStateService
    ) -> None:
        """E08: RecipeRun -[HAS_STEP {sst_semantic: CONTAINS}]-> RecipeStep."""
        _push_run(services)
        handler = RecipeStepHandler(services)
        await handler("recipe:loop_iteration", LOOP_DATA)

        edge = await services.graph.get_edge(RUN_ID, LOOP_NODE_ID)
        assert edge is not None, "E08 edge (RecipeRun -> RecipeStep) must exist"
        assert edge.get("type") == "HAS_STEP"
        assert edge.get("sst_semantic") == "CONTAINS"

    async def test_loop_iteration_sourced_from_edge(
        self, services: HookStateService
    ) -> None:
        """SOURCED_FROM must link RecipeStep to
        make_node_id(SESSION_ID, 'recipe:loop_iteration', TIMESTAMP, 'while-loop-step_2')."""
        _push_run(services)
        handler = RecipeStepHandler(services)
        await handler("recipe:loop_iteration", LOOP_DATA)

        expected_target = make_node_id(
            SESSION_ID, "recipe:loop_iteration", TIMESTAMP, "while-loop-step_2"
        )
        edge = await services.graph.get_edge(LOOP_NODE_ID, expected_target)
        assert edge is not None, (
            "SOURCED_FROM edge must exist from RecipeStep to data_layer_1 node"
        )
        assert edge.get("type") == "SOURCED_FROM"

    async def test_loop_iteration_sets_active_recipe_step_id(
        self, services: HookStateService
    ) -> None:
        """recipe:loop_iteration sets active_recipe_step_id to the compound loop ID."""
        _push_run(services)
        handler = RecipeStepHandler(services)
        await handler("recipe:loop_iteration", LOOP_DATA)

        assert services.data_layer_3.active_recipe_step_id == LOOP_NODE_ID

    async def test_loop_iteration_empty_stack_is_noop(
        self, services: HookStateService
    ) -> None:
        """recipe:loop_iteration with empty stack is a no-op:
        no nodes created, returns HookResult(action='continue')."""
        handler = RecipeStepHandler(services)
        assert services.data_layer_3.active_recipe_run_stack == []

        result = await handler("recipe:loop_iteration", LOOP_DATA)
        assert result.action == "continue"
        assert len(services.graph._nodes) == 0
