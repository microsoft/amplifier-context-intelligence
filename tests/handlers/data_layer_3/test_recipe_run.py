"""Tests for RecipeRunHandler — Phase 2 implementation.

Covers:
- handled_events == frozenset of 5 recipe events
- recipe:start creates RecipeRun:SST_EVENT node at {session_id}::recipe_run::{timestamp}
- recipe:start creates Recipe:SST_CONCEPT node (MERGE semantics, NO SOURCED_FROM)
- E06: Session -[HAS_RECIPE_RUN {sst_semantic: CONTAINS}]-> RecipeRun
- E07: RecipeRun -[HAS_RECIPE {sst_semantic: EXPRESSES}]-> Recipe
- SOURCED_FROM: RecipeRun -> make_node_id(session_id, 'recipe:start', timestamp)
- recipe:start pushes recipe_run_id onto active_recipe_run_stack cursor
- E09: active RecipeStep -[SPAWNED {sst_semantic: LEADS_TO}]-> RecipeRun (if cursor set)
- recipe:complete enriches RecipeRun with ended_at, success, final_status
- recipe:complete pops active_recipe_run_stack
- recipe:complete clears active_recipe_step_id
- recipe:complete adds SOURCED_FROM to L1 complete event
- recipe:complete with empty stack is a no-op guard returning HookResult(action='continue')
"""

from __future__ import annotations

from context_intelligence_server.handlers.data_layer_3.recipe_run import (
    RecipeRunHandler,
)
from context_intelligence_server.services import HookStateService
from context_intelligence_server.utils import make_node_id


# ---------------------------------------------------------------------------
# 1. TestRecipeRunHandlerHandledEvents
# ---------------------------------------------------------------------------


class TestRecipeRunHandlerHandledEvents:
    """handled_events must be a frozenset containing all 5 recipe events."""

    def test_handled_events_is_frozenset(self) -> None:
        """handled_events must be a frozenset."""
        assert isinstance(RecipeRunHandler.handled_events, frozenset)

    def test_recipe_start_in_handled_events(self) -> None:
        """recipe:start must be in handled_events."""
        assert "recipe:start" in RecipeRunHandler.handled_events

    def test_recipe_complete_in_handled_events(self) -> None:
        """recipe:complete must be in handled_events."""
        assert "recipe:complete" in RecipeRunHandler.handled_events

    def test_recipe_approval_in_handled_events(self) -> None:
        """recipe:approval must be in handled_events."""
        assert "recipe:approval" in RecipeRunHandler.handled_events

    def test_recipe_loop_iteration_in_handled_events(self) -> None:
        """recipe:loop_iteration must be in handled_events."""
        assert "recipe:loop_iteration" in RecipeRunHandler.handled_events

    def test_recipe_loop_complete_in_handled_events(self) -> None:
        """recipe:loop_complete must be in handled_events."""
        assert "recipe:loop_complete" in RecipeRunHandler.handled_events

    def test_handled_events_has_exactly_five_events(self) -> None:
        """handled_events must contain exactly 5 events."""
        assert len(RecipeRunHandler.handled_events) == 5


# ---------------------------------------------------------------------------
# 2. TestRecipeRunHandlerStartCreatesNodes
# ---------------------------------------------------------------------------


class TestRecipeRunHandlerStartCreatesNodes:
    """recipe:start creates RecipeRun:SST_EVENT node and Recipe:SST_CONCEPT node."""

    async def test_start_creates_recipe_run_node(
        self, services: HookStateService
    ) -> None:
        """recipe:start must create RecipeRun node at '{session_id}::recipe_run::{timestamp}'."""
        handler = RecipeRunHandler(services)
        data = {
            "session_id": "sess-1",
            "timestamp": "2026-01-01T00:00:00Z",
            "name": "my-recipe",
            "total_steps": 3,
            "status": "running",
        }
        await handler("recipe:start", data)

        recipe_run_id = "sess-1::recipe_run::2026-01-01T00:00:00Z"
        node = await services.graph.get_node(recipe_run_id)
        assert node is not None, f"RecipeRun node must exist at '{recipe_run_id}'"

    async def test_start_recipe_run_node_has_recipe_run_label(
        self, services: HookStateService
    ) -> None:
        """RecipeRun node must have 'RecipeRun' in labels."""
        handler = RecipeRunHandler(services)
        data = {
            "session_id": "sess-1",
            "timestamp": "2026-01-01T00:00:00Z",
            "name": "my-recipe",
            "total_steps": 3,
            "status": "running",
        }
        await handler("recipe:start", data)

        recipe_run_id = "sess-1::recipe_run::2026-01-01T00:00:00Z"
        node = await services.graph.get_node(recipe_run_id)
        assert node is not None
        assert "RecipeRun" in node["labels"]

    async def test_start_recipe_run_node_has_sst_event_label(
        self, services: HookStateService
    ) -> None:
        """RecipeRun node must have 'SST_EVENT' in labels."""
        handler = RecipeRunHandler(services)
        data = {
            "session_id": "sess-1",
            "timestamp": "2026-01-01T00:00:00Z",
            "name": "my-recipe",
            "total_steps": 3,
            "status": "running",
        }
        await handler("recipe:start", data)

        recipe_run_id = "sess-1::recipe_run::2026-01-01T00:00:00Z"
        node = await services.graph.get_node(recipe_run_id)
        assert node is not None
        assert "SST_EVENT" in node["labels"]

    async def test_start_recipe_run_node_has_name(
        self, services: HookStateService
    ) -> None:
        """RecipeRun node must have 'name' property from event data."""
        handler = RecipeRunHandler(services)
        data = {
            "session_id": "sess-1",
            "timestamp": "2026-01-01T00:00:00Z",
            "name": "my-recipe",
            "total_steps": 3,
            "status": "running",
        }
        await handler("recipe:start", data)

        recipe_run_id = "sess-1::recipe_run::2026-01-01T00:00:00Z"
        node = await services.graph.get_node(recipe_run_id)
        assert node is not None
        assert node["name"] == "my-recipe"

    async def test_start_recipe_run_node_has_started_at(
        self, services: HookStateService
    ) -> None:
        """RecipeRun node must have 'started_at' matching event timestamp."""
        handler = RecipeRunHandler(services)
        timestamp = "2026-01-01T00:00:00Z"
        data = {
            "session_id": "sess-1",
            "timestamp": timestamp,
            "name": "my-recipe",
            "total_steps": 3,
            "status": "running",
        }
        await handler("recipe:start", data)

        recipe_run_id = "sess-1::recipe_run::2026-01-01T00:00:00Z"
        node = await services.graph.get_node(recipe_run_id)
        assert node is not None
        assert node["started_at"] == timestamp

    async def test_start_recipe_run_node_has_total_steps(
        self, services: HookStateService
    ) -> None:
        """RecipeRun node must have 'total_steps' from event data."""
        handler = RecipeRunHandler(services)
        data = {
            "session_id": "sess-1",
            "timestamp": "2026-01-01T00:00:00Z",
            "name": "my-recipe",
            "total_steps": 5,
            "status": "running",
        }
        await handler("recipe:start", data)

        recipe_run_id = "sess-1::recipe_run::2026-01-01T00:00:00Z"
        node = await services.graph.get_node(recipe_run_id)
        assert node is not None
        assert node["total_steps"] == 5

    async def test_start_recipe_run_node_has_status(
        self, services: HookStateService
    ) -> None:
        """RecipeRun node must have 'status' from event data."""
        handler = RecipeRunHandler(services)
        data = {
            "session_id": "sess-1",
            "timestamp": "2026-01-01T00:00:00Z",
            "name": "my-recipe",
            "total_steps": 3,
            "status": "running",
        }
        await handler("recipe:start", data)

        recipe_run_id = "sess-1::recipe_run::2026-01-01T00:00:00Z"
        node = await services.graph.get_node(recipe_run_id)
        assert node is not None
        assert node["status"] == "running"

    async def test_start_creates_recipe_concept_node(
        self, services: HookStateService
    ) -> None:
        """recipe:start must create Recipe:SST_CONCEPT node keyed by recipe name."""
        handler = RecipeRunHandler(services)
        data = {
            "session_id": "sess-1",
            "timestamp": "2026-01-01T00:00:00Z",
            "name": "my-recipe",
            "total_steps": 3,
            "status": "running",
        }
        await handler("recipe:start", data)

        node = await services.graph.get_node("my-recipe")
        assert node is not None, "Recipe concept node must exist at the recipe name key"

    async def test_start_recipe_concept_has_sst_concept_label(
        self, services: HookStateService
    ) -> None:
        """Recipe node must have 'SST_CONCEPT' label."""
        handler = RecipeRunHandler(services)
        data = {
            "session_id": "sess-1",
            "timestamp": "2026-01-01T00:00:00Z",
            "name": "my-recipe",
            "total_steps": 3,
            "status": "running",
        }
        await handler("recipe:start", data)

        node = await services.graph.get_node("my-recipe")
        assert node is not None
        assert "SST_CONCEPT" in node["labels"]
        assert "SST_EVENT" not in node["labels"]

    async def test_start_recipe_concept_has_no_sourced_from_edge(
        self, services: HookStateService
    ) -> None:
        """Recipe concept node must NOT have a SOURCED_FROM edge originating from it."""
        handler = RecipeRunHandler(services)
        data = {
            "session_id": "sess-1",
            "timestamp": "2026-01-01T00:00:00Z",
            "name": "my-recipe",
            "total_steps": 3,
            "status": "running",
        }
        await handler("recipe:start", data)

        sourced_from_edges = [
            (src, dst)
            for (src, dst), edge in services.graph._edges.items()
            if src == "my-recipe" and edge.get("type") == "SOURCED_FROM"
        ]
        assert len(sourced_from_edges) == 0, (
            "Recipe concept node must NOT have a SOURCED_FROM edge"
        )


# ---------------------------------------------------------------------------
# 3. TestRecipeRunHandlerStartEdges
# ---------------------------------------------------------------------------


class TestRecipeRunHandlerStartEdges:
    """E06, E07, and SOURCED_FROM edges must be created on recipe:start."""

    async def test_e06_has_recipe_run_contains_edge(
        self, services: HookStateService
    ) -> None:
        """E06: Session -[HAS_RECIPE_RUN {sst_semantic: CONTAINS}]-> RecipeRun."""
        handler = RecipeRunHandler(services)
        data = {
            "session_id": "sess-1",
            "timestamp": "2026-01-01T00:00:00Z",
            "name": "my-recipe",
            "total_steps": 3,
            "status": "running",
        }
        await handler("recipe:start", data)

        recipe_run_id = "sess-1::recipe_run::2026-01-01T00:00:00Z"
        edge = await services.graph.get_edge("sess-1", recipe_run_id)
        assert edge is not None, "E06 edge (Session -> RecipeRun) must exist"
        assert edge.get("type") == "HAS_RECIPE_RUN"
        assert edge.get("sst_semantic") == "CONTAINS"

    async def test_e07_has_recipe_expresses_edge(
        self, services: HookStateService
    ) -> None:
        """E07: RecipeRun -[HAS_RECIPE {sst_semantic: EXPRESSES}]-> Recipe."""
        handler = RecipeRunHandler(services)
        data = {
            "session_id": "sess-1",
            "timestamp": "2026-01-01T00:00:00Z",
            "name": "my-recipe",
            "total_steps": 3,
            "status": "running",
        }
        await handler("recipe:start", data)

        recipe_run_id = "sess-1::recipe_run::2026-01-01T00:00:00Z"
        edge = await services.graph.get_edge(recipe_run_id, "my-recipe")
        assert edge is not None, "E07 edge (RecipeRun -> Recipe) must exist"
        assert edge.get("type") == "HAS_RECIPE"
        assert edge.get("sst_semantic") == "EXPRESSES"

    async def test_sourced_from_edge_on_recipe_start(
        self, services: HookStateService
    ) -> None:
        """SOURCED_FROM must link RecipeRun to make_node_id(session_id, 'recipe:start', timestamp)."""
        handler = RecipeRunHandler(services)
        timestamp = "2026-01-01T00:00:00Z"
        data = {
            "session_id": "sess-1",
            "timestamp": timestamp,
            "name": "my-recipe",
            "total_steps": 3,
            "status": "running",
        }
        await handler("recipe:start", data)

        recipe_run_id = "sess-1::recipe_run::2026-01-01T00:00:00Z"
        expected_target = make_node_id("sess-1", "recipe:start", timestamp)
        edge = await services.graph.get_edge(recipe_run_id, expected_target)
        assert edge is not None, (
            "SOURCED_FROM edge must exist from RecipeRun to data_layer_1 node"
        )
        assert edge.get("type") == "SOURCED_FROM"

    async def test_start_missing_session_id_is_noop(
        self, services: HookStateService
    ) -> None:
        """Missing session_id returns continue with no graph mutations."""
        handler = RecipeRunHandler(services)
        result = await handler(
            "recipe:start",
            {
                "timestamp": "2026-01-01T00:00:00Z",
                "name": "my-recipe",
                "total_steps": 3,
                "status": "running",
            },
        )
        assert result.action == "continue"
        assert len(services.graph._nodes) == 0


# ---------------------------------------------------------------------------
# 4. TestRecipeRunHandlerStartCursor
# ---------------------------------------------------------------------------


class TestRecipeRunHandlerStartCursor:
    """recipe:start must push recipe_run_id onto active_recipe_run_stack."""

    async def test_start_pushes_recipe_run_id_onto_stack(
        self, services: HookStateService
    ) -> None:
        """recipe:start pushes recipe_run_id onto active_recipe_run_stack."""
        handler = RecipeRunHandler(services)
        data = {
            "session_id": "sess-1",
            "timestamp": "2026-01-01T00:00:00Z",
            "name": "my-recipe",
            "total_steps": 3,
            "status": "running",
        }
        await handler("recipe:start", data)

        recipe_run_id = "sess-1::recipe_run::2026-01-01T00:00:00Z"
        assert recipe_run_id in services.data_layer_3.active_recipe_run_stack

    async def test_start_stack_is_empty_before_first_start(
        self, services: HookStateService
    ) -> None:
        """active_recipe_run_stack starts empty."""
        assert services.data_layer_3.active_recipe_run_stack == []

    async def test_start_nested_runs_pushes_multiple_ids(
        self, services: HookStateService
    ) -> None:
        """Two recipe:start events push two IDs onto the stack (inner is last)."""
        handler = RecipeRunHandler(services)
        data_outer = {
            "session_id": "sess-1",
            "timestamp": "2026-01-01T00:00:00Z",
            "name": "outer-recipe",
            "total_steps": 2,
            "status": "running",
        }
        data_inner = {
            "session_id": "sess-1",
            "timestamp": "2026-01-01T00:01:00Z",
            "name": "inner-recipe",
            "total_steps": 1,
            "status": "running",
        }
        await handler("recipe:start", data_outer)
        await handler("recipe:start", data_inner)

        stack = services.data_layer_3.active_recipe_run_stack
        assert len(stack) == 2
        assert stack[0] == "sess-1::recipe_run::2026-01-01T00:00:00Z"
        assert stack[1] == "sess-1::recipe_run::2026-01-01T00:01:00Z"


# ---------------------------------------------------------------------------
# 5. TestRecipeRunHandlerE09
# ---------------------------------------------------------------------------


class TestRecipeRunHandlerE09:
    """E09: active_recipe_step_id -[SPAWNED {sst_semantic: LEADS_TO}]-> RecipeRun (if cursor set)."""

    async def test_e09_edge_created_when_active_recipe_step_set(
        self, services: HookStateService
    ) -> None:
        """E09 edge exists when active_recipe_step_id cursor is set before recipe:start."""
        services.data_layer_3.active_recipe_step_id = "sess-1::step::42"
        handler = RecipeRunHandler(services)
        data = {
            "session_id": "sess-1",
            "timestamp": "2026-01-01T00:00:00Z",
            "name": "my-recipe",
            "total_steps": 3,
            "status": "running",
        }
        await handler("recipe:start", data)

        recipe_run_id = "sess-1::recipe_run::2026-01-01T00:00:00Z"
        edge = await services.graph.get_edge("sess-1::step::42", recipe_run_id)
        assert edge is not None, "E09 edge (RecipeStep -> RecipeRun) must exist"
        assert edge.get("type") == "SPAWNED"
        assert edge.get("sst_semantic") == "LEADS_TO"

    async def test_e09_no_edge_when_no_active_recipe_step(
        self, services: HookStateService
    ) -> None:
        """No E09 edge when active_recipe_step_id is None."""
        assert services.data_layer_3.active_recipe_step_id is None
        handler = RecipeRunHandler(services)
        data = {
            "session_id": "sess-1",
            "timestamp": "2026-01-01T00:00:00Z",
            "name": "my-recipe",
            "total_steps": 3,
            "status": "running",
        }
        await handler("recipe:start", data)

        recipe_run_id = "sess-1::recipe_run::2026-01-01T00:00:00Z"
        spawned_edges = [
            (src, dst)
            for (src, dst), edge in services.graph._edges.items()
            if dst == recipe_run_id and edge.get("type") == "SPAWNED"
        ]
        assert len(spawned_edges) == 0


# ---------------------------------------------------------------------------
# 6. TestRecipeRunHandlerComplete
# ---------------------------------------------------------------------------


class TestRecipeRunHandlerComplete:
    """recipe:complete enriches RecipeRun node and pops the stack."""

    async def test_complete_enriches_recipe_run_with_ended_at(
        self, services: HookStateService
    ) -> None:
        """recipe:complete sets ended_at on the top-of-stack RecipeRun node."""
        handler = RecipeRunHandler(services)
        start_data = {
            "session_id": "sess-1",
            "timestamp": "2026-01-01T00:00:00Z",
            "name": "my-recipe",
            "total_steps": 3,
            "status": "running",
        }
        await handler("recipe:start", start_data)

        complete_timestamp = "2026-01-01T01:00:00Z"
        complete_data = {
            "session_id": "sess-1",
            "timestamp": complete_timestamp,
            "success": True,
            "final_status": "completed",
        }
        await handler("recipe:complete", complete_data)

        recipe_run_id = "sess-1::recipe_run::2026-01-01T00:00:00Z"
        node = await services.graph.get_node(recipe_run_id)
        assert node is not None
        assert node["ended_at"] == complete_timestamp

    async def test_complete_enriches_recipe_run_with_success(
        self, services: HookStateService
    ) -> None:
        """recipe:complete sets success on the top-of-stack RecipeRun node."""
        handler = RecipeRunHandler(services)
        start_data = {
            "session_id": "sess-1",
            "timestamp": "2026-01-01T00:00:00Z",
            "name": "my-recipe",
            "total_steps": 3,
            "status": "running",
        }
        await handler("recipe:start", start_data)

        complete_data = {
            "session_id": "sess-1",
            "timestamp": "2026-01-01T01:00:00Z",
            "success": True,
            "final_status": "completed",
        }
        await handler("recipe:complete", complete_data)

        recipe_run_id = "sess-1::recipe_run::2026-01-01T00:00:00Z"
        node = await services.graph.get_node(recipe_run_id)
        assert node is not None
        assert node["success"] is True

    async def test_complete_enriches_recipe_run_with_final_status(
        self, services: HookStateService
    ) -> None:
        """recipe:complete sets final_status on the top-of-stack RecipeRun node."""
        handler = RecipeRunHandler(services)
        start_data = {
            "session_id": "sess-1",
            "timestamp": "2026-01-01T00:00:00Z",
            "name": "my-recipe",
            "total_steps": 3,
            "status": "running",
        }
        await handler("recipe:start", start_data)

        complete_data = {
            "session_id": "sess-1",
            "timestamp": "2026-01-01T01:00:00Z",
            "success": True,
            "final_status": "completed",
        }
        await handler("recipe:complete", complete_data)

        recipe_run_id = "sess-1::recipe_run::2026-01-01T00:00:00Z"
        node = await services.graph.get_node(recipe_run_id)
        assert node is not None
        assert node["final_status"] == "completed"

    async def test_complete_pops_stack(self, services: HookStateService) -> None:
        """recipe:complete pops the recipe_run_id from active_recipe_run_stack."""
        handler = RecipeRunHandler(services)
        start_data = {
            "session_id": "sess-1",
            "timestamp": "2026-01-01T00:00:00Z",
            "name": "my-recipe",
            "total_steps": 3,
            "status": "running",
        }
        await handler("recipe:start", start_data)

        assert len(services.data_layer_3.active_recipe_run_stack) == 1

        complete_data = {
            "session_id": "sess-1",
            "timestamp": "2026-01-01T01:00:00Z",
            "success": True,
            "final_status": "completed",
        }
        await handler("recipe:complete", complete_data)

        assert services.data_layer_3.active_recipe_run_stack == []

    async def test_complete_clears_active_recipe_step_id(
        self, services: HookStateService
    ) -> None:
        """recipe:complete clears active_recipe_step_id cursor."""
        services.data_layer_3.active_recipe_step_id = "sess-1::step::7"
        handler = RecipeRunHandler(services)
        start_data = {
            "session_id": "sess-1",
            "timestamp": "2026-01-01T00:00:00Z",
            "name": "my-recipe",
            "total_steps": 3,
            "status": "running",
        }
        await handler("recipe:start", start_data)

        # Reset the step ID to simulate it being set during execution
        services.data_layer_3.active_recipe_step_id = "sess-1::step::7"

        complete_data = {
            "session_id": "sess-1",
            "timestamp": "2026-01-01T01:00:00Z",
            "success": True,
            "final_status": "completed",
        }
        await handler("recipe:complete", complete_data)

        assert services.data_layer_3.active_recipe_step_id is None

    async def test_complete_adds_sourced_from_edge(
        self, services: HookStateService
    ) -> None:
        """recipe:complete adds SOURCED_FROM edge to L1 complete event node."""
        handler = RecipeRunHandler(services)
        start_data = {
            "session_id": "sess-1",
            "timestamp": "2026-01-01T00:00:00Z",
            "name": "my-recipe",
            "total_steps": 3,
            "status": "running",
        }
        await handler("recipe:start", start_data)

        complete_timestamp = "2026-01-01T01:00:00Z"
        complete_data = {
            "session_id": "sess-1",
            "timestamp": complete_timestamp,
            "success": True,
            "final_status": "completed",
        }
        await handler("recipe:complete", complete_data)

        recipe_run_id = "sess-1::recipe_run::2026-01-01T00:00:00Z"
        expected_target = make_node_id("sess-1", "recipe:complete", complete_timestamp)
        edge = await services.graph.get_edge(recipe_run_id, expected_target)
        assert edge is not None, (
            "SOURCED_FROM edge must exist from RecipeRun to data_layer_1 complete event node"
        )
        assert edge.get("type") == "SOURCED_FROM"

    async def test_complete_empty_stack_is_noop(
        self, services: HookStateService
    ) -> None:
        """recipe:complete with empty stack returns HookResult(action='continue') without mutations."""
        handler = RecipeRunHandler(services)
        assert services.data_layer_3.active_recipe_run_stack == []

        result = await handler(
            "recipe:complete",
            {
                "session_id": "sess-1",
                "timestamp": "2026-01-01T01:00:00Z",
                "success": True,
                "final_status": "completed",
            },
        )
        assert result.action == "continue"
        # No nodes should have been created (stack was empty)
        assert len(services.graph._nodes) == 0

    async def test_complete_nested_pops_innermost(
        self, services: HookStateService
    ) -> None:
        """Two recipe:start then one recipe:complete pops the inner run, leaving outer."""
        handler = RecipeRunHandler(services)
        data_outer = {
            "session_id": "sess-1",
            "timestamp": "2026-01-01T00:00:00Z",
            "name": "outer-recipe",
            "total_steps": 2,
            "status": "running",
        }
        data_inner = {
            "session_id": "sess-1",
            "timestamp": "2026-01-01T00:01:00Z",
            "name": "inner-recipe",
            "total_steps": 1,
            "status": "running",
        }
        await handler("recipe:start", data_outer)
        await handler("recipe:start", data_inner)

        assert len(services.data_layer_3.active_recipe_run_stack) == 2

        complete_data = {
            "session_id": "sess-1",
            "timestamp": "2026-01-01T00:02:00Z",
            "success": True,
            "final_status": "completed",
        }
        await handler("recipe:complete", complete_data)

        stack = services.data_layer_3.active_recipe_run_stack
        assert len(stack) == 1
        assert stack[0] == "sess-1::recipe_run::2026-01-01T00:00:00Z"


# ---------------------------------------------------------------------------
# 7. TestRecipeRunHandlerApproval
# ---------------------------------------------------------------------------

SESSION_ID = "sess-approval"
T0 = "2026-02-01T10:00:00Z"
T1 = "2026-02-01T10:05:00Z"

APPROVAL_DATA_1 = {
    "session_id": SESSION_ID,
    "timestamp": T0,
    "name": "staged-recipe",
    "stage_name": "deploy",
    "status": "waiting_approval",
    "is_approval_gate": True,
}

APPROVAL_DATA_2 = {
    "session_id": SESSION_ID,
    "timestamp": T1,
    "stage_name": "verify",
}


class TestRecipeRunHandlerApproval:
    """recipe:approval (staged recipe) creates RecipeRun on first call only."""

    async def test_approval_empty_stack_creates_recipe_run_node_with_labels(
        self, services: HookStateService
    ) -> None:
        """First recipe:approval with empty stack creates RecipeRun:SST_EVENT node at
        {SESSION_ID}::recipe_run::{T0} with both 'RecipeRun' and 'SST_EVENT' labels."""
        handler = RecipeRunHandler(services)
        await handler("recipe:approval", APPROVAL_DATA_1)

        recipe_run_id = f"{SESSION_ID}::recipe_run::{T0}"
        node = await services.graph.get_node(recipe_run_id)
        assert node is not None, f"RecipeRun node must exist at '{recipe_run_id}'"
        assert "RecipeRun" in node["labels"]
        assert "SST_EVENT" in node["labels"]

    async def test_approval_creates_recipe_concept_node(
        self, services: HookStateService
    ) -> None:
        """First recipe:approval creates Recipe:SST_CONCEPT node."""
        handler = RecipeRunHandler(services)
        await handler("recipe:approval", APPROVAL_DATA_1)

        node = await services.graph.get_node("staged-recipe")
        assert node is not None, "Recipe:SST_CONCEPT node must exist"
        assert "Recipe" in node["labels"]
        assert "SST_CONCEPT" in node["labels"]

    async def test_approval_creates_e06_and_e07_edges(
        self, services: HookStateService
    ) -> None:
        """First recipe:approval creates E06 (HAS_RECIPE_RUN/CONTAINS) and E07 (HAS_RECIPE/EXPRESSES) edges."""
        handler = RecipeRunHandler(services)
        await handler("recipe:approval", APPROVAL_DATA_1)

        recipe_run_id = f"{SESSION_ID}::recipe_run::{T0}"

        e06 = await services.graph.get_edge(SESSION_ID, recipe_run_id)
        assert e06 is not None, "E06 edge (Session -> RecipeRun) must exist"
        assert e06.get("type") == "HAS_RECIPE_RUN"
        assert e06.get("sst_semantic") == "CONTAINS"

        e07 = await services.graph.get_edge(recipe_run_id, "staged-recipe")
        assert e07 is not None, "E07 edge (RecipeRun -> Recipe) must exist"
        assert e07.get("type") == "HAS_RECIPE"
        assert e07.get("sst_semantic") == "EXPRESSES"

    async def test_approval_sourced_from_uses_stage_name_disambiguator(
        self, services: HookStateService
    ) -> None:
        """SOURCED_FROM target uses stage_name as disambiguator:
        make_node_id(SESSION_ID, 'recipe:approval', T0, 'deploy')."""
        handler = RecipeRunHandler(services)
        await handler("recipe:approval", APPROVAL_DATA_1)

        recipe_run_id = f"{SESSION_ID}::recipe_run::{T0}"
        expected_target = make_node_id(SESSION_ID, "recipe:approval", T0, "deploy")
        edge = await services.graph.get_edge(recipe_run_id, expected_target)
        assert edge is not None, (
            f"SOURCED_FROM edge must exist from RecipeRun to {expected_target!r}"
        )
        assert edge.get("type") == "SOURCED_FROM"

    async def test_approval_pushes_onto_active_recipe_run_stack(
        self, services: HookStateService
    ) -> None:
        """First recipe:approval pushes recipe_run_id onto active_recipe_run_stack."""
        handler = RecipeRunHandler(services)
        assert services.data_layer_3.active_recipe_run_stack == []

        await handler("recipe:approval", APPROVAL_DATA_1)

        recipe_run_id = f"{SESSION_ID}::recipe_run::{T0}"
        assert recipe_run_id in services.data_layer_3.active_recipe_run_stack

    async def test_second_approval_nonempty_stack_no_new_recipe_run(
        self, services: HookStateService
    ) -> None:
        """Second recipe:approval (stack non-empty) does NOT create a second RecipeRun;
        stack unchanged, exactly one RecipeRun node exists."""
        handler = RecipeRunHandler(services)
        await handler("recipe:approval", APPROVAL_DATA_1)

        # Stack now has one item
        assert len(services.data_layer_3.active_recipe_run_stack) == 1

        # Send second approval event with a different timestamp and stage
        await handler("recipe:approval", APPROVAL_DATA_2)

        # Stack should still have exactly one item
        assert len(services.data_layer_3.active_recipe_run_stack) == 1

        # Exactly one RecipeRun node must exist
        recipe_run_nodes = [
            node_id
            for node_id, node in services.graph._nodes.items()
            if "RecipeRun" in node.get("labels", [])
        ]
        assert len(recipe_run_nodes) == 1, (
            "Exactly one RecipeRun node must exist after two approval events"
        )
