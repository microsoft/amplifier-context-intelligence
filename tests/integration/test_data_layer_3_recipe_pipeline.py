"""Integration tests — recipe run and step pipeline via full 12-enricher pipeline.

Tests verify the complete event processing pipeline from raw events through
to graph state for data_layer_3 recipe handlers, covering:
- TestFlatRecipePipeline: flat recipe lifecycle from start through step events to complete
- TestStagedRecipePipeline: staged recipe with approval gates creating RecipeRun on first call
- TestNestedRecipePipeline: nested recipe runs and E09 SPAWNED edge from outer step to inner run
- TestE11ToolPipeline: tool:pre inside a recipe step creates E11 TRIGGERED LEADS_TO edge

Uses the full setup_handlers() pipeline (8 Layer 2 + RecipeRunHandler +
RecipeStepHandler + DelegationHandler + SkillLoadHandler = 12 enrichers total).
"""

from __future__ import annotations

import types
from typing import Any

from context_intelligence_server.pipeline import process_event, setup_handlers
from context_intelligence_server.services import HookStateService
from context_intelligence_server.utils import make_node_id  # noqa: F401


# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------

WORKSPACE = "integ-test-l3-recipe-workspace"
SESSION_ID = "session-recipe-001"
RECIPE_NAME = "deploy-pipeline"

# Ordered nanosecond ISO timestamps
T0 = "2026-01-01T00:00:00.000000000+00:00"
T1 = "2026-01-01T00:00:01.000000000+00:00"
T2 = "2026-01-01T00:00:02.000000000+00:00"
T3 = "2026-01-01T00:00:03.000000000+00:00"
T4 = "2026-01-01T00:00:04.000000000+00:00"
T5 = "2026-01-01T00:00:05.000000000+00:00"


# ---------------------------------------------------------------------------
# Helper factory — creates a worker + services pair without real async queue
# ---------------------------------------------------------------------------


def _make_worker_and_services(
    workspace: str = WORKSPACE,
) -> Any:
    """Return a SimpleNamespace worker with a ``services`` attribute.

    The returned object satisfies the ``SessionWorker`` interface used by
    ``process_event`` — namely ``worker.services`` — and exposes the
    ``HookStateService`` instance at ``worker.services`` for test assertions.

    Annotated ``Any`` so that pyright accepts the duck-typed SimpleNamespace
    in place of the ``SessionWorker`` protocol expected by ``process_event``.
    """
    services = HookStateService(workspace=workspace)
    return types.SimpleNamespace(services=services)


# ===========================================================================
# TestFlatRecipePipeline
# ===========================================================================


class TestFlatRecipePipeline:
    """Flat recipe lifecycle via the full 12-enricher pipeline."""

    async def test_recipe_run_and_recipe_nodes_exist_after_start(self) -> None:
        """recipe:start creates RecipeRun:SST_EVENT and Recipe:SST_CONCEPT (no SST_EVENT label).

        Verifies:
        - RecipeRun node exists at {SESSION_ID}::recipe_run::{T0}
        - RecipeRun has both 'RecipeRun' and 'SST_EVENT' labels
        - Recipe concept node exists at RECIPE_NAME
        - Recipe node has 'SST_CONCEPT' label but NOT 'SST_EVENT'
        """
        worker = _make_worker_and_services()
        services = worker.services
        handlers = setup_handlers(services)

        await process_event(
            worker,
            "recipe:start",
            {
                "session_id": SESSION_ID,
                "timestamp": T0,
                "name": RECIPE_NAME,
                "total_steps": 3,
                "status": "running",
            },
            handlers,
        )

        run_id = f"{SESSION_ID}::recipe_run::{T0}"

        # RecipeRun:SST_EVENT node
        run_node = await services.graph.get_node(run_id)
        assert run_node is not None, f"RecipeRun node must exist at '{run_id}'"
        assert "RecipeRun" in run_node["labels"]
        assert "SST_EVENT" in run_node["labels"]

        # Recipe:SST_CONCEPT node — NOT SST_EVENT
        recipe_node = await services.graph.get_node(RECIPE_NAME)
        assert recipe_node is not None, (
            f"Recipe concept node must exist at '{RECIPE_NAME}'"
        )
        assert "Recipe" in recipe_node["labels"]
        assert "SST_CONCEPT" in recipe_node["labels"]
        assert "SST_EVENT" not in recipe_node["labels"], (
            "Recipe concept node must NOT have SST_EVENT label"
        )

    async def test_recipe_step_creates_step_node_and_e08(self) -> None:
        """recipe:step creates RecipeStep with name='build' and E08 HAS_STEP/CONTAINS edge.

        Sequence: recipe:start (T0) → recipe:step current_step=0 (T1)

        Verifies:
        - RecipeStep node exists at {run_id}::step::0 with name='build'
        - E08 edge: RecipeRun -[HAS_STEP {sst_semantic: CONTAINS}]-> RecipeStep
        """
        worker = _make_worker_and_services()
        services = worker.services
        handlers = setup_handlers(services)

        run_id = f"{SESSION_ID}::recipe_run::{T0}"

        # Start recipe
        await process_event(
            worker,
            "recipe:start",
            {
                "session_id": SESSION_ID,
                "timestamp": T0,
                "name": RECIPE_NAME,
                "total_steps": 2,
                "status": "running",
            },
            handlers,
        )

        # First step
        await process_event(
            worker,
            "recipe:step",
            {
                "session_id": SESSION_ID,
                "timestamp": T1,
                "current_step": 0,
                "steps": [{"name": "build"}, {"name": "deploy"}],
            },
            handlers,
        )

        step_id = f"{run_id}::step::0"

        # RecipeStep node with name='build'
        step_node = await services.graph.get_node(step_id)
        assert step_node is not None, f"RecipeStep node must exist at '{step_id}'"
        assert step_node["name"] == "build"

        # E08 edge
        edge = await services.graph.get_edge(run_id, step_id)
        assert edge is not None, "E08 edge (RecipeRun -> RecipeStep) must exist"
        assert edge["type"] == "HAS_STEP"
        assert edge["sst_semantic"] == "CONTAINS"

    async def test_full_flat_recipe_run_is_enriched_on_complete(self) -> None:
        """Full sequence start→step×2→complete enriches RecipeRun and clears cursors.

        Sequence: recipe:start (T0) → step 0 'build' (T1) → step 1 'deploy' (T2)
                  → recipe:complete (T3)

        Verifies:
        - RecipeRun has ended_at=T3, success=True, final_status='completed'
        - active_recipe_run_stack is empty (popped on complete)
        - active_recipe_step_id is None (cleared on complete)
        - Step 0 node ('build') and Step 1 node ('deploy') both exist
        """
        worker = _make_worker_and_services()
        services = worker.services
        handlers = setup_handlers(services)

        run_id = f"{SESSION_ID}::recipe_run::{T0}"

        # Start recipe
        await process_event(
            worker,
            "recipe:start",
            {
                "session_id": SESSION_ID,
                "timestamp": T0,
                "name": RECIPE_NAME,
                "total_steps": 2,
                "status": "running",
            },
            handlers,
        )

        # Step 0: build
        await process_event(
            worker,
            "recipe:step",
            {
                "session_id": SESSION_ID,
                "timestamp": T1,
                "current_step": 0,
                "steps": [{"name": "build"}, {"name": "deploy"}],
            },
            handlers,
        )

        # Step 1: deploy
        await process_event(
            worker,
            "recipe:step",
            {
                "session_id": SESSION_ID,
                "timestamp": T2,
                "current_step": 1,
                "steps": [{"name": "build"}, {"name": "deploy"}],
            },
            handlers,
        )

        # Complete
        await process_event(
            worker,
            "recipe:complete",
            {
                "session_id": SESSION_ID,
                "timestamp": T3,
                "success": True,
                "final_status": "completed",
            },
            handlers,
        )

        # RecipeRun enriched with completion data
        run_node = await services.graph.get_node(run_id)
        assert run_node is not None, f"RecipeRun node must exist at '{run_id}'"
        assert run_node["ended_at"] == T3
        assert run_node["success"] is True
        assert run_node["final_status"] == "completed"

        # Stack and step cursor cleared
        assert services.data_layer_3.active_recipe_run_stack == [], (
            "active_recipe_run_stack must be empty after recipe:complete"
        )
        assert services.data_layer_3.active_recipe_step_id is None, (
            "active_recipe_step_id must be None after recipe:complete"
        )

        # Both step nodes exist with correct names
        step0 = await services.graph.get_node(f"{run_id}::step::0")
        assert step0 is not None, "Step 0 (build) node must exist"
        assert step0["name"] == "build"

        step1 = await services.graph.get_node(f"{run_id}::step::1")
        assert step1 is not None, "Step 1 (deploy) node must exist"
        assert step1["name"] == "deploy"


# ===========================================================================
# TestStagedRecipePipeline
# ===========================================================================


class TestStagedRecipePipeline:
    """Staged recipe with approval gates via the full 12-enricher pipeline."""

    async def test_first_approval_creates_recipe_run(self) -> None:
        """First recipe:approval with empty stack creates a RecipeRun:SST_EVENT node.

        Verifies:
        - RecipeRun node exists at {SESSION_ID}::recipe_run::{T0}
        - Has 'RecipeRun' and 'SST_EVENT' labels
        """
        worker = _make_worker_and_services()
        services = worker.services
        handlers = setup_handlers(services)

        await process_event(
            worker,
            "recipe:approval",
            {
                "session_id": SESSION_ID,
                "timestamp": T0,
                "name": RECIPE_NAME,
                "stage_name": "deploy",
                "status": "waiting_approval",
                "is_approval_gate": True,
            },
            handlers,
        )

        run_id = f"{SESSION_ID}::recipe_run::{T0}"
        node = await services.graph.get_node(run_id)
        assert node is not None, f"RecipeRun node must exist at '{run_id}'"
        assert "RecipeRun" in node["labels"]
        assert "SST_EVENT" in node["labels"]

    async def test_staged_pipeline_creates_recipe_step_for_each_approval(
        self,
    ) -> None:
        """Two approvals create two RecipeStep nodes with correct stage_name properties.

        Sequence: recipe:approval stage_name='deploy' (T0)
                → recipe:approval stage_name='verify' (T1)

        Verifies:
        - RecipeStep exists at {run_id}::step::deploy with stage_name='deploy'
        - RecipeStep exists at {run_id}::step::verify with stage_name='verify'
        """
        worker = _make_worker_and_services()
        services = worker.services
        handlers = setup_handlers(services)

        run_id = f"{SESSION_ID}::recipe_run::{T0}"

        # First approval — creates RecipeRun and RecipeStep::deploy
        await process_event(
            worker,
            "recipe:approval",
            {
                "session_id": SESSION_ID,
                "timestamp": T0,
                "name": RECIPE_NAME,
                "stage_name": "deploy",
                "status": "waiting_approval",
                "is_approval_gate": True,
            },
            handlers,
        )

        # Second approval — RecipeRun already exists; creates RecipeStep::verify
        await process_event(
            worker,
            "recipe:approval",
            {
                "session_id": SESSION_ID,
                "timestamp": T1,
                "stage_name": "verify",
            },
            handlers,
        )

        # Both RecipeStep nodes must exist with correct stage_name
        step_deploy = await services.graph.get_node(f"{run_id}::step::deploy")
        assert step_deploy is not None, (
            f"RecipeStep node must exist at '{run_id}::step::deploy'"
        )
        assert step_deploy["stage_name"] == "deploy"

        step_verify = await services.graph.get_node(f"{run_id}::step::verify")
        assert step_verify is not None, (
            f"RecipeStep node must exist at '{run_id}::step::verify'"
        )
        assert step_verify["stage_name"] == "verify"

    async def test_staged_complete_enriches_recipe_run(self) -> None:
        """recipe:complete after approval gates enriches the RecipeRun with completion data.

        Sequence: recipe:approval (T0) → recipe:complete (T2)

        Verifies:
        - RecipeRun has ended_at=T2, success=True
        """
        worker = _make_worker_and_services()
        services = worker.services
        handlers = setup_handlers(services)

        run_id = f"{SESSION_ID}::recipe_run::{T0}"

        # First approval creates RecipeRun
        await process_event(
            worker,
            "recipe:approval",
            {
                "session_id": SESSION_ID,
                "timestamp": T0,
                "name": RECIPE_NAME,
                "stage_name": "deploy",
                "status": "waiting_approval",
                "is_approval_gate": True,
            },
            handlers,
        )

        # Complete
        await process_event(
            worker,
            "recipe:complete",
            {
                "session_id": SESSION_ID,
                "timestamp": T2,
                "success": True,
                "final_status": "completed",
            },
            handlers,
        )

        node = await services.graph.get_node(run_id)
        assert node is not None, f"RecipeRun node must exist at '{run_id}'"
        assert node["ended_at"] == T2
        assert node["success"] is True

    async def test_staged_pipeline_has_only_one_recipe_run(self) -> None:
        """Exactly one RecipeRun node exists after two approval events.

        Verifies that the second recipe:approval (non-empty stack) does NOT create
        a second RecipeRun — the RecipeRunHandler is idempotent on subsequent approvals.
        """
        worker = _make_worker_and_services()
        services = worker.services
        handlers = setup_handlers(services)

        # First approval
        await process_event(
            worker,
            "recipe:approval",
            {
                "session_id": SESSION_ID,
                "timestamp": T0,
                "name": RECIPE_NAME,
                "stage_name": "deploy",
                "status": "waiting_approval",
                "is_approval_gate": True,
            },
            handlers,
        )

        # Second approval
        await process_event(
            worker,
            "recipe:approval",
            {
                "session_id": SESSION_ID,
                "timestamp": T1,
                "stage_name": "verify",
            },
            handlers,
        )

        # Count RecipeRun nodes
        recipe_run_nodes = [
            node_id
            for node_id, node in services.graph._nodes.items()
            if "RecipeRun" in node.get("labels", [])
        ]
        assert len(recipe_run_nodes) == 1, (
            f"Exactly one RecipeRun node must exist after two approval events; "
            f"found: {recipe_run_nodes}"
        )


# ===========================================================================
# TestNestedRecipePipeline
# ===========================================================================


class TestNestedRecipePipeline:
    """Nested recipe runs and E09 SPAWNED edge via the full 12-enricher pipeline."""

    async def test_e09_edge_from_outer_step_to_inner_recipe_run(self) -> None:
        """Outer recipe:step cursor sets active_recipe_step_id; inner recipe:start creates E09.

        Sequence: outer recipe:start (T0)
                → outer recipe:step current_step=0 (T1) — sets active_recipe_step_id
                → inner recipe:start (T2) — creates E09 from outer_step to inner_run

        Verifies:
        - E09 SPAWNED LEADS_TO edge from outer_step_id to inner_run_id
        """
        worker = _make_worker_and_services()
        services = worker.services
        handlers = setup_handlers(services)

        outer_run_id = f"{SESSION_ID}::recipe_run::{T0}"
        outer_step_id = f"{outer_run_id}::step::0"
        inner_run_id = f"{SESSION_ID}::recipe_run::{T2}"

        # Outer recipe:start
        await process_event(
            worker,
            "recipe:start",
            {
                "session_id": SESSION_ID,
                "timestamp": T0,
                "name": RECIPE_NAME,
                "total_steps": 2,
                "status": "running",
            },
            handlers,
        )

        # Outer recipe:step (sets active_recipe_step_id)
        await process_event(
            worker,
            "recipe:step",
            {
                "session_id": SESSION_ID,
                "timestamp": T1,
                "current_step": 0,
                "steps": [{"name": "build"}, {"name": "deploy"}],
            },
            handlers,
        )

        # Inner recipe:start (should create E09 from outer_step to inner_run)
        await process_event(
            worker,
            "recipe:start",
            {
                "session_id": SESSION_ID,
                "timestamp": T2,
                "name": "inner-recipe",
                "total_steps": 1,
                "status": "running",
            },
            handlers,
        )

        # Verify E09 SPAWNED edge from outer step to inner run
        edge = await services.graph.get_edge(outer_step_id, inner_run_id)
        assert edge is not None, (
            f"E09 SPAWNED edge must exist from '{outer_step_id}' to '{inner_run_id}'"
        )
        assert edge["type"] == "SPAWNED"
        assert edge["sst_semantic"] == "LEADS_TO"

    async def test_nested_recipe_stack_depth_and_recovery(self) -> None:
        """Stack grows to 2 on nested start; each complete pops one level back to 0.

        Sequence:
        - outer recipe:start (T0) → stack depth = 1
        - inner recipe:start (T1) → stack depth = 2
        - inner recipe:complete (T2) → stack depth = 1
        - outer recipe:complete (T3) → stack depth = 0

        Verifies stack depth at each step and that the final stack is empty.
        """
        worker = _make_worker_and_services()
        services = worker.services
        handlers = setup_handlers(services)

        # Outer recipe:start
        await process_event(
            worker,
            "recipe:start",
            {
                "session_id": SESSION_ID,
                "timestamp": T0,
                "name": RECIPE_NAME,
                "total_steps": 2,
                "status": "running",
            },
            handlers,
        )
        assert len(services.data_layer_3.active_recipe_run_stack) == 1, (
            "Stack depth must be 1 after outer recipe:start"
        )

        # Inner recipe:start
        await process_event(
            worker,
            "recipe:start",
            {
                "session_id": SESSION_ID,
                "timestamp": T1,
                "name": "inner-recipe",
                "total_steps": 1,
                "status": "running",
            },
            handlers,
        )
        assert len(services.data_layer_3.active_recipe_run_stack) == 2, (
            "Stack depth must be 2 after inner recipe:start"
        )

        # Inner recipe:complete (pops inner)
        await process_event(
            worker,
            "recipe:complete",
            {
                "session_id": SESSION_ID,
                "timestamp": T2,
                "success": True,
                "final_status": "completed",
            },
            handlers,
        )
        assert len(services.data_layer_3.active_recipe_run_stack) == 1, (
            "Stack depth must be 1 after inner recipe:complete"
        )

        # Outer recipe:complete (pops outer)
        await process_event(
            worker,
            "recipe:complete",
            {
                "session_id": SESSION_ID,
                "timestamp": T3,
                "success": True,
                "final_status": "completed",
            },
            handlers,
        )
        assert len(services.data_layer_3.active_recipe_run_stack) == 0, (
            "Stack depth must be 0 after outer recipe:complete"
        )


# ===========================================================================
# TestE11ToolPipeline
# ===========================================================================


class TestE11ToolPipeline:
    """E11 TRIGGERED edge from RecipeStep to ToolCall via the full 12-enricher pipeline."""

    async def test_e11_triggered_edge_when_tool_pre_inside_step(self) -> None:
        """recipe:start → recipe:step → tool:pre creates TRIGGERED LEADS_TO edge.

        Sequence: recipe:start (T0)
                → recipe:step current_step=0 (T1) — sets active_recipe_step_id
                → tool:pre (T2) — creates E11 TRIGGERED LEADS_TO from RecipeStep

        Verifies:
        - E11 edge from RecipeStep to ToolCall with type='TRIGGERED', sst_semantic='LEADS_TO'
        """
        worker = _make_worker_and_services()
        services = worker.services
        handlers = setup_handlers(services)

        tool_call_id = "tc-recipe-001"
        run_id = f"{SESSION_ID}::recipe_run::{T0}"
        step_id = f"{run_id}::step::0"

        # Start recipe
        await process_event(
            worker,
            "recipe:start",
            {
                "session_id": SESSION_ID,
                "timestamp": T0,
                "name": RECIPE_NAME,
                "total_steps": 1,
                "status": "running",
            },
            handlers,
        )

        # Recipe step (sets active_recipe_step_id)
        await process_event(
            worker,
            "recipe:step",
            {
                "session_id": SESSION_ID,
                "timestamp": T1,
                "current_step": 0,
                "steps": [{"name": "build"}],
            },
            handlers,
        )

        # tool:pre — creates E11 from active step to tool_call_id
        await process_event(
            worker,
            "tool:pre",
            {
                "session_id": SESSION_ID,
                "timestamp": T2,
                "tool_call_id": tool_call_id,
                "tool_name": "bash",
            },
            handlers,
        )

        edge = await services.graph.get_edge(step_id, tool_call_id)
        assert edge is not None, (
            f"E11 TRIGGERED edge must exist from '{step_id}' to '{tool_call_id}'"
        )
        assert edge["type"] == "TRIGGERED"
        assert edge["sst_semantic"] == "LEADS_TO"

    async def test_e11_no_triggered_edge_when_tool_pre_outside_recipe(self) -> None:
        """tool:pre fired before any recipe:step is a no-op (no TRIGGERED edges from steps).

        Sends tool:pre without a preceding recipe:start or recipe:step so that
        active_recipe_step_id is None.

        Verifies:
        - No edges where '::step::' is in the source node ID and type='TRIGGERED'
        """
        worker = _make_worker_and_services()
        services = worker.services
        handlers = setup_handlers(services)

        # tool:pre with no prior recipe lifecycle — active_recipe_step_id is None
        await process_event(
            worker,
            "tool:pre",
            {
                "session_id": SESSION_ID,
                "timestamp": T0,
                "tool_call_id": "tc-outside-001",
                "tool_name": "bash",
            },
            handlers,
        )

        # Filter edges where '::step::' in source and type='TRIGGERED'
        triggered_from_steps = [
            (src, dst)
            for (src, dst), edge in services.graph._edges.items()
            if "::step::" in src and edge.get("type") == "TRIGGERED"
        ]
        assert len(triggered_from_steps) == 0, (
            "No TRIGGERED edges from step nodes must exist when "
            "tool:pre fires outside a recipe step"
        )
