"""RecipeStepHandler — correlates recipe step lifecycle events into RecipeStep nodes.

Each recipe step lifecycle produces up to four events:
  recipe:step          — flat step execution started
  recipe:approval      — approval gate reached within a recipe run
  recipe:loop_iteration — loop iteration within a recipe run step
  tool:pre (E11)       — tool call triggered by the active recipe step

This handler creates RecipeStep:SST_EVENT nodes per recipe step, keyed as
'{run_id}::step::{current_step}', and manages the active_recipe_step_id cursor
so DelegationHandler can create E10 edges connecting tool calls to the currently
active recipe step.

Semantic edges created:
  E08: RecipeRun -[HAS_STEP {sst_semantic: CONTAINS}]-> RecipeStep
  E11: RecipeStep -[TRIGGERED {sst_semantic: LEADS_TO}]-> ToolCall (tool:pre)
  SOURCED_FROM: RecipeStep -> data_layer_1 event node
"""

from __future__ import annotations

import logging
from typing import Any

from context_intelligence_server.protocol import HookResult
from context_intelligence_server.services import HookStateService
from context_intelligence_server.utils import make_node_id

logger = logging.getLogger(__name__)


class RecipeStepHandler:
    """Enricher handler for recipe step lifecycle events.

    Correlates recipe:step, recipe:approval, recipe:loop_iteration, and
    tool:pre events, creating RecipeStep:SST_EVENT nodes and managing the
    active_recipe_step_id cursor.
    """

    handled_events: frozenset[str] = frozenset(
        {
            "recipe:step",
            "recipe:approval",
            "recipe:loop_iteration",
            "tool:pre",
        }
    )

    def __init__(self, services: HookStateService) -> None:
        self.services = services

    async def __call__(self, event: str, data: dict[str, Any]) -> HookResult:
        """Handle a recipe step lifecycle event and dispatch to sub-handler."""
        session_id: str = data.get("session_id", "")

        if event == "recipe:step":
            return await self._handle_step(session_id, data)
        elif event == "recipe:approval":
            await self._handle_approval(session_id, data)
        elif event == "recipe:loop_iteration":
            await self._handle_loop_iteration(session_id, data)
        elif event == "tool:pre":
            await self._handle_tool_pre(session_id, data)

        return HookResult(action="continue")

    async def _handle_step(self, session_id: str, data: dict[str, Any]) -> HookResult:
        """Create RecipeStep node for a flat recipe:step event.

        Always clears active_recipe_step_id cursor first (before the empty-stack
        guard), so that an interrupted recipe step is never seen by a subsequent
        handler as still active.

        Creates:
        - RecipeStep:SST_EVENT node at '{run_id}::step::{current_step}'
          with name, started_at, current_step properties
        - E08: RecipeRun -[HAS_STEP {sst_semantic: CONTAINS}]-> RecipeStep
        - SOURCED_FROM: RecipeStep -> make_node_id(session_id, 'recipe:step',
          timestamp, str(current_step))

        Sets active_recipe_step_id cursor to the new step node ID.
        Returns HookResult(action='continue') in all cases.
        """
        # Clear cursor first — before the empty-stack guard
        self.services.data_layer_3.active_recipe_step_id = None

        # Guard: empty stack means no active recipe run to attach to
        stack = self.services.data_layer_3.active_recipe_run_stack
        if not stack:
            return HookResult(action="continue")

        run_id = stack[-1]
        timestamp: str = data.get("timestamp", "")
        current_step: Any = data.get("current_step", 0)

        # Extract step_name from steps array with fallback to str(current_step)
        try:
            step_name: str = data["steps"][current_step]["name"]
        except (IndexError, KeyError, TypeError):
            step_name = str(current_step)

        step_id = f"{run_id}::step::{current_step}"

        # Upsert RecipeStep:SST_EVENT node
        await self.services.graph.upsert_node(
            step_id,
            {
                "labels": ["RecipeStep", "SST_EVENT"],
                "name": step_name,
                "started_at": timestamp,
                "current_step": current_step,
            },
        )

        # E08: RecipeRun -[HAS_STEP {sst_semantic: CONTAINS}]-> RecipeStep
        await self.services.graph.upsert_edge(
            run_id,
            step_id,
            {"type": "HAS_STEP", "sst_semantic": "CONTAINS"},
        )

        # SOURCED_FROM: RecipeStep -> data_layer_1 recipe:step event node
        data_layer_1_node_id = make_node_id(
            session_id, "recipe:step", timestamp, str(current_step)
        )
        await self.services.graph.upsert_edge(
            step_id, data_layer_1_node_id, {"type": "SOURCED_FROM"}
        )

        # Set cursor to the new step
        self.services.data_layer_3.active_recipe_step_id = step_id

        return HookResult(action="continue")

    async def _handle_approval(self, session_id: str, data: dict[str, Any]) -> None:
        """Create RecipeStep node for a recipe:approval gate event.

        Always clears active_recipe_step_id cursor first — even if the empty-stack
        guard fires (i.e. cursor is cleared regardless of stack state).

        Creates:
        - RecipeStep:SST_EVENT node at '{run_id}::step::{stage_name}'
          with stage_name, prompt (when present), started_at properties
        - E08: RecipeRun -[HAS_STEP {sst_semantic: CONTAINS}]-> RecipeStep
        - SOURCED_FROM: RecipeStep -> make_node_id(session_id, 'recipe:approval',
          timestamp, stage_name)

        Sets active_recipe_step_id cursor to the new step node ID.
        """
        # Clear cursor first — even if guard fires
        self.services.data_layer_3.active_recipe_step_id = None

        stack = self.services.data_layer_3.active_recipe_run_stack
        if not stack:
            return

        run_id = stack[-1]
        timestamp: str = data.get("timestamp", "")
        stage_name: str = data.get("stage_name", "")
        prompt: Any = data.get("prompt")

        step_id = f"{run_id}::step::{stage_name}"

        node_data: dict[str, Any] = {
            "labels": ["RecipeStep", "SST_EVENT"],
            "stage_name": stage_name,
            "name": stage_name,
            "started_at": timestamp,
        }
        if prompt is not None:
            node_data["prompt"] = prompt

        await self.services.graph.upsert_node(step_id, node_data)

        # E08: RecipeRun -[HAS_STEP {sst_semantic: CONTAINS}]-> RecipeStep
        await self.services.graph.upsert_edge(
            run_id,
            step_id,
            {"type": "HAS_STEP", "sst_semantic": "CONTAINS"},
        )

        # SOURCED_FROM with stage_name as disambiguator
        data_layer_1_node_id = make_node_id(
            session_id, "recipe:approval", timestamp, stage_name
        )
        await self.services.graph.upsert_edge(
            step_id, data_layer_1_node_id, {"type": "SOURCED_FROM"}
        )

        # Set cursor
        self.services.data_layer_3.active_recipe_step_id = step_id

    async def _handle_loop_iteration(
        self, session_id: str, data: dict[str, Any]
    ) -> None:
        """Create RecipeStep node for a recipe:loop_iteration event.

        Always clears active_recipe_step_id cursor first, before the guard.

        Creates:
        - RecipeStep:SST_EVENT node at '{run_id}::step::{step_id}::loop::{iteration}'
          with name='loop iteration {iteration}', step_id, iteration,
          max_iterations properties
        - E08: RecipeRun -[HAS_STEP {sst_semantic: CONTAINS}]-> RecipeStep
        - SOURCED_FROM: RecipeStep -> make_node_id(session_id,
          'recipe:loop_iteration', timestamp, '{step_id}_{iteration}')

        Sets active_recipe_step_id cursor to the new step node ID.
        """
        # Clear cursor first
        self.services.data_layer_3.active_recipe_step_id = None

        stack = self.services.data_layer_3.active_recipe_run_stack
        if not stack:
            return

        run_id = stack[-1]
        timestamp: str = data.get("timestamp", "")
        loop_step_id: str = data.get("step_id", "")
        iteration: Any = data.get("iteration")
        max_iterations: Any = data.get("max_iterations")

        node_key = f"{run_id}::step::{loop_step_id}::loop::{iteration}"

        node_data: dict[str, Any] = {
            "labels": ["RecipeStep", "SST_EVENT"],
            "name": f"loop iteration {iteration}",
            "step_id": loop_step_id,
            "iteration": iteration,
            "max_iterations": max_iterations,
            "started_at": timestamp,
        }

        await self.services.graph.upsert_node(node_key, node_data)

        # E08: RecipeRun -[HAS_STEP {sst_semantic: CONTAINS}]-> RecipeStep
        await self.services.graph.upsert_edge(
            run_id,
            node_key,
            {"type": "HAS_STEP", "sst_semantic": "CONTAINS"},
        )

        # SOURCED_FROM with '{step_id}_{iteration}' as disambiguator
        disambiguator = f"{loop_step_id}_{iteration}"
        data_layer_1_node_id = make_node_id(
            session_id, "recipe:loop_iteration", timestamp, disambiguator
        )
        await self.services.graph.upsert_edge(
            node_key, data_layer_1_node_id, {"type": "SOURCED_FROM"}
        )

        # Set cursor
        self.services.data_layer_3.active_recipe_step_id = node_key

    async def _handle_tool_pre(self, session_id: str, data: dict[str, Any]) -> None:
        """Create E11 TRIGGERED LEADS_TO edge from active RecipeStep to ToolCall.

        Creates:
        - E11: RecipeStep -[TRIGGERED {sst_semantic: LEADS_TO}]-> ToolCall
          only when both active_recipe_step_id and tool_call_id are present.
        """
        active_step_id = self.services.data_layer_3.active_recipe_step_id
        tool_call_id: str | None = data.get("tool_call_id")

        if active_step_id is not None and tool_call_id is not None:
            await self.services.graph.upsert_edge(
                active_step_id,
                tool_call_id,
                {"type": "TRIGGERED", "sst_semantic": "LEADS_TO"},
            )
