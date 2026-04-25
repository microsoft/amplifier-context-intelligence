"""RecipeRunHandler — correlates recipe lifecycle events into RecipeRun nodes.

Each recipe run lifecycle produces up to five events:
  recipe:start          — recipe execution started
  recipe:complete       — recipe execution finished
  recipe:approval       — approval gate reached (may create run on first call)
  recipe:loop_iteration — loop iteration occurred within a run
  recipe:loop_complete  — loop finished within a run

This handler creates a single RecipeRun:SST_EVENT node per run, keyed by
'{session_id}::recipe_run::{timestamp}', and a Recipe:SST_CONCEPT node per
unique recipe name (MERGE semantics, no SOURCED_FROM edge).

Semantic edges created:
  E06: Session -[HAS_RECIPE_RUN {sst_semantic: CONTAINS}]-> RecipeRun
  E07: RecipeRun -[HAS_RECIPE {sst_semantic: EXPRESSES}]-> Recipe
  E09: RecipeStep -[SPAWNED {sst_semantic: LEADS_TO}]-> RecipeRun (when cursor set)
  SOURCED_FROM: RecipeRun -> data_layer_1 event node
"""

from __future__ import annotations

import logging
from typing import Any

from context_intelligence_server.protocol import HookResult
from context_intelligence_server.services import HookStateService
from context_intelligence_server.utils import make_node_id

logger = logging.getLogger(__name__)


class RecipeRunHandler:
    """Enricher handler for recipe run lifecycle events.

    Correlates recipe:start, recipe:complete, recipe:approval,
    recipe:loop_iteration, and recipe:loop_complete events into a single
    RecipeRun node per recipe invocation, keyed as
    '{session_id}::recipe_run::{timestamp}'.
    """

    handled_events: frozenset[str] = frozenset(
        {
            "recipe:start",
            "recipe:complete",
            "recipe:approval",
            "recipe:loop_iteration",
            "recipe:loop_complete",
        }
    )

    def __init__(self, services: HookStateService) -> None:
        self.services = services

    async def __call__(self, event: str, data: dict[str, Any]) -> HookResult:
        """Handle a recipe lifecycle event.

        Extracts session_id; returns continue without mutations if absent.
        """
        session_id: str | None = data.get("session_id")
        if not session_id:
            return HookResult(action="continue")

        if event == "recipe:start":
            await self._handle_start(session_id, data)
        elif event == "recipe:complete":
            return await self._handle_complete(session_id, data)
        elif event == "recipe:approval":
            await self._handle_approval(session_id, data)
        elif event == "recipe:loop_iteration":
            await self._handle_loop_iteration(session_id, data)
        elif event == "recipe:loop_complete":
            await self._handle_loop_complete(session_id, data)

        return HookResult(action="continue")

    async def _handle_start(self, session_id: str, data: dict[str, Any]) -> None:
        """Create RecipeRun node, Recipe concept node, and all start-path edges.

        Creates:
        - RecipeRun:SST_EVENT node at '{session_id}::recipe_run::{timestamp}'
          with name, started_at, total_steps, status
        - Recipe:SST_CONCEPT node keyed by recipe name (MERGE, no SOURCED_FROM)
        - E06: Session -[HAS_RECIPE_RUN {sst_semantic: CONTAINS}]-> RecipeRun
        - E07: RecipeRun -[HAS_RECIPE {sst_semantic: EXPRESSES}]-> Recipe
        - SOURCED_FROM: RecipeRun -> make_node_id(session_id, 'recipe:start', timestamp)
        - E09: active_recipe_step_id -[SPAWNED {sst_semantic: LEADS_TO}]-> RecipeRun
          (only when active_recipe_step_id cursor is set)

        Pushes recipe_run_id onto active_recipe_run_stack.
        """
        timestamp: str = data.get("timestamp", "")
        name: str = data.get("name", "")
        total_steps: Any = data.get("total_steps")
        status: Any = data.get("status")

        recipe_run_id = f"{session_id}::recipe_run::{timestamp}"

        # Create the RecipeRun:SST_EVENT node
        await self.services.graph.upsert_node(
            recipe_run_id,
            {
                "labels": ["RecipeRun", "SST_EVENT"],
                "name": name,
                "started_at": timestamp,
                "total_steps": total_steps,
                "status": status,
            },
        )

        # Create Recipe:SST_CONCEPT node — MERGE semantics, NO SOURCED_FROM edge
        await self.services.graph.upsert_node(
            name,
            {
                "labels": ["Recipe", "SST_CONCEPT"],
                "name": name,
            },
        )

        # E06: Session -[HAS_RECIPE_RUN {sst_semantic: CONTAINS}]-> RecipeRun
        await self.services.graph.upsert_edge(
            session_id,
            recipe_run_id,
            {"type": "HAS_RECIPE_RUN", "sst_semantic": "CONTAINS"},
        )

        # E07: RecipeRun -[HAS_RECIPE {sst_semantic: EXPRESSES}]-> Recipe
        await self.services.graph.upsert_edge(
            recipe_run_id,
            name,
            {"type": "HAS_RECIPE", "sst_semantic": "EXPRESSES"},
        )

        # SOURCED_FROM bridge: RecipeRun -> data_layer_1 recipe:start event
        data_layer_1_node_id = make_node_id(session_id, "recipe:start", timestamp)
        await self.services.graph.upsert_edge(
            recipe_run_id, data_layer_1_node_id, {"type": "SOURCED_FROM"}
        )

        # E09: active_recipe_step_id -> RecipeRun (if cursor set)
        active_step_id = self.services.data_layer_3.active_recipe_step_id
        if active_step_id is not None:
            await self.services.graph.upsert_edge(
                active_step_id,
                recipe_run_id,
                {"type": "SPAWNED", "sst_semantic": "LEADS_TO"},
            )

        # Push recipe_run_id onto active_recipe_run_stack
        self.services.data_layer_3.active_recipe_run_stack.append(recipe_run_id)

    async def _handle_complete(
        self, session_id: str, data: dict[str, Any]
    ) -> HookResult:
        """Enrich top-of-stack RecipeRun with completion data and pop the stack.

        Returns HookResult(action='continue') immediately when the stack is empty
        (guard against orphaned recipe:complete events).

        - Enriches RecipeRun with ended_at, success, final_status
        - Pops active_recipe_run_stack
        - Clears active_recipe_step_id cursor
        - Adds SOURCED_FROM edge to L1 recipe:complete event node
        """
        stack = self.services.data_layer_3.active_recipe_run_stack
        if not stack:
            return HookResult(action="continue")

        recipe_run_id = stack[-1]
        timestamp: str = data.get("timestamp", "")
        success: Any = data.get("success")
        final_status: Any = data.get("final_status")

        # Enrich the RecipeRun node with completion data
        await self.services.graph.upsert_node(
            recipe_run_id,
            {
                "labels": ["RecipeRun", "SST_EVENT"],
                "ended_at": timestamp,
                "success": success,
                "final_status": final_status,
            },
        )

        # SOURCED_FROM bridge: RecipeRun -> data_layer_1 recipe:complete event
        data_layer_1_node_id = make_node_id(session_id, "recipe:complete", timestamp)
        await self.services.graph.upsert_edge(
            recipe_run_id, data_layer_1_node_id, {"type": "SOURCED_FROM"}
        )

        # Pop the stack
        stack.pop()

        # Clear active_recipe_step_id cursor
        self.services.data_layer_3.active_recipe_step_id = None

        return HookResult(action="continue")

    async def _handle_approval(self, session_id: str, data: dict[str, Any]) -> None:
        """Handle recipe:approval — creates RecipeRun on first call only (empty stack guard).

        When the stack is empty, this acts like recipe:start using the approval
        event data to bootstrap the RecipeRun node. When the stack is non-empty,
        this is a no-op (the run already exists).
        """
        stack = self.services.data_layer_3.active_recipe_run_stack
        if stack:
            # Run already exists — nothing to do for approval in this handler
            return

        # First approval with no prior recipe:start — bootstrap the run
        await self._handle_start(session_id, data)

    async def _handle_loop_iteration(
        self, session_id: str, data: dict[str, Any]
    ) -> None:
        """Enrich top-of-stack RecipeRun with loop iteration data.

        No new nodes are created — this is a property enrichment only.
        Returns immediately when the stack is empty.
        """
        stack = self.services.data_layer_3.active_recipe_run_stack
        if not stack:
            return

        recipe_run_id = stack[-1]
        timestamp: str = data.get("timestamp", "")
        iteration: Any = data.get("iteration")

        enrichment: dict[str, Any] = {
            "labels": ["RecipeRun", "SST_EVENT"],
            "last_loop_iteration_at": timestamp,
        }
        if iteration is not None:
            enrichment["last_loop_iteration"] = iteration

        await self.services.graph.upsert_node(recipe_run_id, enrichment)

    async def _handle_loop_complete(
        self, session_id: str, data: dict[str, Any]
    ) -> None:
        """Enrich top-of-stack RecipeRun with loop completion data.

        No new nodes are created — this is a property enrichment only.
        Returns immediately when the stack is empty.
        """
        stack = self.services.data_layer_3.active_recipe_run_stack
        if not stack:
            return

        recipe_run_id = stack[-1]
        timestamp: str = data.get("timestamp", "")
        total_iterations: Any = data.get("total_iterations")

        enrichment: dict[str, Any] = {
            "labels": ["RecipeRun", "SST_EVENT"],
            "loop_completed_at": timestamp,
        }
        if total_iterations is not None:
            enrichment["total_iterations"] = total_iterations

        await self.services.graph.upsert_node(recipe_run_id, enrichment)
