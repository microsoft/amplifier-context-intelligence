"""RecipeStepHandler — Phase 2 stub for recipe step lifecycle events.

This handler is intentionally deferred to Phase 2 pending forensic
verification of recipe:approval field names in the event schema.

In Phase 2, this handler will handle recipe:step, recipe:approval,
recipe:loop_iteration, and tool:pre (E11) events. It will create
RecipeStep nodes and manage the active_recipe_step_id cursor so that
DelegationHandler can create E10 edges connecting tool calls to the
currently active recipe step.

handled_events is an empty frozenset so no events are claimed during
Phase 1, ensuring the handler participates in the pipeline without
mutating the graph.
"""

from __future__ import annotations

from typing import Any

from context_intelligence_server.protocol import HookResult
from context_intelligence_server.services import HookStateService


class RecipeStepHandler:
    """Phase 2 stub enricher for recipe step lifecycle events.

    handled_events is intentionally empty — Phase 2 deferred pending
    forensic verification of recipe:approval field names.

    In Phase 2, will handle:
    - recipe:step       — create RecipeStep node, set active_recipe_step_id cursor
    - recipe:approval   — enrich RecipeStep with approval state
    - recipe:loop_iteration — track loop iteration on RecipeStep
    - tool:pre (E11)    — edge from active RecipeStep to ToolCall node
    """

    handled_events: frozenset[str] = frozenset()

    def __init__(self, services: HookStateService) -> None:
        self.services = services

    async def __call__(self, event: str, data: dict[str, Any]) -> HookResult:
        """No-op stub — returns continue without any graph mutations.

        Phase 2 implementation will handle recipe step lifecycle events.
        """
        return HookResult(action="continue")
