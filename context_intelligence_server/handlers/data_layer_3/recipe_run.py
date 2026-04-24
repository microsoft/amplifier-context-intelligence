"""RecipeRunHandler — Phase 2 stub for recipe run lifecycle events.

This handler is intentionally deferred to Phase 2 pending forensic
verification of recipe:approval field names in the event schema.

In Phase 2, this handler will correlate recipe:run_started and
recipe:run_completed events into RecipeRun nodes, creating E-series
semantic edges between Recipe orchestration and the session graph.

handled_events is an empty frozenset so no events are claimed during
Phase 1, ensuring the handler participates in the pipeline without
mutating the graph.
"""

from __future__ import annotations

from typing import Any

from context_intelligence_server.protocol import HookResult
from context_intelligence_server.services import HookStateService


class RecipeRunHandler:
    """Phase 2 stub enricher for recipe run lifecycle events.

    handled_events is intentionally empty — Phase 2 deferred pending
    forensic verification of recipe:approval field names.
    """

    handled_events: frozenset[str] = frozenset()

    def __init__(self, services: HookStateService) -> None:
        self.services = services

    async def __call__(self, event: str, data: dict[str, Any]) -> HookResult:
        """No-op stub — returns continue without any graph mutations.

        Phase 2 implementation will handle recipe run lifecycle events.
        """
        return HookResult(action="continue")
