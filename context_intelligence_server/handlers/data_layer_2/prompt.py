"""PromptHandler — Prompt node creation and turn-flow cursor edges E05, E15.

Creates Prompt:SST_EVENT nodes from prompt:submit events and wires the
semantic edges E05 and E15.

Edges created here:
  E05 — Session        -[:HAS_PART {sst_semantic: 'CONTAINS'}]-> Prompt
  E15 — OrchestratorRun -[:ENABLES  {sst_semantic: 'LEADS_TO'}]->  Prompt
"""

from __future__ import annotations

from typing import Any

from context_intelligence_server.protocol import HookResult
from context_intelligence_server.services import HookStateService
from context_intelligence_server.utils import make_node_id


class PromptHandler:
    """Handles prompt submission events.

    Claimed events: prompt:submit.

    Creates a Prompt:SST_EVENT node, wires E05 (Session -> Prompt) always,
    and E15 (OrchestratorRun -> Prompt) conditionally when the
    last_completed_orch_run_id cursor is set. Updates the last_prompt_id
    cursor for downstream E14 creation by OrchestratorRunHandler.
    """

    handled_events: frozenset[str] = frozenset({"prompt:submit"})

    def __init__(self, services: HookStateService) -> None:
        self.services = services

    async def __call__(self, event: str, data: dict[str, Any]) -> HookResult:
        """Handle a prompt:submit event.

        Returns HookResult(action='continue') immediately when session_id is
        absent — no graph mutations are performed.
        """
        session_id: str | None = data.get("session_id")
        if not session_id:
            return HookResult(action="continue")

        timestamp: str = data.get("timestamp", "")
        prompt_text: str = data.get("prompt", "")

        # Compute the compound key for the Prompt node
        prompt_node_id = f"{session_id}::prompt::{timestamp}"

        # Create Prompt:SST_EVENT node
        await self.services.graph.upsert_node(
            prompt_node_id,
            {
                "labels": ["Prompt", "SST_EVENT"],
                "session_id": session_id,
                "prompt": prompt_text,
                "occurred_at": timestamp,
            },
        )

        # SOURCED_FROM bridge: Prompt -> data_layer_1 prompt:submit event
        data_layer_1_node_id = make_node_id(session_id, "prompt:submit", timestamp)
        await self.services.graph.upsert_edge(
            prompt_node_id, data_layer_1_node_id, {"type": "SOURCED_FROM"}
        )

        # E05: Session -[:HAS_PART {sst_semantic: 'CONTAINS'}]-> Prompt (always)
        await self.services.graph.upsert_edge(
            session_id,
            prompt_node_id,
            {
                "type": "HAS_PART",
                "sst_semantic": "CONTAINS",
            },
        )

        # E15 (conditional): OrchestratorRun -[:ENABLES {sst_semantic: 'LEADS_TO'}]-> Prompt
        last_orch_run_id = self.services.data_layer_2.last_completed_orch_run_id
        if last_orch_run_id is not None:
            await self.services.graph.upsert_edge(
                last_orch_run_id,
                prompt_node_id,
                {
                    "type": "ENABLES",
                    "sst_semantic": "LEADS_TO",
                },
            )
            # Clear the cursor after creating E15
            self.services.data_layer_2.last_completed_orch_run_id = None

        # Update last_prompt_id cursor for E14 (used by OrchestratorRunHandler)
        self.services.data_layer_2.last_prompt_id = prompt_node_id

        return HookResult(action="continue")
