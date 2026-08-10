"""IterationHandler — provider/LLM triplet assembly.

Assembles Iteration:SST_EVENT nodes from the provider:request / llm:request /
llm:response event triplet, and wires the semantic edge E06.

Edges created here:
  E06 — OrchestratorRun -[:HAS_PART {sst_semantic: 'CONTAINS'}]-> Iteration
"""

from __future__ import annotations

import logging
from typing import Any

from context_intelligence_server.protocol import HookResult
from context_intelligence_server.services import HookStateService
from context_intelligence_server.utils import make_node_id

logger = logging.getLogger(__name__)


class IterationHandler:
    """Handles provider/LLM triplet events to assemble Iteration nodes.

    Claimed events: provider:request, llm:request, llm:response.
    """

    handled_events: frozenset[str] = frozenset(
        {
            "provider:request",
            "llm:request",
            "llm:response",
        }
    )

    def __init__(self, services: HookStateService) -> None:
        self.services = services

    async def __call__(self, event: str, data: dict[str, Any]) -> HookResult:
        """Dispatch to the appropriate sub-handler.

        Returns HookResult(action='continue') immediately when session_id is
        absent — no graph mutations are performed.
        """
        session_id: str | None = data.get("session_id")
        if not session_id:
            return HookResult(action="continue")

        if event == "provider:request":
            await self._handle_provider_request(session_id, data)
        elif event == "llm:request":
            await self._handle_llm_request(data)
        elif event == "llm:response":
            await self._handle_llm_response(data)

        return HookResult(action="continue")

    # ------------------------------------------------------------------
    # Sub-handlers
    # ------------------------------------------------------------------

    async def _handle_provider_request(
        self, session_id: str, data: dict[str, Any]
    ) -> None:
        """Create Iteration node and set active_iteration_id cursor.

        - Computes iteration_id as run-scoped:
          '{session_id}::orch_run::{execution_start_ts}::iteration::{iteration_number}'
          when an orchestrator run is active (execution_start_ts cursor set), using the
          SAME disambiguator OrchestratorRun uses for its own node_id (P2.1 / I5 fix).
          Falls back to the bare '{session_id}::iteration::{iteration_number}' shape when
          no run is active -- this bare shape is also what historical (pre-fix) Iteration
          nodes look like, making them detectable as suspect. See CHANGELOG.md.
        - Sets active_iteration_id cursor on DataLayer2State
        - Creates Iteration:SST_EVENT node with session_id, iteration_number, started_at
        - Conditionally creates E06: OrchestratorRun -[:HAS_PART {sst_semantic: 'CONTAINS'}]->
          Iteration when execution_start_ts cursor is set

        Without run-scoping, iteration_number alone (a counter scoped to the whole
        session, not the run) can repeat across orchestrator runs -- e.g. after a
        drainer restart/replay resets the in-memory counter -- causing distinct runs'
        Iteration nodes to MERGE onto the same node_id (I5) and their usage figures to
        clobber each other (I3, usage_cache_write in particular).
        """
        # Increment counter to get the next iteration number
        self.services.data_layer_2.iteration_count += 1
        iteration_number = self.services.data_layer_2.iteration_count

        timestamp: str = data.get("timestamp", "")

        execution_start_ts = self.services.data_layer_2.execution_start_ts
        orch_run_id: str | None = None
        if execution_start_ts is not None:
            orch_run_id = f"{session_id}::orch_run::{execution_start_ts}"
            iteration_id = f"{orch_run_id}::iteration::{iteration_number}"
        else:
            iteration_id = f"{session_id}::iteration::{iteration_number}"

        # Set cursor so llm:request and llm:response can find this iteration
        self.services.data_layer_2.active_iteration_id = iteration_id

        # Create the Iteration node
        await self.services.graph.upsert_node(
            iteration_id,
            {
                "labels": ["Iteration", "SST_EVENT"],
                "session_id": session_id,
                "iteration_number": iteration_number,
                "started_at": timestamp,
            },
        )

        # E06 (conditional): OrchestratorRun -[:HAS_PART {sst_semantic: 'CONTAINS'}]-> Iteration
        if orch_run_id is not None:
            await self.services.graph.upsert_edge(
                orch_run_id,
                iteration_id,
                {
                    "type": "HAS_PART",
                    "sst_semantic": "CONTAINS",
                },
            )

        # SOURCED_FROM bridge: Iteration -> data_layer_1 provider:request event
        data_layer_1_node_id = make_node_id(session_id, "provider:request", timestamp)
        await self.services.graph.upsert_edge(
            iteration_id, data_layer_1_node_id, {"type": "SOURCED_FROM"}
        )

    async def _handle_llm_request(self, data: dict[str, Any]) -> None:
        """Enrich active Iteration with provider, model, message_count, has_system.

        Returns immediately when active_iteration_id cursor is not set.
        """
        iteration_id = self.services.data_layer_2.active_iteration_id
        if iteration_id is None:
            return

        await self.services.graph.upsert_node(
            iteration_id,
            {
                "labels": ["Iteration", "SST_EVENT"],
                "provider": data.get("provider"),
                "model": data.get("model"),
                "message_count": data.get("message_count"),
                "has_system": data.get("has_system"),
            },
        )

        # SOURCED_FROM bridge: Iteration -> data_layer_1 llm:request event
        session_id: str = data.get("session_id", "")
        timestamp: str = data.get("timestamp", "")
        data_layer_1_node_id = make_node_id(session_id, "llm:request", timestamp)
        await self.services.graph.upsert_edge(
            iteration_id, data_layer_1_node_id, {"type": "SOURCED_FROM"}
        )

    async def _handle_llm_response(self, data: dict[str, Any]) -> None:
        """Enrich active Iteration with usage fields.

        Returns immediately when active_iteration_id cursor is not set.
        Handles missing usage dict gracefully.
        """
        iteration_id = self.services.data_layer_2.active_iteration_id
        if iteration_id is None:
            return

        usage: dict[str, Any] = data.get("usage", {}) or {}

        await self.services.graph.upsert_node(
            iteration_id,
            {
                "labels": ["Iteration", "SST_EVENT"],
                "usage_input": usage.get("input_tokens"),
                "usage_output": usage.get("output_tokens"),
                "usage_cache_write": usage.get("cache_creation_input_tokens"),
            },
        )

        # SOURCED_FROM bridge: Iteration -> data_layer_1 llm:response event
        session_id: str = data.get("session_id", "")
        timestamp: str = data.get("timestamp", "")
        data_layer_1_node_id = make_node_id(session_id, "llm:response", timestamp)
        await self.services.graph.upsert_edge(
            iteration_id, data_layer_1_node_id, {"type": "SOURCED_FROM"}
        )
