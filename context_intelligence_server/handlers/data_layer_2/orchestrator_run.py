"""OrchestratorRunHandler — execution lifecycle assembly.

Assembles OrchestratorRun:SST_EVENT nodes from the execution:start /
execution:end / orchestrator:complete event triplet, and wires the
semantic edges E01, E03, and E14.

Edges created here:
  E01 — Session -[:HAS_EXECUTION {sst_semantic: 'CONTAINS'}]-> OrchestratorRun
  E03 — Session -[:HAS_ATTRIBUTE {sst_semantic: 'EXPRESSES'}]-> Orchestrator
  E14 — Prompt  -[:TRIGGERS    {sst_semantic: 'LEADS_TO'}]->   OrchestratorRun
"""

from __future__ import annotations

import logging
from typing import Any

from context_intelligence_server.protocol import HookResult
from context_intelligence_server.services import HookStateService
from context_intelligence_server.utils import make_node_id

logger = logging.getLogger(__name__)


class OrchestratorRunHandler:
    """Handles execution lifecycle events.

    Claimed events: execution:start, execution:end, orchestrator:complete.
    """

    handled_events: frozenset[str] = frozenset(
        {
            "execution:start",
            "execution:end",
            "orchestrator:complete",
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

        if event == "execution:start":
            await self._handle_execution_start(session_id, data)
        elif event == "execution:end":
            await self._handle_execution_end(session_id, data)
        elif event == "orchestrator:complete":
            await self._handle_orchestrator_complete(session_id, data)

        return HookResult(action="continue")

    # ------------------------------------------------------------------
    # Sub-handlers
    # ------------------------------------------------------------------

    async def _handle_execution_start(
        self, session_id: str, data: dict[str, Any]
    ) -> None:
        """Create OrchestratorRun node and wire E01 (and optionally E14).

        - Computes orch_run_id as '{session_id}::orch_run::{timestamp}'
        - Sets execution_start_ts cursor on DataLayer2State
        - Creates OrchestratorRun:SST_EVENT node with session_id + started_at
        - Creates E01: Session -[:HAS_EXECUTION {sst_semantic: 'CONTAINS'}]-> OrchestratorRun
        - Conditionally creates E14: Prompt -[:TRIGGERS {sst_semantic: 'LEADS_TO'}]->
          OrchestratorRun when last_prompt_id cursor is set
        """
        timestamp: str = data.get("timestamp", "")
        orch_run_id = f"{session_id}::orch_run::{timestamp}"

        # Store cursor so execution:end and orchestrator:complete can find this run
        self.services.data_layer_2.execution_start_ts = timestamp

        # Create the OrchestratorRun node
        await self.services.graph.upsert_node(
            orch_run_id,
            {
                "labels": ["OrchestratorRun", "SST_EVENT"],
                "session_id": session_id,
                "started_at": timestamp,
            },
        )

        # E01: Session -[:HAS_EXECUTION {sst_semantic: 'CONTAINS'}]-> OrchestratorRun
        await self.services.graph.upsert_edge(
            session_id,
            orch_run_id,
            {
                "type": "HAS_EXECUTION",
                "sst_semantic": "CONTAINS",
            },
        )

        # SOURCED_FROM bridge: OrchestratorRun -> data_layer_1 execution:start event
        data_layer_1_node_id = make_node_id(session_id, "execution:start", timestamp)
        await self.services.graph.upsert_edge(
            orch_run_id, data_layer_1_node_id, {"type": "SOURCED_FROM"}
        )

        # E14 (conditional): Prompt -[:TRIGGERS {sst_semantic: 'LEADS_TO'}]-> OrchestratorRun
        last_prompt_id = self.services.data_layer_2.last_prompt_id
        if last_prompt_id is not None:
            await self.services.graph.upsert_edge(
                last_prompt_id,
                orch_run_id,
                {
                    "type": "TRIGGERS",
                    "sst_semantic": "LEADS_TO",
                },
            )

    async def _handle_execution_end(
        self, session_id: str, data: dict[str, Any]
    ) -> None:
        """Enrich OrchestratorRun with ended_at, status, and optional response.

        Returns immediately when execution_start_ts cursor is not set
        (guard against orphaned events).
        """
        ts = self.services.data_layer_2.execution_start_ts
        if ts is None:
            return

        orch_run_id = f"{session_id}::orch_run::{ts}"
        timestamp: str = data.get("timestamp", "")

        node_data: dict[str, Any] = {
            "labels": ["OrchestratorRun", "SST_EVENT"],
            "ended_at": timestamp,
            "status": data.get("status"),
        }
        if "response" in data:
            node_data["response"] = data["response"]

        await self.services.graph.upsert_node(orch_run_id, node_data)

        # SOURCED_FROM bridge: OrchestratorRun -> data_layer_1 execution:end event
        data_layer_1_node_id = make_node_id(session_id, "execution:end", timestamp)
        await self.services.graph.upsert_edge(
            orch_run_id, data_layer_1_node_id, {"type": "SOURCED_FROM"}
        )

    async def _handle_orchestrator_complete(
        self, session_id: str, data: dict[str, Any]
    ) -> None:
        """Enrich OrchestratorRun, create Orchestrator concept node, wire E03.

        - Upserts OrchestratorRun with orchestrator_name, turn_count, completed_at
        - Creates Orchestrator:SST_CONCEPT node keyed by name string
        - Creates E03: Session -[:HAS_ATTRIBUTE {sst_semantic: 'EXPRESSES'}]-> Orchestrator
        - Sets last_completed_orch_run_id cursor
        - Clears execution_start_ts cursor (marks lifecycle as complete)

        Returns immediately when execution_start_ts cursor is not set
        (guard against orphaned events).
        """
        ts = self.services.data_layer_2.execution_start_ts
        if ts is None:
            return

        orch_run_id = f"{session_id}::orch_run::{ts}"
        timestamp: str = data.get("timestamp", "")
        orchestrator: str = data.get("orchestrator", "")
        turn_count: Any = data.get("turn_count")

        # Enrich the OrchestratorRun node
        await self.services.graph.upsert_node(
            orch_run_id,
            {
                "labels": ["OrchestratorRun", "SST_EVENT"],
                "orchestrator_name": orchestrator,
                "turn_count": turn_count,
                "completed_at": timestamp,
            },
        )

        # Create Orchestrator concept node — keyed by name, with name as property
        await self.services.graph.upsert_node(
            orchestrator,
            {
                "labels": ["Orchestrator", "SST_CONCEPT"],
                "orchestrator": orchestrator,
            },
        )

        # E03: Session -[:HAS_ATTRIBUTE {sst_semantic: 'EXPRESSES'}]-> Orchestrator
        await self.services.graph.upsert_edge(
            session_id,
            orchestrator,
            {
                "type": "HAS_ATTRIBUTE",
                "sst_semantic": "EXPRESSES",
            },
        )

        # SOURCED_FROM bridge: OrchestratorRun -> data_layer_1 orchestrator:complete event
        data_layer_1_node_id = make_node_id(
            session_id, "orchestrator:complete", timestamp
        )
        await self.services.graph.upsert_edge(
            orch_run_id, data_layer_1_node_id, {"type": "SOURCED_FROM"}
        )

        # Update cursors
        self.services.data_layer_2.last_completed_orch_run_id = orch_run_id
        self.services.data_layer_2.execution_start_ts = None
