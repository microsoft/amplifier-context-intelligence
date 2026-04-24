"""DelegationHandler — correlates delegate events into Delegation nodes.

Each agent delegation lifecycle produces up to five events:
  delegate:agent_spawned    — agent spawn started
  delegate:agent_completed  — agent completed successfully
  delegate:agent_resumed    — agent resumed from a prior session
  delegate:agent_cancelled  — agent was cancelled
  delegate:error            — agent failed with an error

This handler creates a single Delegation node per delegation (keyed by
'{parent_session_id}::delegation::{tool_call_id}') with the SST_EVENT label,
and creates an Agent:SST_CONCEPT node per unique agent name (MERGE semantics,
no SOURCED_FROM edge). Phase B semantic edges (E01, E02, E03, E04, E10) are
created by _handle_spawned when the corresponding data is present.
"""

from __future__ import annotations

import logging
from typing import Any

from context_intelligence_server.protocol import HookResult
from context_intelligence_server.services import HookStateService
from context_intelligence_server.utils import make_node_id

logger = logging.getLogger(__name__)


class DelegationHandler:
    """Enricher handler for delegate lifecycle events.

    Correlates delegate:agent_spawned, delegate:agent_completed,
    delegate:agent_resumed, delegate:agent_cancelled, and delegate:error
    events into a single Delegation node per delegation invocation.
    The Delegation node ID is a compound key:
    '{parent_session_id}::delegation::{tool_call_id}'.
    """

    handled_events: frozenset[str] = frozenset(
        {
            "delegate:agent_spawned",
            "delegate:agent_completed",
            "delegate:agent_resumed",
            "delegate:agent_cancelled",
            "delegate:error",
        }
    )

    def __init__(self, services: HookStateService) -> None:
        self.services = services
        self._parallel_groups: dict[str, list[str]] = {}

    async def __call__(self, event: str, data: dict[str, Any]) -> HookResult:
        """Handle a delegate lifecycle event.

        Extracts parent_session_id and tool_call_id; returns continue without
        mutations if either is missing.
        """
        parent_session_id: str | None = data.get("parent_session_id")
        tool_call_id: str | None = data.get("tool_call_id")

        if not parent_session_id or not tool_call_id:
            return HookResult(action="continue")

        if event == "delegate:agent_spawned":
            await self._handle_spawned(parent_session_id, tool_call_id, data)
        else:
            await self._handle_lifecycle(parent_session_id, tool_call_id, event, data)

        return HookResult(action="continue")

    async def _handle_spawned(
        self,
        parent_session_id: str,
        tool_call_id: str,
        data: dict[str, Any],
    ) -> None:
        """Create Delegation node and all spawn-path edges.

        Creates:
        - Delegation:SST_EVENT node at compound ID
        - Agent:SST_CONCEPT node keyed by agent name (MERGE, no SOURCED_FROM)
        - ensures sub_session_id Session node exists
        - E01: Session(sub) -[:HAS_AGENT {sst_semantic: 'EXPRESSES'}]-> Agent
        - E02: Delegation -[:ENCOMPASSES {sst_semantic: 'CONTAINS'}]-> Session(sub)
        - E03: ToolCall -[:TRIGGERED {sst_semantic: 'LEADS_TO'}]-> Delegation
        - E04: PARALLEL_AGENT edges within parallel_group_id (if present)
        - E10: active_recipe_step_id -[:TRIGGERED]-> Delegation (if cursor set)
        - SOURCED_FROM: Delegation -> data_layer_1 delegate:agent_spawned event
        """
        timestamp: str = data.get("timestamp", "")
        agent: str = data.get("agent", "")
        sub_session_id: str = data.get("sub_session_id", "")
        parallel_group_id: str | None = data.get("parallel_group_id")
        context_depth: str | None = data.get("context_depth")
        context_scope: str | None = data.get("context_scope")
        model_role: str | None = data.get("model_role")

        delegation_id = f"{parent_session_id}::delegation::{tool_call_id}"

        # Create the Delegation:SST_EVENT node
        node_data: dict[str, Any] = {
            "labels": ["Delegation", "SST_EVENT"],
            "agent": agent,
            "parent_session_id": parent_session_id,
            "sub_session_id": sub_session_id,
            "tool_call_id": tool_call_id,
            "started_at": timestamp,
        }
        if parallel_group_id is not None:
            node_data["parallel_group_id"] = parallel_group_id
        if context_depth is not None:
            node_data["context_depth"] = context_depth
        if context_scope is not None:
            node_data["context_scope"] = context_scope
        if model_role is not None:
            node_data["model_role"] = model_role

        await self.services.graph.upsert_node(delegation_id, node_data)

        # Create Agent:SST_CONCEPT node — MERGE semantics, NO SOURCED_FROM edge
        await self.services.graph.upsert_node(
            agent,
            {
                "labels": ["Agent", "SST_CONCEPT"],
                "agent": agent,
            },
        )

        # Ensure sub-session node exists
        await self.services.ensure_session_node(sub_session_id, {})

        # E01: Session(sub) -[:HAS_AGENT {sst_semantic: 'EXPRESSES'}]-> Agent
        await self.services.graph.upsert_edge(
            sub_session_id,
            agent,
            {"type": "HAS_AGENT", "sst_semantic": "EXPRESSES"},
        )

        # E02: Delegation -[:ENCOMPASSES {sst_semantic: 'CONTAINS'}]-> Session(sub)
        await self.services.graph.upsert_edge(
            delegation_id,
            sub_session_id,
            {"type": "ENCOMPASSES", "sst_semantic": "CONTAINS"},
        )

        # E03: ToolCall(tool_call_id) -[:TRIGGERED {sst_semantic: 'LEADS_TO'}]-> Delegation
        await self.services.graph.upsert_edge(
            tool_call_id,
            delegation_id,
            {"type": "TRIGGERED", "sst_semantic": "LEADS_TO"},
        )

        # E04: parallel_group handling
        if parallel_group_id is not None:
            group = self._parallel_groups.setdefault(parallel_group_id, [])
            for prior_delegation_id in group:
                await self.services.graph.upsert_edge(
                    delegation_id,
                    prior_delegation_id,
                    {"type": "PARALLEL_AGENT", "sst_semantic": "NEAR"},
                )
            group.append(delegation_id)

        # E10: active_recipe_step_id -> delegation (if cursor set)
        active_step_id = self.services.data_layer_3.active_recipe_step_id
        if active_step_id is not None:
            await self.services.graph.upsert_edge(
                active_step_id,
                delegation_id,
                {"type": "TRIGGERED", "sst_semantic": "LEADS_TO"},
            )

        # SOURCED_FROM bridge: Delegation -> data_layer_1 delegate:agent_spawned event
        data_layer_1_node_id = make_node_id(
            parent_session_id, "delegate:agent_spawned", timestamp, tool_call_id
        )
        await self.services.graph.upsert_edge(
            delegation_id, data_layer_1_node_id, {"type": "SOURCED_FROM"}
        )

    async def _handle_lifecycle(
        self,
        parent_session_id: str,
        tool_call_id: str,
        event: str,
        data: dict[str, Any],
    ) -> None:
        """Enrich existing Delegation node with lifecycle event properties.

        Sets appropriate enrichment properties based on event type and
        adds a SOURCED_FROM edge to the corresponding data_layer_1 event node.
        """
        timestamp: str = data.get("timestamp", "")
        delegation_id = f"{parent_session_id}::delegation::{tool_call_id}"

        enrichment: dict[str, Any]
        if event == "delegate:agent_completed":
            enrichment = {"ended_at": timestamp, "success": True}
        elif event == "delegate:agent_resumed":
            enrichment = {"resumed_at": timestamp}
        elif event == "delegate:agent_cancelled":
            enrichment = {"cancelled_at": timestamp}
        else:  # delegate:error
            enrichment = {
                "ended_at": timestamp,
                "success": False,
                "error": data.get("error"),
            }

        node_data: dict[str, Any] = {
            "labels": ["Delegation", "SST_EVENT"],
            **enrichment,
        }
        await self.services.graph.upsert_node(delegation_id, node_data)

        # SOURCED_FROM bridge: Delegation -> data_layer_1 event node
        data_layer_1_node_id = make_node_id(
            parent_session_id, event, timestamp, delegation_id
        )
        await self.services.graph.upsert_edge(
            delegation_id, data_layer_1_node_id, {"type": "SOURCED_FROM"}
        )
