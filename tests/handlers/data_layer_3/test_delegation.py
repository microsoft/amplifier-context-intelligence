"""Tests for DelegationHandler — spawn path with Delegation node, Agent concept, E01-E03.

Covers:
- handled_events == frozenset({'delegate:agent_spawned', 'delegate:agent_completed',
  'delegate:agent_resumed', 'delegate:agent_cancelled', 'delegate:error'})
- delegate:agent_spawned creates Delegation:SST_EVENT node keyed as
  '{parent_session_id}::delegation::{tool_call_id}' with all properties
- Agent:SST_CONCEPT node keyed by agent name (MERGE semantics, NO SOURCED_FROM edge)
- E01: Session(sub) -[:HAS_AGENT {sst_semantic: 'EXPRESSES'}]-> Agent
- E02: Delegation -[:ENCOMPASSES {sst_semantic: 'CONTAINS'}]-> Session(sub)
- E03: ToolCall(tool_call_id) -[:TRIGGERED {sst_semantic: 'LEADS_TO'}]-> Delegation
- SOURCED_FROM: Delegation -> make_node_id(parent_session_id, 'delegate:agent_spawned', timestamp, tool_call_id)
- Guard: missing parent_session_id or tool_call_id returns continue with no graph mutations
"""

from __future__ import annotations

from context_intelligence_server.handlers.data_layer_3.delegation import (
    DelegationHandler,
)
from context_intelligence_server.services import HookStateService
from context_intelligence_server.utils import make_node_id


# ---------------------------------------------------------------------------
# 1. TestDelegationHandlerHandledEvents
# ---------------------------------------------------------------------------


class TestDelegationHandlerHandledEvents:
    """handled_events must be a frozenset containing all 5 delegate events."""

    def test_handled_events_is_frozenset(self) -> None:
        """handled_events must be a frozenset."""
        assert isinstance(DelegationHandler.handled_events, frozenset)

    def test_delegate_agent_spawned_in_handled_events(self) -> None:
        """delegate:agent_spawned must be in handled_events."""
        assert "delegate:agent_spawned" in DelegationHandler.handled_events

    def test_delegate_agent_completed_in_handled_events(self) -> None:
        """delegate:agent_completed must be in handled_events."""
        assert "delegate:agent_completed" in DelegationHandler.handled_events

    def test_delegate_agent_resumed_in_handled_events(self) -> None:
        """delegate:agent_resumed must be in handled_events."""
        assert "delegate:agent_resumed" in DelegationHandler.handled_events

    def test_delegate_agent_cancelled_in_handled_events(self) -> None:
        """delegate:agent_cancelled must be in handled_events."""
        assert "delegate:agent_cancelled" in DelegationHandler.handled_events

    def test_delegate_error_in_handled_events(self) -> None:
        """delegate:error must be in handled_events."""
        assert "delegate:error" in DelegationHandler.handled_events


# ---------------------------------------------------------------------------
# 2. TestDelegationSpawnCreatesNode
# ---------------------------------------------------------------------------


class TestDelegationSpawnCreatesNode:
    """delegate:agent_spawned creates Delegation:SST_EVENT at compound ID."""

    async def test_spawn_creates_node_with_compound_id(
        self, services: HookStateService
    ) -> None:
        """Delegation node must be at '{parent_session_id}::delegation::{tool_call_id}'."""
        handler = DelegationHandler(services)
        data = {
            "parent_session_id": "ps1",
            "tool_call_id": "tc-abc",
            "sub_session_id": "ss1",
            "agent": "foundation:explorer",
            "timestamp": "2026-01-01T00:00:00Z",
            "context_depth": "recent",
            "context_scope": "conversation",
        }
        await handler("delegate:agent_spawned", data)

        node_id = "ps1::delegation::tc-abc"
        node = await services.graph.get_node(node_id)
        assert node is not None, f"Delegation node must exist at '{node_id}'"

    async def test_spawn_node_has_delegation_label(
        self, services: HookStateService
    ) -> None:
        """Delegation node must have 'Delegation' in labels."""
        handler = DelegationHandler(services)
        data = {
            "parent_session_id": "ps1",
            "tool_call_id": "tc-abc",
            "sub_session_id": "ss1",
            "agent": "foundation:explorer",
            "timestamp": "2026-01-01T00:00:00Z",
        }
        await handler("delegate:agent_spawned", data)

        node = await services.graph.get_node("ps1::delegation::tc-abc")
        assert node is not None
        assert "Delegation" in node["labels"]

    async def test_spawn_node_has_sst_event_label(
        self, services: HookStateService
    ) -> None:
        """Delegation node must have 'SST_EVENT' in labels."""
        handler = DelegationHandler(services)
        data = {
            "parent_session_id": "ps1",
            "tool_call_id": "tc-abc",
            "sub_session_id": "ss1",
            "agent": "foundation:explorer",
            "timestamp": "2026-01-01T00:00:00Z",
        }
        await handler("delegate:agent_spawned", data)

        node = await services.graph.get_node("ps1::delegation::tc-abc")
        assert node is not None
        assert "SST_EVENT" in node["labels"]

    async def test_spawn_node_has_started_at(self, services: HookStateService) -> None:
        """Delegation node must have started_at matching the event timestamp."""
        handler = DelegationHandler(services)
        data = {
            "parent_session_id": "ps1",
            "tool_call_id": "tc-abc",
            "sub_session_id": "ss1",
            "agent": "foundation:explorer",
            "timestamp": "2026-01-01T00:00:00Z",
        }
        await handler("delegate:agent_spawned", data)

        node = await services.graph.get_node("ps1::delegation::tc-abc")
        assert node is not None
        assert node["started_at"] == "2026-01-01T00:00:00Z"

    async def test_spawn_node_has_context_depth_and_scope(
        self, services: HookStateService
    ) -> None:
        """Delegation node must carry context_depth and context_scope properties."""
        handler = DelegationHandler(services)
        data = {
            "parent_session_id": "ps1",
            "tool_call_id": "tc-abc",
            "sub_session_id": "ss1",
            "agent": "foundation:explorer",
            "timestamp": "2026-01-01T00:00:00Z",
            "context_depth": "recent",
            "context_scope": "conversation",
        }
        await handler("delegate:agent_spawned", data)

        node = await services.graph.get_node("ps1::delegation::tc-abc")
        assert node is not None
        assert node["context_depth"] == "recent"
        assert node["context_scope"] == "conversation"


# ---------------------------------------------------------------------------
# 3. TestAgentConceptNodeCreated
# ---------------------------------------------------------------------------


class TestAgentConceptNodeCreated:
    """Agent:SST_CONCEPT node keyed by agent name — MERGE semantics, NO SOURCED_FROM edge."""

    async def test_agent_node_exists_at_agent_name(
        self, services: HookStateService
    ) -> None:
        """Agent node must exist at the agent name key."""
        handler = DelegationHandler(services)
        data = {
            "parent_session_id": "ps1",
            "tool_call_id": "tc-abc",
            "sub_session_id": "ss1",
            "agent": "foundation:explorer",
            "timestamp": "2026-01-01T00:00:00Z",
        }
        await handler("delegate:agent_spawned", data)

        node = await services.graph.get_node("foundation:explorer")
        assert node is not None, "Agent node must exist at the agent name key"

    async def test_agent_node_has_sst_concept_not_sst_event_label(
        self, services: HookStateService
    ) -> None:
        """Agent node must have 'SST_CONCEPT' label (not SST_EVENT)."""
        handler = DelegationHandler(services)
        data = {
            "parent_session_id": "ps1",
            "tool_call_id": "tc-abc",
            "sub_session_id": "ss1",
            "agent": "foundation:explorer",
            "timestamp": "2026-01-01T00:00:00Z",
        }
        await handler("delegate:agent_spawned", data)

        node = await services.graph.get_node("foundation:explorer")
        assert node is not None
        assert "SST_CONCEPT" in node["labels"]
        assert "SST_EVENT" not in node["labels"]

    async def test_agent_node_has_no_sourced_from_edge(
        self, services: HookStateService
    ) -> None:
        """Agent node must NOT have a SOURCED_FROM edge originating from it."""
        handler = DelegationHandler(services)
        data = {
            "parent_session_id": "ps1",
            "tool_call_id": "tc-abc",
            "sub_session_id": "ss1",
            "agent": "foundation:explorer",
            "timestamp": "2026-01-01T00:00:00Z",
        }
        await handler("delegate:agent_spawned", data)

        # No SOURCED_FROM edge should originate from the agent node
        sourced_from_edges = [
            (src, dst)
            for (src, dst), edge in services.graph._edges.items()
            if src == "foundation:explorer" and edge.get("type") == "SOURCED_FROM"
        ]
        assert len(sourced_from_edges) == 0, (
            "Agent concept node must NOT have a SOURCED_FROM edge"
        )


# ---------------------------------------------------------------------------
# 4. TestSpawnEdges
# ---------------------------------------------------------------------------


class TestSpawnEdges:
    """E01/E02/E03 edges and SOURCED_FROM must be created on delegate:agent_spawned."""

    async def test_e01_has_agent_expresses_edge(
        self, services: HookStateService
    ) -> None:
        """E01: Session(sub) -[:HAS_AGENT {sst_semantic: 'EXPRESSES'}]-> Agent."""
        handler = DelegationHandler(services)
        data = {
            "parent_session_id": "ps1",
            "tool_call_id": "tc-abc",
            "sub_session_id": "ss1",
            "agent": "foundation:explorer",
            "timestamp": "2026-01-01T00:00:00Z",
        }
        await handler("delegate:agent_spawned", data)

        edge = await services.graph.get_edge("ss1", "foundation:explorer")
        assert edge is not None, "E01 edge (Session(sub) -> Agent) must exist"
        assert edge.get("type") == "HAS_AGENT"
        assert edge.get("sst_semantic") == "EXPRESSES"

    async def test_e02_encompasses_contains_edge(
        self, services: HookStateService
    ) -> None:
        """E02: Delegation -[:ENCOMPASSES {sst_semantic: 'CONTAINS'}]-> Session(sub)."""
        handler = DelegationHandler(services)
        data = {
            "parent_session_id": "ps1",
            "tool_call_id": "tc-abc",
            "sub_session_id": "ss1",
            "agent": "foundation:explorer",
            "timestamp": "2026-01-01T00:00:00Z",
        }
        await handler("delegate:agent_spawned", data)

        delegation_id = "ps1::delegation::tc-abc"
        edge = await services.graph.get_edge(delegation_id, "ss1")
        assert edge is not None, "E02 edge (Delegation -> Session(sub)) must exist"
        assert edge.get("type") == "ENCOMPASSES"
        assert edge.get("sst_semantic") == "CONTAINS"

    async def test_e03_triggered_leads_to_edge(
        self, services: HookStateService
    ) -> None:
        """E03: ToolCall(tool_call_id) -[:TRIGGERED {sst_semantic: 'LEADS_TO'}]-> Delegation."""
        handler = DelegationHandler(services)
        data = {
            "parent_session_id": "ps1",
            "tool_call_id": "tc-abc",
            "sub_session_id": "ss1",
            "agent": "foundation:explorer",
            "timestamp": "2026-01-01T00:00:00Z",
        }
        await handler("delegate:agent_spawned", data)

        delegation_id = "ps1::delegation::tc-abc"
        edge = await services.graph.get_edge("tc-abc", delegation_id)
        assert edge is not None, "E03 edge (ToolCall -> Delegation) must exist"
        assert edge.get("type") == "TRIGGERED"
        assert edge.get("sst_semantic") == "LEADS_TO"

    async def test_sourced_from_uses_make_node_id_with_tool_call_id_disambiguator(
        self, services: HookStateService
    ) -> None:
        """SOURCED_FROM must link Delegation to make_node_id(parent_session_id, 'delegate:agent_spawned', timestamp, tool_call_id)."""
        handler = DelegationHandler(services)
        timestamp = "2026-01-01T00:00:00Z"
        data = {
            "parent_session_id": "ps1",
            "tool_call_id": "tc-abc",
            "sub_session_id": "ss1",
            "agent": "foundation:explorer",
            "timestamp": timestamp,
        }
        await handler("delegate:agent_spawned", data)

        delegation_id = "ps1::delegation::tc-abc"
        expected_target = make_node_id(
            "ps1", "delegate:agent_spawned", timestamp, "tc-abc"
        )
        edge = await services.graph.get_edge(delegation_id, expected_target)
        assert edge is not None, (
            "SOURCED_FROM edge must exist from Delegation to data_layer_1 node"
        )
        assert edge.get("type") == "SOURCED_FROM"


# ---------------------------------------------------------------------------
# 5. TestDelegationHandlerGuards
# ---------------------------------------------------------------------------


class TestDelegationHandlerGuards:
    """Missing parent_session_id or tool_call_id short-circuits without graph mutations."""

    async def test_missing_parent_session_id_short_circuits(
        self, services: HookStateService
    ) -> None:
        """Missing parent_session_id returns continue with no graph mutations."""
        handler = DelegationHandler(services)
        result = await handler(
            "delegate:agent_spawned",
            {
                "tool_call_id": "tc-abc",
                "sub_session_id": "ss1",
                "agent": "foundation:explorer",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        assert result.action == "continue"
        assert len(services.graph._nodes) == 0

    async def test_missing_tool_call_id_short_circuits(
        self, services: HookStateService
    ) -> None:
        """Missing tool_call_id returns continue with no graph mutations."""
        handler = DelegationHandler(services)
        result = await handler(
            "delegate:agent_spawned",
            {
                "parent_session_id": "ps1",
                "sub_session_id": "ss1",
                "agent": "foundation:explorer",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        assert result.action == "continue"
        assert len(services.graph._nodes) == 0


# ---------------------------------------------------------------------------
# 6. TestDelegationLifecycleEvents
# ---------------------------------------------------------------------------


class TestDelegationLifecycleEvents:
    """Lifecycle events enrich the Delegation node and create a SOURCED_FROM edge."""

    async def test_agent_completed_sets_ended_at(
        self, services: HookStateService
    ) -> None:
        """spawn then complete; node['ended_at'] == completion timestamp."""
        handler = DelegationHandler(services)
        spawn_data = {
            "parent_session_id": "ps1",
            "tool_call_id": "tc-abc",
            "sub_session_id": "ss1",
            "agent": "foundation:explorer",
            "timestamp": "2026-01-01T00:00:00Z",
        }
        await handler("delegate:agent_spawned", spawn_data)

        complete_timestamp = "2026-01-01T01:00:00Z"
        complete_data = {
            "parent_session_id": "ps1",
            "tool_call_id": "tc-abc",
            "timestamp": complete_timestamp,
        }
        await handler("delegate:agent_completed", complete_data)

        node = await services.graph.get_node("ps1::delegation::tc-abc")
        assert node is not None
        assert node["ended_at"] == complete_timestamp

    async def test_agent_completed_sets_success_true(
        self, services: HookStateService
    ) -> None:
        """node['success'] is True after delegate:agent_completed."""
        handler = DelegationHandler(services)
        complete_data = {
            "parent_session_id": "ps1",
            "tool_call_id": "tc-abc",
            "timestamp": "2026-01-01T01:00:00Z",
        }
        await handler("delegate:agent_completed", complete_data)

        node = await services.graph.get_node("ps1::delegation::tc-abc")
        assert node is not None
        assert node["success"] is True

    async def test_agent_resumed_sets_resumed_at(
        self, services: HookStateService
    ) -> None:
        """node['resumed_at'] == resume timestamp after delegate:agent_resumed."""
        handler = DelegationHandler(services)
        resume_timestamp = "2026-01-01T02:00:00Z"
        resume_data = {
            "parent_session_id": "ps1",
            "tool_call_id": "tc-abc",
            "timestamp": resume_timestamp,
        }
        await handler("delegate:agent_resumed", resume_data)

        node = await services.graph.get_node("ps1::delegation::tc-abc")
        assert node is not None
        assert node["resumed_at"] == resume_timestamp

    async def test_agent_cancelled_sets_cancelled_at(
        self, services: HookStateService
    ) -> None:
        """node['cancelled_at'] == cancel timestamp after delegate:agent_cancelled."""
        handler = DelegationHandler(services)
        cancel_timestamp = "2026-01-01T03:00:00Z"
        cancel_data = {
            "parent_session_id": "ps1",
            "tool_call_id": "tc-abc",
            "timestamp": cancel_timestamp,
        }
        await handler("delegate:agent_cancelled", cancel_data)

        node = await services.graph.get_node("ps1::delegation::tc-abc")
        assert node is not None
        assert node["cancelled_at"] == cancel_timestamp

    async def test_delegate_error_sets_success_false_and_error(
        self, services: HookStateService
    ) -> None:
        """node['success'] is False, node['error']=='SubagentError', node['ended_at'] set."""
        handler = DelegationHandler(services)
        error_timestamp = "2026-01-01T04:00:00Z"
        error_data = {
            "parent_session_id": "ps1",
            "tool_call_id": "tc-abc",
            "timestamp": error_timestamp,
            "error": "SubagentError",
        }
        await handler("delegate:error", error_data)

        node = await services.graph.get_node("ps1::delegation::tc-abc")
        assert node is not None
        assert node["success"] is False
        assert node["error"] == "SubagentError"
        assert node["ended_at"] == error_timestamp

    async def test_lifecycle_event_creates_sourced_from_edge(
        self, services: HookStateService
    ) -> None:
        """delegate:agent_completed creates SOURCED_FROM edge using delegation_id as disambiguator."""
        handler = DelegationHandler(services)
        complete_timestamp = "2026-01-01T01:00:00Z"
        complete_data = {
            "parent_session_id": "ps1",
            "tool_call_id": "tc-abc",
            "timestamp": complete_timestamp,
        }
        await handler("delegate:agent_completed", complete_data)

        delegation_id = "ps1::delegation::tc-abc"
        expected_target = make_node_id(
            "ps1", "delegate:agent_completed", complete_timestamp, delegation_id
        )
        edge = await services.graph.get_edge(delegation_id, expected_target)
        assert edge is not None, (
            "SOURCED_FROM edge must exist from Delegation to data_layer_1 lifecycle event node"
        )
        assert edge.get("type") == "SOURCED_FROM"
