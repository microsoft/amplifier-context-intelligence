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

    async def test_empty_tool_call_id_falls_back_to_sub_session_id(
        self, services: HookStateService
    ) -> None:
        """Empty tool_call_id uses sub_session_id as fallback key.

        Real Amplifier versions emit tool_call_id='' (empty string) in
        delegate:agent_spawned.  The handler must still create a Delegation
        node, using sub_session_id as the compound-key fallback.
        When agent='self' and there is no parent Delegation and no parent
        Session node at all, resolved_agent falls to the 'unresolved'
        sentinel (never the old 'root-agent' fallback) and
        is_self_delegation is set to True.
        """
        handler = DelegationHandler(services)
        result = await handler(
            "delegate:agent_spawned",
            {
                "parent_session_id": "ps1",
                "sub_session_id": "ss1",
                "agent": "self",
                "timestamp": "2026-01-01T00:00:00Z",
                "tool_call_id": "",  # empty, as emitted by real Amplifier
            },
        )
        assert result.action == "continue"
        # Delegation node must be created using sub_session_id as fallback key
        delegation_id = "ps1::delegation::ss1"
        assert delegation_id in services.graph._nodes
        assert services.graph._nodes[delegation_id]["agent"] == "self"
        assert services.graph._nodes[delegation_id]["is_self_delegation"] is True
        assert services.graph._nodes[delegation_id]["resolved_agent"] == "unresolved"

    async def test_missing_both_tool_call_id_and_sub_session_id_short_circuits(
        self, services: HookStateService
    ) -> None:
        """Handler short-circuits if both tool_call_id and sub_session_id are absent."""
        handler = DelegationHandler(services)
        result = await handler(
            "delegate:agent_spawned",
            {
                "parent_session_id": "ps1",
                # no tool_call_id, no sub_session_id
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


# ---------------------------------------------------------------------------
# 7. TestE04ParallelAgentEdges
# ---------------------------------------------------------------------------


class TestE04ParallelAgentEdges:
    """E04 PARALLEL_AGENT edges between co-spawned delegations within a parallel_group_id."""

    async def test_e04_edge_created_between_two_parallel_delegations(
        self, services: HookStateService
    ) -> None:
        """Two spawns sharing parallel_group_id='pg-1' produce edge delegation_b -> delegation_a with type=PARALLEL_AGENT, sst_semantic=NEAR."""
        handler = DelegationHandler(services)
        data_a = {
            "parent_session_id": "ps1",
            "tool_call_id": "tc-a",
            "sub_session_id": "ss-a",
            "agent": "foundation:explorer",
            "timestamp": "2026-01-01T00:00:00Z",
            "parallel_group_id": "pg-1",
        }
        data_b = {
            "parent_session_id": "ps1",
            "tool_call_id": "tc-b",
            "sub_session_id": "ss-b",
            "agent": "foundation:builder",
            "timestamp": "2026-01-01T00:00:01Z",
            "parallel_group_id": "pg-1",
        }
        await handler("delegate:agent_spawned", data_a)
        await handler("delegate:agent_spawned", data_b)

        delegation_a_id = "ps1::delegation::tc-a"
        delegation_b_id = "ps1::delegation::tc-b"
        edge = await services.graph.get_edge(delegation_b_id, delegation_a_id)
        assert edge is not None, "E04 edge (delegation_b -> delegation_a) must exist"
        assert edge.get("type") == "PARALLEL_AGENT"
        assert edge.get("sst_semantic") == "NEAR"

    async def test_e04_three_parallel_delegations_produce_three_edges(
        self, services: HookStateService
    ) -> None:
        """3 parallel spawns produce exactly 3 PARALLEL_AGENT edges."""
        handler = DelegationHandler(services)
        for i, (tc, ss) in enumerate(
            [("tc-a", "ss-a"), ("tc-b", "ss-b"), ("tc-c", "ss-c")]
        ):
            data = {
                "parent_session_id": "ps1",
                "tool_call_id": tc,
                "sub_session_id": ss,
                "agent": f"foundation:agent-{i}",
                "timestamp": f"2026-01-01T00:00:0{i}Z",
                "parallel_group_id": "pg-1",
            }
            await handler("delegate:agent_spawned", data)

        parallel_edges = [
            edge
            for edge in services.graph._edges.values()
            if edge.get("type") == "PARALLEL_AGENT"
        ]
        assert len(parallel_edges) == 3

    async def test_e04_no_edge_when_no_parallel_group_id(
        self, services: HookStateService
    ) -> None:
        """Spawn without parallel_group_id produces 0 PARALLEL_AGENT edges."""
        handler = DelegationHandler(services)
        data = {
            "parent_session_id": "ps1",
            "tool_call_id": "tc-a",
            "sub_session_id": "ss-a",
            "agent": "foundation:explorer",
            "timestamp": "2026-01-01T00:00:00Z",
        }
        await handler("delegate:agent_spawned", data)

        parallel_edges = [
            edge
            for edge in services.graph._edges.values()
            if edge.get("type") == "PARALLEL_AGENT"
        ]
        assert len(parallel_edges) == 0

    def test_handler_has_parallel_groups_dict(self) -> None:
        """DelegationHandler instance has _parallel_groups attribute that is a dict."""
        from context_intelligence_server.services import (
            HookStateService as _HookStateService,
        )

        svc = _HookStateService(workspace="test")
        handler = DelegationHandler(svc)
        assert hasattr(handler, "_parallel_groups")
        assert isinstance(handler._parallel_groups, dict)


# ---------------------------------------------------------------------------
# 8. TestE10RecipeStepAttribution
# ---------------------------------------------------------------------------


class TestE10RecipeStepAttribution:
    """E10 edges from active_recipe_step_id -> Delegation when cursor is set."""

    async def test_e10_edge_created_when_active_recipe_step_set(
        self, services: HookStateService
    ) -> None:
        """Set active_recipe_step_id before spawn; edge exists from step -> delegation with type=TRIGGERED, sst_semantic=LEADS_TO."""
        services.data_layer_3.active_recipe_step_id = "run-001::step::1"
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
        edge = await services.graph.get_edge("run-001::step::1", delegation_id)
        assert edge is not None, "E10 edge (RecipeStep -> Delegation) must exist"
        assert edge.get("type") == "TRIGGERED"
        assert edge.get("sst_semantic") == "LEADS_TO"

    async def test_e10_no_edge_when_no_active_recipe_step(
        self, services: HookStateService
    ) -> None:
        """No active_recipe_step_id set means no E10 edge; only E03 TRIGGERED edge targets delegation."""
        assert services.data_layer_3.active_recipe_step_id is None
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
        # Collect all TRIGGERED edges pointing to delegation_id
        triggered_to_delegation = [
            (src, dst)
            for (src, dst), edge in services.graph._edges.items()
            if dst == delegation_id and edge.get("type") == "TRIGGERED"
        ]
        # Only E03 should exist — tool_call_id ("tc-abc") -> delegation_id
        assert len(triggered_to_delegation) == 1
        assert triggered_to_delegation[0][0] == "tc-abc"


# ---------------------------------------------------------------------------
# 9. TestSelfDelegationResolution
# ---------------------------------------------------------------------------


class TestSelfDelegationResolution:
    """self-delegation agent resolution.

    agent='self' must resolve (additively, via resolved_agent) to the nearest
    non-self ancestor's real agent name, read from the PARENT DELEGATION node
    (never the parent Session node, which structurally never carries an
    'agent' property — see delegation.py Defect B / design doc §3).

    Fixtures reflect the real graph shape: a normal spawn creates a Delegation
    node keyed by the PARENT's own parent_session_id, with sub_session_id ==
    the spawned sub-session. A self-delegation's resolver walks
    find_delegation_by_sub_session(parent_session_id) to find that Delegation.
    """

    async def test_self_resolves_from_parent_delegation_agent(
        self, services: HookStateService
    ) -> None:
        """agent='self' resolves to the parent DELEGATION's agent (single hop).

        A normal delegation D1 (agent='foundation:explorer') spawns
        'ps-named'. A self-delegation is then spawned FROM 'ps-named'. The
        resolver must walk D1 (found via
        find_delegation_by_sub_session('ps-named')), NOT read a Session
        node's 'agent' property.

        After spawn:
        - Delegation node carries agent='self', resolved_agent='foundation:explorer',
          is_self_delegation=True.
        - Agent concept node exists at 'foundation:explorer' (not 'self').
        - HAS_AGENT edge targets 'foundation:explorer'.
        """
        handler = DelegationHandler(services)
        await handler(
            "delegate:agent_spawned",
            {
                "parent_session_id": "root-ps",
                "tool_call_id": "tc-parent",
                "sub_session_id": "ps-named",
                "agent": "foundation:explorer",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        await handler(
            "delegate:agent_spawned",
            {
                "parent_session_id": "ps-named",
                "sub_session_id": "ss-self-1",
                "agent": "self",
                "timestamp": "2026-01-01T00:00:01Z",
                "tool_call_id": "",  # empty — sub_session_id used as fallback key
            },
        )

        delegation_id = "ps-named::delegation::ss-self-1"
        assert services.graph._nodes[delegation_id]["agent"] == "self"
        assert (
            services.graph._nodes[delegation_id]["resolved_agent"]
            == "foundation:explorer"
        )
        assert services.graph._nodes[delegation_id]["is_self_delegation"] is True
        assert "foundation:explorer" in services.graph._nodes, (
            "Agent concept node must be at 'foundation:explorer', not 'self'"
        )
        assert "self" not in services.graph._nodes, (
            "No singleton 'self' agent concept node must be created"
        )
        assert ("ss-self-1", "foundation:explorer") in services.graph._edges, (
            "HAS_AGENT edge must target 'foundation:explorer'"
        )

    async def test_self_resolves_to_root_when_parent_is_root_session(
        self, services: HookStateService
    ) -> None:
        """No parent Delegation exists; parent Session is labeled RootSession -> 'root'.

        This is the correct terminal state for a genuine root self-delegation:
        there is no spawning agent because the parent is a root session, not
        a race-miss.
        """
        await services.graph.upsert_node(
            "ps-root", {"labels": ["Session", "RootSession"]}
        )
        handler = DelegationHandler(services)
        await handler(
            "delegate:agent_spawned",
            {
                "parent_session_id": "ps-root",
                "sub_session_id": "ss-self-2",
                "agent": "self",
                "timestamp": "2026-01-01T00:00:00Z",
                "tool_call_id": "",
            },
        )

        delegation_id = "ps-root::delegation::ss-self-2"
        assert services.graph._nodes[delegation_id]["resolved_agent"] == "root"
        assert services.graph._nodes[delegation_id]["is_self_delegation"] is True
        assert "root" in services.graph._nodes, (
            "Agent concept node must exist at the 'root' sentinel"
        )

    async def test_self_resolves_to_forked_when_parent_is_forked_session(
        self, services: HookStateService
    ) -> None:
        """No parent Delegation exists; parent Session is labeled ForkedSession -> 'forked'.

        A fork's origin is a FORK/HAS_FORK edge, not an agent Delegation, so
        'no spawner Delegation' is expected here — a correct, non-failure
        state, not a race-miss. Keeping this distinct from 'unresolved' keeps
        the monitored miss-count a real signal (design doc §8c).
        """
        await services.graph.upsert_node(
            "ps-forked", {"labels": ["Session", "ForkedSession"]}
        )
        handler = DelegationHandler(services)
        await handler(
            "delegate:agent_spawned",
            {
                "parent_session_id": "ps-forked",
                "sub_session_id": "ss-self-3",
                "agent": "self",
                "timestamp": "2026-01-01T00:00:00Z",
                "tool_call_id": "",
            },
        )

        delegation_id = "ps-forked::delegation::ss-self-3"
        assert services.graph._nodes[delegation_id]["resolved_agent"] == "forked"

    async def test_self_resolves_to_unresolved_when_parent_session_missing(
        self, services: HookStateService
    ) -> None:
        """No parent Delegation AND no parent Session at all -> 'unresolved', never 'root'.

        This is the ordering-race case: the spawning record has not been
        flushed yet (ingestion ordering is not guaranteed). The resolver must
        fail loud with an explicit, monitored sentinel — never silently guess
        'root' (that was Defect A/B's bug: a fallback that lied quietly).
        """
        handler = DelegationHandler(services)
        await handler(
            "delegate:agent_spawned",
            {
                "parent_session_id": "ps-missing",
                "sub_session_id": "ss-self-4",
                "agent": "self",
                "timestamp": "2026-01-01T00:00:00Z",
                "tool_call_id": "",
            },
        )

        delegation_id = "ps-missing::delegation::ss-self-4"
        assert services.graph._nodes[delegation_id]["resolved_agent"] == "unresolved"

    async def test_self_resolves_to_unresolved_when_parent_session_has_no_terminal_label(
        self, services: HookStateService
    ) -> None:
        """Parent Session exists but carries no terminal label (bare stub) -> 'unresolved'.

        A bare Session node (e.g. from ensure_session_node's safety-net stub,
        before SessionHandler enriches it with a type label) must not be
        mistaken for a genuine root.
        """
        await services.graph.upsert_node("ps-bare", {"labels": ["Session"]})
        handler = DelegationHandler(services)
        await handler(
            "delegate:agent_spawned",
            {
                "parent_session_id": "ps-bare",
                "sub_session_id": "ss-self-5",
                "agent": "self",
                "timestamp": "2026-01-01T00:00:00Z",
                "tool_call_id": "",
            },
        )

        delegation_id = "ps-bare::delegation::ss-self-5"
        assert services.graph._nodes[delegation_id]["resolved_agent"] == "unresolved"

    async def test_self_resolves_to_root_despite_incomplete_colabel(
        self, services: HookStateService
    ) -> None:
        """Parent Session labeled RootSession AND IncompleteSession -> 'root', NOT 'unresolved'.

        Live graph data shows IncompleteSession co-labels a terminal label
        ~41% of the time (a session can reach session:end with
        session:start/fork permanently missed, out of order — see
        SessionHandler._handle_end / SessionLabelStateMachine.classify). The
        discriminator MUST branch on the terminal label only; treating
        IncompleteSession as a signal would mis-flag hundreds of genuine
        roots as unresolved (the critical guard from design doc §6.1).
        """
        await services.graph.upsert_node(
            "ps-root-incomplete",
            {"labels": ["Session", "RootSession", "IncompleteSession"]},
        )
        handler = DelegationHandler(services)
        await handler(
            "delegate:agent_spawned",
            {
                "parent_session_id": "ps-root-incomplete",
                "sub_session_id": "ss-self-6",
                "agent": "self",
                "timestamp": "2026-01-01T00:00:00Z",
                "tool_call_id": "",
            },
        )

        delegation_id = "ps-root-incomplete::delegation::ss-self-6"
        assert services.graph._nodes[delegation_id]["resolved_agent"] == "root"

    async def test_self_resolves_through_chained_self_delegation(
        self, services: HookStateService
    ) -> None:
        """self -> self -> real agent: resolves to the nearest non-self ancestor (depth 2).

        D1 (agent='foundation:explorer') spawns 'ps-mid'.
        D2 (agent='self') is spawned FROM 'ps-mid' and itself spawns 'ps-leaf'.
        D3 (agent='self') is spawned FROM 'ps-leaf' — its resolver must walk
        PAST D2 (agent='self') to D1's real agent, not stop at D2.
        """
        handler = DelegationHandler(services)
        # D1: real agent spawns ps-mid
        await handler(
            "delegate:agent_spawned",
            {
                "parent_session_id": "root-ps",
                "tool_call_id": "tc-d1",
                "sub_session_id": "ps-mid",
                "agent": "foundation:explorer",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        # D2: self-delegation spawned FROM ps-mid, itself spawning ps-leaf
        await handler(
            "delegate:agent_spawned",
            {
                "parent_session_id": "ps-mid",
                "sub_session_id": "ps-leaf",
                "agent": "self",
                "timestamp": "2026-01-01T00:00:01Z",
                "tool_call_id": "",
            },
        )
        # D3: self-delegation spawned FROM ps-leaf — must walk past D2 to D1
        await handler(
            "delegate:agent_spawned",
            {
                "parent_session_id": "ps-leaf",
                "sub_session_id": "ss-self-final",
                "agent": "self",
                "timestamp": "2026-01-01T00:00:02Z",
                "tool_call_id": "",
            },
        )

        delegation_id = "ps-leaf::delegation::ss-self-final"
        assert (
            services.graph._nodes[delegation_id]["resolved_agent"]
            == "foundation:explorer"
        )

    async def test_self_delegation_cycle_guard_terminates_unresolved(
        self, services: HookStateService
    ) -> None:
        """A cyclic parent-Delegation chain terminates at 'unresolved', never loops forever.

        Two self-delegations reference each other's sub_session_id as
        parent_session_id, forming a cycle. The visited-set guard must break
        out and return 'unresolved' rather than looping until the depth cap
        (or worse, forever).
        """
        await services.graph.upsert_node(
            "cycle-a::delegation::x",
            {
                "labels": ["Delegation", "SST_EVENT"],
                "agent": "self",
                "parent_session_id": "node-b",
                "sub_session_id": "node-a",
            },
        )
        await services.graph.upsert_node(
            "cycle-b::delegation::y",
            {
                "labels": ["Delegation", "SST_EVENT"],
                "agent": "self",
                "parent_session_id": "node-a",
                "sub_session_id": "node-b",
            },
        )

        handler = DelegationHandler(services)
        await handler(
            "delegate:agent_spawned",
            {
                "parent_session_id": "node-a",
                "sub_session_id": "ss-cycle-entry",
                "agent": "self",
                "timestamp": "2026-01-01T00:00:00Z",
                "tool_call_id": "",
            },
        )

        delegation_id = "node-a::delegation::ss-cycle-entry"
        assert services.graph._nodes[delegation_id]["resolved_agent"] == "unresolved"

    async def test_non_self_stores_agent_on_sub_session_node(
        self, services: HookStateService
    ) -> None:
        """Non-self delegation writes the canonical agent name to the sub-session node.

        This validates the write path that all future 'self' resolutions depend on:
        when a parent session is spawned with a real agent name, that name is stored
        on the sub-session node via ensure_session_node so that child self-delegations
        can read it back from get_node(parent_session_id).
        """
        handler = DelegationHandler(services)
        await handler(
            "delegate:agent_spawned",
            {
                "parent_session_id": "ps1",
                "sub_session_id": "ss-named",
                "agent": "foundation:explorer",
                "timestamp": "2026-01-01T00:00:00Z",
                "tool_call_id": "tc-named",
            },
        )

        assert services.graph._nodes["ss-named"]["agent"] == "foundation:explorer"


# ---------------------------------------------------------------------------
# 10. TestIsSelfDelegationFlagBoolean
# ---------------------------------------------------------------------------


class TestIsSelfDelegationFlagBoolean:
    """Brick 1: is_self_delegation is written unconditionally as True or False, never null.

    Previously written only inside the agent=='self' branch (only as True),
    so upsert_node's merge semantics left the field null for every non-self
    delegation — indistinguishable from 'unwritten' under Cypher's
    null=false comparison.
    """

    async def test_flag_is_true_for_self_delegation(
        self, services: HookStateService
    ) -> None:
        handler = DelegationHandler(services)
        await handler(
            "delegate:agent_spawned",
            {
                "parent_session_id": "ps1",
                "sub_session_id": "ss-self",
                "agent": "self",
                "timestamp": "2026-01-01T00:00:00Z",
                "tool_call_id": "",
            },
        )
        node = services.graph._nodes["ps1::delegation::ss-self"]
        assert node["is_self_delegation"] is True

    async def test_flag_is_false_for_named_delegation(
        self, services: HookStateService
    ) -> None:
        handler = DelegationHandler(services)
        await handler(
            "delegate:agent_spawned",
            {
                "parent_session_id": "ps1",
                "sub_session_id": "ss-named",
                "agent": "foundation:explorer",
                "timestamp": "2026-01-01T00:00:00Z",
                "tool_call_id": "tc-named",
            },
        )
        node = services.graph._nodes["ps1::delegation::tc-named"]
        assert node["is_self_delegation"] is False


# ---------------------------------------------------------------------------
# 11. TestFindDelegationBySubSessionParity
# ---------------------------------------------------------------------------


class TestFindDelegationBySubSessionParity:
    """Brick 3: GraphState.find_delegation_by_sub_session, required by the resolver."""

    async def test_finds_delegation_by_sub_session_id(
        self, services: HookStateService
    ) -> None:
        """Returns the Delegation node whose sub_session_id property matches."""
        handler = DelegationHandler(services)
        await handler(
            "delegate:agent_spawned",
            {
                "parent_session_id": "ps1",
                "tool_call_id": "tc-abc",
                "sub_session_id": "ss1",
                "agent": "foundation:explorer",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )

        found = await services.graph.find_delegation_by_sub_session(
            "ss1", services.graph.workspace
        )
        assert found is not None
        assert found["agent"] == "foundation:explorer"
        assert found["sub_session_id"] == "ss1"

    async def test_returns_none_when_no_matching_sub_session(
        self, services: HookStateService
    ) -> None:
        found = await services.graph.find_delegation_by_sub_session(
            "nonexistent-sub-session", services.graph.workspace
        )
        assert found is None
