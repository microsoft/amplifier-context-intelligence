"""Tests for OrchestratorRunHandler — execution lifecycle assembly.

Covers:
- handled_events == frozenset({'execution:start', 'execution:end', 'orchestrator:complete'})
- execution:start creates OrchestratorRun:SST_EVENT node keyed as
  '{session_id}::orch_run::{timestamp}' with session_id and started_at
- execution:start creates E01 HAS_EXECUTION edge
  (Session -[:HAS_EXECUTION {sst_semantic: 'CONTAINS'}]-> OrchestratorRun)
- execution:start sets execution_start_ts cursor on DataLayer2State
- execution:end enriches OrchestratorRun with ended_at, status, response
- orchestrator:complete enriches OrchestratorRun with name, turn_count, completed_at
- orchestrator:complete creates Orchestrator:SST_CONCEPT node keyed by name string
- orchestrator:complete creates E03 HAS_ATTRIBUTE edge
  (Session -[:HAS_ATTRIBUTE {sst_semantic: 'EXPRESSES'}]-> Orchestrator)
- orchestrator:complete sets last_completed_orch_run_id cursor and clears execution_start_ts
- E14: Prompt -[:TRIGGERS {sst_semantic: 'LEADS_TO'}]-> OrchestratorRun
  created when last_prompt_id cursor is set; NOT created when last_prompt_id is None
- Guard: missing session_id returns continue with zero graph mutations
"""

from __future__ import annotations

from context_intelligence_server.handlers.data_layer_2.orchestrator_run import (
    OrchestratorRunHandler,
)
from context_intelligence_server.services import HookStateService
from context_intelligence_server.utils import make_node_id


# ---------------------------------------------------------------------------
# 1. TestOrchestratorRunHandlerHandledEvents
# ---------------------------------------------------------------------------


class TestOrchestratorRunHandlerHandledEvents:
    """handled_events == frozenset({'execution:start', 'execution:end', 'orchestrator:complete'})."""

    def test_handled_events_is_exact_frozenset(self) -> None:
        """handled_events must be exactly frozenset({'execution:start', 'execution:end', 'orchestrator:complete'})."""
        assert OrchestratorRunHandler.handled_events == frozenset(
            {"execution:start", "execution:end", "orchestrator:complete"}
        )

    def test_execution_error_not_in_handled_events(self) -> None:
        """execution:error must NOT be in handled_events."""
        assert "execution:error" not in OrchestratorRunHandler.handled_events


# ---------------------------------------------------------------------------
# 2. TestExecutionStartCreatesOrchestratorRun
# ---------------------------------------------------------------------------


class TestExecutionStartCreatesOrchestratorRun:
    """execution:start creates OrchestratorRun:SST_EVENT node with correct key and properties."""

    async def test_node_created_with_correct_compound_key(
        self, services: HookStateService
    ) -> None:
        """execution:start must create node at '{session_id}::orch_run::{timestamp}'."""
        handler = OrchestratorRunHandler(services)
        await handler(
            "execution:start",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        node_id = "s1::orch_run::2026-01-01T00:00:00Z"
        node = await services.graph.get_node(node_id)
        assert node is not None, f"execution:start must create node at '{node_id}'"

    async def test_node_has_orchestrator_run_and_sst_event_labels(
        self, services: HookStateService
    ) -> None:
        """OrchestratorRun node must have 'OrchestratorRun' and 'SST_EVENT' labels."""
        handler = OrchestratorRunHandler(services)
        await handler(
            "execution:start",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        node = await services.graph.get_node("s1::orch_run::2026-01-01T00:00:00Z")
        assert node is not None
        assert "OrchestratorRun" in node["labels"], (
            f"OrchestratorRun label missing. Got: {node['labels']}"
        )
        assert "SST_EVENT" in node["labels"], (
            f"SST_EVENT label missing. Got: {node['labels']}"
        )

    async def test_node_has_session_id_and_started_at(
        self, services: HookStateService
    ) -> None:
        """OrchestratorRun node must carry session_id and started_at properties."""
        handler = OrchestratorRunHandler(services)
        await handler(
            "execution:start",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        node = await services.graph.get_node("s1::orch_run::2026-01-01T00:00:00Z")
        assert node is not None
        assert node.get("session_id") == "s1", (
            f"session_id property missing or wrong. Got: {node!r}"
        )
        assert node.get("started_at") == "2026-01-01T00:00:00Z", (
            f"started_at property missing or wrong. Got: {node!r}"
        )

    async def test_e01_has_execution_edge_created(
        self, services: HookStateService
    ) -> None:
        """execution:start must create E01: Session -[:HAS_EXECUTION {sst_semantic: 'CONTAINS'}]-> OrchestratorRun."""
        handler = OrchestratorRunHandler(services)
        await handler(
            "execution:start",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        orch_run_id = "s1::orch_run::2026-01-01T00:00:00Z"
        edge = await services.graph.get_edge("s1", orch_run_id)
        assert edge is not None, (
            f"E01 HAS_EXECUTION edge from 's1' to '{orch_run_id}' must exist"
        )
        assert edge.get("type") == "HAS_EXECUTION", (
            f"E01 edge must have type='HAS_EXECUTION'. Got: {edge.get('type')}"
        )
        assert edge.get("sst_semantic") == "CONTAINS", (
            f"E01 edge must have sst_semantic='CONTAINS'. Got: {edge.get('sst_semantic')}"
        )


# ---------------------------------------------------------------------------
# 3. TestExecutionStartSetsCursor
# ---------------------------------------------------------------------------


class TestExecutionStartSetsCursor:
    """execution:start sets execution_start_ts cursor on DataLayer2State."""

    async def test_execution_start_sets_execution_start_ts_cursor(
        self, services: HookStateService
    ) -> None:
        """execution:start must set services.data_layer_2.execution_start_ts to the event timestamp."""
        handler = OrchestratorRunHandler(services)
        assert services.data_layer_2.execution_start_ts is None, (
            "execution_start_ts must be None before any event"
        )
        await handler(
            "execution:start",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        assert services.data_layer_2.execution_start_ts == "2026-01-01T00:00:00Z", (
            f"execution_start_ts must be set to event timestamp. "
            f"Got: {services.data_layer_2.execution_start_ts!r}"
        )


# ---------------------------------------------------------------------------
# 4. TestExecutionEndUpsertsProperties
# ---------------------------------------------------------------------------


class TestExecutionEndUpsertsProperties:
    """execution:end enriches OrchestratorRun node using execution_start_ts cursor."""

    async def test_execution_end_sets_ended_at(
        self, services: HookStateService
    ) -> None:
        """execution:end must set ended_at on the OrchestratorRun node."""
        handler = OrchestratorRunHandler(services)
        await handler(
            "execution:start",
            {"session_id": "s1", "timestamp": "2026-01-01T00:00:00Z"},
        )
        await handler(
            "execution:end",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:01:00Z",
                "status": "completed",
            },
        )
        node = await services.graph.get_node("s1::orch_run::2026-01-01T00:00:00Z")
        assert node is not None
        assert node.get("ended_at") == "2026-01-01T00:01:00Z", (
            f"ended_at must be set by execution:end. Got: {node!r}"
        )

    async def test_execution_end_sets_status(self, services: HookStateService) -> None:
        """execution:end must set status on the OrchestratorRun node."""
        handler = OrchestratorRunHandler(services)
        await handler(
            "execution:start",
            {"session_id": "s1", "timestamp": "2026-01-01T00:00:00Z"},
        )
        await handler(
            "execution:end",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:01:00Z",
                "status": "completed",
            },
        )
        node = await services.graph.get_node("s1::orch_run::2026-01-01T00:00:00Z")
        assert node is not None
        assert node.get("status") == "completed", (
            f"status must be set by execution:end. Got: {node!r}"
        )

    async def test_execution_end_sets_response_properties(
        self, services: HookStateService
    ) -> None:
        """execution:end must set response property when provided."""
        handler = OrchestratorRunHandler(services)
        await handler(
            "execution:start",
            {"session_id": "s1", "timestamp": "2026-01-01T00:00:00Z"},
        )
        await handler(
            "execution:end",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:01:00Z",
                "status": "completed",
                "response": "final answer text",
            },
        )
        node = await services.graph.get_node("s1::orch_run::2026-01-01T00:00:00Z")
        assert node is not None
        assert node.get("response") == "final answer text", (
            f"response must be set by execution:end. Got: {node!r}"
        )


# ---------------------------------------------------------------------------
# 5. TestOrchestratorCompleteUpsertsProperties
# ---------------------------------------------------------------------------


class TestOrchestratorCompleteUpsertsProperties:
    """orchestrator:complete enriches OrchestratorRun and creates Orchestrator:SST_CONCEPT."""

    async def test_orchestrator_complete_enriches_name_turn_count_completed_at(
        self, services: HookStateService
    ) -> None:
        """orchestrator:complete must set orchestrator_name, turn_count, and completed_at on OrchestratorRun."""
        handler = OrchestratorRunHandler(services)
        await handler(
            "execution:start",
            {"session_id": "s1", "timestamp": "2026-01-01T00:00:00Z"},
        )
        await handler(
            "orchestrator:complete",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:02:00Z",
                "orchestrator": "my-orchestrator",
                "turn_count": 3,
            },
        )
        node = await services.graph.get_node("s1::orch_run::2026-01-01T00:00:00Z")
        assert node is not None
        assert node.get("orchestrator_name") == "my-orchestrator", (
            f"orchestrator_name must be set by orchestrator:complete. Got: {node!r}"
        )
        assert node.get("turn_count") == 3, (
            f"turn_count must be set by orchestrator:complete. Got: {node!r}"
        )
        assert node.get("completed_at") == "2026-01-01T00:02:00Z", (
            f"completed_at must be set by orchestrator:complete. Got: {node!r}"
        )

    async def test_orchestrator_complete_creates_orchestrator_concept_node(
        self, services: HookStateService
    ) -> None:
        """orchestrator:complete must create an Orchestrator:SST_CONCEPT node keyed by name string."""
        handler = OrchestratorRunHandler(services)
        await handler(
            "execution:start",
            {"session_id": "s1", "timestamp": "2026-01-01T00:00:00Z"},
        )
        await handler(
            "orchestrator:complete",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:02:00Z",
                "orchestrator": "my-orchestrator",
                "turn_count": 1,
            },
        )
        orch_concept = await services.graph.get_node("my-orchestrator")
        assert orch_concept is not None, (
            "orchestrator:complete must create an Orchestrator node keyed by name 'my-orchestrator'"
        )
        assert "Orchestrator" in orch_concept["labels"], (
            f"Orchestrator label missing. Got: {orch_concept['labels']}"
        )
        assert "SST_CONCEPT" in orch_concept["labels"], (
            f"SST_CONCEPT label missing. Got: {orch_concept['labels']}"
        )

    async def test_orchestrator_complete_creates_e03_has_attribute_edge(
        self, services: HookStateService
    ) -> None:
        """orchestrator:complete must create E03: Session -[:HAS_ATTRIBUTE {sst_semantic: 'EXPRESSES'}]-> Orchestrator."""
        handler = OrchestratorRunHandler(services)
        await handler(
            "execution:start",
            {"session_id": "s1", "timestamp": "2026-01-01T00:00:00Z"},
        )
        await handler(
            "orchestrator:complete",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:02:00Z",
                "orchestrator": "my-orchestrator",
                "turn_count": 1,
            },
        )
        edge = await services.graph.get_edge("s1", "my-orchestrator")
        assert edge is not None, (
            "E03 HAS_ATTRIBUTE edge from 's1' to 'my-orchestrator' must exist"
        )
        assert edge.get("type") == "HAS_ATTRIBUTE", (
            f"E03 edge must have type='HAS_ATTRIBUTE'. Got: {edge.get('type')}"
        )
        assert edge.get("sst_semantic") == "EXPRESSES", (
            f"E03 edge must have sst_semantic='EXPRESSES'. Got: {edge.get('sst_semantic')}"
        )


# ---------------------------------------------------------------------------
# 6. TestOrchestratorCompleteCursorLifecycle
# ---------------------------------------------------------------------------


class TestOrchestratorCompleteCursorLifecycle:
    """orchestrator:complete sets last_completed_orch_run_id and clears execution_start_ts."""

    async def test_orchestrator_complete_sets_last_completed_orch_run_id(
        self, services: HookStateService
    ) -> None:
        """orchestrator:complete must set last_completed_orch_run_id cursor to OrchestratorRun node key."""
        handler = OrchestratorRunHandler(services)
        await handler(
            "execution:start",
            {"session_id": "s1", "timestamp": "2026-01-01T00:00:00Z"},
        )
        await handler(
            "orchestrator:complete",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:02:00Z",
                "orchestrator": "my-orchestrator",
                "turn_count": 1,
            },
        )
        expected_run_id = "s1::orch_run::2026-01-01T00:00:00Z"
        assert services.data_layer_2.last_completed_orch_run_id == expected_run_id, (
            f"last_completed_orch_run_id must be set to '{expected_run_id}'. "
            f"Got: {services.data_layer_2.last_completed_orch_run_id!r}"
        )

    async def test_orchestrator_complete_clears_execution_start_ts(
        self, services: HookStateService
    ) -> None:
        """orchestrator:complete must clear execution_start_ts cursor (set to None)."""
        handler = OrchestratorRunHandler(services)
        await handler(
            "execution:start",
            {"session_id": "s1", "timestamp": "2026-01-01T00:00:00Z"},
        )
        assert services.data_layer_2.execution_start_ts == "2026-01-01T00:00:00Z"
        await handler(
            "orchestrator:complete",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:02:00Z",
                "orchestrator": "my-orchestrator",
                "turn_count": 1,
            },
        )
        assert services.data_layer_2.execution_start_ts is None, (
            "execution_start_ts cursor must be cleared (None) after orchestrator:complete. "
            f"Got: {services.data_layer_2.execution_start_ts!r}"
        )


# ---------------------------------------------------------------------------
# 7. TestE14TriggersEdge
# ---------------------------------------------------------------------------


class TestE14TriggersEdge:
    """E14: Prompt -[:TRIGGERS {sst_semantic: 'LEADS_TO'}]-> OrchestratorRun."""

    async def test_e14_prompt_triggers_orchestrator_run_edge_created(
        self, services: HookStateService
    ) -> None:
        """E14 edge must be created when last_prompt_id cursor is set before execution:start."""
        handler = OrchestratorRunHandler(services)
        # Simulate that a Prompt node was previously tracked
        services.data_layer_2.last_prompt_id = "s1::prompt::2026-01-01T00:00:00Z"

        await handler(
            "execution:start",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:01:00Z",
            },
        )
        orch_run_id = "s1::orch_run::2026-01-01T00:01:00Z"
        prompt_id = "s1::prompt::2026-01-01T00:00:00Z"
        edge = await services.graph.get_edge(prompt_id, orch_run_id)
        assert edge is not None, (
            f"E14 TRIGGERS edge from '{prompt_id}' to '{orch_run_id}' must exist "
            "when last_prompt_id cursor is set"
        )
        assert edge.get("type") == "TRIGGERS", (
            f"E14 edge must have type='TRIGGERS'. Got: {edge.get('type')}"
        )
        assert edge.get("sst_semantic") == "LEADS_TO", (
            f"E14 edge must have sst_semantic='LEADS_TO'. Got: {edge.get('sst_semantic')}"
        )

    async def test_e14_not_created_when_no_last_prompt_id(
        self, services: HookStateService
    ) -> None:
        """E14 must NOT be created when last_prompt_id cursor is None; only E01 edge exists."""
        handler = OrchestratorRunHandler(services)
        # last_prompt_id is None by default (no prior Prompt event)
        assert services.data_layer_2.last_prompt_id is None

        await handler(
            "execution:start",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        # Only 2 edges should exist: E01 HAS_EXECUTION (Session -> OrchestratorRun) + SOURCED_FROM
        assert len(services.graph._edges) == 2, (
            f"Only E01 + SOURCED_FROM edges should exist when no last_prompt_id. "
            f"Got {len(services.graph._edges)} edges: {list(services.graph._edges.keys())}"
        )


# ---------------------------------------------------------------------------
# 8. TestOrchestratorRunHandlerGuards
# ---------------------------------------------------------------------------


class TestOrchestratorRunHandlerGuards:
    """Missing session_id must short-circuit before any graph mutation."""

    async def test_missing_session_id_returns_continue_with_zero_mutations(
        self, services: HookStateService
    ) -> None:
        """Missing session_id must return HookResult(action='continue') with zero graph mutations."""
        handler = OrchestratorRunHandler(services)
        result = await handler(
            "execution:start",
            {
                "timestamp": "2026-01-01T00:00:00Z",
                # session_id intentionally omitted
            },
        )
        assert result.action == "continue", (
            f"Missing session_id must return action='continue'. Got: {result.action!r}"
        )
        assert len(services.graph._nodes) == 0, (
            f"No graph mutations must occur when session_id is missing. "
            f"Got {len(services.graph._nodes)} nodes."
        )
        assert len(services.graph._edges) == 0, (
            f"No graph mutations must occur when session_id is missing. "
            f"Got {len(services.graph._edges)} edges."
        )


# ---------------------------------------------------------------------------
# 9. TestOrchestratorRunSourcedFrom
# ---------------------------------------------------------------------------


class TestOrchestratorRunSourcedFrom:
    """SOURCED_FROM bridge: OrchestratorRun -> data_layer_1 event node."""

    async def test_execution_start_creates_sourced_from_edge(
        self, services: HookStateService
    ) -> None:
        """execution:start must create SOURCED_FROM edge from OrchestratorRun to data_layer_1 node."""
        handler = OrchestratorRunHandler(services)
        await handler(
            "execution:start",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        orch_run_id = "s1::orch_run::2026-01-01T00:00:00Z"
        data_layer_1_node_id = make_node_id(
            "s1", "execution:start", "2026-01-01T00:00:00Z"
        )
        edge = await services.graph.get_edge(orch_run_id, data_layer_1_node_id)
        assert edge is not None, (
            f"SOURCED_FROM edge from '{orch_run_id}' to '{data_layer_1_node_id}' must exist"
        )
        assert edge.get("type") == "SOURCED_FROM", (
            f"Edge must have type='SOURCED_FROM'. Got: {edge.get('type')}"
        )

    async def test_execution_end_creates_sourced_from_edge(
        self, services: HookStateService
    ) -> None:
        """execution:end must create SOURCED_FROM edge from OrchestratorRun to data_layer_1 node."""
        handler = OrchestratorRunHandler(services)
        # Must call execution:start first to set the cursor
        await handler(
            "execution:start",
            {"session_id": "s1", "timestamp": "2026-01-01T00:00:00Z"},
        )
        await handler(
            "execution:end",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:01:00Z",
                "status": "completed",
            },
        )
        orch_run_id = "s1::orch_run::2026-01-01T00:00:00Z"
        data_layer_1_node_id = make_node_id(
            "s1", "execution:end", "2026-01-01T00:01:00Z"
        )
        edge = await services.graph.get_edge(orch_run_id, data_layer_1_node_id)
        assert edge is not None, (
            f"SOURCED_FROM edge from '{orch_run_id}' to '{data_layer_1_node_id}' must exist"
        )
        assert edge.get("type") == "SOURCED_FROM", (
            f"Edge must have type='SOURCED_FROM'. Got: {edge.get('type')}"
        )

    async def test_orchestrator_complete_creates_sourced_from_edge(
        self, services: HookStateService
    ) -> None:
        """orchestrator:complete must create SOURCED_FROM edge from OrchestratorRun to data_layer_1 node."""
        handler = OrchestratorRunHandler(services)
        # Must call execution:start first to set the cursor
        await handler(
            "execution:start",
            {"session_id": "s1", "timestamp": "2026-01-01T00:00:00Z"},
        )
        await handler(
            "orchestrator:complete",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:02:00Z",
                "orchestrator": "my-orchestrator",
                "turn_count": 3,
            },
        )
        orch_run_id = "s1::orch_run::2026-01-01T00:00:00Z"
        data_layer_1_node_id = make_node_id(
            "s1", "orchestrator:complete", "2026-01-01T00:02:00Z"
        )
        edge = await services.graph.get_edge(orch_run_id, data_layer_1_node_id)
        assert edge is not None, (
            f"SOURCED_FROM edge from '{orch_run_id}' to '{data_layer_1_node_id}' must exist"
        )
        assert edge.get("type") == "SOURCED_FROM", (
            f"Edge must have type='SOURCED_FROM'. Got: {edge.get('type')}"
        )
