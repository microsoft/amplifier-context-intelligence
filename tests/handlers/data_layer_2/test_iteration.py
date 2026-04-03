"""Tests for IterationHandler — provider/LLM triplet assembly.

Covers:
- handled_events == frozenset({'provider:request', 'llm:request', 'llm:response'})
- provider:request creates Iteration:SST_EVENT node keyed as
  '{session_id}::iteration::{iteration_number}' with session_id, iteration_number,
  and started_at; sets active_iteration_id cursor
- E06: OrchestratorRun -[:HAS_PART {sst_semantic: 'CONTAINS'}]-> Iteration
  created when execution_start_ts cursor is set; NOT created when None
- llm:request enriches active Iteration with provider, model, message_count, has_system;
  noop when active_iteration_id is None
- llm:response enriches active Iteration with usage_input, usage_output, usage_cache_write;
  handles missing usage dict without crash; noop when active_iteration_id is None
- Guard: missing session_id returns continue with zero graph mutations
"""

from __future__ import annotations

from context_intelligence_server.handlers.data_layer_2.iteration import IterationHandler
from context_intelligence_server.services import HookStateService
from context_intelligence_server.utils import make_node_id


# ---------------------------------------------------------------------------
# 1. TestIterationHandlerHandledEvents
# ---------------------------------------------------------------------------


class TestIterationHandlerHandledEvents:
    """handled_events == frozenset({'provider:request', 'llm:request', 'llm:response'})."""

    def test_handled_events_is_exact_frozenset(self) -> None:
        """handled_events must be exactly frozenset({'provider:request', 'llm:request', 'llm:response'})."""
        assert IterationHandler.handled_events == frozenset(
            {"provider:request", "llm:request", "llm:response"}
        )

    def test_provider_response_not_in_handled_events(self) -> None:
        """provider:response must NOT be in handled_events."""
        assert "provider:response" not in IterationHandler.handled_events


# ---------------------------------------------------------------------------
# 2. TestProviderRequestCreatesIteration
# ---------------------------------------------------------------------------


class TestProviderRequestCreatesIteration:
    """provider:request creates Iteration:SST_EVENT node with correct key and properties."""

    async def test_node_created_with_correct_compound_key(
        self, services: HookStateService
    ) -> None:
        """provider:request must create node at '{session_id}::iteration::{iteration_number}'."""
        handler = IterationHandler(services)
        await handler(
            "provider:request",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        node_id = "s1::iteration::1"
        node = await services.graph.get_node(node_id)
        assert node is not None, f"provider:request must create node at '{node_id}'"

    async def test_node_has_iteration_and_sst_event_labels(
        self, services: HookStateService
    ) -> None:
        """Iteration node must have 'Iteration' and 'SST_EVENT' labels."""
        handler = IterationHandler(services)
        await handler(
            "provider:request",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        node = await services.graph.get_node("s1::iteration::1")
        assert node is not None
        assert "Iteration" in node["labels"], (
            f"Iteration label missing. Got: {node['labels']}"
        )
        assert "SST_EVENT" in node["labels"], (
            f"SST_EVENT label missing. Got: {node['labels']}"
        )

    async def test_node_has_session_id_started_at_and_iteration_number(
        self, services: HookStateService
    ) -> None:
        """Iteration node must carry session_id, started_at, and iteration_number properties."""
        handler = IterationHandler(services)
        await handler(
            "provider:request",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        node = await services.graph.get_node("s1::iteration::1")
        assert node is not None
        assert node.get("session_id") == "s1", (
            f"session_id property missing or wrong. Got: {node!r}"
        )
        assert node.get("started_at") == "2026-01-01T00:00:00Z", (
            f"started_at property missing or wrong. Got: {node!r}"
        )
        assert node.get("iteration_number") == 1, (
            f"iteration_number property missing or wrong. Got: {node!r}"
        )

    async def test_iteration_number_increments_on_subsequent_provider_requests(
        self, services: HookStateService
    ) -> None:
        """Second provider:request must create node at '{session_id}::iteration::2'."""
        handler = IterationHandler(services)
        await handler(
            "provider:request",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        await handler(
            "provider:request",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:01:00Z",
            },
        )
        node1 = await services.graph.get_node("s1::iteration::1")
        node2 = await services.graph.get_node("s1::iteration::2")
        assert node1 is not None, (
            "First Iteration node must exist at 's1::iteration::1'"
        )
        assert node2 is not None, (
            "Second provider:request must create node at 's1::iteration::2'"
        )
        assert node2.get("iteration_number") == 2, (
            f"Second Iteration node must have iteration_number=2. Got: {node2!r}"
        )

    async def test_provider_request_sets_active_iteration_id_cursor(
        self, services: HookStateService
    ) -> None:
        """provider:request must set services.data_layer_2.active_iteration_id to the new node key."""
        handler = IterationHandler(services)
        assert services.data_layer_2.active_iteration_id is None, (
            "active_iteration_id must be None before any event"
        )
        await handler(
            "provider:request",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        assert services.data_layer_2.active_iteration_id == "s1::iteration::1", (
            f"active_iteration_id must be set to 's1::iteration::1'. "
            f"Got: {services.data_layer_2.active_iteration_id!r}"
        )


# ---------------------------------------------------------------------------
# 3. TestE06HasPartEdge
# ---------------------------------------------------------------------------


class TestE06HasPartEdge:
    """E06: OrchestratorRun -[:HAS_PART {sst_semantic: 'CONTAINS'}]-> Iteration."""

    async def test_e06_has_part_edge_created_when_execution_start_ts_is_set(
        self, services: HookStateService
    ) -> None:
        """E06 edge must be created when execution_start_ts cursor is set before provider:request."""
        handler = IterationHandler(services)
        # Simulate that execution:start previously fired and set the cursor
        services.data_layer_2.execution_start_ts = "2026-01-01T00:00:00Z"

        await handler(
            "provider:request",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:01Z",
            },
        )
        orch_run_id = "s1::orch_run::2026-01-01T00:00:00Z"
        iteration_id = "s1::iteration::1"
        edge = await services.graph.get_edge(orch_run_id, iteration_id)
        assert edge is not None, (
            f"E06 HAS_PART edge from '{orch_run_id}' to '{iteration_id}' must exist "
            "when execution_start_ts cursor is set"
        )
        assert edge.get("type") == "HAS_PART", (
            f"E06 edge must have type='HAS_PART'. Got: {edge.get('type')}"
        )
        assert edge.get("sst_semantic") == "CONTAINS", (
            f"E06 edge must have sst_semantic='CONTAINS'. Got: {edge.get('sst_semantic')}"
        )

    async def test_e06_not_created_when_execution_start_ts_is_none(
        self, services: HookStateService
    ) -> None:
        """E06 must NOT be created when execution_start_ts cursor is None; no edges created."""
        handler = IterationHandler(services)
        # execution_start_ts is None by default (no prior execution:start event)
        assert services.data_layer_2.execution_start_ts is None

        await handler(
            "provider:request",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        # Only SOURCED_FROM edge should be created when execution_start_ts is None
        assert len(services.graph._edges) == 1, (
            f"Only SOURCED_FROM edge should exist when execution_start_ts is None. "
            f"Got {len(services.graph._edges)} edges: {list(services.graph._edges.keys())}"
        )


# ---------------------------------------------------------------------------
# 4. TestLlmRequestUpsertsProperties
# ---------------------------------------------------------------------------


class TestLlmRequestUpsertsProperties:
    """llm:request enriches active Iteration with provider, model, message_count, has_system."""

    async def test_llm_request_enriches_provider_and_model(
        self, services: HookStateService
    ) -> None:
        """llm:request must set provider and model on the active Iteration node."""
        handler = IterationHandler(services)
        # First fire provider:request to create the Iteration node and set the cursor
        await handler(
            "provider:request",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        await handler(
            "llm:request",
            {
                "session_id": "s1",
                "provider": "anthropic",
                "model": "claude-3-5-sonnet",
                "message_count": 5,
                "has_system": True,
            },
        )
        node = await services.graph.get_node("s1::iteration::1")
        assert node is not None
        assert node.get("provider") == "anthropic", (
            f"llm:request must set provider on active Iteration. Got: {node!r}"
        )
        assert node.get("model") == "claude-3-5-sonnet", (
            f"llm:request must set model on active Iteration. Got: {node!r}"
        )

    async def test_llm_request_enriches_message_count_and_has_system(
        self, services: HookStateService
    ) -> None:
        """llm:request must set message_count and has_system on the active Iteration node."""
        handler = IterationHandler(services)
        await handler(
            "provider:request",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        await handler(
            "llm:request",
            {
                "session_id": "s1",
                "provider": "anthropic",
                "model": "claude-3-5-sonnet",
                "message_count": 7,
                "has_system": False,
            },
        )
        node = await services.graph.get_node("s1::iteration::1")
        assert node is not None
        assert node.get("message_count") == 7, (
            f"llm:request must set message_count on active Iteration. Got: {node!r}"
        )
        assert node.get("has_system") is False, (
            f"llm:request must set has_system on active Iteration. Got: {node!r}"
        )

    async def test_llm_request_noop_when_active_iteration_id_is_none(
        self, services: HookStateService
    ) -> None:
        """llm:request must return continue without mutation when active_iteration_id is None."""
        handler = IterationHandler(services)
        # active_iteration_id is None — no provider:request has fired
        assert services.data_layer_2.active_iteration_id is None

        result = await handler(
            "llm:request",
            {
                "session_id": "s1",
                "provider": "anthropic",
                "model": "claude-3-5-sonnet",
                "message_count": 5,
                "has_system": True,
            },
        )
        assert result.action == "continue", (
            f"llm:request with no active_iteration_id must return action='continue'. "
            f"Got: {result.action!r}"
        )
        assert len(services.graph._nodes) == 0, (
            "No graph mutations must occur when active_iteration_id is None. "
            f"Got {len(services.graph._nodes)} nodes."
        )


# ---------------------------------------------------------------------------
# 5. TestLlmResponseUpsertsUsage
# ---------------------------------------------------------------------------


class TestLlmResponseUpsertsUsage:
    """llm:response enriches active Iteration with usage_input, usage_output, usage_cache_write."""

    async def test_llm_response_enriches_usage_input_and_output(
        self, services: HookStateService
    ) -> None:
        """llm:response must set usage_input and usage_output on the active Iteration node."""
        handler = IterationHandler(services)
        await handler(
            "provider:request",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        await handler(
            "llm:response",
            {
                "session_id": "s1",
                "usage": {
                    "input_tokens": 1000,
                    "output_tokens": 250,
                    "cache_creation_input_tokens": 0,
                },
            },
        )
        node = await services.graph.get_node("s1::iteration::1")
        assert node is not None
        assert node.get("usage_input") == 1000, (
            f"llm:response must set usage_input from usage.input_tokens. Got: {node!r}"
        )
        assert node.get("usage_output") == 250, (
            f"llm:response must set usage_output from usage.output_tokens. Got: {node!r}"
        )

    async def test_llm_response_enriches_usage_cache_write(
        self, services: HookStateService
    ) -> None:
        """llm:response must set usage_cache_write from usage.cache_creation_input_tokens."""
        handler = IterationHandler(services)
        await handler(
            "provider:request",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        await handler(
            "llm:response",
            {
                "session_id": "s1",
                "usage": {
                    "input_tokens": 500,
                    "output_tokens": 100,
                    "cache_creation_input_tokens": 200,
                },
            },
        )
        node = await services.graph.get_node("s1::iteration::1")
        assert node is not None
        assert node.get("usage_cache_write") == 200, (
            f"llm:response must set usage_cache_write from usage.cache_creation_input_tokens. "
            f"Got: {node!r}"
        )

    async def test_llm_response_handles_missing_usage_dict_without_crash(
        self, services: HookStateService
    ) -> None:
        """llm:response must not crash when usage dict is absent from event data."""
        handler = IterationHandler(services)
        await handler(
            "provider:request",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        # Should not raise any exception
        result = await handler(
            "llm:response",
            {
                "session_id": "s1",
                # usage key intentionally omitted
            },
        )
        assert result.action == "continue", (
            f"llm:response with missing usage must still return action='continue'. "
            f"Got: {result.action!r}"
        )

    async def test_llm_response_noop_when_active_iteration_id_is_none(
        self, services: HookStateService
    ) -> None:
        """llm:response must return continue without mutation when active_iteration_id is None."""
        handler = IterationHandler(services)
        assert services.data_layer_2.active_iteration_id is None

        result = await handler(
            "llm:response",
            {
                "session_id": "s1",
                "usage": {
                    "input_tokens": 100,
                    "output_tokens": 50,
                    "cache_creation_input_tokens": 0,
                },
            },
        )
        assert result.action == "continue", (
            f"llm:response with no active_iteration_id must return action='continue'. "
            f"Got: {result.action!r}"
        )
        assert len(services.graph._nodes) == 0, (
            "No graph mutations must occur when active_iteration_id is None. "
            f"Got {len(services.graph._nodes)} nodes."
        )


# ---------------------------------------------------------------------------
# 6. TestIterationHandlerGuards
# ---------------------------------------------------------------------------


class TestIterationHandlerGuards:
    """Missing session_id must short-circuit before any graph mutation."""

    async def test_missing_session_id_returns_continue(
        self, services: HookStateService
    ) -> None:
        """Missing session_id must return HookResult(action='continue')."""
        handler = IterationHandler(services)
        result = await handler(
            "provider:request",
            {
                "timestamp": "2026-01-01T00:00:00Z",
                # session_id intentionally omitted
            },
        )
        assert result.action == "continue", (
            f"Missing session_id must return action='continue'. Got: {result.action!r}"
        )

    async def test_missing_session_id_results_in_zero_graph_mutations(
        self, services: HookStateService
    ) -> None:
        """Missing session_id must not create any nodes or edges."""
        handler = IterationHandler(services)
        await handler(
            "provider:request",
            {
                "timestamp": "2026-01-01T00:00:00Z",
                # session_id intentionally omitted
            },
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
# 7. TestIterationSourcedFrom
# ---------------------------------------------------------------------------


class TestIterationSourcedFrom:
    """SOURCED_FROM edges from Iteration to data_layer_1 event nodes."""

    async def test_provider_request_creates_sourced_from_edge(
        self, services: HookStateService
    ) -> None:
        """provider:request must create SOURCED_FROM edge from Iteration to data_layer_1 provider:request node."""
        handler = IterationHandler(services)
        timestamp = "2026-01-01T00:00:00Z"
        await handler(
            "provider:request",
            {
                "session_id": "s1",
                "timestamp": timestamp,
            },
        )
        iteration_id = "s1::iteration::1"
        data_layer_1_node_id = make_node_id("s1", "provider:request", timestamp)
        edge = await services.graph.get_edge(iteration_id, data_layer_1_node_id)
        assert edge is not None, (
            f"SOURCED_FROM edge from '{iteration_id}' to '{data_layer_1_node_id}' "
            "must exist after provider:request"
        )
        assert edge.get("type") == "SOURCED_FROM", (
            f"Edge type must be 'SOURCED_FROM'. Got: {edge.get('type')!r}"
        )

    async def test_llm_request_creates_sourced_from_edge(
        self, services: HookStateService
    ) -> None:
        """llm:request must create SOURCED_FROM edge from Iteration to data_layer_1 llm:request node."""
        handler = IterationHandler(services)
        # Call provider:request first to set the active_iteration_id cursor
        await handler(
            "provider:request",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        timestamp = "2026-01-01T00:00:01Z"
        await handler(
            "llm:request",
            {
                "session_id": "s1",
                "timestamp": timestamp,
                "provider": "anthropic",
                "model": "claude-3-5-sonnet",
                "message_count": 5,
                "has_system": True,
            },
        )
        iteration_id = "s1::iteration::1"
        data_layer_1_node_id = make_node_id("s1", "llm:request", timestamp)
        edge = await services.graph.get_edge(iteration_id, data_layer_1_node_id)
        assert edge is not None, (
            f"SOURCED_FROM edge from '{iteration_id}' to '{data_layer_1_node_id}' "
            "must exist after llm:request"
        )
        assert edge.get("type") == "SOURCED_FROM", (
            f"Edge type must be 'SOURCED_FROM'. Got: {edge.get('type')!r}"
        )

    async def test_llm_response_creates_sourced_from_edge(
        self, services: HookStateService
    ) -> None:
        """llm:response must create SOURCED_FROM edge from Iteration to data_layer_1 llm:response node."""
        handler = IterationHandler(services)
        # Call provider:request first to set the active_iteration_id cursor
        await handler(
            "provider:request",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        timestamp = "2026-01-01T00:00:02Z"
        await handler(
            "llm:response",
            {
                "session_id": "s1",
                "timestamp": timestamp,
                "usage": {
                    "input_tokens": 1000,
                    "output_tokens": 250,
                    "cache_creation_input_tokens": 0,
                },
            },
        )
        iteration_id = "s1::iteration::1"
        data_layer_1_node_id = make_node_id("s1", "llm:response", timestamp)
        edge = await services.graph.get_edge(iteration_id, data_layer_1_node_id)
        assert edge is not None, (
            f"SOURCED_FROM edge from '{iteration_id}' to '{data_layer_1_node_id}' "
            "must exist after llm:response"
        )
        assert edge.get("type") == "SOURCED_FROM", (
            f"Edge type must be 'SOURCED_FROM'. Got: {edge.get('type')!r}"
        )
