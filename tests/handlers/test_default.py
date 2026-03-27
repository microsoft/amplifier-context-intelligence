"""Tests for DefaultHandler — Event node creation for unclaimed events.

3-level label hierarchy: [FullPascal, Category, 'Event']
No run-awareness — events always attach to session node directly.
"""

from __future__ import annotations

import json
import logging

from context_intelligence_server.handlers.default import DefaultHandler
from context_intelligence_server.services import HookStateService
from context_intelligence_server.utils import make_node_id


class TestDeriveLabelConversions:
    """DefaultHandler.derive_labels returns [FullPascalEvent, CategoryEvent, 'Event']."""

    def test_tool_pre(self) -> None:
        assert DefaultHandler.derive_labels("tool:pre") == [
            "ToolPreEvent",
            "ToolEvent",
            "Event",
        ]

    def test_session_start(self) -> None:
        assert DefaultHandler.derive_labels("session:start") == [
            "SessionStartEvent",
            "SessionEvent",
            "Event",
        ]

    def test_recipe_loop_iteration(self) -> None:
        assert DefaultHandler.derive_labels("recipe:loop_iteration") == [
            "RecipeLoopIterationEvent",
            "RecipeEvent",
            "Event",
        ]

    def test_context_compaction(self) -> None:
        assert DefaultHandler.derive_labels("context:compaction") == [
            "ContextCompactionEvent",
            "ContextEvent",
            "Event",
        ]

    def test_delegate_agent_spawned(self) -> None:
        assert DefaultHandler.derive_labels("delegate:agent_spawned") == [
            "DelegateAgentSpawnedEvent",
            "DelegateEvent",
            "Event",
        ]

    def test_cancel_requested(self) -> None:
        assert DefaultHandler.derive_labels("cancel:requested") == [
            "CancelRequestedEvent",
            "CancelEvent",
            "Event",
        ]

    def test_underscore_only(self) -> None:
        """No colon — FullPascalEvent == CategoryEvent."""
        assert DefaultHandler.derive_labels("my_event") == [
            "MyEventEvent",
            "MyEventEvent",
            "Event",
        ]

    def test_single_word(self) -> None:
        """Single word — FullPascalEvent == CategoryEvent."""
        assert DefaultHandler.derive_labels("ping") == [
            "PingEvent",
            "PingEvent",
            "Event",
        ]

    def test_session_fork(self) -> None:
        """session:fork produces SessionForkEvent — distinct from SubSession/Session entity labels."""
        assert DefaultHandler.derive_labels("session:fork") == [
            "SessionForkEvent",
            "SessionEvent",
            "Event",
        ]


class TestDefaultHandlerCreatesEventNodes:
    """DefaultHandler creates Event nodes with 3-level labels + HAS_EVENT edges."""

    async def test_creates_event_node_with_three_labels(
        self, services: HookStateService
    ) -> None:
        """Event node has 3 labels: FullPascal, Category, and 'Event'."""
        handler = DefaultHandler(services)
        await handler(
            "session:resume",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T02:00:00Z",
            },
        )
        event_id = make_node_id("s1", "session:resume", "2026-01-01T02:00:00Z")
        node = await services.graph.get_node(event_id)
        assert node is not None
        labels = set(node["labels"])
        assert "SessionResumeEvent" in labels
        assert "SessionEvent" in labels
        assert "Event" in labels
        assert node["occurred_at"] == "2026-01-01T02:00:00Z"
        assert node["event_name"] == "session:resume"

    async def test_creates_has_event_edge_from_session_no_run_awareness(
        self, services: HookStateService
    ) -> None:
        """HAS_EVENT edge goes from session_id to event node (no run-awareness)."""
        handler = DefaultHandler(services)
        await handler(
            "session:resume",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T02:00:00Z",
            },
        )
        event_id = make_node_id("s1", "session:resume", "2026-01-01T02:00:00Z")
        edge = await services.graph.get_edge("s1", event_id)
        assert edge is not None
        assert edge["occurred_at"] == "2026-01-01T02:00:00Z"

    async def test_skips_event_without_session_id(
        self, services: HookStateService
    ) -> None:
        handler = DefaultHandler(services)
        result = await handler(
            "session:resume",
            {"timestamp": "2026-01-01T02:00:00Z"},
        )
        assert result.action == "continue"
        # No nodes should have been created
        node = await services.graph.get_node("s1")
        assert node is None

    async def test_works_with_arbitrary_unclaimed_event(
        self, services: HookStateService
    ) -> None:
        """DefaultHandler is generic — custom:my_event → CustomMyEventEvent, CustomEvent, Event."""
        handler = DefaultHandler(services)
        await handler(
            "custom:my_event",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T03:00:00Z",
            },
        )
        event_id = make_node_id("s1", "custom:my_event", "2026-01-01T03:00:00Z")
        node = await services.graph.get_node(event_id)
        assert node is not None
        labels = set(node["labels"])
        assert "CustomMyEventEvent" in labels
        assert "CustomEvent" in labels
        assert "Event" in labels
        assert node["event_name"] == "custom:my_event"

    async def test_returns_continue(self, services: HookStateService) -> None:
        handler = DefaultHandler(services)
        result = await handler(
            "session:resume",
            {"session_id": "s1", "timestamp": "2026-01-01T02:00:00Z"},
        )
        assert result.action == "continue"


class TestDefaultHandlerTiebreaker:
    """tool_call_id used as disambiguator for tool events."""

    async def test_tool_pre_uses_tool_call_id_as_disambiguator(
        self, services: HookStateService
    ) -> None:
        """tool:pre passes tool_call_id to make_node_id as fourth arg."""
        handler = DefaultHandler(services)
        await handler(
            "tool:pre",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T02:00:00Z",
                "tool_call_id": "tc-abc",
            },
        )
        event_id = make_node_id("s1", "tool:pre", "2026-01-01T02:00:00Z", "tc-abc")
        node = await services.graph.get_node(event_id)
        assert node is not None

    async def test_any_event_with_tool_call_id_uses_disambiguator(
        self, services: HookStateService
    ) -> None:
        """Any event carrying tool_call_id uses it as disambiguator (not just tool:*).

        Behaviour change: delegate:agent_spawned (and any other event) with a
        tool_call_id gets the disambiguator embedded in its node_id, preventing
        collisions when the same event fires multiple times at the same timestamp
        for parallel tool calls.
        """
        handler = DefaultHandler(services)
        await handler(
            "delegate:agent_spawned",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T02:00:00Z",
                "tool_call_id": "tc-abc",
            },
        )
        # Node should exist WITH the disambiguator in its node_id
        event_id_with_disambiguator = make_node_id(
            "s1", "delegate:agent_spawned", "2026-01-01T02:00:00Z", "tc-abc"
        )
        node = await services.graph.get_node(event_id_with_disambiguator)
        assert node is not None
        # Node WITHOUT disambiguator should NOT exist
        event_id_no_disambiguator = make_node_id(
            "s1", "delegate:agent_spawned", "2026-01-01T02:00:00Z"
        )
        node_wrong = await services.graph.get_node(event_id_no_disambiguator)
        assert node_wrong is None

    async def test_parallel_tool_calls_produce_distinct_nodes(
        self, services: HookStateService
    ) -> None:
        """Same timestamp, different tool_call_ids produce distinct nodes."""
        handler = DefaultHandler(services)
        await handler(
            "tool:pre",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T02:00:00Z",
                "tool_call_id": "tc-001",
            },
        )
        await handler(
            "tool:pre",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T02:00:00Z",
                "tool_call_id": "tc-002",
            },
        )
        event_id_1 = make_node_id("s1", "tool:pre", "2026-01-01T02:00:00Z", "tc-001")
        event_id_2 = make_node_id("s1", "tool:pre", "2026-01-01T02:00:00Z", "tc-002")
        assert event_id_1 != event_id_2
        node1 = await services.graph.get_node(event_id_1)
        node2 = await services.graph.get_node(event_id_2)
        assert node1 is not None
        assert node2 is not None


class TestDefaultHandlerDataProperty:
    """DefaultHandler stores full event payload in 'data' property as JSON string."""

    async def test_stores_data_property(self, services: HookStateService) -> None:
        """Event node has 'data' property containing the full JSON payload."""
        handler = DefaultHandler(services)
        await handler(
            "session:resume",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T02:00:00Z",
                "custom_info": "extra-value",
            },
        )
        event_id = make_node_id("s1", "session:resume", "2026-01-01T02:00:00Z")
        node = await services.graph.get_node(event_id)
        assert node is not None
        data = json.loads(node["data"])
        assert data["session_id"] == "s1"
        assert data["custom_info"] == "extra-value"


class TestDefaultHandlerEdgeType:
    """HAS_EVENT edge has type='HAS_EVENT'."""

    async def test_has_event_edge_type(self, services: HookStateService) -> None:
        """Edge from session → event node must have type='HAS_EVENT'."""
        handler = DefaultHandler(services)
        await handler(
            "session:resume",
            {"session_id": "s1", "timestamp": "2026-01-01T02:00:00Z"},
        )
        event_id = make_node_id("s1", "session:resume", "2026-01-01T02:00:00Z")
        edge = await services.graph.get_edge("s1", event_id)
        assert edge is not None
        assert edge.get("type") == "HAS_EVENT"


class TestDefaultHandlerDroppedEventLogging:
    """D-1: DefaultHandler must log at WARNING when dropping events without session_id."""

    async def test_missing_session_id_logs_warning(self, services, caplog):
        handler = DefaultHandler(services)
        with caplog.at_level(logging.WARNING):
            result = await handler(
                "session:resume", {"timestamp": "2026-01-01T02:00:00Z"}
            )
        assert result.action == "continue"
        assert "DefaultHandler" in caplog.text
        assert "session:resume" in caplog.text
        warning_records = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warning_records) >= 1


class TestDefaultHandlerFieldLifters:
    """DefaultHandler integrates FieldLifters to expose lifted fields on Event nodes."""

    def test_has_lifters_class_attribute(self) -> None:
        """DefaultHandler must expose a _LIFTERS class-level list."""
        assert hasattr(DefaultHandler, "_LIFTERS")
        assert isinstance(DefaultHandler._LIFTERS, list)
        assert len(DefaultHandler._LIFTERS) == 6

    def test_lifters_include_all_six_types(self) -> None:
        """_LIFTERS must contain instances of all 6 lifter types in the correct order."""
        from context_intelligence_server.handlers.field_lifters import (
            DelegateLifter,
            LlmLifter,
            PromptLifter,
            SessionLifter,
            ToolLifter,
            UniversalLifter,
        )

        lifter_types = [type(lifter) for lifter in DefaultHandler._LIFTERS]
        assert lifter_types[0] is UniversalLifter, "UniversalLifter must be first"
        assert SessionLifter in lifter_types
        assert ToolLifter in lifter_types
        assert DelegateLifter in lifter_types
        assert LlmLifter in lifter_types
        assert PromptLifter in lifter_types

    async def test_universal_lifter_session_id_on_event_node(
        self, services: HookStateService
    ) -> None:
        """UniversalLifter must expose session_id as a top-level property on Event nodes."""
        handler = DefaultHandler(services)
        await handler(
            "session:resume",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T02:00:00Z",
            },
        )
        event_id = make_node_id("s1", "session:resume", "2026-01-01T02:00:00Z")
        node = await services.graph.get_node(event_id)
        assert node is not None
        assert node.get("session_id") == "s1"

    async def test_tool_lifter_tool_name_on_tool_event_node(
        self, services: HookStateService
    ) -> None:
        """ToolLifter must expose tool_name as a top-level property on tool:* Event nodes."""
        handler = DefaultHandler(services)
        await handler(
            "tool:pre",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T02:00:00Z",
                "tool_call_id": "tc-abc",
                "tool_name": "bash",
            },
        )
        event_id = make_node_id("s1", "tool:pre", "2026-01-01T02:00:00Z", "tc-abc")
        node = await services.graph.get_node(event_id)
        assert node is not None
        assert node.get("tool_name") == "bash"

    async def test_delegate_lifter_agent_on_delegate_event_node(
        self, services: HookStateService
    ) -> None:
        """DelegateLifter must expose agent as a top-level property on delegate:* Event nodes."""
        handler = DefaultHandler(services)
        await handler(
            "delegate:agent_spawned",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T02:00:00Z",
                "agent": "foundation:explorer",
                "sub_session_id": "sub-42",
            },
        )
        event_id = make_node_id("s1", "delegate:agent_spawned", "2026-01-01T02:00:00Z")
        node = await services.graph.get_node(event_id)
        assert node is not None
        assert node.get("agent") == "foundation:explorer"
        assert node.get("sub_session_id") == "sub-42"

    async def test_llm_lifter_model_on_llm_event_node(
        self, services: HookStateService
    ) -> None:
        """LlmLifter must expose model/provider as top-level properties on llm:* Event nodes."""
        handler = DefaultHandler(services)
        await handler(
            "llm:request",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T02:00:00Z",
                "model": "claude-3-5-sonnet",
                "provider": "anthropic",
            },
        )
        event_id = make_node_id("s1", "llm:request", "2026-01-01T02:00:00Z")
        node = await services.graph.get_node(event_id)
        assert node is not None
        assert node.get("model") == "claude-3-5-sonnet"
        assert node.get("provider") == "anthropic"

    async def test_full_data_blob_still_stored_alongside_lifted_fields(
        self, services: HookStateService
    ) -> None:
        """Full data blob must remain in 'data' property even when fields are lifted."""
        handler = DefaultHandler(services)
        await handler(
            "tool:pre",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T02:00:00Z",
                "tool_call_id": "tc-abc",
                "tool_name": "bash",
                "extra_field": "should-be-in-data",
            },
        )
        event_id = make_node_id("s1", "tool:pre", "2026-01-01T02:00:00Z", "tc-abc")
        node = await services.graph.get_node(event_id)
        assert node is not None
        # Lifted field as top-level prop
        assert node.get("tool_name") == "bash"
        # Full blob still stored
        assert "data" in node
        blob = json.loads(node["data"])
        assert blob["extra_field"] == "should-be-in-data"
        assert blob["tool_name"] == "bash"

    async def test_multiple_lifters_fire_for_matching_events(
        self, services: HookStateService
    ) -> None:
        """ALL matching lifters fire (not first-match-wins): universal + tool:* both match tool:pre."""
        handler = DefaultHandler(services)
        await handler(
            "tool:pre",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T02:00:00Z",
                "tool_call_id": "tc-xyz",
                "tool_name": "read_file",
                "parallel_group_id": "pg-1",
            },
        )
        event_id = make_node_id("s1", "tool:pre", "2026-01-01T02:00:00Z", "tc-xyz")
        node = await services.graph.get_node(event_id)
        assert node is not None
        # From UniversalLifter (fires for all events)
        assert node.get("session_id") == "s1"
        assert node.get("tool_call_id") == "tc-xyz"
        assert node.get("parallel_group_id") == "pg-1"
        # From ToolLifter (fires for tool:*)
        assert node.get("tool_name") == "read_file"


class TestDefaultHandlerFieldLifting:
    """Integration tests: end-to-end field lifting for each lifter type."""

    async def test_tool_pre_lifts_navigation_and_tool_fields(
        self, services: HookStateService
    ) -> None:
        """tool:pre node has tool_name, tool_call_id, parallel_group_id, tool_input at top level."""
        handler = DefaultHandler(services)
        await handler(
            "tool:pre",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T04:00:00Z",
                "tool_name": "bash",
                "tool_call_id": "tc-lift",
                "parallel_group_id": "pg-lift",
                "tool_input": {"cmd": "ls -la"},
            },
        )
        event_id = make_node_id("s1", "tool:pre", "2026-01-01T04:00:00Z", "tc-lift")
        node = await services.graph.get_node(event_id)
        assert node is not None
        # Navigation fields (UniversalLifter)
        assert node.get("tool_call_id") == "tc-lift"
        assert node.get("parallel_group_id") == "pg-lift"
        # Tool fields (ToolLifter)
        assert node.get("tool_name") == "bash"
        assert node.get("tool_input") == {"cmd": "ls -la"}

    async def test_session_fork_lifts_parent_and_metadata(
        self, services: HookStateService
    ) -> None:
        """session:fork node has parent_id (UniversalLifter), parent (SessionLifter),
        agent_name (SessionLifter metadata), tool_call_id overridden by SessionLifter metadata.
        """
        handler = DefaultHandler(services)
        await handler(
            "session:fork",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T04:01:00Z",
                "parent_id": "parent-sess-id",
                "parent": "parent-ref-value",
                "metadata": {
                    "agent_name": "foundation:explorer",
                    "tool_call_id": "meta-tc-override",
                },
            },
        )
        # No top-level tool_call_id, so no disambiguator in node_id
        event_id = make_node_id("s1", "session:fork", "2026-01-01T04:01:00Z")
        node = await services.graph.get_node(event_id)
        assert node is not None
        # parent_id from UniversalLifter (top-level field)
        assert node.get("parent_id") == "parent-sess-id"
        # parent from SessionLifter (top-level parent field)
        assert node.get("parent") == "parent-ref-value"
        # agent_name from SessionLifter metadata
        assert node.get("agent_name") == "foundation:explorer"
        # tool_call_id overridden by SessionLifter metadata value
        assert node.get("tool_call_id") == "meta-tc-override"

    async def test_llm_response_lifts_model_and_provider(
        self, services: HookStateService
    ) -> None:
        """llm:response node has model and provider lifted by LlmLifter."""
        handler = DefaultHandler(services)
        await handler(
            "llm:response",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T04:02:00Z",
                "model": "claude-3-5-sonnet",
                "provider": "anthropic",
            },
        )
        event_id = make_node_id("s1", "llm:response", "2026-01-01T04:02:00Z")
        node = await services.graph.get_node(event_id)
        assert node is not None
        assert node.get("model") == "claude-3-5-sonnet"
        assert node.get("provider") == "anthropic"

    async def test_prompt_submit_lifts_prompt(self, services: HookStateService) -> None:
        """prompt:submit node has prompt lifted by PromptLifter."""
        handler = DefaultHandler(services)
        await handler(
            "prompt:submit",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T04:03:00Z",
                "prompt": "What is AI?",
            },
        )
        event_id = make_node_id("s1", "prompt:submit", "2026-01-01T04:03:00Z")
        node = await services.graph.get_node(event_id)
        assert node is not None
        assert node.get("prompt") == "What is AI?"

    async def test_prompt_complete_lifts_prompt_and_response_preview(
        self, services: HookStateService
    ) -> None:
        """prompt:complete node has both prompt and response_preview lifted by PromptLifter."""
        handler = DefaultHandler(services)
        await handler(
            "prompt:complete",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T04:04:00Z",
                "prompt": "What is AI?",
                "response_preview": "AI stands for Artificial Intelligence...",
            },
        )
        event_id = make_node_id("s1", "prompt:complete", "2026-01-01T04:04:00Z")
        node = await services.graph.get_node(event_id)
        assert node is not None
        assert node.get("prompt") == "What is AI?"
        assert (
            node.get("response_preview") == "AI stands for Artificial Intelligence..."
        )

    async def test_data_blob_still_present_alongside_lifted_fields(
        self, services: HookStateService
    ) -> None:
        """Full data blob JSON still stored even when fields are lifted; extra_field in blob but not at top level."""
        handler = DefaultHandler(services)
        await handler(
            "tool:pre",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T04:05:00Z",
                "tool_call_id": "tc-blob",
                "tool_name": "read_file",
                "extra_field": "only-in-blob",
            },
        )
        event_id = make_node_id("s1", "tool:pre", "2026-01-01T04:05:00Z", "tc-blob")
        node = await services.graph.get_node(event_id)
        assert node is not None
        # Lifted field at top level
        assert node.get("tool_name") == "read_file"
        # Full blob still stored
        assert "data" in node
        blob = json.loads(node["data"])
        assert blob["extra_field"] == "only-in-blob"
        assert blob["tool_name"] == "read_file"
        # extra_field NOT present as top-level property (only in blob)
        assert node.get("extra_field") is None

    async def test_none_lifted_fields_not_written_to_node(
        self, services: HookStateService
    ) -> None:
        """parent_id=None produces no parent_id key on the event node."""
        handler = DefaultHandler(services)
        await handler(
            "session:resume",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T04:06:00Z",
                "parent_id": None,
            },
        )
        event_id = make_node_id("s1", "session:resume", "2026-01-01T04:06:00Z")
        node = await services.graph.get_node(event_id)
        assert node is not None
        # parent_id=None must NOT appear as a top-level node property
        assert "parent_id" not in node

    async def test_delegate_spawned_lifts_agent_and_session_ids(
        self, services: HookStateService
    ) -> None:
        """delegate:agent_spawned has agent, sub_session_id, parent_session_id lifted by DelegateLifter."""
        handler = DefaultHandler(services)
        await handler(
            "delegate:agent_spawned",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T04:07:00Z",
                "agent": "foundation:explorer",
                "sub_session_id": "sub-sess-99",
                "parent_session_id": "parent-sess-42",
            },
        )
        event_id = make_node_id("s1", "delegate:agent_spawned", "2026-01-01T04:07:00Z")
        node = await services.graph.get_node(event_id)
        assert node is not None
        assert node.get("agent") == "foundation:explorer"
        assert node.get("sub_session_id") == "sub-sess-99"
        assert node.get("parent_session_id") == "parent-sess-42"

    async def test_unknown_event_still_gets_universal_fields(
        self, services: HookStateService
    ) -> None:
        """custom:something gets session_id and parent_id from UniversalLifter."""
        handler = DefaultHandler(services)
        await handler(
            "custom:something",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T04:08:00Z",
                "parent_id": "parent-sess-custom",
            },
        )
        event_id = make_node_id("s1", "custom:something", "2026-01-01T04:08:00Z")
        node = await services.graph.get_node(event_id)
        assert node is not None
        # UniversalLifter always fires regardless of event type
        assert node.get("session_id") == "s1"
        assert node.get("parent_id") == "parent-sess-custom"
