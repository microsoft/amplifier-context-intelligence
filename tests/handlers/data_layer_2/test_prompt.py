"""Tests for PromptHandler — Prompt node creation and turn-flow cursor management.

Covers:
- handled_events == frozenset({'prompt:submit'})
- prompt:submit creates Prompt:SST_EVENT node keyed as
  '{session_id}::prompt::{timestamp}' with prompt and occurred_at properties
- E05: Session -[:HAS_PART {sst_semantic: 'CONTAINS'}]-> Prompt (always created)
- E15: OrchestratorRun -[:ENABLES {sst_semantic: 'LEADS_TO'}]-> Prompt
  created when last_completed_orch_run_id is set; NOT created when None
- E15 creation clears last_completed_orch_run_id cursor
- last_prompt_id cursor set to prompt_node_id after prompt:submit
- Guard: missing session_id returns continue with zero graph mutations
"""

from __future__ import annotations

from context_intelligence_server.handlers.data_layer_2.prompt import PromptHandler
from context_intelligence_server.services import HookStateService


# ---------------------------------------------------------------------------
# 1. TestPromptHandlerHandledEvents
# ---------------------------------------------------------------------------


class TestPromptHandlerHandledEvents:
    """handled_events == frozenset({'prompt:submit'})."""

    def test_handled_events_is_exact_frozenset(self) -> None:
        """handled_events must be exactly frozenset({'prompt:submit'})."""
        assert PromptHandler.handled_events == frozenset({"prompt:submit"})

    def test_prompt_start_not_in_handled_events(self) -> None:
        """prompt:start must NOT be in handled_events."""
        assert "prompt:start" not in PromptHandler.handled_events

    def test_prompt_end_not_in_handled_events(self) -> None:
        """prompt:end must NOT be in handled_events."""
        assert "prompt:end" not in PromptHandler.handled_events


# ---------------------------------------------------------------------------
# 2. TestPromptSubmitCreatesNode
# ---------------------------------------------------------------------------


class TestPromptSubmitCreatesNode:
    """prompt:submit creates Prompt:SST_EVENT node with correct key and properties."""

    async def test_node_created_with_correct_compound_key(
        self, services: HookStateService
    ) -> None:
        """prompt:submit must create node at '{session_id}::prompt::{timestamp}'."""
        handler = PromptHandler(services)
        await handler(
            "prompt:submit",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
                "prompt": "hello world",
            },
        )
        node_id = "s1::prompt::2026-01-01T00:00:00Z"
        node = await services.graph.get_node(node_id)
        assert node is not None, f"prompt:submit must create node at '{node_id}'"

    async def test_node_has_prompt_and_sst_event_labels(
        self, services: HookStateService
    ) -> None:
        """Prompt node must have 'Prompt' and 'SST_EVENT' labels."""
        handler = PromptHandler(services)
        await handler(
            "prompt:submit",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
                "prompt": "hello world",
            },
        )
        node = await services.graph.get_node("s1::prompt::2026-01-01T00:00:00Z")
        assert node is not None
        assert "Prompt" in node["labels"], (
            f"Prompt label missing. Got: {node['labels']}"
        )
        assert "SST_EVENT" in node["labels"], (
            f"SST_EVENT label missing. Got: {node['labels']}"
        )

    async def test_node_has_session_id_and_occurred_at(
        self, services: HookStateService
    ) -> None:
        """Prompt node must carry session_id and occurred_at properties."""
        handler = PromptHandler(services)
        await handler(
            "prompt:submit",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
                "prompt": "hello world",
            },
        )
        node = await services.graph.get_node("s1::prompt::2026-01-01T00:00:00Z")
        assert node is not None
        assert node["session_id"] == "s1"
        assert node["occurred_at"] == "2026-01-01T00:00:00Z"

    async def test_node_has_prompt_text(self, services: HookStateService) -> None:
        """Prompt node must carry the prompt text."""
        handler = PromptHandler(services)
        await handler(
            "prompt:submit",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
                "prompt": "hello world",
            },
        )
        node = await services.graph.get_node("s1::prompt::2026-01-01T00:00:00Z")
        assert node is not None
        assert node["prompt"] == "hello world"


# ---------------------------------------------------------------------------
# 3. TestPromptE05Edge
# ---------------------------------------------------------------------------


class TestPromptE05Edge:
    """E05: Session -[:HAS_PART {sst_semantic: 'CONTAINS'}]-> Prompt (always created)."""

    async def test_e05_edge_created(self, services: HookStateService) -> None:
        """prompt:submit must always create E05 HAS_PART edge from Session to Prompt."""
        handler = PromptHandler(services)
        await handler(
            "prompt:submit",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
                "prompt": "hello",
            },
        )
        edge = await services.graph.get_edge("s1", "s1::prompt::2026-01-01T00:00:00Z")
        assert edge is not None, "E05 edge must exist"
        assert edge["type"] == "HAS_PART"
        assert edge["sst_semantic"] == "CONTAINS"


# ---------------------------------------------------------------------------
# 4. TestPromptE15Edge
# ---------------------------------------------------------------------------


class TestPromptE15Edge:
    """E15: OrchestratorRun -[:ENABLES {sst_semantic: 'LEADS_TO'}]-> Prompt (conditional)."""

    async def test_e15_edge_created_when_cursor_set(
        self, services: HookStateService
    ) -> None:
        """E15 ENABLES edge must be created when last_completed_orch_run_id is set."""
        services.data_layer_2.last_completed_orch_run_id = "s1::orch_run::t0"
        handler = PromptHandler(services)
        await handler(
            "prompt:submit",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:01Z",
                "prompt": "next turn",
            },
        )
        edge = await services.graph.get_edge(
            "s1::orch_run::t0", "s1::prompt::2026-01-01T00:00:01Z"
        )
        assert edge is not None, "E15 edge must exist when cursor is set"
        assert edge["type"] == "ENABLES"
        assert edge["sst_semantic"] == "LEADS_TO"

    async def test_e15_not_created_when_cursor_none(
        self, services: HookStateService
    ) -> None:
        """E15 ENABLES edge must NOT be created when last_completed_orch_run_id is None."""
        assert services.data_layer_2.last_completed_orch_run_id is None
        handler = PromptHandler(services)
        await handler(
            "prompt:submit",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
                "prompt": "first prompt",
            },
        )
        # No orch_run exists, so no edge can be formed
        # Verify by checking the graph has exactly 1 edge (E05 only)
        assert len(services.graph._edges) == 1, (
            f"Expected exactly 1 edge (E05), got {len(services.graph._edges)}"
        )

    async def test_e15_clears_cursor_after_creation(
        self, services: HookStateService
    ) -> None:
        """After E15 is created, last_completed_orch_run_id must be cleared to None."""
        services.data_layer_2.last_completed_orch_run_id = "s1::orch_run::t0"
        handler = PromptHandler(services)
        await handler(
            "prompt:submit",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:01Z",
                "prompt": "next turn",
            },
        )
        assert services.data_layer_2.last_completed_orch_run_id is None, (
            "last_completed_orch_run_id must be cleared after E15 creation"
        )


# ---------------------------------------------------------------------------
# 5. TestPromptSetsLastPromptIdCursor
# ---------------------------------------------------------------------------


class TestPromptSetsLastPromptIdCursor:
    """prompt:submit must set last_prompt_id cursor for E14 (used by OrchestratorRunHandler)."""

    async def test_last_prompt_id_set_after_prompt_submit(
        self, services: HookStateService
    ) -> None:
        """last_prompt_id must equal the prompt_node_id after prompt:submit."""
        assert services.data_layer_2.last_prompt_id is None
        handler = PromptHandler(services)
        await handler(
            "prompt:submit",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
                "prompt": "hello",
            },
        )
        assert (
            services.data_layer_2.last_prompt_id == "s1::prompt::2026-01-01T00:00:00Z"
        )

    async def test_last_prompt_id_updated_on_subsequent_prompt(
        self, services: HookStateService
    ) -> None:
        """A second prompt:submit must overwrite last_prompt_id with the new prompt_node_id."""
        handler = PromptHandler(services)
        await handler(
            "prompt:submit",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
                "prompt": "first",
            },
        )
        await handler(
            "prompt:submit",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:01Z",
                "prompt": "second",
            },
        )
        assert (
            services.data_layer_2.last_prompt_id == "s1::prompt::2026-01-01T00:00:01Z"
        )


# ---------------------------------------------------------------------------
# 6. TestPromptSessionIdGuard
# ---------------------------------------------------------------------------


class TestPromptSessionIdGuard:
    """Missing session_id must short-circuit without any graph mutations."""

    async def test_missing_session_id_creates_no_nodes(
        self, services: HookStateService
    ) -> None:
        """prompt:submit with no session_id must create zero nodes."""
        handler = PromptHandler(services)
        await handler(
            "prompt:submit",
            {"timestamp": "2026-01-01T00:00:00Z", "prompt": "hello"},
        )
        assert len(services.graph._nodes) == 0

    async def test_missing_session_id_creates_no_edges(
        self, services: HookStateService
    ) -> None:
        """prompt:submit with no session_id must create zero edges."""
        handler = PromptHandler(services)
        await handler(
            "prompt:submit",
            {"timestamp": "2026-01-01T00:00:00Z", "prompt": "hello"},
        )
        assert len(services.graph._edges) == 0

    async def test_missing_session_id_returns_continue(
        self, services: HookStateService
    ) -> None:
        """prompt:submit with no session_id must return HookResult(action='continue')."""
        handler = PromptHandler(services)
        result = await handler(
            "prompt:submit",
            {"timestamp": "2026-01-01T00:00:00Z", "prompt": "hello"},
        )
        assert result.action == "continue"
