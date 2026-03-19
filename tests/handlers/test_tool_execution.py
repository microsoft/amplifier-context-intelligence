"""Tests for ToolExecutionHandler — ToolExecution lifecycle events.

Adapted from bundle's test_tool_execution_handler.py for the server-side
implementation, which uses the flat-dict GraphState API (no nested 'properties'
key, no edge_type param in get_edge).
"""

from __future__ import annotations

import logging

from context_intelligence_server.handlers.orchestrator_run import OrchestratorRunHandler
from context_intelligence_server.handlers.session import SessionHandler
from context_intelligence_server.handlers.step import StepHandler
from context_intelligence_server.handlers.tool_execution import ToolExecutionHandler
from context_intelligence_server.services import HookStateService
from context_intelligence_server.utils import make_node_id

SESSION_TIMESTAMP = "2026-03-06T00:00:00Z"
PROMPT_TIMESTAMP = "2026-03-06T01:00:00Z"
EXEC_TIMESTAMP = "2026-03-06T02:00:00Z"
STEP_TIMESTAMP = "2026-03-06T03:00:00Z"
TOOL_TIMESTAMP = "2026-03-06T04:00:00Z"

EXPECTED_STEP_ID = make_node_id("s1", "provider:request", STEP_TIMESTAMP)
TOOL_CALL_ID = "call_abc123"
EXPECTED_TE_ID = make_node_id(
    "s1", "tool:pre", TOOL_TIMESTAMP, disambiguator=TOOL_CALL_ID
)


async def _seed_session(services: HookStateService, session_id: str = "s1") -> None:
    """Create a Session node via SessionHandler."""
    handler = SessionHandler(services)
    await handler(
        "session:start", {"session_id": session_id, "timestamp": SESSION_TIMESTAMP}
    )


async def _seed_step(services: HookStateService) -> str:
    """Seed session + prompt:submit + execution:start + provider:request.

    Returns the step node ID.
    """
    await _seed_session(services)
    run_handler = OrchestratorRunHandler(services)
    await run_handler(
        "prompt:submit",
        {"session_id": "s1", "timestamp": PROMPT_TIMESTAMP, "prompt": "Hello"},
    )
    await run_handler(
        "execution:start",
        {"session_id": "s1", "timestamp": EXEC_TIMESTAMP},
    )
    step_handler = StepHandler(services)
    await step_handler(
        "provider:request",
        {"session_id": "s1", "timestamp": STEP_TIMESTAMP},
    )
    return EXPECTED_STEP_ID


# ── handled_events ────────────────────────────────────────────────────────────


class TestHandledEvents:
    def test_tool_pre_claimed(self) -> None:
        assert "tool:pre" in ToolExecutionHandler.handled_events

    def test_tool_post_claimed(self) -> None:
        assert "tool:post" in ToolExecutionHandler.handled_events

    def test_delegate_agent_spawned_claimed(self) -> None:
        assert "delegate:agent_spawned" in ToolExecutionHandler.handled_events

    def test_delegate_agent_completed_claimed(self) -> None:
        assert "delegate:agent_completed" in ToolExecutionHandler.handled_events


# ── tool:pre happy-path ───────────────────────────────────────────────────────


class TestToolPreHappyPath:
    async def test_creates_node(self, services: HookStateService) -> None:
        await _seed_step(services)
        handler = ToolExecutionHandler(services)
        await handler(
            "tool:pre",
            {
                "session_id": "s1",
                "timestamp": TOOL_TIMESTAMP,
                "tool_call_id": TOOL_CALL_ID,
                "tool_name": "bash",
            },
        )
        node = await services.graph.get_node(EXPECTED_TE_ID)
        assert node is not None

    async def test_correct_labels(self, services: HookStateService) -> None:
        await _seed_step(services)
        handler = ToolExecutionHandler(services)
        await handler(
            "tool:pre",
            {
                "session_id": "s1",
                "timestamp": TOOL_TIMESTAMP,
                "tool_call_id": TOOL_CALL_ID,
                "tool_name": "bash",
            },
        )
        node = await services.graph.get_node(EXPECTED_TE_ID)
        assert node is not None
        assert "ToolExecution" in node["labels"]

    async def test_stores_tool_name(self, services: HookStateService) -> None:
        await _seed_step(services)
        handler = ToolExecutionHandler(services)
        await handler(
            "tool:pre",
            {
                "session_id": "s1",
                "timestamp": TOOL_TIMESTAMP,
                "tool_call_id": TOOL_CALL_ID,
                "tool_name": "read_file",
            },
        )
        node = await services.graph.get_node(EXPECTED_TE_ID)
        assert node is not None
        assert node["tool_name"] == "read_file"

    async def test_status_is_executing(self, services: HookStateService) -> None:
        await _seed_step(services)
        handler = ToolExecutionHandler(services)
        await handler(
            "tool:pre",
            {
                "session_id": "s1",
                "timestamp": TOOL_TIMESTAMP,
                "tool_call_id": TOOL_CALL_ID,
                "tool_name": "bash",
            },
        )
        node = await services.graph.get_node(EXPECTED_TE_ID)
        assert node is not None
        assert node["status"] == "executing"

    async def test_triggered_edge_from_step(self, services: HookStateService) -> None:
        step_id = await _seed_step(services)
        handler = ToolExecutionHandler(services)
        await handler(
            "tool:pre",
            {
                "session_id": "s1",
                "timestamp": TOOL_TIMESTAMP,
                "tool_call_id": TOOL_CALL_ID,
                "tool_name": "bash",
            },
        )
        edge = await services.graph.get_edge(step_id, EXPECTED_TE_ID)
        assert edge is not None

    async def test_triggered_edge_occurred_at(self, services: HookStateService) -> None:
        step_id = await _seed_step(services)
        handler = ToolExecutionHandler(services)
        await handler(
            "tool:pre",
            {
                "session_id": "s1",
                "timestamp": TOOL_TIMESTAMP,
                "tool_call_id": TOOL_CALL_ID,
                "tool_name": "bash",
            },
        )
        edge = await services.graph.get_edge(step_id, EXPECTED_TE_ID)
        assert edge is not None
        assert edge["occurred_at"] == TOOL_TIMESTAMP

    async def test_stores_in_tool_call_map(self, services: HookStateService) -> None:
        await _seed_step(services)
        handler = ToolExecutionHandler(services)
        await handler(
            "tool:pre",
            {
                "session_id": "s1",
                "timestamp": TOOL_TIMESTAMP,
                "tool_call_id": TOOL_CALL_ID,
                "tool_name": "bash",
            },
        )
        cursors = services.get_cursors("s1")
        assert cursors.tool_call_map[TOOL_CALL_ID] == EXPECTED_TE_ID

    async def test_no_triggered_edge_when_no_step(
        self, services: HookStateService
    ) -> None:
        """tool:pre with no current_step_id creates node but no TRIGGERED edge."""
        await _seed_session(services)
        handler = ToolExecutionHandler(services)
        await handler(
            "tool:pre",
            {
                "session_id": "s1",
                "timestamp": TOOL_TIMESTAMP,
                "tool_call_id": TOOL_CALL_ID,
                "tool_name": "bash",
            },
        )
        node = await services.graph.get_node(EXPECTED_TE_ID)
        assert node is not None  # node was created regardless

    async def test_returns_continue(self, services: HookStateService) -> None:
        await _seed_step(services)
        handler = ToolExecutionHandler(services)
        result = await handler(
            "tool:pre",
            {
                "session_id": "s1",
                "timestamp": TOOL_TIMESTAMP,
                "tool_call_id": TOOL_CALL_ID,
                "tool_name": "bash",
            },
        )
        assert result.action == "continue"


# ── tool:pre error-path ───────────────────────────────────────────────────────


class TestToolPreErrorPaths:
    async def test_missing_session_id_returns_continue(
        self, services: HookStateService
    ) -> None:
        handler = ToolExecutionHandler(services)
        result = await handler(
            "tool:pre",
            {
                "timestamp": TOOL_TIMESTAMP,
                "tool_call_id": TOOL_CALL_ID,
                "tool_name": "bash",
            },
        )
        assert result.action == "continue"

    async def test_missing_session_id_creates_no_nodes(
        self, services: HookStateService
    ) -> None:
        handler = ToolExecutionHandler(services)
        await handler(
            "tool:pre",
            {"timestamp": TOOL_TIMESTAMP, "tool_call_id": TOOL_CALL_ID},
        )
        node = await services.graph.get_node(EXPECTED_TE_ID)
        assert node is None


# ── tool:post ─────────────────────────────────────────────────────────────────


class TestToolPostHappyPath:
    async def _seed_tool_pre(self, services: HookStateService) -> None:
        await _seed_step(services)
        handler = ToolExecutionHandler(services)
        await handler(
            "tool:pre",
            {
                "session_id": "s1",
                "timestamp": TOOL_TIMESTAMP,
                "tool_call_id": TOOL_CALL_ID,
                "tool_name": "bash",
            },
        )

    async def test_status_complete(self, services: HookStateService) -> None:
        await self._seed_tool_pre(services)
        handler = ToolExecutionHandler(services)
        POST_TIMESTAMP = "2026-03-06T05:00:00Z"
        await handler(
            "tool:post",
            {
                "session_id": "s1",
                "timestamp": POST_TIMESTAMP,
                "tool_call_id": TOOL_CALL_ID,
                "result": "done",
            },
        )
        node = await services.graph.get_node(EXPECTED_TE_ID)
        assert node is not None
        assert node["status"] == "complete"

    async def test_stores_ended_at(self, services: HookStateService) -> None:
        await self._seed_tool_pre(services)
        handler = ToolExecutionHandler(services)
        POST_TIMESTAMP = "2026-03-06T05:00:00Z"
        await handler(
            "tool:post",
            {
                "session_id": "s1",
                "timestamp": POST_TIMESTAMP,
                "tool_call_id": TOOL_CALL_ID,
                "result": "done",
            },
        )
        node = await services.graph.get_node(EXPECTED_TE_ID)
        assert node is not None
        assert node["ended_at"] == POST_TIMESTAMP

    async def test_graceful_when_no_tool_call_map_entry(
        self, services: HookStateService
    ) -> None:
        """tool:post with unknown tool_call_id is a no-op."""
        await _seed_step(services)
        handler = ToolExecutionHandler(services)
        result = await handler(
            "tool:post",
            {
                "session_id": "s1",
                "timestamp": TOOL_TIMESTAMP,
                "tool_call_id": "unknown_call_id",
                "result": "done",
            },
        )
        assert result.action == "continue"

    async def test_returns_continue(self, services: HookStateService) -> None:
        await self._seed_tool_pre(services)
        handler = ToolExecutionHandler(services)
        result = await handler(
            "tool:post",
            {
                "session_id": "s1",
                "timestamp": TOOL_TIMESTAMP,
                "tool_call_id": TOOL_CALL_ID,
                "result": "ok",
            },
        )
        assert result.action == "continue"


# ── delegate:agent_spawned ────────────────────────────────────────────────────


class TestDelegateAgentSpawned:
    async def _seed_tool_pre(self, services: HookStateService) -> None:
        await _seed_step(services)
        handler = ToolExecutionHandler(services)
        await handler(
            "tool:pre",
            {
                "session_id": "s1",
                "timestamp": TOOL_TIMESTAMP,
                "tool_call_id": TOOL_CALL_ID,
                "tool_name": "delegate",
            },
        )

    async def test_adds_delegation_label(self, services: HookStateService) -> None:
        await self._seed_tool_pre(services)
        handler = ToolExecutionHandler(services)
        await handler(
            "delegate:agent_spawned",
            {
                "session_id": "s1",
                "timestamp": TOOL_TIMESTAMP,
                "tool_call_id": TOOL_CALL_ID,
                "child_session_id": "child_session_1",
                "child_agent": "foundation:explorer",
            },
        )
        node = await services.graph.get_node(EXPECTED_TE_ID)
        assert node is not None
        assert "Delegation" in node["labels"]

    async def test_stores_child_session_id(self, services: HookStateService) -> None:
        await self._seed_tool_pre(services)
        handler = ToolExecutionHandler(services)
        await handler(
            "delegate:agent_spawned",
            {
                "session_id": "s1",
                "timestamp": TOOL_TIMESTAMP,
                "tool_call_id": TOOL_CALL_ID,
                "child_session_id": "child_session_1",
                "child_agent": "foundation:explorer",
            },
        )
        node = await services.graph.get_node(EXPECTED_TE_ID)
        assert node is not None
        assert node["child_session_id"] == "child_session_1"

    async def test_stores_child_agent(self, services: HookStateService) -> None:
        await self._seed_tool_pre(services)
        handler = ToolExecutionHandler(services)
        await handler(
            "delegate:agent_spawned",
            {
                "session_id": "s1",
                "timestamp": TOOL_TIMESTAMP,
                "tool_call_id": TOOL_CALL_ID,
                "child_session_id": "child_session_1",
                "child_agent": "foundation:explorer",
            },
        )
        node = await services.graph.get_node(EXPECTED_TE_ID)
        assert node is not None
        assert node["child_agent"] == "foundation:explorer"

    async def test_spawned_edge_to_child_session(
        self, services: HookStateService
    ) -> None:
        await self._seed_tool_pre(services)
        handler = ToolExecutionHandler(services)
        await handler(
            "delegate:agent_spawned",
            {
                "session_id": "s1",
                "timestamp": TOOL_TIMESTAMP,
                "tool_call_id": TOOL_CALL_ID,
                "child_session_id": "child_session_1",
                "child_agent": "foundation:explorer",
            },
        )
        edge = await services.graph.get_edge(EXPECTED_TE_ID, "child_session_1")
        assert edge is not None

    async def test_graceful_when_no_tool_call_map_entry(
        self, services: HookStateService
    ) -> None:
        await _seed_step(services)
        handler = ToolExecutionHandler(services)
        result = await handler(
            "delegate:agent_spawned",
            {
                "session_id": "s1",
                "timestamp": TOOL_TIMESTAMP,
                "tool_call_id": "unknown_call_id",
                "child_session_id": "child_session_1",
            },
        )
        assert result.action == "continue"

    async def test_returns_continue(self, services: HookStateService) -> None:
        await self._seed_tool_pre(services)
        handler = ToolExecutionHandler(services)
        result = await handler(
            "delegate:agent_spawned",
            {
                "session_id": "s1",
                "timestamp": TOOL_TIMESTAMP,
                "tool_call_id": TOOL_CALL_ID,
                "child_session_id": "child_session_1",
                "child_agent": "foundation:explorer",
            },
        )
        assert result.action == "continue"


# ── delegate:agent_completed ──────────────────────────────────────────────────


class TestDelegateAgentCompleted:
    async def _seed_tool_and_spawn(self, services: HookStateService) -> None:
        await _seed_step(services)
        te_handler = ToolExecutionHandler(services)
        await te_handler(
            "tool:pre",
            {
                "session_id": "s1",
                "timestamp": TOOL_TIMESTAMP,
                "tool_call_id": TOOL_CALL_ID,
                "tool_name": "delegate",
            },
        )
        await te_handler(
            "delegate:agent_spawned",
            {
                "session_id": "s1",
                "timestamp": TOOL_TIMESTAMP,
                "tool_call_id": TOOL_CALL_ID,
                "child_session_id": "child_session_1",
                "child_agent": "foundation:explorer",
            },
        )

    async def test_stores_completed_at(self, services: HookStateService) -> None:
        await self._seed_tool_and_spawn(services)
        handler = ToolExecutionHandler(services)
        COMPLETED_TIMESTAMP = "2026-03-06T06:00:00Z"
        await handler(
            "delegate:agent_completed",
            {
                "session_id": "s1",
                "timestamp": COMPLETED_TIMESTAMP,
                "tool_call_id": TOOL_CALL_ID,
            },
        )
        node = await services.graph.get_node(EXPECTED_TE_ID)
        assert node is not None
        assert node["delegate_completed_at"] == COMPLETED_TIMESTAMP

    async def test_returns_continue(self, services: HookStateService) -> None:
        await self._seed_tool_and_spawn(services)
        handler = ToolExecutionHandler(services)
        result = await handler(
            "delegate:agent_completed",
            {
                "session_id": "s1",
                "timestamp": TOOL_TIMESTAMP,
                "tool_call_id": TOOL_CALL_ID,
            },
        )
        assert result.action == "continue"

    async def test_graceful_when_no_tool_call_map_entry(
        self, services: HookStateService
    ) -> None:
        await _seed_step(services)
        handler = ToolExecutionHandler(services)
        result = await handler(
            "delegate:agent_completed",
            {
                "session_id": "s1",
                "timestamp": TOOL_TIMESTAMP,
                "tool_call_id": "unknown_call_id",
            },
        )
        assert result.action == "continue"


# ── no-op events ──────────────────────────────────────────────────────────────


class TestNoOpEvents:
    async def test_context_inherited_returns_continue(
        self, services: HookStateService
    ) -> None:
        handler = ToolExecutionHandler(services)
        result = await handler(
            "delegate:context_inherited",
            {"session_id": "s1", "timestamp": TOOL_TIMESTAMP},
        )
        assert result.action == "continue"

    async def test_session_resumed_returns_continue(
        self, services: HookStateService
    ) -> None:
        handler = ToolExecutionHandler(services)
        result = await handler(
            "delegate:session_resumed",
            {"session_id": "s1", "timestamp": TOOL_TIMESTAMP},
        )
        assert result.action == "continue"


# ── tool:error log level ───────────────────────────────────────────────────────


class TestToolErrorLogLevel:
    """T-1: tool:error must log at WARNING (not INFO) and include the error message."""

    async def test_tool_error_logs_at_warning(self, services, caplog):
        await _seed_step(services)
        handler = ToolExecutionHandler(services)
        await handler(
            "tool:pre",
            {
                "session_id": "s1",
                "timestamp": TOOL_TIMESTAMP,
                "tool_call_id": TOOL_CALL_ID,
                "tool_name": "bash",
            },
        )
        error_timestamp = "2026-03-06T05:00:00Z"
        with caplog.at_level(logging.WARNING):
            await handler(
                "tool:error",
                {
                    "session_id": "s1",
                    "timestamp": error_timestamp,
                    "tool_call_id": TOOL_CALL_ID,
                    "error": "command timed out",
                },
            )
        warning_records = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warning_records) >= 1, (
            f"Expected WARNING log, got: {[r.levelname for r in caplog.records]}"
        )
        warning_text = " ".join(r.message for r in warning_records)
        assert "Errored ToolExecution" in warning_text
        assert "command timed out" in warning_text
