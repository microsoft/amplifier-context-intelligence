"""Tests for OrchestratorRunHandler — full lifecycle events.

Adapted from bundle's test_orchestrator_run_handler.py for the server-side
implementation, which uses the flat-dict GraphState API (no nested 'properties'
key, no edge_type param in get_edge).
"""

from __future__ import annotations

import json

from context_intelligence_server.handlers.orchestrator_run import (
    PREVIEW_MAX_LEN,
    OrchestratorRunHandler,
    _STATUS_MAP,
)
from context_intelligence_server.handlers.session import SessionHandler
from context_intelligence_server.handlers.step import StepHandler
from context_intelligence_server.services import HookStateService, RunTokens
from context_intelligence_server.utils import make_node_id


def test_status_map_matches_spec() -> None:
    assert _STATUS_MAP == {
        "success": "complete",
        "cancelled": "cancelled",
        "error": "error",
    }


TIMESTAMP = "2026-03-06T01:00:00Z"
EXPECTED_NODE_ID = "s1__prompt_submit__1772758800000"


async def _seed_session(services: HookStateService, session_id: str = "s1") -> None:
    """Create a Session node via SessionHandler so it exists in the graph."""
    session_handler = SessionHandler(services)
    await session_handler(
        "session:start",
        {
            "session_id": session_id,
            "timestamp": "2026-03-06T00:00:00Z",
        },
    )


# ── Happy-path tests ─────────────────────────────────────────────────────────


class TestPromptSubmitHappyPath:
    async def test_creates_node(self, services: HookStateService) -> None:
        await _seed_session(services)
        handler = OrchestratorRunHandler(services)
        await handler(
            "prompt:submit",
            {"session_id": "s1", "timestamp": TIMESTAMP, "prompt": "Hello"},
        )
        node = await services.graph.get_node(EXPECTED_NODE_ID)
        assert node is not None

    async def test_correct_labels(self, services: HookStateService) -> None:
        await _seed_session(services)
        handler = OrchestratorRunHandler(services)
        await handler(
            "prompt:submit",
            {"session_id": "s1", "timestamp": TIMESTAMP, "prompt": "Hello"},
        )
        node = await services.graph.get_node(EXPECTED_NODE_ID)
        assert node is not None
        assert set(node["labels"]) == {"Step", "PromptStep"}

    async def test_stores_prompt_text(self, services: HookStateService) -> None:
        await _seed_session(services)
        handler = OrchestratorRunHandler(services)
        await handler(
            "prompt:submit",
            {"session_id": "s1", "timestamp": TIMESTAMP, "prompt": "Hello world"},
        )
        node = await services.graph.get_node(EXPECTED_NODE_ID)
        assert node is not None
        assert node["prompt_text"] == "Hello world"

    async def test_stores_prompt_preview(self, services: HookStateService) -> None:
        await _seed_session(services)
        handler = OrchestratorRunHandler(services)
        await handler(
            "prompt:submit",
            {"session_id": "s1", "timestamp": TIMESTAMP, "prompt": "Hello world"},
        )
        node = await services.graph.get_node(EXPECTED_NODE_ID)
        assert node is not None
        assert node["prompt_preview"] == "Hello world"

    async def test_preview_truncated_to_200_chars(
        self, services: HookStateService
    ) -> None:
        await _seed_session(services)
        handler = OrchestratorRunHandler(services)
        long_prompt = "x" * 300
        await handler(
            "prompt:submit",
            {"session_id": "s1", "timestamp": TIMESTAMP, "prompt": long_prompt},
        )
        node = await services.graph.get_node(EXPECTED_NODE_ID)
        assert node is not None
        assert node["prompt_preview"] == "x" * 200
        assert node["prompt_text"] == long_prompt

    async def test_properties(self, services: HookStateService) -> None:
        await _seed_session(services)
        handler = OrchestratorRunHandler(services)
        await handler(
            "prompt:submit",
            {"session_id": "s1", "timestamp": TIMESTAMP, "prompt": "Hi"},
        )
        node = await services.graph.get_node(EXPECTED_NODE_ID)
        assert node is not None
        assert node["iteration"] == 0
        assert node["occurred_at"] == TIMESTAMP
        assert node["session_id"] == "s1"

    async def test_no_edges_created(self, services: HookStateService) -> None:
        await _seed_session(services)
        handler = OrchestratorRunHandler(services)
        await handler(
            "prompt:submit",
            {"session_id": "s1", "timestamp": TIMESTAMP, "prompt": "Hi"},
        )
        # No HAS_STEP edge should be created from session to prompt step
        edge = await services.graph.get_edge("s1", EXPECTED_NODE_ID)
        assert edge is None

    async def test_updates_cursors(self, services: HookStateService) -> None:
        await _seed_session(services)
        handler = OrchestratorRunHandler(services)
        # Set initial cursor state to verify changes
        cursors = services.get_cursors("s1")
        cursors.current_step_id = "old_step"
        cursors.prompt_preview = "old preview"

        await handler(
            "prompt:submit",
            {"session_id": "s1", "timestamp": TIMESTAMP, "prompt": "Hello world"},
        )

        cursors = services.get_cursors("s1")
        assert cursors.current_step_id == EXPECTED_NODE_ID  # set to new node
        assert cursors.prompt_preview == "Hello world"  # stored

    async def test_node_id_matches_make_node_id(
        self, services: HookStateService
    ) -> None:
        await _seed_session(services)
        handler = OrchestratorRunHandler(services)
        await handler(
            "prompt:submit",
            {"session_id": "s1", "timestamp": TIMESTAMP, "prompt": "Hi"},
        )
        expected = make_node_id("s1", "prompt:submit", TIMESTAMP)
        assert expected == EXPECTED_NODE_ID
        node = await services.graph.get_node(expected)
        assert node is not None

    async def test_returns_hook_result_continue(
        self, services: HookStateService
    ) -> None:
        await _seed_session(services)
        handler = OrchestratorRunHandler(services)
        result = await handler(
            "prompt:submit",
            {"session_id": "s1", "timestamp": TIMESTAMP, "prompt": "Hi"},
        )
        assert result.action == "continue"


# ── Error-path tests ──────────────────────────────────────────────────────────


class TestPromptSubmitErrorPaths:
    async def test_missing_session_id_returns_continue(
        self, services: HookStateService
    ) -> None:
        handler = OrchestratorRunHandler(services)
        result = await handler(
            "prompt:submit",
            {"timestamp": TIMESTAMP, "prompt": "Hi"},
        )
        assert result.action == "continue"

    async def test_missing_session_id_creates_no_nodes(
        self, services: HookStateService
    ) -> None:
        handler = OrchestratorRunHandler(services)
        await handler(
            "prompt:submit",
            {"timestamp": TIMESTAMP, "prompt": "Hi"},
        )
        # No PromptStep node should exist
        node = await services.graph.get_node(EXPECTED_NODE_ID)
        assert node is None

    async def test_session_not_found_returns_continue(
        self, services: HookStateService
    ) -> None:
        handler = OrchestratorRunHandler(services)
        # session_id provided but no session node seeded
        result = await handler(
            "prompt:submit",
            {"session_id": "s1", "timestamp": TIMESTAMP, "prompt": "Hi"},
        )
        assert result.action == "continue"

    async def test_session_not_found_creates_stub_and_prompt_step(
        self, services: HookStateService
    ) -> None:
        """D-06 fix: session not found → stub session node created, processing continues.

        Previously this returned early and created no nodes.  The fix calls
        ensure_session_node() to create a stub Session node, then continues to
        create the PromptStep node so graph topology is preserved.
        """
        handler = OrchestratorRunHandler(services)
        await handler(
            "prompt:submit",
            {"session_id": "s1", "timestamp": TIMESTAMP, "prompt": "Hi"},
        )
        # PromptStep node MUST now be created (not dropped)
        node = await services.graph.get_node(EXPECTED_NODE_ID)
        assert node is not None, (
            "PromptStep node must be created even when session:start was missed"
        )
        # A stub Session node must exist for s1
        session_node = await services.graph.get_node("s1")
        assert session_node is not None, "Stub Session node must be created for s1"
        assert "Session" in session_node.get("labels", [])
        # cursor must reflect the new step
        cursors = services.get_cursors("s1")
        assert cursors.current_step_id == EXPECTED_NODE_ID


# ── execution:start constants ─────────────────────────────────────────────────

EXEC_TIMESTAMP = "2026-03-06T02:00:00Z"
EXPECTED_RUN_NODE_ID = make_node_id("s1", "execution:start", EXEC_TIMESTAMP)


async def _seed_session_and_prompt(services: HookStateService) -> None:
    """Seed a session and a prompt:submit so cursors are populated."""
    await _seed_session(services)
    handler = OrchestratorRunHandler(services)
    await handler(
        "prompt:submit",
        {"session_id": "s1", "timestamp": TIMESTAMP, "prompt": "Hello world"},
    )


# ── execution:start happy-path tests ─────────────────────────────────────────


class TestExecutionStartHappyPath:
    async def test_creates_node(self, services: HookStateService) -> None:
        await _seed_session_and_prompt(services)
        handler = OrchestratorRunHandler(services)
        await handler(
            "execution:start",
            {"session_id": "s1", "timestamp": EXEC_TIMESTAMP},
        )
        node = await services.graph.get_node(EXPECTED_RUN_NODE_ID)
        assert node is not None

    async def test_correct_labels(self, services: HookStateService) -> None:
        await _seed_session_and_prompt(services)
        handler = OrchestratorRunHandler(services)
        await handler(
            "execution:start",
            {"session_id": "s1", "timestamp": EXEC_TIMESTAMP},
        )
        node = await services.graph.get_node(EXPECTED_RUN_NODE_ID)
        assert node is not None
        assert set(node["labels"]) == {"OrchestratorRun"}

    async def test_properties(self, services: HookStateService) -> None:
        await _seed_session_and_prompt(services)
        handler = OrchestratorRunHandler(services)
        await handler(
            "execution:start",
            {"session_id": "s1", "timestamp": EXEC_TIMESTAMP},
        )
        node = await services.graph.get_node(EXPECTED_RUN_NODE_ID)
        assert node is not None
        assert "run_number" not in node  # seq/run_number removed from handler
        assert node["started_at"] == EXEC_TIMESTAMP
        assert node["status"] == "in_progress"
        assert node["prompt_preview"] == "Hello world"
        assert node["session_id"] == "s1"

    async def test_has_run_edge(self, services: HookStateService) -> None:
        await _seed_session_and_prompt(services)
        handler = OrchestratorRunHandler(services)
        await handler(
            "execution:start",
            {"session_id": "s1", "timestamp": EXEC_TIMESTAMP},
        )
        edge = await services.graph.get_edge("s1", EXPECTED_RUN_NODE_ID)
        assert edge is not None
        assert "seq" not in edge  # seq removed from HAS_RUN edge
        assert edge["occurred_at"] == EXEC_TIMESTAMP

    async def test_has_step_edge_to_prompt_step(
        self, services: HookStateService
    ) -> None:
        await _seed_session_and_prompt(services)
        handler = OrchestratorRunHandler(services)
        # Grab current_step_id from cursors (set by prompt:submit)
        cursors = services.get_cursors("s1")
        prompt_step_id = cursors.current_step_id
        assert prompt_step_id is not None

        await handler(
            "execution:start",
            {"session_id": "s1", "timestamp": EXEC_TIMESTAMP},
        )
        edge = await services.graph.get_edge(EXPECTED_RUN_NODE_ID, prompt_step_id)
        assert edge is not None
        assert "seq" not in edge  # seq removed from HAS_STEP edge
        assert edge["occurred_at"] == EXEC_TIMESTAMP

    async def test_updates_current_run_id(self, services: HookStateService) -> None:
        await _seed_session_and_prompt(services)
        handler = OrchestratorRunHandler(services)
        await handler(
            "execution:start",
            {"session_id": "s1", "timestamp": EXEC_TIMESTAMP},
        )
        cursors = services.get_cursors("s1")
        assert cursors.current_run_id == EXPECTED_RUN_NODE_ID

    async def test_returns_continue(self, services: HookStateService) -> None:
        await _seed_session_and_prompt(services)
        handler = OrchestratorRunHandler(services)
        result = await handler(
            "execution:start",
            {"session_id": "s1", "timestamp": EXEC_TIMESTAMP},
        )
        assert result.action == "continue"


# ── execution:start error-path tests ─────────────────────────────────────────


class TestExecutionStartErrorPaths:
    async def test_missing_session_id_returns_continue(
        self, services: HookStateService
    ) -> None:
        handler = OrchestratorRunHandler(services)
        result = await handler(
            "execution:start",
            {"timestamp": EXEC_TIMESTAMP},
        )
        assert result.action == "continue"


# ── execution:end constants ───────────────────────────────────────────────────

END_TIMESTAMP = "2026-03-06T03:00:00Z"


async def _seed_full_run(services: HookStateService) -> str:
    """Seed session + prompt + execution:start, return run node ID."""
    await _seed_session_and_prompt(services)
    handler = OrchestratorRunHandler(services)
    await handler(
        "execution:start",
        {"session_id": "s1", "timestamp": EXEC_TIMESTAMP},
    )
    return EXPECTED_RUN_NODE_ID


# ── execution:end tests ───────────────────────────────────────────────────────


class TestExecutionEnd:
    async def test_enriches_with_timestamp(self, services: HookStateService) -> None:
        run_id = await _seed_full_run(services)
        handler = OrchestratorRunHandler(services)
        await handler(
            "execution:end",
            {"session_id": "s1", "timestamp": END_TIMESTAMP},
        )
        node = await services.graph.get_node(run_id)
        assert node is not None
        assert node["execution_ended_at"] == END_TIMESTAMP

    async def test_preserves_existing_status(self, services: HookStateService) -> None:
        run_id = await _seed_full_run(services)
        handler = OrchestratorRunHandler(services)
        await handler(
            "execution:end",
            {"session_id": "s1", "timestamp": END_TIMESTAMP},
        )
        node = await services.graph.get_node(run_id)
        assert node is not None
        # Status should still be "in_progress" from execution:start — NOT changed
        assert node["status"] == "in_progress"

    async def test_stores_response_preview(self, services: HookStateService) -> None:
        run_id = await _seed_full_run(services)
        handler = OrchestratorRunHandler(services)
        long_response = "y" * 300
        await handler(
            "execution:end",
            {
                "session_id": "s1",
                "timestamp": END_TIMESTAMP,
                "response": long_response,
            },
        )
        node = await services.graph.get_node(run_id)
        assert node is not None
        assert node["response_preview"] == "y" * PREVIEW_MAX_LEN

    async def test_graceful_when_no_current_run(
        self, services: HookStateService
    ) -> None:
        await _seed_session(services)
        handler = OrchestratorRunHandler(services)
        # No execution:start fired, so current_run_id is None
        result = await handler(
            "execution:end",
            {"session_id": "s1", "timestamp": END_TIMESTAMP},
        )
        assert result.action == "continue"

    async def test_missing_session_id(self, services: HookStateService) -> None:
        handler = OrchestratorRunHandler(services)
        result = await handler(
            "execution:end",
            {"timestamp": END_TIMESTAMP},
        )
        assert result.action == "continue"


# ── orchestrator:complete constants ──────────────────────────────────────────

COMPLETE_TIMESTAMP = "2026-03-06T04:00:00Z"


# ── orchestrator:complete tests ───────────────────────────────────────────────


class TestOrchestratorComplete:
    async def test_closes_with_complete_status(
        self, services: HookStateService
    ) -> None:
        run_id = await _seed_full_run(services)
        handler = OrchestratorRunHandler(services)
        await handler(
            "orchestrator:complete",
            {"session_id": "s1", "timestamp": COMPLETE_TIMESTAMP, "status": "success"},
        )
        node = await services.graph.get_node(run_id)
        assert node is not None
        assert node["status"] == "complete"
        assert node["ended_at"] == COMPLETE_TIMESTAMP

    async def test_maps_cancelled_status(self, services: HookStateService) -> None:
        run_id = await _seed_full_run(services)
        handler = OrchestratorRunHandler(services)
        await handler(
            "orchestrator:complete",
            {
                "session_id": "s1",
                "timestamp": COMPLETE_TIMESTAMP,
                "status": "cancelled",
            },
        )
        node = await services.graph.get_node(run_id)
        assert node is not None
        assert node["status"] == "cancelled"

    async def test_maps_error_status(self, services: HookStateService) -> None:
        run_id = await _seed_full_run(services)
        handler = OrchestratorRunHandler(services)
        await handler(
            "orchestrator:complete",
            {"session_id": "s1", "timestamp": COMPLETE_TIMESTAMP, "status": "error"},
        )
        node = await services.graph.get_node(run_id)
        assert node is not None
        assert node["status"] == "error"

    async def test_stores_turn_count(self, services: HookStateService) -> None:
        run_id = await _seed_full_run(services)
        handler = OrchestratorRunHandler(services)
        await handler(
            "orchestrator:complete",
            {
                "session_id": "s1",
                "timestamp": COMPLETE_TIMESTAMP,
                "status": "success",
                "turn_count": 7,
            },
        )
        node = await services.graph.get_node(run_id)
        assert node is not None
        assert node["turn_count"] == 7

    async def test_clears_current_run_id(self, services: HookStateService) -> None:
        await _seed_full_run(services)
        handler = OrchestratorRunHandler(services)
        cursors = services.get_cursors("s1")
        assert cursors.current_run_id is not None  # sanity check

        await handler(
            "orchestrator:complete",
            {"session_id": "s1", "timestamp": COMPLETE_TIMESTAMP, "status": "success"},
        )
        assert cursors.current_run_id is None

    async def test_clears_tool_call_map(self, services: HookStateService) -> None:
        await _seed_full_run(services)
        handler = OrchestratorRunHandler(services)
        cursors = services.get_cursors("s1")
        cursors.tool_call_map["call_1"] = "tool_node_1"  # seed some data

        await handler(
            "orchestrator:complete",
            {"session_id": "s1", "timestamp": COMPLETE_TIMESTAMP, "status": "success"},
        )
        assert cursors.tool_call_map == {}

    async def test_graceful_when_no_current_run(
        self, services: HookStateService
    ) -> None:
        await _seed_session(services)
        handler = OrchestratorRunHandler(services)
        # No execution:start fired, so current_run_id is None
        result = await handler(
            "orchestrator:complete",
            {"session_id": "s1", "timestamp": COMPLETE_TIMESTAMP, "status": "success"},
        )
        assert result.action == "continue"

    async def test_missing_session_id(self, services: HookStateService) -> None:
        handler = OrchestratorRunHandler(services)
        result = await handler(
            "orchestrator:complete",
            {"timestamp": COMPLETE_TIMESTAMP, "status": "success"},
        )
        assert result.action == "continue"

    async def test_unknown_status_passes_through(
        self, services: HookStateService
    ) -> None:
        run_id = await _seed_full_run(services)
        handler = OrchestratorRunHandler(services)
        await handler(
            "orchestrator:complete",
            {"session_id": "s1", "timestamp": COMPLETE_TIMESTAMP, "status": "timeout"},
        )
        node = await services.graph.get_node(run_id)
        assert node is not None
        # Unknown status not in _STATUS_MAP passes through unchanged
        assert node["status"] == "timeout"

    async def test_defaults_to_success_when_status_missing(
        self, services: HookStateService
    ) -> None:
        run_id = await _seed_full_run(services)
        handler = OrchestratorRunHandler(services)
        await handler(
            "orchestrator:complete",
            {"session_id": "s1", "timestamp": COMPLETE_TIMESTAMP},
        )
        node = await services.graph.get_node(run_id)
        assert node is not None
        # No status in event data → defaults to 'success' → maps to 'complete'
        assert node["status"] == "complete"


# ── data property tests ───────────────────────────────────────────────────────


class TestPromptSubmitDataProperty:
    async def test_stores_data_property(self, services: HookStateService) -> None:
        """prompt:submit stores full event dict as 'data' JSON property."""
        await _seed_session(services)
        handler = OrchestratorRunHandler(services)
        event_data = {
            "session_id": "s1",
            "timestamp": TIMESTAMP,
            "prompt": "Hello world",
        }
        await handler("prompt:submit", event_data)
        node = await services.graph.get_node(EXPECTED_NODE_ID)
        assert node is not None
        stored_data = json.loads(node["data"])
        assert stored_data["session_id"] == "s1"
        assert stored_data["prompt"] == "Hello world"

    async def test_data_is_complete_event_clone(
        self, services: HookStateService
    ) -> None:
        """data property preserves extra fields not otherwise used by the handler."""
        await _seed_session(services)
        handler = OrchestratorRunHandler(services)
        event_data = {
            "session_id": "s1",
            "timestamp": TIMESTAMP,
            "prompt": "Hello",
            "extra_field": "preserved",
        }
        await handler("prompt:submit", event_data)
        node = await services.graph.get_node(EXPECTED_NODE_ID)
        assert node is not None
        stored_data = json.loads(node["data"])
        assert stored_data["extra_field"] == "preserved"


class TestExecutionStartDataProperty:
    async def test_stores_data_property(self, services: HookStateService) -> None:
        """execution:start stores full event dict as 'data' JSON property."""
        await _seed_session_and_prompt(services)
        handler = OrchestratorRunHandler(services)
        event_data = {"session_id": "s1", "timestamp": EXEC_TIMESTAMP}
        await handler("execution:start", event_data)
        node = await services.graph.get_node(EXPECTED_RUN_NODE_ID)
        assert node is not None
        stored_data = json.loads(node["data"])
        assert stored_data["session_id"] == "s1"
        assert stored_data["timestamp"] == EXEC_TIMESTAMP


class TestExecutionEndDataProperty:
    async def test_stores_data_execution_end_property(
        self, services: HookStateService
    ) -> None:
        """execution:end enriches OrchestratorRun with 'data_execution_end' JSON property."""
        run_id = await _seed_full_run(services)
        handler = OrchestratorRunHandler(services)
        event_data = {
            "session_id": "s1",
            "timestamp": END_TIMESTAMP,
            "response": "some response",
        }
        await handler("execution:end", event_data)
        node = await services.graph.get_node(run_id)
        assert node is not None
        stored_data = json.loads(node["data_execution_end"])
        assert stored_data["session_id"] == "s1"
        assert stored_data["timestamp"] == END_TIMESTAMP
        assert stored_data["response"] == "some response"


class TestOrchestratorCompleteDataProperty:
    async def test_stores_data_and_calls_flush(
        self, services: HookStateService
    ) -> None:
        """orchestrator:complete enriches with 'data_orchestrator_complete' and calls flush()."""
        run_id = await _seed_full_run(services)
        handler = OrchestratorRunHandler(services)
        event_data = {
            "session_id": "s1",
            "timestamp": COMPLETE_TIMESTAMP,
            "status": "success",
        }

        # Track flush calls via a simple counter wrapper
        flush_call_count = 0
        original_flush = services.graph.flush

        async def tracking_flush() -> None:
            nonlocal flush_call_count
            flush_call_count += 1
            await original_flush()

        services.graph.flush = tracking_flush  # type: ignore[method-assign]

        await handler("orchestrator:complete", event_data)

        node = await services.graph.get_node(run_id)
        assert node is not None
        stored_data = json.loads(node["data_orchestrator_complete"])
        assert stored_data["session_id"] == "s1"
        assert stored_data["status"] == "success"
        assert flush_call_count == 1, (
            "flush() must be called exactly once on orchestrator:complete"
        )


# ── run_tokens reset / flush helpers ─────────────────────────────────────────

SESSION_TS = "2026-03-06T00:00:00Z"
PROMPT_TS = "2026-03-06T01:00:00Z"
EXEC_TS = "2026-03-06T02:00:00Z"


async def _seed_run_with_steps(
    services: HookStateService,
    *,
    session_id: str = "s1",
    n_steps: int = 1,
    input_tokens_per_step: int = 100,
    output_tokens_per_step: int = 50,
    model_per_step: str = "claude-3-5-sonnet",
) -> str:
    """Fire session:start → prompt:submit → execution:start → (provider:request →
    llm:request → llm:response) × n_steps.

    Returns the OrchestratorRun node ID.
    """
    session_handler = SessionHandler(services)
    await session_handler(
        "session:start",
        {"session_id": session_id, "timestamp": SESSION_TS},
    )

    run_handler = OrchestratorRunHandler(services)
    await run_handler(
        "prompt:submit",
        {"session_id": session_id, "timestamp": PROMPT_TS, "prompt": "Hello"},
    )
    await run_handler(
        "execution:start",
        {"session_id": session_id, "timestamp": EXEC_TS},
    )

    step_handler = StepHandler(services)
    for i in range(n_steps):
        step_ts = f"2026-03-06T03:{i:02d}:00Z"
        response_ts = f"2026-03-06T03:{i:02d}:30Z"
        await step_handler(
            "provider:request",
            {"session_id": session_id, "timestamp": step_ts, "provider": "anthropic"},
        )
        await step_handler(
            "llm:request",
            {
                "session_id": session_id,
                "timestamp": step_ts,
                "model": model_per_step,
            },
        )
        await step_handler(
            "llm:response",
            {
                "session_id": session_id,
                "timestamp": response_ts,
                "usage": {
                    "input_tokens": input_tokens_per_step,
                    "output_tokens": output_tokens_per_step,
                },
            },
        )

    return make_node_id(session_id, "execution:start", EXEC_TS)


# ── TestRunTokensResetAtExecutionStart ────────────────────────────────────────


class TestRunTokensResetAtExecutionStart:
    async def test_stale_tokens_cleared_on_execution_start(
        self, services: HookStateService
    ) -> None:
        """Stale token data from a previous run is wiped at execution:start."""
        await _seed_run_with_steps(services, input_tokens_per_step=200)

        # After the first run we have accumulated tokens in run_tokens
        cursors = services.get_cursors("s1")
        assert cursors.run_tokens.input_tokens == 200  # sanity: tokens are there

        # Fire a second execution:start (simulate new run prompt cycle)
        run_handler = OrchestratorRunHandler(services)
        await run_handler(
            "prompt:submit",
            {
                "session_id": "s1",
                "timestamp": "2026-03-06T05:00:00Z",
                "prompt": "Again",
            },
        )
        await run_handler(
            "execution:start",
            {"session_id": "s1", "timestamp": "2026-03-06T06:00:00Z"},
        )

        cursors = services.get_cursors("s1")
        assert cursors.run_tokens.input_tokens == 0, (
            "run_tokens.input_tokens must be reset to 0 on execution:start"
        )
        assert cursors.run_tokens.output_tokens == 0
        assert cursors.run_tokens.models_used == set()

    async def test_run_tokens_is_fresh_instance(
        self, services: HookStateService
    ) -> None:
        """execution:start replaces run_tokens with a brand-new RunTokens() object."""
        await _seed_run_with_steps(services)

        cursors = services.get_cursors("s1")
        old_run_tokens = cursors.run_tokens  # capture reference

        run_handler = OrchestratorRunHandler(services)
        await run_handler(
            "prompt:submit",
            {
                "session_id": "s1",
                "timestamp": "2026-03-06T05:00:00Z",
                "prompt": "Again",
            },
        )
        await run_handler(
            "execution:start",
            {"session_id": "s1", "timestamp": "2026-03-06T06:00:00Z"},
        )

        cursors = services.get_cursors("s1")
        assert cursors.run_tokens is not old_run_tokens, (
            "run_tokens must be a new instance after execution:start (atomic replace)"
        )
        assert isinstance(cursors.run_tokens, RunTokens)


# ── TestTokenFlushAtOrchestratorComplete ─────────────────────────────────────


class TestTokenFlushAtOrchestratorComplete:
    async def test_total_input_tokens_written(self, services: HookStateService) -> None:
        """total_input_tokens is written to the OrchestratorRun node."""
        run_id = await _seed_run_with_steps(services, input_tokens_per_step=123)
        handler = OrchestratorRunHandler(services)
        await handler(
            "orchestrator:complete",
            {"session_id": "s1", "timestamp": COMPLETE_TIMESTAMP, "status": "success"},
        )
        node = await services.graph.get_node(run_id)
        assert node is not None
        assert node["total_input_tokens"] == 123

    async def test_total_output_tokens_written(
        self, services: HookStateService
    ) -> None:
        """total_output_tokens is written to the OrchestratorRun node."""
        run_id = await _seed_run_with_steps(services, output_tokens_per_step=77)
        handler = OrchestratorRunHandler(services)
        await handler(
            "orchestrator:complete",
            {"session_id": "s1", "timestamp": COMPLETE_TIMESTAMP, "status": "success"},
        )
        node = await services.graph.get_node(run_id)
        assert node is not None
        assert node["total_output_tokens"] == 77

    async def test_multi_step_sums_tokens(self, services: HookStateService) -> None:
        """Token totals are the sum over all steps in the run."""
        run_id = await _seed_run_with_steps(
            services,
            n_steps=3,
            input_tokens_per_step=100,
            output_tokens_per_step=50,
        )
        handler = OrchestratorRunHandler(services)
        await handler(
            "orchestrator:complete",
            {"session_id": "s1", "timestamp": COMPLETE_TIMESTAMP, "status": "success"},
        )
        node = await services.graph.get_node(run_id)
        assert node is not None
        assert node["total_input_tokens"] == 300  # 3 × 100
        assert node["total_output_tokens"] == 150  # 3 × 50

    async def test_models_used_written_as_sorted_list(
        self, services: HookStateService
    ) -> None:
        """models_used is serialized as a sorted list (set → sorted list for Neo4j)."""
        # Use two different models across two steps
        session_id = "s1"
        session_handler = SessionHandler(services)
        await session_handler(
            "session:start", {"session_id": session_id, "timestamp": SESSION_TS}
        )
        run_handler = OrchestratorRunHandler(services)
        await run_handler(
            "prompt:submit",
            {"session_id": session_id, "timestamp": PROMPT_TS, "prompt": "Hi"},
        )
        await run_handler(
            "execution:start",
            {"session_id": session_id, "timestamp": EXEC_TS},
        )
        run_id = make_node_id(session_id, "execution:start", EXEC_TS)

        step_handler = StepHandler(services)
        for model in ["claude-opus-4", "claude-sonnet-4"]:
            step_ts = "2026-03-06T03:00:00Z"
            await step_handler(
                "provider:request",
                {
                    "session_id": session_id,
                    "timestamp": step_ts,
                    "provider": "anthropic",
                },
            )
            await step_handler(
                "llm:request",
                {"session_id": session_id, "timestamp": step_ts, "model": model},
            )
            await step_handler(
                "llm:response",
                {
                    "session_id": session_id,
                    "timestamp": step_ts,
                    "usage": {"input_tokens": 10, "output_tokens": 5},
                },
            )

        await run_handler(
            "orchestrator:complete",
            {
                "session_id": session_id,
                "timestamp": COMPLETE_TIMESTAMP,
                "status": "success",
            },
        )
        node = await services.graph.get_node(run_id)
        assert node is not None
        models = node["models_used"]
        assert isinstance(models, list), "models_used must be a list, not a set"
        assert models == sorted(models), "models_used must be sorted"
        assert set(models) == {"claude-opus-4", "claude-sonnet-4"}

    async def test_zero_tokens_still_written(self, services: HookStateService) -> None:
        """Zero-token runs still have the token fields present (value 0, empty list)."""
        # Seed a run with NO steps (no llm:response events)
        session_handler = SessionHandler(services)
        await session_handler(
            "session:start", {"session_id": "s1", "timestamp": SESSION_TS}
        )
        run_handler = OrchestratorRunHandler(services)
        await run_handler(
            "prompt:submit",
            {"session_id": "s1", "timestamp": PROMPT_TS, "prompt": "Hi"},
        )
        await run_handler(
            "execution:start",
            {"session_id": "s1", "timestamp": EXEC_TS},
        )
        run_id = make_node_id("s1", "execution:start", EXEC_TS)

        await run_handler(
            "orchestrator:complete",
            {"session_id": "s1", "timestamp": COMPLETE_TIMESTAMP, "status": "success"},
        )
        node = await services.graph.get_node(run_id)
        assert node is not None
        assert "total_input_tokens" in node
        assert "total_output_tokens" in node
        assert "cached_tokens" in node
        assert "reasoning_tokens" in node
        assert "models_used" in node
        assert node["total_input_tokens"] == 0
        assert node["total_output_tokens"] == 0
        assert node["cached_tokens"] == 0
        assert node["reasoning_tokens"] == 0
        assert node["models_used"] == []
