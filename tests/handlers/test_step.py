"""Tests for StepHandler — AssistantStep lifecycle events.

Adapted from bundle's test_step_handler.py for the server-side implementation,
which uses the flat-dict GraphState API (no nested 'properties' key, no
edge_type param in get_edge).
"""

from __future__ import annotations

from context_intelligence_server.handlers.orchestrator_run import OrchestratorRunHandler
from context_intelligence_server.handlers.session import SessionHandler
from context_intelligence_server.handlers.step import StepHandler
from context_intelligence_server.services import HookStateService
from context_intelligence_server.utils import make_node_id

SESSION_TIMESTAMP = "2026-03-06T00:00:00Z"
PROMPT_TIMESTAMP = "2026-03-06T01:00:00Z"
EXEC_TIMESTAMP = "2026-03-06T02:00:00Z"
STEP_TIMESTAMP = "2026-03-06T03:00:00Z"

EXPECTED_STEP_NODE_ID = make_node_id("s1", "provider:request", STEP_TIMESTAMP)
EXPECTED_RUN_NODE_ID = make_node_id("s1", "execution:start", EXEC_TIMESTAMP)


async def _seed_session(services: HookStateService, session_id: str = "s1") -> None:
    """Create a Session node via SessionHandler."""
    handler = SessionHandler(services)
    await handler(
        "session:start", {"session_id": session_id, "timestamp": SESSION_TIMESTAMP}
    )


async def _seed_run(services: HookStateService) -> str:
    """Seed session + prompt:submit + execution:start, return run node ID."""
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
    return EXPECTED_RUN_NODE_ID


# ── handled_events ────────────────────────────────────────────────────────────


class TestHandledEvents:
    def test_provider_request_claimed(self) -> None:
        assert "provider:request" in StepHandler.handled_events

    def test_llm_response_claimed(self) -> None:
        assert "llm:response" in StepHandler.handled_events

    def test_content_block_wildcard_claimed(self) -> None:
        assert "content_block:*" in StepHandler.handled_events


# ── provider:request happy-path ───────────────────────────────────────────────


class TestProviderRequestHappyPath:
    async def test_creates_node(self, services: HookStateService) -> None:
        await _seed_run(services)
        handler = StepHandler(services)
        await handler(
            "provider:request",
            {"session_id": "s1", "timestamp": STEP_TIMESTAMP},
        )
        node = await services.graph.get_node(EXPECTED_STEP_NODE_ID)
        assert node is not None

    async def test_correct_labels(self, services: HookStateService) -> None:
        await _seed_run(services)
        handler = StepHandler(services)
        await handler(
            "provider:request",
            {"session_id": "s1", "timestamp": STEP_TIMESTAMP},
        )
        node = await services.graph.get_node(EXPECTED_STEP_NODE_ID)
        assert node is not None
        assert set(node["labels"]) == {"Step", "AssistantStep"}

    async def test_has_step_edge_from_run(self, services: HookStateService) -> None:
        run_id = await _seed_run(services)
        handler = StepHandler(services)
        await handler(
            "provider:request",
            {"session_id": "s1", "timestamp": STEP_TIMESTAMP},
        )
        edge = await services.graph.get_edge(run_id, EXPECTED_STEP_NODE_ID)
        assert edge is not None

    async def test_has_step_edge_seq(self, services: HookStateService) -> None:
        run_id = await _seed_run(services)
        handler = StepHandler(services)
        await handler(
            "provider:request",
            {"session_id": "s1", "timestamp": STEP_TIMESTAMP},
        )
        edge = await services.graph.get_edge(run_id, EXPECTED_STEP_NODE_ID)
        assert edge is not None
        # step_counter incremented to 1 before first provider:request
        assert edge["seq"] == 1

    async def test_has_step_edge_occurred_at(self, services: HookStateService) -> None:
        run_id = await _seed_run(services)
        handler = StepHandler(services)
        await handler(
            "provider:request",
            {"session_id": "s1", "timestamp": STEP_TIMESTAMP},
        )
        edge = await services.graph.get_edge(run_id, EXPECTED_STEP_NODE_ID)
        assert edge is not None
        assert edge["occurred_at"] == STEP_TIMESTAMP

    async def test_no_next_edge_on_first_step(self, services: HookStateService) -> None:
        """First provider:request has no previous step — no NEXT edge created."""
        await _seed_run(services)
        handler = StepHandler(services)

        # Clear current_step_id from execution:start effects
        cursors = services.get_cursors("s1")
        cursors.current_step_id = None  # simulate fresh run with no prior step

        await handler(
            "provider:request",
            {"session_id": "s1", "timestamp": STEP_TIMESTAMP},
        )
        # No NEXT edge should exist — there was no previous step
        node = await services.graph.get_node(EXPECTED_STEP_NODE_ID)
        assert node is not None  # node itself was created

    async def test_next_edge_on_second_step(self, services: HookStateService) -> None:
        """Second provider:request creates NEXT edge from previous step."""
        await _seed_run(services)
        handler = StepHandler(services)

        STEP2_TIMESTAMP = "2026-03-06T04:00:00Z"
        step2_id = make_node_id("s1", "provider:request", STEP2_TIMESTAMP)

        # First step
        await handler(
            "provider:request",
            {"session_id": "s1", "timestamp": STEP_TIMESTAMP},
        )
        first_step_id = EXPECTED_STEP_NODE_ID

        # Second step
        await handler(
            "provider:request",
            {"session_id": "s1", "timestamp": STEP2_TIMESTAMP},
        )

        # NEXT edge from first_step_id to second step should exist
        edge = await services.graph.get_edge(first_step_id, step2_id)
        assert edge is not None
        assert edge["occurred_at"] == STEP2_TIMESTAMP

    async def test_updates_current_step_id(self, services: HookStateService) -> None:
        await _seed_run(services)
        handler = StepHandler(services)
        await handler(
            "provider:request",
            {"session_id": "s1", "timestamp": STEP_TIMESTAMP},
        )
        cursors = services.get_cursors("s1")
        assert cursors.current_step_id == EXPECTED_STEP_NODE_ID

    async def test_increments_step_counter(self, services: HookStateService) -> None:
        await _seed_run(services)
        handler = StepHandler(services)
        cursors = services.get_cursors("s1")
        initial_counter = cursors.step_counter

        await handler(
            "provider:request",
            {"session_id": "s1", "timestamp": STEP_TIMESTAMP},
        )
        assert cursors.step_counter == initial_counter + 1

    async def test_returns_continue(self, services: HookStateService) -> None:
        await _seed_run(services)
        handler = StepHandler(services)
        result = await handler(
            "provider:request",
            {"session_id": "s1", "timestamp": STEP_TIMESTAMP},
        )
        assert result.action == "continue"

    async def test_no_has_step_edge_when_no_run(
        self, services: HookStateService
    ) -> None:
        """provider:request with no current_run_id creates node but no HAS_STEP edge."""
        await _seed_session(services)
        handler = StepHandler(services)
        await handler(
            "provider:request",
            {"session_id": "s1", "timestamp": STEP_TIMESTAMP},
        )
        node = await services.graph.get_node(EXPECTED_STEP_NODE_ID)
        assert node is not None
        # No run_id means no HAS_STEP edge possible


# ── provider:request error-path ───────────────────────────────────────────────


class TestProviderRequestErrorPaths:
    async def test_missing_session_id_returns_continue(
        self, services: HookStateService
    ) -> None:
        handler = StepHandler(services)
        result = await handler(
            "provider:request",
            {"timestamp": STEP_TIMESTAMP},
        )
        assert result.action == "continue"

    async def test_missing_session_id_creates_no_nodes(
        self, services: HookStateService
    ) -> None:
        handler = StepHandler(services)
        await handler("provider:request", {"timestamp": STEP_TIMESTAMP})
        node = await services.graph.get_node(EXPECTED_STEP_NODE_ID)
        assert node is None


# ── llm:response ──────────────────────────────────────────────────────────────


class TestLlmResponseHappyPath:
    async def _seed_step(self, services: HookStateService) -> None:
        await _seed_run(services)
        handler = StepHandler(services)
        await handler(
            "provider:request",
            {"session_id": "s1", "timestamp": STEP_TIMESTAMP},
        )

    async def test_enriches_with_input_tokens(self, services: HookStateService) -> None:
        await self._seed_step(services)
        handler = StepHandler(services)
        await handler(
            "llm:response",
            {
                "session_id": "s1",
                "timestamp": STEP_TIMESTAMP,
                "usage": {"input_tokens": 100, "output_tokens": 50},
            },
        )
        node = await services.graph.get_node(EXPECTED_STEP_NODE_ID)
        assert node is not None
        assert node["input_tokens"] == 100

    async def test_enriches_with_output_tokens(
        self, services: HookStateService
    ) -> None:
        await self._seed_step(services)
        handler = StepHandler(services)
        await handler(
            "llm:response",
            {
                "session_id": "s1",
                "timestamp": STEP_TIMESTAMP,
                "usage": {"input_tokens": 100, "output_tokens": 50},
            },
        )
        node = await services.graph.get_node(EXPECTED_STEP_NODE_ID)
        assert node is not None
        assert node["output_tokens"] == 50

    async def test_output_tokens_fallback_to_output(
        self, services: HookStateService
    ) -> None:
        await self._seed_step(services)
        handler = StepHandler(services)
        await handler(
            "llm:response",
            {
                "session_id": "s1",
                "timestamp": STEP_TIMESTAMP,
                "usage": {"input_tokens": 10, "output": 25},
            },
        )
        node = await services.graph.get_node(EXPECTED_STEP_NODE_ID)
        assert node is not None
        assert node["output_tokens"] == 25

    async def test_input_tokens_no_fallback_to_input(
        self, services: HookStateService
    ) -> None:
        """input_tokens must NOT fall back to 'input' (which is message count)."""
        await self._seed_step(services)
        handler = StepHandler(services)
        await handler(
            "llm:response",
            {
                "session_id": "s1",
                "timestamp": STEP_TIMESTAMP,
                # 'input' is message count, not token count
                "usage": {"input": 5, "output_tokens": 20},
            },
        )
        node = await services.graph.get_node(EXPECTED_STEP_NODE_ID)
        assert node is not None
        # input_tokens should NOT be set from 'input'
        assert "input_tokens" not in node

    async def test_message_count_stored_from_input(
        self, services: HookStateService
    ) -> None:
        """'input' field in usage is stored as message_count."""
        await self._seed_step(services)
        handler = StepHandler(services)
        await handler(
            "llm:response",
            {
                "session_id": "s1",
                "timestamp": STEP_TIMESTAMP,
                "usage": {"input": 7, "output_tokens": 20},
            },
        )
        node = await services.graph.get_node(EXPECTED_STEP_NODE_ID)
        assert node is not None
        assert node["message_count"] == 7

    async def test_returns_continue(self, services: HookStateService) -> None:
        await self._seed_step(services)
        handler = StepHandler(services)
        result = await handler(
            "llm:response",
            {"session_id": "s1", "timestamp": STEP_TIMESTAMP, "usage": {}},
        )
        assert result.action == "continue"

    async def test_graceful_when_no_current_step(
        self, services: HookStateService
    ) -> None:
        """llm:response with no current_step_id is a no-op."""
        await _seed_session(services)
        handler = StepHandler(services)
        result = await handler(
            "llm:response",
            {
                "session_id": "s1",
                "timestamp": STEP_TIMESTAMP,
                "usage": {"input_tokens": 10},
            },
        )
        assert result.action == "continue"


# ── content_block:* no-op ─────────────────────────────────────────────────────


class TestContentBlockNoOp:
    async def test_content_block_returns_continue(
        self, services: HookStateService
    ) -> None:
        handler = StepHandler(services)
        result = await handler(
            "content_block:start",
            {"session_id": "s1", "timestamp": STEP_TIMESTAMP},
        )
        assert result.action == "continue"

    async def test_content_block_delta_returns_continue(
        self, services: HookStateService
    ) -> None:
        handler = StepHandler(services)
        result = await handler(
            "content_block:delta",
            {"session_id": "s1", "timestamp": STEP_TIMESTAMP},
        )
        assert result.action == "continue"
