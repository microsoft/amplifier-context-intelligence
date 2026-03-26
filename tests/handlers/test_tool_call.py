"""Tests for ToolCallHandler — Phase 2 stub for tool:pre/post/error events.

TDD RED phase: these tests must fail before tool_call.py is created (conftest.py
injects a stub with different handled_events). They pass after the real module is
created and the conftest.py stub injection is removed.
"""

from __future__ import annotations

import pytest

from context_intelligence_server.services import HookStateService


class TestToolCallHandlerClass:
    """ToolCallHandler class structure requirements."""

    def test_tool_call_handler_importable(self) -> None:
        """ToolCallHandler can be imported from context_intelligence_server.handlers.tool_call."""
        from context_intelligence_server.handlers.tool_call import ToolCallHandler

        assert ToolCallHandler is not None

    def test_handled_events_is_frozenset(self) -> None:
        """handled_events must be a frozenset."""
        from context_intelligence_server.handlers.tool_call import ToolCallHandler

        assert isinstance(ToolCallHandler.handled_events, frozenset)

    def test_handled_events_contains_tool_pre(self) -> None:
        """handled_events must contain 'tool:pre'."""
        from context_intelligence_server.handlers.tool_call import ToolCallHandler

        assert "tool:pre" in ToolCallHandler.handled_events

    def test_handled_events_contains_tool_post(self) -> None:
        """handled_events must contain 'tool:post'."""
        from context_intelligence_server.handlers.tool_call import ToolCallHandler

        assert "tool:post" in ToolCallHandler.handled_events

    def test_handled_events_contains_tool_error(self) -> None:
        """handled_events must contain 'tool:error'."""
        from context_intelligence_server.handlers.tool_call import ToolCallHandler

        assert "tool:error" in ToolCallHandler.handled_events

    def test_handled_events_exact_set(self) -> None:
        """handled_events must be exactly frozenset({'tool:pre', 'tool:post', 'tool:error'})."""
        from context_intelligence_server.handlers.tool_call import ToolCallHandler

        assert ToolCallHandler.handled_events == frozenset(
            {"tool:pre", "tool:post", "tool:error"}
        )


class TestToolCallHandlerInit:
    """ToolCallHandler.__init__ stores services."""

    def test_init_stores_services(self, services: HookStateService) -> None:
        """__init__ must store services as self.services."""
        from context_intelligence_server.handlers.tool_call import ToolCallHandler

        handler = ToolCallHandler(services)
        assert handler.services is services

    def test_init_accepts_hook_state_service(self, services: HookStateService) -> None:
        """__init__ must accept a HookStateService instance without error."""
        from context_intelligence_server.handlers.tool_call import ToolCallHandler

        handler = ToolCallHandler(services)
        assert handler is not None


class TestToolCallHandlerCall:
    """ToolCallHandler.__call__ returns HookResult(action='continue')."""

    @pytest.mark.anyio
    async def test_call_returns_hook_result_action_continue(
        self, services: HookStateService
    ) -> None:
        """__call__ must return HookResult with action='continue'."""
        from context_intelligence_server.handlers.tool_call import ToolCallHandler

        handler = ToolCallHandler(services)
        result = await handler("tool:pre", {"session_id": "s1"})
        assert result.action == "continue"

    @pytest.mark.anyio
    async def test_call_returns_continue_for_tool_post(
        self, services: HookStateService
    ) -> None:
        """__call__ must return HookResult(action='continue') for tool:post."""
        from context_intelligence_server.handlers.tool_call import ToolCallHandler

        handler = ToolCallHandler(services)
        result = await handler("tool:post", {"session_id": "s1"})
        assert result.action == "continue"

    @pytest.mark.anyio
    async def test_call_returns_continue_for_tool_error(
        self, services: HookStateService
    ) -> None:
        """__call__ must return HookResult(action='continue') for tool:error."""
        from context_intelligence_server.handlers.tool_call import ToolCallHandler

        handler = ToolCallHandler(services)
        result = await handler("tool:error", {"session_id": "s1", "error": "timeout"})
        assert result.action == "continue"


class TestPipelineIntegration:
    """Full import chain: setup_handlers returns ToolCallHandler as enricher."""

    def test_setup_handlers_includes_tool_call_handler(self) -> None:
        """setup_handlers must include ToolCallHandler with correct type name."""
        from context_intelligence_server.pipeline import setup_handlers

        h = setup_handlers(HookStateService())
        assert type(h.enrichers[1]).__name__ == "ToolCallHandler"

    def test_tool_call_handler_enricher_has_correct_events(self) -> None:
        """ToolCallHandler enricher in pipeline must have tool:pre/post/error events."""
        from context_intelligence_server.pipeline import setup_handlers

        h = setup_handlers(HookStateService())
        tool_handler = h.enrichers[1]
        assert tool_handler.handled_events == frozenset(
            {"tool:pre", "tool:post", "tool:error"}
        )
