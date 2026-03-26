"""Tests for handlers/__init__.py — verifies correct exports.

Phase 2 Task 7: DefaultHandler, SessionHandler, and ToolCallHandler are all exported.
"""

from __future__ import annotations

import context_intelligence_server.handlers as handlers_module


class TestHandlersInit:
    """handlers/__init__.py should export DefaultHandler, SessionHandler, and ToolCallHandler."""

    def test_default_handler_importable(self) -> None:
        """DefaultHandler can be imported from context_intelligence_server.handlers."""
        from context_intelligence_server.handlers import DefaultHandler

        assert DefaultHandler is not None

    def test_session_handler_importable(self) -> None:
        """SessionHandler can be imported from context_intelligence_server.handlers."""
        from context_intelligence_server.handlers import SessionHandler

        assert SessionHandler is not None

    def test_tool_call_handler_importable(self) -> None:
        """ToolCallHandler can be imported from context_intelligence_server.handlers."""
        from context_intelligence_server.handlers import ToolCallHandler

        assert ToolCallHandler is not None

    def test_all_contains_all_three_handlers(self) -> None:
        """__all__ must contain exactly DefaultHandler, SessionHandler, and ToolCallHandler."""
        assert set(handlers_module.__all__) == {
            "DefaultHandler",
            "SessionHandler",
            "ToolCallHandler",
        }

    def test_old_handlers_not_exported(self) -> None:
        """Old handlers (OrchestratorRunHandler, RecipeHandler, etc.) are removed."""
        old_handlers = [
            "OrchestratorRunHandler",
            "RecipeHandler",
            "StepHandler",
            "SystemEventHandler",
            "ToolExecutionHandler",
        ]
        for name in old_handlers:
            assert name not in handlers_module.__all__, (
                f"{name} should not be in __all__"
            )
