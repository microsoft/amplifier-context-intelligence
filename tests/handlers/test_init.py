"""Tests for handlers/__init__.py — verifies correct exports.

Phase 1: Only DefaultHandler and SessionHandler are exported.
ToolCallHandler is NOT yet imported (added in Phase 2 Task 6).
"""

from __future__ import annotations

import context_intelligence_server.handlers as handlers_module


class TestHandlersInit:
    """handlers/__init__.py should export exactly DefaultHandler and SessionHandler."""

    def test_default_handler_importable(self) -> None:
        """DefaultHandler can be imported from context_intelligence_server.handlers."""
        from context_intelligence_server.handlers import DefaultHandler

        assert DefaultHandler is not None

    def test_session_handler_importable(self) -> None:
        """SessionHandler can be imported from context_intelligence_server.handlers."""
        from context_intelligence_server.handlers import SessionHandler

        assert SessionHandler is not None

    def test_all_contains_only_default_and_session(self) -> None:
        """__all__ must contain exactly DefaultHandler and SessionHandler."""
        assert set(handlers_module.__all__) == {"DefaultHandler", "SessionHandler"}

    def test_tool_call_handler_not_exported(self) -> None:
        """ToolCallHandler is NOT yet exported (it does not exist until Phase 2 Task 6)."""
        assert "ToolCallHandler" not in handlers_module.__all__

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
