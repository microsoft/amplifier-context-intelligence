"""Tests for handlers/__init__.py — verifies NO re-exports.

Task 12: handlers/__init__.py must be a docstring-only module.
Handlers are imported directly from their sub-package paths.
"""

from __future__ import annotations

import context_intelligence_server.handlers as handlers_module


class TestHandlersInitNoReExports:
    """handlers/__init__.py must NOT export any handler classes."""

    def test_no_default_handler_export(self) -> None:
        """DefaultHandler must NOT be accessible from context_intelligence_server.handlers."""
        assert not hasattr(handlers_module, "DefaultHandler")

    def test_no_session_handler_export(self) -> None:
        """SessionHandler must NOT be accessible from context_intelligence_server.handlers."""
        assert not hasattr(handlers_module, "SessionHandler")

    def test_no_tool_call_handler_export(self) -> None:
        """ToolCallHandler must NOT be accessible from context_intelligence_server.handlers."""
        assert not hasattr(handlers_module, "ToolCallHandler")

    def test_no_all_attribute(self) -> None:
        """handlers/__init__.py must NOT define __all__."""
        assert not hasattr(handlers_module, "__all__")

    def test_direct_layer_imports_work(self) -> None:
        """Direct imports from data_layer_1 and data_layer_2 sub-packages must succeed."""
        from context_intelligence_server.handlers.data_layer_1.default import (
            DefaultHandler,
        )
        from context_intelligence_server.handlers.data_layer_2.session import (
            SessionHandler,
        )
        from context_intelligence_server.handlers.data_layer_2.tool_call import (
            ToolCallHandler,
        )

        assert DefaultHandler is not None
        assert SessionHandler is not None
        assert ToolCallHandler is not None
