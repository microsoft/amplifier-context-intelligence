"""TDD RED test: verify data_layer_2 import paths for SessionHandler and ToolCallHandler.

These tests verify that the source files are accessible from their new
location under handlers/data_layer_2/ after the move.
"""

from __future__ import annotations


class TestDataLayer2Imports:
    """Source files must be importable from handlers/data_layer_2/."""

    def test_session_handler_importable_from_data_layer_2(self) -> None:
        """SessionHandler must be importable from data_layer_2 subpackage."""
        from context_intelligence_server.handlers.data_layer_2.session import (  # noqa: PLC0415
            SessionHandler,
        )

        assert SessionHandler is not None

    def test_tool_call_handler_importable_from_data_layer_2(self) -> None:
        """ToolCallHandler must be importable from data_layer_2 subpackage."""
        from context_intelligence_server.handlers.data_layer_2.tool_call import (  # noqa: PLC0415
            ToolCallHandler,
        )

        assert ToolCallHandler is not None

    def test_pipeline_imports_from_data_layer_2(self) -> None:
        """pipeline.py must import handlers from data_layer_2 (not top-level handlers)."""
        import inspect

        from context_intelligence_server import pipeline

        source = inspect.getsource(pipeline)
        assert "handlers.data_layer_2.session" in source, (
            "pipeline.py must import SessionHandler from handlers.data_layer_2.session"
        )
        assert "handlers.data_layer_2.tool_call" in source, (
            "pipeline.py must import ToolCallHandler from handlers.data_layer_2.tool_call"
        )

    def test_handlers_init_re_exports_from_data_layer_2(self) -> None:
        """handlers/__init__.py must re-export from data_layer_2 subpackage."""
        import inspect

        from context_intelligence_server import handlers

        source = inspect.getsource(handlers)
        assert "handlers.data_layer_2.session" in source, (
            "handlers/__init__.py must re-export SessionHandler from data_layer_2"
        )
        assert "handlers.data_layer_2.tool_call" in source, (
            "handlers/__init__.py must re-export ToolCallHandler from data_layer_2"
        )
