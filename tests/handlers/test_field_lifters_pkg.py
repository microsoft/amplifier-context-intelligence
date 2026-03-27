"""Tests for field_lifters package __init__.py — verifies all exports.

Task-8: All FieldLifter implementations are importable from the package root.
"""

from __future__ import annotations

import context_intelligence_server.handlers.field_lifters as field_lifters_module


class TestFieldLiftersPkgExports:
    """field_lifters/__init__.py should export all FieldLifter implementations."""

    def test_universal_lifter_importable(self) -> None:
        """UniversalLifter can be imported from field_lifters package."""
        from context_intelligence_server.handlers.field_lifters import UniversalLifter

        assert UniversalLifter is not None

    def test_session_lifter_importable(self) -> None:
        """SessionLifter can be imported from field_lifters package."""
        from context_intelligence_server.handlers.field_lifters import SessionLifter

        assert SessionLifter is not None

    def test_tool_lifter_importable(self) -> None:
        """ToolLifter can be imported from field_lifters package."""
        from context_intelligence_server.handlers.field_lifters import ToolLifter

        assert ToolLifter is not None

    def test_delegate_lifter_importable(self) -> None:
        """DelegateLifter can be imported from field_lifters package."""
        from context_intelligence_server.handlers.field_lifters import DelegateLifter

        assert DelegateLifter is not None

    def test_llm_lifter_importable(self) -> None:
        """LlmLifter can be imported from field_lifters package."""
        from context_intelligence_server.handlers.field_lifters import LlmLifter

        assert LlmLifter is not None

    def test_prompt_lifter_importable(self) -> None:
        """PromptLifter can be imported from field_lifters package."""
        from context_intelligence_server.handlers.field_lifters import PromptLifter

        assert PromptLifter is not None

    def test_field_lifter_base_importable(self) -> None:
        """FieldLifter base class can be imported from field_lifters package."""
        from context_intelligence_server.handlers.field_lifters import FieldLifter

        assert FieldLifter is not None

    def test_reserved_props_importable(self) -> None:
        """RESERVED_PROPS can be imported from field_lifters package."""
        from context_intelligence_server.handlers.field_lifters import RESERVED_PROPS

        assert RESERVED_PROPS is not None

    def test_all_contains_expected_exports(self) -> None:
        """__all__ must contain all 8 expected names."""
        assert set(field_lifters_module.__all__) == {
            "DelegateLifter",
            "FieldLifter",
            "LlmLifter",
            "PromptLifter",
            "RESERVED_PROPS",
            "SessionLifter",
            "ToolLifter",
            "UniversalLifter",
        }
