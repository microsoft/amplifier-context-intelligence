"""Tests for context_intelligence_server.protocol and context_intelligence_server.utils."""

from __future__ import annotations

import logging
from collections.abc import Set as AbstractSet
from typing import Any
from unittest.mock import MagicMock

import pytest

from context_intelligence_server.protocol import EventHandler, HookResult
from context_intelligence_server.utils import (
    EventLogContext,
    HandlerLogger,
    make_edge_id,
    make_node_id,
)


class TestMakeNodeId:
    def test_basic_iso_timestamp(self) -> None:
        """make_node_id produces correct pattern for a basic UTC timestamp."""
        node_id = make_node_id("sess123", "tool:call", "2024-01-15T10:30:00Z")
        # Colons in event name replaced with underscores
        assert "sess123" in node_id
        assert "tool_call" in node_id
        # Pattern: {session_id}__{safe_event}__{epoch_ms}
        parts = node_id.split("__")
        assert len(parts) == 3
        assert parts[0] == "sess123"
        assert parts[1] == "tool_call"
        assert parts[2].isdigit()

    def test_fractional_seconds(self) -> None:
        """make_node_id handles fractional seconds in ISO timestamps."""
        node_id = make_node_id("sess-abc", "event:fired", "2024-06-01T12:00:00.500Z")
        parts = node_id.split("__")
        assert len(parts) == 3
        # epoch_ms should reflect fractional seconds
        epoch_ms = int(parts[2])
        assert epoch_ms > 0
        # 500ms should be reflected in the epoch_ms
        assert epoch_ms % 1000 == 500

    def test_timezone_offset(self) -> None:
        """make_node_id normalises timezone offsets to UTC epoch ms."""
        # UTC+01:00 means the UTC time is 1 hour earlier
        node_id_utc = make_node_id("s", "e", "2024-01-01T01:00:00+00:00")
        node_id_plus1 = make_node_id("s", "e", "2024-01-01T02:00:00+01:00")
        # Both represent the same moment in time
        parts_utc = node_id_utc.split("__")
        parts_plus1 = node_id_plus1.split("__")
        assert parts_utc[2] == parts_plus1[2]

    def test_determinism(self) -> None:
        """make_node_id is deterministic: same inputs always produce the same output."""
        ts = "2024-03-14T15:09:26.535897Z"
        id1 = make_node_id("session-xyz", "step:start", ts)
        id2 = make_node_id("session-xyz", "step:start", ts)
        assert id1 == id2

    def test_disambiguator_appended(self) -> None:
        """make_node_id appends disambiguator as fourth segment when provided."""
        node_id = make_node_id("s1", "tool:exec", "2024-01-01T00:00:00Z", "call-42")
        parts = node_id.split("__")
        assert len(parts) == 4
        assert parts[3] == "call-42"

    def test_no_disambiguator_three_segments(self) -> None:
        """Without disambiguator, node ID has exactly three segments."""
        node_id = make_node_id("s1", "tool:exec", "2024-01-01T00:00:00Z")
        parts = node_id.split("__")
        assert len(parts) == 3

    def test_colon_replacement_in_event_name(self) -> None:
        """Colons in event name are replaced with underscores."""
        node_id = make_node_id("s", "hook:tool:call", "2024-01-01T00:00:00Z")
        assert "hook_tool_call" in node_id
        assert ":" not in node_id.split("__")[1]


class TestMakeEdgeId:
    def test_construction(self) -> None:
        """make_edge_id builds the correct pattern."""
        edge_id = make_edge_id("node-a", "node-b", "CAUSES")
        assert edge_id == "node-a==[CAUSES]==node-b"

    def test_determinism(self) -> None:
        """make_edge_id is deterministic."""
        e1 = make_edge_id("src", "tgt", "LEADS_TO")
        e2 = make_edge_id("src", "tgt", "LEADS_TO")
        assert e1 == e2

    def test_parseability(self) -> None:
        """Edge ID is unambiguously parseable back into components."""
        source = "sess__tool_call__1700000000000"
        target = "sess__step_end__1700000001000"
        edge_type = "TRIGGERS"
        edge_id = make_edge_id(source, target, edge_type)
        # Should contain the separator pattern
        assert "==[" in edge_id
        assert "]==" in edge_id
        # Parse back
        left, rest = edge_id.split("==[", 1)
        etype, right = rest.split("]==", 1)
        assert left == source
        assert etype == edge_type
        assert right == target

    def test_different_edge_types_produce_different_ids(self) -> None:
        """Different edge types produce different edge IDs."""
        e1 = make_edge_id("a", "b", "TYPE_A")
        e2 = make_edge_id("a", "b", "TYPE_B")
        assert e1 != e2


class TestHandlerLogger:
    def test_with_event_returns_event_log_context(self) -> None:
        """HandlerLogger.with_event returns an EventLogContext instance."""
        logger = logging.getLogger("test")
        handler_logger = HandlerLogger("MyHandler", logger)
        ctx = handler_logger.with_event("tool:call", {"session_id": "sess-1"})
        assert isinstance(ctx, EventLogContext)

    def test_with_event_uses_session_id_from_data(self) -> None:
        """HandlerLogger.with_event extracts session_id from data dict."""
        logger = MagicMock(spec=logging.Logger)
        handler_logger = HandlerLogger("TestHandler", logger)
        ctx = handler_logger.with_event("event:name", {"session_id": "my-session"})
        ctx.info("test message")
        # Verify the prefix includes the session_id
        call_args = logger.info.call_args[0]
        formatted = call_args[0] % call_args[1:]
        assert "my-session" in formatted

    def test_with_event_missing_session_id_defaults_empty(self) -> None:
        """HandlerLogger.with_event uses empty string when session_id missing."""
        logger = logging.getLogger("test")
        handler_logger = HandlerLogger("H", logger)
        # Should not raise even when session_id not in data
        ctx = handler_logger.with_event("e", {})
        assert isinstance(ctx, EventLogContext)


class TestEventLogContext:
    def _make_context(
        self, handler_name: str = "H", session_id: str = "S", event: str = "E"
    ) -> tuple[EventLogContext, MagicMock]:
        mock_logger = MagicMock(spec=logging.Logger)
        ctx = EventLogContext(handler_name, session_id, event, mock_logger)
        return ctx, mock_logger

    def test_info_prefix_format(self) -> None:
        """EventLogContext.info logs with [handler][session][event] prefix."""
        ctx, mock_logger = self._make_context("MyHandler", "sess-42", "tool:call")
        ctx.info("some message")
        mock_logger.info.assert_called_once()
        call_args = mock_logger.info.call_args[0]
        # First arg is format string, second is prefix
        assert "[MyHandler]" in call_args[1]
        assert "[sess-42]" in call_args[1]
        assert "[tool:call]" in call_args[1]

    def test_warning_prefix_format(self) -> None:
        """EventLogContext.warning logs with correct prefix."""
        ctx, mock_logger = self._make_context("WarnHandler", "s", "event:warn")
        ctx.warning("a warning")
        mock_logger.warning.assert_called_once()
        call_args = mock_logger.warning.call_args[0]
        assert "[WarnHandler]" in call_args[1]

    def test_error_prefix_format(self) -> None:
        """EventLogContext.error logs with correct prefix."""
        ctx, mock_logger = self._make_context("ErrHandler", "s", "event:error")
        ctx.error("an error")
        mock_logger.error.assert_called_once()
        call_args = mock_logger.error.call_args[0]
        assert "[ErrHandler]" in call_args[1]

    def test_lazy_formatting_passes_args(self) -> None:
        """EventLogContext methods support lazy formatting with *args."""
        ctx, mock_logger = self._make_context()
        ctx.info("value is %s and %d", "hello", 42)
        call_args = mock_logger.info.call_args[0]
        # The extra args should be passed through for lazy formatting
        assert "hello" in call_args
        assert 42 in call_args

    def test_info_message_in_format_string(self) -> None:
        """The message is concatenated into the format string."""
        ctx, mock_logger = self._make_context()
        ctx.info("my specific message")
        call_args = mock_logger.info.call_args[0]
        format_str = call_args[0]
        assert "my specific message" in format_str


class TestHookResult:
    def test_default_action(self) -> None:
        """HookResult default action is 'continue'."""
        result = HookResult()
        assert result.action == "continue"

    def test_custom_action(self) -> None:
        """HookResult accepts custom action values."""
        result = HookResult(action="deny")
        assert result.action == "deny"

    def test_is_dataclass(self) -> None:
        """HookResult is a dataclass."""
        import dataclasses

        assert dataclasses.is_dataclass(HookResult)


class TestEventHandlerProtocol:
    def test_protocol_is_runtime_checkable(self) -> None:
        """EventHandler protocol supports isinstance checks at runtime."""

        # Should not raise TypeError
        class NotAHandler:
            pass

        # Runtime-checkable protocols allow isinstance checks
        try:
            isinstance(NotAHandler(), EventHandler)
        except TypeError:
            pytest.fail("EventHandler is not runtime_checkable")

    def test_conforming_class_passes_isinstance(self) -> None:
        """A class with all required attributes passes isinstance check."""

        class ConformingHandler:
            handled_events: AbstractSet[str] = frozenset({"test:event"})
            services: Any = None

            async def __call__(self, event: str, data: dict[str, Any]) -> HookResult:
                return HookResult()

        handler = ConformingHandler()
        assert isinstance(handler, EventHandler)

    def test_missing_handled_events_fails_isinstance(self) -> None:
        """A class missing handled_events fails isinstance check."""

        class MissingHandledEvents:
            services: Any = None

            async def __call__(self, event: str, data: dict[str, Any]) -> HookResult:
                return HookResult()

        handler = MissingHandledEvents()
        assert not isinstance(handler, EventHandler)

    def test_missing_services_fails_isinstance(self) -> None:
        """A class missing services fails isinstance check."""

        class MissingServices:
            handled_events: AbstractSet[str] = frozenset({"test:event"})

            async def __call__(self, event: str, data: dict[str, Any]) -> HookResult:
                return HookResult()

        handler = MissingServices()
        assert not isinstance(handler, EventHandler)
