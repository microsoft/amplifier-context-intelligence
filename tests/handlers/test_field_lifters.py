"""Tests for FieldLifter base class and safe_prop utility."""

from __future__ import annotations

import pytest

from context_intelligence_server.handlers.field_lifters.base import (
    FieldLifter,
    safe_prop,
)
from context_intelligence_server.handlers.field_lifters.navigation import (
    UniversalLifter,
)


class TestSafeProp:
    """safe_prop returns key unchanged unless it collides with RESERVED_PROPS."""

    def test_normal_key_unchanged(self) -> None:
        assert safe_prop("tool_name") == "tool_name"

    def test_reserved_node_id_prefixed(self) -> None:
        assert safe_prop("node_id") == "data_node_id"

    def test_reserved_data_prefixed(self) -> None:
        assert safe_prop("data") == "data_data"

    def test_reserved_labels_prefixed(self) -> None:
        assert safe_prop("labels") == "data_labels"

    def test_reserved_occurred_at_prefixed(self) -> None:
        assert safe_prop("occurred_at") == "data_occurred_at"

    def test_reserved_event_name_prefixed(self) -> None:
        assert safe_prop("event_name") == "data_event_name"


class TestFieldLifterMatches:
    """FieldLifter.matches uses fnmatch to compare event names against event_pattern."""

    class WildcardLifter(FieldLifter):
        event_pattern = "*"

        def extract(self, event: str, data: dict) -> dict:
            raise NotImplementedError

    class ToolLifter(FieldLifter):
        event_pattern = "tool:*"

        def extract(self, event: str, data: dict) -> dict:
            raise NotImplementedError

    def test_wildcard_matches_everything(self) -> None:
        lifter = self.WildcardLifter()
        assert lifter.matches("tool:pre") is True
        assert lifter.matches("session:start") is True
        assert lifter.matches("anything") is True

    def test_prefix_pattern_matches_only_matching_events(self) -> None:
        lifter = self.ToolLifter()
        assert lifter.matches("tool:pre") is True
        assert lifter.matches("tool:post") is True
        assert lifter.matches("session:start") is False

    def test_extract_raises_not_implemented(self) -> None:
        lifter = self.WildcardLifter()
        with pytest.raises(NotImplementedError):
            lifter.extract("tool:pre", {})


class TestUniversalLifter:
    """Tests for UniversalLifter — extracts 4 navigation fields from any event."""

    def test_matches_all_events(self) -> None:
        lifter = UniversalLifter()
        assert lifter.matches("tool:pre") is True
        assert lifter.matches("session:start") is True
        assert lifter.matches("anything:here") is True

    def test_extracts_all_four_fields(self) -> None:
        lifter = UniversalLifter()
        data = {
            "session_id": "sess-1",
            "parent_id": "sess-0",
            "tool_call_id": "tc-42",
            "parallel_group_id": "pg-7",
        }
        result = lifter.extract("tool:pre", data)
        assert result == {
            "session_id": "sess-1",
            "parent_id": "sess-0",
            "tool_call_id": "tc-42",
            "parallel_group_id": "pg-7",
        }

    def test_skips_none_values(self) -> None:
        lifter = UniversalLifter()
        data = {"session_id": "sess-1", "parent_id": None}
        result = lifter.extract("tool:pre", data)
        assert "parent_id" not in result
        assert "tool_call_id" not in result
        assert "parallel_group_id" not in result
        assert result["session_id"] == "sess-1"

    def test_skips_missing_keys(self) -> None:
        lifter = UniversalLifter()
        data = {"session_id": "sess-1"}
        result = lifter.extract("tool:pre", data)
        assert "parent_id" not in result
        assert "tool_call_id" not in result
        assert "parallel_group_id" not in result
        assert result["session_id"] == "sess-1"

    def test_empty_data_returns_empty(self) -> None:
        lifter = UniversalLifter()
        result = lifter.extract("tool:pre", {})
        assert result == {}
