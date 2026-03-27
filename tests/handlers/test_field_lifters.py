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


class TestSessionLifter:
    """Tests for SessionLifter — extracts parent and metadata fields from session:* events."""

    def setup_method(self) -> None:
        from context_intelligence_server.handlers.field_lifters.session import (
            SessionLifter,
        )

        self.lifter = SessionLifter()

    def test_matches_only_session_events(self) -> None:
        assert self.lifter.matches("session:start") is True
        assert self.lifter.matches("session:fork") is True
        assert self.lifter.matches("session:end") is True
        assert self.lifter.matches("session:resume") is True
        assert self.lifter.matches("tool:pre") is False

    def test_lifts_parent_from_fork(self) -> None:
        data = {"parent": "sess-parent-123"}
        result = self.lifter.extract("session:fork", data)
        assert result["parent"] == "sess-parent-123"

    def test_parent_absent_skipped(self) -> None:
        data = {"session_id": "sess-1"}
        result = self.lifter.extract("session:start", data)
        assert "parent" not in result

    def test_none_parent_value_skipped(self) -> None:
        data = {"parent": None}
        result = self.lifter.extract("session:fork", data)
        assert "parent" not in result

    def test_lifts_metadata_agent_name(self) -> None:
        data = {"metadata": {"agent_name": "my-agent"}}
        result = self.lifter.extract("session:start", data)
        assert result["agent_name"] == "my-agent"

    def test_lifts_all_metadata_keys(self) -> None:
        data = {
            "metadata": {
                "agent_name": "my-agent",
                "tool_call_id": "tc-42",
                "parallel_group_id": "pg-7",
                "recipe_name": "my-recipe",
                "recipe_step": "step-one",
                "recipe_step_index": 3,
            }
        }
        result = self.lifter.extract("session:start", data)
        assert result["agent_name"] == "my-agent"
        assert result["tool_call_id"] == "tc-42"
        assert result["parallel_group_id"] == "pg-7"
        assert result["recipe_name"] == "my-recipe"
        assert result["recipe_step"] == "step-one"
        assert result["recipe_step_index"] == 3

    def test_missing_metadata_returns_empty(self) -> None:
        data = {"session_id": "sess-1"}
        result = self.lifter.extract("session:start", data)
        assert result == {}

    def test_none_metadata_values_skipped(self) -> None:
        data = {
            "metadata": {
                "agent_name": "my-agent",
                "tool_call_id": None,
                "recipe_name": None,
            }
        }
        result = self.lifter.extract("session:start", data)
        assert result["agent_name"] == "my-agent"
        assert "tool_call_id" not in result
        assert "recipe_name" not in result

    def test_metadata_not_a_dict_returns_empty(self) -> None:
        data = {"metadata": "not-a-dict"}
        result = self.lifter.extract("session:start", data)
        assert result == {}


class TestToolLifter:
    """Tests for ToolLifter — extracts tool_name and tool_input from tool:* events."""

    def setup_method(self) -> None:
        from context_intelligence_server.handlers.field_lifters.tool import (
            ToolLifter,
        )

        self.lifter = ToolLifter()

    def test_matches_only_tool_events(self) -> None:
        assert self.lifter.matches("tool:pre") is True
        assert self.lifter.matches("tool:post") is True
        assert self.lifter.matches("tool:error") is True
        assert self.lifter.matches("session:start") is False
        assert self.lifter.matches("delegate:agent_spawned") is False

    def test_lifts_tool_name(self) -> None:
        data = {"tool_name": "bash", "tool_input": {"command": "ls"}}
        result = self.lifter.extract("tool:pre", data)
        assert result["tool_name"] == "bash"

    def test_lifts_tool_input_when_present(self) -> None:
        tool_input = {"command": "ls -la", "cwd": "/tmp"}
        data = {"tool_name": "bash", "tool_input": tool_input}
        result = self.lifter.extract("tool:pre", data)
        assert result["tool_input"] == tool_input

    def test_skips_absent_tool_input(self) -> None:
        data = {"tool_name": "bash"}
        result = self.lifter.extract("tool:pre", data)
        assert result["tool_name"] == "bash"
        assert "tool_input" not in result

    def test_tool_input_blob_ref_lifted_as_is(self) -> None:
        blob_ref = {"$blob_ref": "ci-blob://abc123/tool_input"}
        data = {"tool_name": "read_file", "tool_input": blob_ref}
        result = self.lifter.extract("tool:pre", data)
        assert result["tool_input"] == blob_ref


class TestDelegateLifter:
    """Tests for DelegateLifter — extracts agent, sub_session_id, parent_session_id from delegate:* events."""

    def setup_method(self) -> None:
        from context_intelligence_server.handlers.field_lifters.delegate import (
            DelegateLifter,
        )

        self.lifter = DelegateLifter()

    def test_matches_only_delegate_events(self) -> None:
        assert self.lifter.matches("delegate:agent_spawned") is True
        assert self.lifter.matches("delegate:agent_completed") is True
        assert self.lifter.matches("delegate:error") is True
        assert self.lifter.matches("tool:pre") is False

    def test_lifts_all_three_fields(self) -> None:
        data = {
            "agent": "my-agent",
            "sub_session_id": "sess-child",
            "parent_session_id": "sess-parent",
        }
        result = self.lifter.extract("delegate:agent_spawned", data)
        assert result == {
            "agent": "my-agent",
            "sub_session_id": "sess-child",
            "parent_session_id": "sess-parent",
        }

    def test_skips_missing_fields(self) -> None:
        data = {"agent": "my-agent"}
        result = self.lifter.extract("delegate:agent_spawned", data)
        assert result == {"agent": "my-agent"}
        assert "sub_session_id" not in result
        assert "parent_session_id" not in result

    def test_empty_data_returns_empty(self) -> None:
        result = self.lifter.extract("delegate:agent_spawned", {})
        assert result == {}


class TestLlmLifter:
    """Tests for LlmLifter — extracts model and provider from llm:* events."""

    def setup_method(self) -> None:
        from context_intelligence_server.handlers.field_lifters.llm import (
            LlmLifter,
        )

        self.lifter = LlmLifter()

    def test_matches_llm_events(self) -> None:
        assert self.lifter.matches("llm:request") is True
        assert self.lifter.matches("llm:response") is True
        assert self.lifter.matches("tool:pre") is False

    def test_lifts_model_and_provider(self) -> None:
        data = {"model": "claude-3-5-sonnet", "provider": "anthropic", "status": "ok"}
        result = self.lifter.extract("llm:request", data)
        assert result == {"model": "claude-3-5-sonnet", "provider": "anthropic"}

    def test_skips_missing_provider(self) -> None:
        data = {"model": "claude-3-5-sonnet"}
        result = self.lifter.extract("llm:request", data)
        assert result == {"model": "claude-3-5-sonnet"}
        assert "provider" not in result

    def test_empty_data_returns_empty(self) -> None:
        result = self.lifter.extract("llm:request", {})
        assert result == {}


class TestPromptLifter:
    """Tests for PromptLifter — extracts prompt and response_preview from prompt:* events."""

    def setup_method(self) -> None:
        from context_intelligence_server.handlers.field_lifters.prompt import (
            PromptLifter,
        )

        self.lifter = PromptLifter()

    def test_matches_prompt_events(self) -> None:
        assert self.lifter.matches("prompt:submit") is True
        assert self.lifter.matches("prompt:complete") is True
        assert self.lifter.matches("llm:request") is False

    def test_lifts_prompt_on_submit(self) -> None:
        data = {"prompt": "What is the capital of France?"}
        result = self.lifter.extract("prompt:submit", data)
        assert result["prompt"] == "What is the capital of France?"

    def test_lifts_prompt_and_response_preview_on_complete(self) -> None:
        data = {
            "prompt": "What is the capital of France?",
            "response_preview": "The capital of France is Paris.",
            "length": 31,
        }
        result = self.lifter.extract("prompt:complete", data)
        assert result["prompt"] == "What is the capital of France?"
        assert result["response_preview"] == "The capital of France is Paris."
        assert "length" not in result

    def test_skips_absent_response_preview_on_submit(self) -> None:
        data = {"prompt": "Hello?"}
        result = self.lifter.extract("prompt:submit", data)
        assert result["prompt"] == "Hello?"
        assert "response_preview" not in result

    def test_empty_data_returns_empty(self) -> None:
        result = self.lifter.extract("prompt:submit", {})
        assert result == {}


class TestToolLifterNewFields:
    """Tests for new tool_call_id and parallel_group_id fields in ToolLifter."""

    def setup_method(self) -> None:
        from context_intelligence_server.handlers.field_lifters.tool import ToolLifter

        self.lifter = ToolLifter()

    def test_lifts_tool_call_id(self) -> None:
        data = {"tool_name": "bash", "tool_call_id": "tc-001"}
        result = self.lifter.extract("tool:pre", data)
        assert result["tool_call_id"] == "tc-001"

    def test_lifts_parallel_group_id(self) -> None:
        data = {"tool_name": "bash", "parallel_group_id": "pg-abc"}
        result = self.lifter.extract("tool:pre", data)
        assert result["parallel_group_id"] == "pg-abc"

    def test_lifts_all_four_fields(self) -> None:
        data = {
            "tool_name": "delegate",
            "tool_input": {"agent": "test:agent"},
            "tool_call_id": "tc-xyz",
            "parallel_group_id": "pg-xyz",
        }
        result = self.lifter.extract("tool:pre", data)
        assert result["tool_name"] == "delegate"
        assert result["tool_input"] == {"agent": "test:agent"}
        assert result["tool_call_id"] == "tc-xyz"
        assert result["parallel_group_id"] == "pg-xyz"


class TestSkillLifter:
    """Tests for SkillLifter — extracts skill_directory and skill_name from skill:* events."""

    def setup_method(self) -> None:
        from context_intelligence_server.handlers.field_lifters.skill import SkillLifter

        self.lifter = SkillLifter()

    def test_matches_skill_events(self) -> None:
        assert self.lifter.matches("skill:loaded")
        assert self.lifter.matches("skill:executed")
        assert not self.lifter.matches("tool:pre")

    def test_lifts_skill_directory_and_name(self) -> None:
        data = {"skill_directory": "/path/to/skills", "skill_name": "brainstorming"}
        result = self.lifter.extract("skill:loaded", data)
        assert result == {"skill_directory": "/path/to/skills", "skill_name": "brainstorming"}

    def test_skips_missing_fields(self) -> None:
        data = {"skill_name": "my-skill"}
        result = self.lifter.extract("skill:loaded", data)
        assert result == {"skill_name": "my-skill"}
        assert "skill_directory" not in result

    def test_skips_none_values(self) -> None:
        data = {"skill_directory": None, "skill_name": "test"}
        result = self.lifter.extract("skill:loaded", data)
        assert "skill_directory" not in result
        assert result["skill_name"] == "test"


class TestRecipeLifter:
    """Tests for RecipeLifter — extracts orchestration fields from recipe:* events."""

    def setup_method(self) -> None:
        from context_intelligence_server.handlers.field_lifters.recipe import RecipeLifter

        self.lifter = RecipeLifter()

    def test_matches_recipe_events(self) -> None:
        assert self.lifter.matches("recipe:start")
        assert self.lifter.matches("recipe:step")
        assert self.lifter.matches("recipe:complete")
        assert self.lifter.matches("recipe:loop_iteration")
        assert not self.lifter.matches("skill:loaded")

    def test_lifts_all_six_fields(self) -> None:
        data = {
            "recipe_name": "subagent-driven-development",
            "current_step": 3,
            "description": "Execute implementation plan",
            "status": "running",
            "step_id": "implement-task",
            "total_steps": 7,
        }
        result = self.lifter.extract("recipe:step", data)
        assert result["recipe_name"] == "subagent-driven-development"
        assert result["current_step"] == 3
        assert result["description"] == "Execute implementation plan"
        assert result["status"] == "running"
        assert result["step_id"] == "implement-task"
        assert result["total_steps"] == 7

    def test_partial_fields(self) -> None:
        data = {"recipe_name": "my-recipe", "status": "completed"}
        result = self.lifter.extract("recipe:complete", data)
        assert result == {"recipe_name": "my-recipe", "status": "completed"}
        assert "current_step" not in result


class TestArtifactLifter:
    """Tests for ArtifactLifter — extracts bytes and path from artifact:* events."""

    def setup_method(self) -> None:
        from context_intelligence_server.handlers.field_lifters.artifact import ArtifactLifter

        self.lifter = ArtifactLifter()

    def test_matches_artifact_events(self) -> None:
        assert self.lifter.matches("artifact:read")
        assert self.lifter.matches("artifact:write")
        assert not self.lifter.matches("tool:pre")

    def test_lifts_bytes_and_path(self) -> None:
        data = {"bytes": 4528, "path": "/home/user/AGENTS.md", "session_id": "s1"}
        result = self.lifter.extract("artifact:write", data)
        assert result == {"bytes": 4528, "path": "/home/user/AGENTS.md"}

    def test_skips_missing_fields(self) -> None:
        data = {"path": "/tmp/output.txt"}
        result = self.lifter.extract("artifact:read", data)
        assert result == {"path": "/tmp/output.txt"}
        assert "bytes" not in result
