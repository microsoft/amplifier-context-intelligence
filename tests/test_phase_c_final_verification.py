"""Task-12 Final Verification — Phase C completion checks.

This test file verifies all Phase C deliverables are complete:

Test coverage:
- handler_directory_completeness (10 required .py files present)
- all_8_enrichers_registered (count exactly 8)
- complete_enricher_order (all 8 in correct dispatch order)
- enricher_handled_events (each enricher has exactly correct handled_events)
"""

from __future__ import annotations

import importlib.util
from pathlib import Path


# ---------------------------------------------------------------------------
# Helper: locate the data_layer_2 handler package
# ---------------------------------------------------------------------------


def _data_layer_2_dir() -> Path:
    spec = importlib.util.find_spec("context_intelligence_server.handlers.data_layer_2")
    assert spec is not None, "data_layer_2 package not found"
    assert spec.submodule_search_locations is not None
    return Path(list(spec.submodule_search_locations)[0])


# ---------------------------------------------------------------------------
# Verification 1 — handler directory completeness
# ---------------------------------------------------------------------------


class TestHandlerDirectoryCompleteness:
    """The data_layer_2 handler directory must contain exactly 10 .py files."""

    EXPECTED_FILES = {
        "__init__.py",
        "cancellation.py",
        "content_block.py",
        "context_compaction.py",
        "iteration.py",
        "orchestrator_run.py",
        "prompt.py",
        "session.py",
        "state.py",
        "tool_call.py",
    }

    def test_all_10_expected_files_present(self) -> None:
        """All 10 expected .py files are present in data_layer_2/."""
        handler_dir = _data_layer_2_dir()
        actual_files = {p.name for p in handler_dir.glob("*.py")}
        missing = self.EXPECTED_FILES - actual_files
        assert not missing, f"Missing handler files: {sorted(missing)}"

    def test_no_unexpected_files(self) -> None:
        """No unexpected .py files beyond the 10 expected ones."""
        handler_dir = _data_layer_2_dir()
        actual_files = {p.name for p in handler_dir.glob("*.py")}
        extra = actual_files - self.EXPECTED_FILES
        assert not extra, f"Unexpected handler files: {sorted(extra)}"

    def test_file_count_is_exactly_10(self) -> None:
        """Exactly 10 .py files in data_layer_2/."""
        handler_dir = _data_layer_2_dir()
        actual_files = {p.name for p in handler_dir.glob("*.py")}
        assert len(actual_files) == 10, (
            f"Expected 10 .py files, found {len(actual_files)}: {sorted(actual_files)}"
        )


# ---------------------------------------------------------------------------
# Verification 2 — all 8 enrichers in pipeline
# ---------------------------------------------------------------------------


class TestAll8EnrichersRegistered:
    """setup_handlers must register exactly 12 enrichers in the correct order
    (8 data_layer_2 + 4 data_layer_3)."""

    def _get_enrichers(self) -> list:
        from context_intelligence_server.pipeline import setup_handlers
        from context_intelligence_server.services import HookStateService

        services = HookStateService(workspace="test")
        result = setup_handlers(services)
        return list(result.enrichers)

    def test_enricher_count_is_8(self) -> None:
        """Exactly 12 enrichers are registered (8 L2 + 4 L3)."""
        enrichers = self._get_enrichers()
        assert len(enrichers) == 12, (
            f"Expected 12 enrichers, got {len(enrichers)}: "
            f"{[type(e).__name__ for e in enrichers]}"
        )

    def test_complete_enricher_order_all_8(self) -> None:
        """All 12 enrichers are present in the correct dispatch order."""
        enrichers = self._get_enrichers()
        expected_names = [
            "SessionHandler",
            "OrchestratorRunHandler",
            "IterationHandler",
            "ContentBlockHandler",
            "ToolCallHandler",
            "PromptHandler",
            "CancellationHandler",
            "ContextCompactionHandler",
            "DelegationHandler",
            "SkillLoadHandler",
            "RecipeRunHandler",
            "RecipeStepHandler",
        ]
        actual_names = [type(e).__name__ for e in enrichers]
        assert actual_names == expected_names, (
            f"Enricher order mismatch.\n"
            f"  Expected: {expected_names}\n"
            f"  Got:      {actual_names}"
        )

    def test_session_handler_handled_events(self) -> None:
        """SessionHandler must handle session:start, session:end, session:fork."""
        enrichers = self._get_enrichers()
        handler = enrichers[0]
        assert type(handler).__name__ == "SessionHandler"
        assert handler.handled_events == {
            "session:start",
            "session:end",
            "session:fork",
        }

    def test_orchestrator_run_handler_handled_events(self) -> None:
        """OrchestratorRunHandler must handle execution:start, execution:end, orchestrator:complete."""
        enrichers = self._get_enrichers()
        handler = enrichers[1]
        assert type(handler).__name__ == "OrchestratorRunHandler"
        assert handler.handled_events == {
            "execution:start",
            "execution:end",
            "orchestrator:complete",
        }

    def test_iteration_handler_handled_events(self) -> None:
        """IterationHandler must handle llm:request, llm:response, provider:request."""
        enrichers = self._get_enrichers()
        handler = enrichers[2]
        assert type(handler).__name__ == "IterationHandler"
        assert handler.handled_events == {
            "llm:request",
            "llm:response",
            "provider:request",
        }

    def test_content_block_handler_handled_events(self) -> None:
        """ContentBlockHandler must handle content_block:start and content_block:end."""
        enrichers = self._get_enrichers()
        handler = enrichers[3]
        assert type(handler).__name__ == "ContentBlockHandler"
        assert handler.handled_events == {"content_block:start", "content_block:end"}

    def test_tool_call_handler_handled_events(self) -> None:
        """ToolCallHandler must handle tool:pre, tool:post, and tool:error."""
        enrichers = self._get_enrichers()
        handler = enrichers[4]
        assert type(handler).__name__ == "ToolCallHandler"
        assert handler.handled_events == {"tool:pre", "tool:post", "tool:error"}

    def test_prompt_handler_handled_events(self) -> None:
        """PromptHandler must handle prompt:submit."""
        enrichers = self._get_enrichers()
        handler = enrichers[5]
        assert type(handler).__name__ == "PromptHandler"
        assert handler.handled_events == {"prompt:submit"}

    def test_cancellation_handler_handled_events(self) -> None:
        """CancellationHandler must handle cancel:completed."""
        enrichers = self._get_enrichers()
        handler = enrichers[6]
        assert type(handler).__name__ == "CancellationHandler"
        assert handler.handled_events == {"cancel:completed"}

    def test_context_compaction_handler_handled_events(self) -> None:
        """ContextCompactionHandler must handle context:pre_compact and context:post_compact."""
        enrichers = self._get_enrichers()
        handler = enrichers[7]
        assert type(handler).__name__ == "ContextCompactionHandler"
        assert handler.handled_events == {
            "context:pre_compact",
            "context:post_compact",
        }
