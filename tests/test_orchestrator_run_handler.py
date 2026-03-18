"""Regression tests for OrchestratorRunHandler — bug D-06 and related behaviour.

Bug D-06: _handle_prompt_submit() contained a hard early-return guard when the
Session node was not found in the graph.  This caused a cascade failure:
- PromptStep node was never created
- cursor.current_step_id was never set
- execution:start would create an OrchestratorRun with no PromptStep to link

The fix replaces the hard return with ensure_session_node() recovery, so that
processing continues normally after creating a stub Session node.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

from context_intelligence_server.handlers.orchestrator_run import OrchestratorRunHandler
from context_intelligence_server.services import HookStateService


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_services(workspace: str = "test-ws") -> HookStateService:
    svc = HookStateService(workspace=workspace)
    svc.graph.upsert_node = AsyncMock()  # type: ignore[method-assign]
    svc.graph.upsert_edge = AsyncMock()  # type: ignore[method-assign]
    return svc


# ---------------------------------------------------------------------------
# Bug D-06 regression tests
# ---------------------------------------------------------------------------


async def test_prompt_submit_creates_stub_session_when_session_not_found():
    """prompt:submit must create a stub Session node and continue processing
    when the session node was never created (session:start missed or server restart).

    Previously the handler returned early and silently dropped all processing:
    - PromptStep node was never created
    - cursor.current_step_id was never set
    - execution:start would create an OrchestratorRun with no PromptStep linked
    """
    svc = _make_services()
    svc.graph.get_node = AsyncMock(return_value=None)  # session not found
    svc.ensure_session_node = AsyncMock()  # type: ignore[method-assign]  # track calls

    handler = OrchestratorRunHandler(svc)
    result = await handler(
        "prompt:submit",
        {
            "session_id": "sess-missing",
            "timestamp": "2026-01-01T00:00:00.000Z",
            "prompt": "hello world",
        },
    )

    # ensure_session_node must have been called to create stub
    svc.ensure_session_node.assert_called_once_with("sess-missing", {})

    # PromptStep node must have been created (upsert_node called at least once)
    assert svc.graph.upsert_node.call_count >= 1, "PromptStep node must be created"  # type: ignore[union-attr]

    # cursor must be updated with current_step_id
    cursors = svc.get_cursors("sess-missing")
    assert cursors.current_step_id is not None, "current_step_id must be set"

    assert result.action == "continue"


async def test_prompt_submit_proceeds_normally_when_session_exists():
    """When session node exists, prompt:submit works normally (no stub creation)."""
    svc = _make_services()
    svc.graph.get_node = AsyncMock(  # type: ignore[method-assign]
        return_value={"node_id": "sess-1", "status": "running"}
    )
    svc.ensure_session_node = AsyncMock()  # type: ignore[method-assign]

    handler = OrchestratorRunHandler(svc)
    result = await handler(
        "prompt:submit",
        {
            "session_id": "sess-1",
            "timestamp": "2026-01-01T00:00:00.000Z",
            "prompt": "hello",
        },
    )

    # ensure_session_node must NOT be called when session already exists
    svc.ensure_session_node.assert_not_called()

    # PromptStep node must still be created
    assert svc.graph.upsert_node.call_count >= 1  # type: ignore[union-attr]

    # cursor must be updated
    cursors = svc.get_cursors("sess-1")
    assert cursors.current_step_id is not None

    assert result.action == "continue"


async def test_prompt_submit_missing_session_id_still_returns_early():
    """prompt:submit without session_id returns early — unrelated to D-06 fix."""
    svc = _make_services()
    svc.ensure_session_node = AsyncMock()  # type: ignore[method-assign]

    handler = OrchestratorRunHandler(svc)
    result = await handler(
        "prompt:submit",
        {"timestamp": "2026-01-01T00:00:00.000Z", "prompt": "oops"},
    )

    # No session_id → hard early return is correct here
    svc.ensure_session_node.assert_not_called()
    assert svc.graph.upsert_node.call_count == 0  # type: ignore[union-attr]
    assert result.action == "continue"
