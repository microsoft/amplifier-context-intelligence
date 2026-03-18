"""Regression tests for RecipeHandler — bug D-03 and related behaviour.

Bug D-03: _persist_event() was hard-coded to attach HAS_EVENT edges to
session_id, ignoring any active OrchestratorRun.  It must mirror
DefaultHandler behaviour: use current_run_id when one is active, fall back
to session_id otherwise.
"""

from __future__ import annotations

from unittest.mock import AsyncMock


from context_intelligence_server.handlers.recipe import RecipeHandler
from context_intelligence_server.services import HookStateService


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_RECIPE_START_PAYLOAD: dict = {
    "session_id": "sess-abc",
    "timestamp": "2026-01-01T00:00:00.000Z",
    "recipe_name": "test-recipe",
    "description": "",
    "total_steps": 1,
    "status": "started",
}


def _make_services(workspace: str = "test-ws") -> HookStateService:
    svc = HookStateService(workspace=workspace)
    svc.graph.upsert_node = AsyncMock()  # type: ignore[method-assign]
    svc.graph.upsert_edge = AsyncMock()  # type: ignore[method-assign]
    return svc


def _find_has_event_src(svc: HookStateService) -> str | None:
    """Return the src_id from the HAS_EVENT upsert_edge call, or None."""
    for call in svc.graph.upsert_edge.call_args_list:  # type: ignore[union-attr]
        edge_data = call[0][2] if len(call[0]) > 2 else {}
        if edge_data.get("type") == "HAS_EVENT":
            return call[0][0]
    return None


# ---------------------------------------------------------------------------
# Bug D-03 regression tests
# ---------------------------------------------------------------------------


async def test_recipe_handler_has_event_uses_current_run_id_when_active():
    """HAS_EVENT source must be current_run_id when an orchestrator run is active.

    Before the fix _persist_event() always used session_id, so recipe events
    were attached to the Session node even inside an active OrchestratorRun —
    making it impossible to query 'all events for run X' and include recipe events.
    """
    svc = _make_services()
    session_id = "sess-abc"
    run_id = "run-xyz"

    cursors = svc.get_cursors(session_id)
    cursors.current_run_id = run_id

    handler = RecipeHandler(svc)
    await handler("recipe:start", {**_RECIPE_START_PAYLOAD, "session_id": session_id})

    src = _find_has_event_src(svc)
    assert src == run_id, (
        f"HAS_EVENT source must be current_run_id={run_id!r} when a run is active, got {src!r}"
    )


async def test_recipe_handler_has_event_falls_back_to_session_id_when_no_active_run():
    """HAS_EVENT source must fall back to session_id when no run is active."""
    svc = _make_services()
    session_id = "sess-abc"
    # current_run_id is None by default — no active run

    handler = RecipeHandler(svc)
    await handler("recipe:start", {**_RECIPE_START_PAYLOAD, "session_id": session_id})

    src = _find_has_event_src(svc)
    assert src == session_id, (
        f"HAS_EVENT source must be session_id={session_id!r} when no run is active, got {src!r}"
    )


async def test_recipe_handler_has_event_uses_run_id_for_loop_events():
    """Loop events (recipe:loop_iteration) also attach HAS_EVENT to current_run_id."""
    svc = _make_services()
    session_id = "sess-loop"
    run_id = "run-loop"

    cursors = svc.get_cursors(session_id)
    cursors.current_run_id = run_id

    handler = RecipeHandler(svc)
    await handler(
        "recipe:loop_iteration",
        {
            "session_id": session_id,
            "timestamp": "2026-01-01T00:00:00.000Z",
            "step_id": "step-1",
            "max_iterations": 5,
            "iteration": 1,
        },
    )

    src = _find_has_event_src(svc)
    assert src == run_id, (
        f"Loop event HAS_EVENT source must be run_id={run_id!r}, got {src!r}"
    )
