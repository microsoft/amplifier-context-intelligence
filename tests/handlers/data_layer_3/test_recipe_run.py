"""Tests for RecipeRunHandler stub — Phase 2 placeholder.

Covers:
- handled_events == frozenset() (empty — Phase 2 deferred)
- __init__ accepts services and stores it
- __call__ returns HookResult(action='continue') for any event
- satisfies EventHandler protocol
"""

from __future__ import annotations

from context_intelligence_server.handlers.data_layer_3.recipe_run import (
    RecipeRunHandler,
)
from context_intelligence_server.protocol import EventHandler, HookResult
from context_intelligence_server.services import HookStateService


# ---------------------------------------------------------------------------
# 1. TestRecipeRunHandlerHandledEvents
# ---------------------------------------------------------------------------


class TestRecipeRunHandlerHandledEvents:
    """handled_events must be an empty frozenset (Phase 2 stub)."""

    def test_handled_events_is_frozenset(self) -> None:
        """handled_events must be a frozenset."""
        assert isinstance(RecipeRunHandler.handled_events, frozenset)

    def test_handled_events_is_empty(self) -> None:
        """handled_events must be empty — Phase 2 deferred."""
        assert RecipeRunHandler.handled_events == frozenset()


# ---------------------------------------------------------------------------
# 2. TestRecipeRunHandlerInit
# ---------------------------------------------------------------------------


class TestRecipeRunHandlerInit:
    """__init__ must accept services and store it."""

    def test_init_stores_services(self) -> None:
        """RecipeRunHandler.__init__ must store the services argument."""
        services = HookStateService(workspace="test")
        handler = RecipeRunHandler(services)
        assert handler.services is services


# ---------------------------------------------------------------------------
# 3. TestRecipeRunHandlerCall
# ---------------------------------------------------------------------------


class TestRecipeRunHandlerCall:
    """__call__ must return HookResult(action='continue') without mutations."""

    async def test_call_returns_hook_result(self) -> None:
        """__call__ must return a HookResult."""
        services = HookStateService(workspace="test")
        handler = RecipeRunHandler(services)
        result = await handler("recipe:run_started", {"session_id": "sess-1"})
        assert isinstance(result, HookResult)

    async def test_call_returns_continue(self) -> None:
        """__call__ must return HookResult with action='continue'."""
        services = HookStateService(workspace="test")
        handler = RecipeRunHandler(services)
        result = await handler("recipe:run_started", {"session_id": "sess-1"})
        assert result.action == "continue"

    async def test_call_returns_continue_for_any_event(self) -> None:
        """__call__ must return continue for any event type."""
        services = HookStateService(workspace="test")
        handler = RecipeRunHandler(services)
        result = await handler("recipe:run_completed", {"session_id": "sess-1"})
        assert result.action == "continue"


# ---------------------------------------------------------------------------
# 4. TestRecipeRunHandlerProtocol
# ---------------------------------------------------------------------------


class TestRecipeRunHandlerProtocol:
    """RecipeRunHandler must satisfy the EventHandler protocol."""

    def test_satisfies_event_handler_protocol(self) -> None:
        """RecipeRunHandler instance must be an EventHandler."""
        services = HookStateService(workspace="test")
        handler = RecipeRunHandler(services)
        assert isinstance(handler, EventHandler)
