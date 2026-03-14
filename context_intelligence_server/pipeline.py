"""Server-side event processing pipeline with error isolation.

Provides three public functions:

- ``setup_handlers(services)``  — instantiate all 7 handlers and return a
  handler registry dict with ``"entity"`` and ``"default"`` keys.
- ``_find_handler(event, handlers)`` — first-match-wins dispatch resolution
  with fnmatch wildcard support (e.g. ``content_block:*``).
- ``process_event(worker, event, data, handlers)`` — full pipeline step:
  ensure-session-node → dispatch → terminal flush, all wrapped in a
  broad try/except so the drain loop is never interrupted by handler errors.

This module replaces the bundle's ``_wrap_with_session_guarantee`` pattern for
the server-side deployment model.
"""

from __future__ import annotations

import fnmatch
import logging
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from context_intelligence_server.registry import SessionWorker

from context_intelligence_server.handlers.default import DefaultHandler
from context_intelligence_server.handlers.event import SystemEventHandler
from context_intelligence_server.handlers.orchestrator_run import OrchestratorRunHandler
from context_intelligence_server.handlers.recipe import RecipeHandler
from context_intelligence_server.handlers.session import SessionHandler
from context_intelligence_server.handlers.step import StepHandler
from context_intelligence_server.handlers.tool_execution import ToolExecutionHandler
from context_intelligence_server.services import HookStateService

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TERMINAL_EVENTS: frozenset[str] = frozenset(
    {
        "session:end",
        "execution:end",
        "orchestrator:complete",
    }
)


# ---------------------------------------------------------------------------
# setup_handlers
# ---------------------------------------------------------------------------


def setup_handlers(services: HookStateService) -> dict[str, Any]:
    """Instantiate all 7 handlers and return the handler registry.

    Returns a dict with:
    - ``"entity"``: list of 6 entity handlers in dispatch-priority order
      (SessionHandler, OrchestratorRunHandler, StepHandler, RecipeHandler,
      ToolExecutionHandler, SystemEventHandler)
    - ``"default"``: DefaultHandler instance that catches unclaimed events

    All handlers receive the same *services* instance.
    """
    entity: list[Any] = [
        SessionHandler(services),
        OrchestratorRunHandler(services),
        StepHandler(services),
        RecipeHandler(services),
        ToolExecutionHandler(services),
        SystemEventHandler(services),
    ]
    default = DefaultHandler(services)
    return {"entity": entity, "default": default}


# ---------------------------------------------------------------------------
# _find_handler
# ---------------------------------------------------------------------------


def _find_handler(event: str, handlers: dict[str, Any]) -> Any:
    """Return the first matching entity handler, or the default handler.

    Iterates through ``handlers["entity"]`` in order.  For each handler,
    checks whether *event* matches any pattern in ``handler.handled_events``
    using :func:`fnmatch.fnmatch` (so patterns like ``content_block:*`` work).

    First match wins.  Falls back to ``handlers["default"]`` when no entity
    handler claims the event.
    """
    for handler in handlers["entity"]:
        for pattern in handler.handled_events:
            if fnmatch.fnmatch(event, pattern):
                return handler
    return handlers["default"]


# ---------------------------------------------------------------------------
# process_event
# ---------------------------------------------------------------------------


async def process_event(
    worker: "SessionWorker",
    event: str,
    data: dict[str, Any],
    handlers: dict[str, Any],
) -> None:
    """Process one event through the full pipeline with complete error isolation.

    Steps
    -----
    1. Extract ``session_id`` from *data*.
    2. If *session_id* is present, call ``worker.services.ensure_session_node``
       to idempotently create a Session node before any handler runs.
    3. Resolve the matching handler via :func:`_find_handler`.
    4. Invoke the handler.
    5. If *event* is in :data:`TERMINAL_EVENTS`, call ``worker.services.graph.flush``
       to persist all buffered writes.

    All of the above is wrapped in a single ``try/except Exception`` block so
    that handler errors are *logged with structured context* but **never
    propagate** — the drain loop must continue regardless of per-event failures.
    """
    try:
        session_id: str | None = (
            data.get("session_id") if isinstance(data, dict) else None
        )

        # Step 2 — ensure Session node exists for known sessions
        if session_id:
            await worker.services.ensure_session_node(session_id, data)

        # Step 3 — resolve handler
        handler = _find_handler(event, handlers)

        # Step 4 — dispatch
        await handler(event, data)

        # Step 5 — terminal flush
        if event in TERMINAL_EVENTS:
            await worker.services.graph.flush()

    except Exception:
        logger.exception(
            "pipeline: unhandled error processing event",
            extra={
                "event": event,
                "session_id": (
                    data.get("session_id") if isinstance(data, dict) else None
                ),
            },
        )
