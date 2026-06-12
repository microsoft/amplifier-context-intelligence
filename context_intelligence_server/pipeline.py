"""Server-side event processing pipeline — always-default + ordered enrichers model.

Provides four public exports:

- ``TERMINAL_EVENTS`` — frozenset containing only ``'session:end'``
- ``PipelineHandlers`` — NamedTuple with ``default`` (DefaultHandler) and
  ``enrichers`` (list of ordered enrichers)
- ``setup_handlers(services)`` — return a PipelineHandlers with DefaultHandler,
  all 8 data_layer_2 enrichers, and all 4 data_layer_3 enrichers
- ``process_event(worker, event, data, handlers)`` — full pipeline step:
  ensure-session-node → blob processing → always-default dispatch →
  enricher dispatch (for matching events) → touch_session (last_updated),
  all wrapped in a broad try/except so the drain loop is never interrupted
  by handler errors. process_event does NOT flush (Task 6): the drainer's
  gated ``_flush_barrier`` is the sole Neo4j-write trigger.
"""

from __future__ import annotations

import logging
from typing import Any, NamedTuple, TYPE_CHECKING

if TYPE_CHECKING:
    from context_intelligence_server.registry import SessionWorker

from context_intelligence_server.blob_processor import process_event_data
from context_intelligence_server.handlers.data_layer_1.default import DefaultHandler
from context_intelligence_server.services import HookStateService
from context_intelligence_server.utils import make_node_id

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TERMINAL_EVENTS: frozenset[str] = frozenset({"session:end"})


# ---------------------------------------------------------------------------
# PipelineHandlers
# ---------------------------------------------------------------------------


class PipelineHandlers(NamedTuple):
    """Holds the always-called default handler and the ordered enrichers list."""

    default: DefaultHandler
    enrichers: list[Any]


# ---------------------------------------------------------------------------
# setup_handlers
# ---------------------------------------------------------------------------


def setup_handlers(services: HookStateService) -> PipelineHandlers:
    """Instantiate handlers and return a PipelineHandlers.

    Returns a PipelineHandlers with:
    - ``default``: DefaultHandler instance (always called for every event)
    - ``enrichers``: all 8 data_layer_2 enrichers followed by all 4
      data_layer_3 enrichers in dispatch order (called additionally for
      events they claim)

    All handlers receive the same *services* instance.

    Layer 3 enrichers are appended after all Layer 2 enrichers to guarantee
    that ToolCallHandler (Layer 2) creates ToolCall nodes before
    RecipeStepHandler (Layer 3) creates E11 edges on the same tool:pre event.
    """
    # Local imports to allow tests to stub handlers via sys.modules
    # before they are fully implemented in the handlers package.
    from context_intelligence_server.handlers.data_layer_2.content_block import (
        ContentBlockHandler,
    )  # noqa: PLC0415
    from context_intelligence_server.handlers.data_layer_2.iteration import (
        IterationHandler,
    )  # noqa: PLC0415
    from context_intelligence_server.handlers.data_layer_2.orchestrator_run import (
        OrchestratorRunHandler,
    )  # noqa: PLC0415
    from context_intelligence_server.handlers.data_layer_2.session import SessionHandler  # noqa: PLC0415
    from context_intelligence_server.handlers.data_layer_2.tool_call import (
        ToolCallHandler,
    )  # noqa: PLC0415
    from context_intelligence_server.handlers.data_layer_2.prompt import PromptHandler  # noqa: PLC0415
    from context_intelligence_server.handlers.data_layer_2.cancellation import (
        CancellationHandler,
    )  # noqa: PLC0415
    from context_intelligence_server.handlers.data_layer_2.context_compaction import (
        ContextCompactionHandler,
    )  # noqa: PLC0415
    from context_intelligence_server.handlers.data_layer_3.delegation import (
        DelegationHandler,
    )  # noqa: PLC0415
    from context_intelligence_server.handlers.data_layer_3.skill_load import (
        SkillLoadHandler,
    )  # noqa: PLC0415
    from context_intelligence_server.handlers.data_layer_3.recipe_run import (
        RecipeRunHandler,
    )  # noqa: PLC0415
    from context_intelligence_server.handlers.data_layer_3.recipe_step import (
        RecipeStepHandler,
    )  # noqa: PLC0415

    return PipelineHandlers(
        default=DefaultHandler(services),
        enrichers=[
            SessionHandler(services),
            OrchestratorRunHandler(services),
            IterationHandler(services),
            ContentBlockHandler(services),
            ToolCallHandler(services),
            PromptHandler(services),
            CancellationHandler(services),
            ContextCompactionHandler(services),
            DelegationHandler(services),
            SkillLoadHandler(services),
            RecipeRunHandler(services),  # stub — implemented in Phase 2
            RecipeStepHandler(services),  # stub — implemented in Phase 2
        ],
    )


# ---------------------------------------------------------------------------
# process_event
# ---------------------------------------------------------------------------


async def process_event(
    worker: "SessionWorker",
    event: str,
    data: dict[str, Any],
    handlers: PipelineHandlers,
) -> None:
    """Process one event through the always-default + enrichers pipeline.

    Steps
    -----
    1. Extract ``session_id`` from *data*.
    2. If *session_id* is present, call ``worker.services.ensure_session_node``
       to idempotently create a Session node before any handler runs.
    3. Blob processing: if session_id + timestamp + blob_store are all present,
       call ``process_event_data``.  Log a WARNING if blob_store is present but
       timestamp is missing.
    4. **Always** invoke ``handlers.default`` (records every event as an
       Event node in the graph).
    5. For each enricher in ``handlers.enrichers``, if the event is in the
       enricher's ``handled_events``, call the enricher additionally.
    6. If *session_id* and *timestamp* are present, call
       ``worker.services.touch_session`` to update ``last_updated`` on the
       session node and propagate to ancestors.
    7. Flush strategy: ``process_event`` does NOT flush.  The drainer
       (``registry.drain_worker``) owns the single semaphore-gated flush
       barrier per batch, and terminal-event (``session:end``) durability is
       handled there before the session closes.

    All of the above is wrapped in a single ``try/except Exception`` block so
    that handler errors are *logged with structured context* but **never
    propagate** — the drain loop must continue regardless of per-event
    failures.
    """
    session_id: str | None = data.get("session_id") if isinstance(data, dict) else None
    try:
        # Step 2 — ensure Session node exists for known sessions
        if session_id:
            await worker.services.ensure_session_node(session_id, data)

        # Step 3 — blob processing (after ensure_session_node, before dispatch)
        timestamp: str | None = (
            data.get("timestamp") if isinstance(data, dict) else None
        )
        if session_id and timestamp and worker.services.blob_store:
            node_id = make_node_id(session_id, event, timestamp)
            await process_event_data(
                data, worker.services.blob_store, session_id, node_id
            )
        elif session_id and worker.services.blob_store and not timestamp:
            logger.warning(
                "blob_processing_skipped session=%s event=%s: missing timestamp",
                session_id,
                event,
            )

        # Step 4 — always call default handler
        await handlers.default(event, data)

        # Step 5 — call matching enrichers additionally
        for enricher in handlers.enrichers:
            if event in enricher.handled_events:
                await enricher(event, data)

        # Step 6 — update last_updated on session and ancestors
        if session_id and timestamp:
            await worker.services.touch_session(session_id, timestamp)

    except Exception:
        # Phase B2 (USER DECISION option a): a handler error in steps 2-6
        # (ensure_session_node, blob processing, the default handler that
        # creates the raw :Event node, the enrichers, or touch_session) has no
        # genuinely-benign condition to swallow. Swallowing it here let the
        # drainer commit the offset past a never-persisted event (silent loss).
        # Log with structured context, then RE-RAISE so the drainer routes this
        # line to dead-letter instead of acking it. No "fourth state".
        logger.exception(
            "pipeline: unhandled error processing event",
            extra={
                "event": event,
                "session_id": session_id,
            },
        )
        raise

    # NOTE (Task 6): process_event no longer self-flushes. The drainer
    # (registry.drain_worker) owns the SINGLE semaphore-gated flush barrier per
    # batch (commit-after-flush via registry._flush_barrier). This intentionally
    # removed the previous un-gated per-event background flush that ran outside
    # the write semaphore for every non-terminal event — under multi-session
    # replay that un-gated path made the shared write semaphore a near no-op.
    # Routing every Neo4j
    # write through the gated barrier is what makes the semaphore actually bound
    # concurrent writes. ``TERMINAL_EVENTS`` stays defined for the drainer
    # (registry._process_batch imports it to detect session:end).
