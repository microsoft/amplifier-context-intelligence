"""Integration tests — full event pipeline end-to-end.

Tests verify the complete event processing pipeline from raw events through
to graph state, covering:
- TestFullEventSequence: realistic session from start to completion
- TestWorkspaceSetCorrectly: workspace propagated to graph
- TestHandlerErrorIsolation: bad timestamps don't crash pipeline
- TestUnclaimedEventsFlowToDefault: session:resume via DefaultHandler
- TestSystemEventsCreateNodes: context:compaction and cancel events create Event nodes
- TestSessionEndWorkerCleanup: session:end triggers worker removal and CompletedSession recording
- TestStatusIncludesCompletedSessions: build_status_response includes completed sessions after drain
"""

from __future__ import annotations

import time
import types
from typing import Any
from unittest.mock import AsyncMock, patch

from context_intelligence_server.dashboard import build_status_response
from context_intelligence_server.pipeline import process_event, setup_handlers
from context_intelligence_server.registry import SessionRegistry, SessionWorker
from context_intelligence_server.services import HookStateService
from context_intelligence_server.utils import make_node_id


# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------

WORKSPACE = "integ-test-workspace"
SESSION_ID = "session-integ-001"

# Ordered timestamps for the full pipeline sequence
T0 = "2024-06-01T09:00:00+00:00"  # session:start
T1 = "2024-06-01T09:00:01+00:00"  # prompt:submit
T2 = "2024-06-01T09:00:02+00:00"  # execution:start
T3 = "2024-06-01T09:00:03+00:00"  # provider:request
T4 = "2024-06-01T09:00:04+00:00"  # tool:pre
T5 = "2024-06-01T09:00:05+00:00"  # tool:post
T6 = "2024-06-01T09:00:06+00:00"  # orchestrator:complete


# ---------------------------------------------------------------------------
# Helper factory — creates a services + worker pair without real async queue
# ---------------------------------------------------------------------------


def _make_worker_and_services(
    workspace: str = WORKSPACE,
) -> tuple[Any, HookStateService]:
    """Return a (worker, services) pair for pipeline tests.

    ``worker`` is a lightweight namespace that satisfies the ``SessionWorker``
    interface used by ``process_event`` — namely ``worker.services``.
    """
    services = HookStateService(workspace=workspace)
    worker = types.SimpleNamespace(services=services)
    return worker, services


def _make_registry_and_worker(
    workspace: str = WORKSPACE,
    session_id: str = SESSION_ID,
) -> tuple[SessionRegistry, SessionWorker]:
    """Return a (registry, worker) pair for cleanup and status tests.

    ``services.graph.close`` is mocked so the async cleanup in
    ``SessionWorker`` does not attempt to close a real graph connection.
    """
    reg = SessionRegistry()
    services = HookStateService(workspace=workspace)
    services.graph.close = AsyncMock()  # type: ignore[method-assign]
    worker = SessionWorker(
        session_id=session_id,
        workspace=workspace,
        services=services,
    )
    reg._register_for_test(worker)
    return reg, worker


# ===========================================================================
# TestFullEventSequence
# ===========================================================================


class TestFullEventSequence:
    """Process a realistic session through the full pipeline and verify graph
    state at every step.

    Event sequence:
        session:start → prompt:submit → execution:start → provider:request
        → tool:pre → tool:post → orchestrator:complete
    """

    async def test_session_start_creates_root_session_node(self) -> None:
        """After session:start the graph must have a Session+Root node."""
        worker, services = _make_worker_and_services()
        handlers = setup_handlers(services)

        await process_event(
            worker,
            "session:start",
            {"session_id": SESSION_ID, "timestamp": T0},
            handlers,
        )

        node = await services.graph.get_node(SESSION_ID)
        assert node is not None
        assert "Session" in node["labels"]
        assert "Root" in node["labels"]
        assert node["status"] == "running"

    async def test_prompt_submit_creates_prompt_step_node(self) -> None:
        """After prompt:submit a PromptStep+Step node must exist."""
        worker, services = _make_worker_and_services()
        handlers = setup_handlers(services)

        await process_event(
            worker,
            "session:start",
            {"session_id": SESSION_ID, "timestamp": T0},
            handlers,
        )
        await process_event(
            worker,
            "prompt:submit",
            {"session_id": SESSION_ID, "timestamp": T1, "prompt": "Hello world"},
            handlers,
        )

        prompt_id = make_node_id(SESSION_ID, "prompt:submit", T1)
        node = await services.graph.get_node(prompt_id)
        assert node is not None
        assert "PromptStep" in node["labels"]
        assert "Step" in node["labels"]

    async def test_execution_start_creates_orchestrator_run_with_has_run_edge(
        self,
    ) -> None:
        """After execution:start an OrchestratorRun node and HAS_RUN edge must exist."""
        worker, services = _make_worker_and_services()
        handlers = setup_handlers(services)

        await process_event(
            worker,
            "session:start",
            {"session_id": SESSION_ID, "timestamp": T0},
            handlers,
        )
        await process_event(
            worker,
            "prompt:submit",
            {"session_id": SESSION_ID, "timestamp": T1, "prompt": "Hello"},
            handlers,
        )
        await process_event(
            worker,
            "execution:start",
            {"session_id": SESSION_ID, "timestamp": T2},
            handlers,
        )

        run_id = make_node_id(SESSION_ID, "execution:start", T2)
        run_node = await services.graph.get_node(run_id)
        assert run_node is not None
        assert "OrchestratorRun" in run_node["labels"]
        assert run_node["status"] == "in_progress"

        # HAS_RUN edge: Session → OrchestratorRun
        has_run_edge = await services.graph.get_edge(SESSION_ID, run_id)
        assert has_run_edge is not None
        assert has_run_edge["type"] == "HAS_RUN"

    async def test_execution_start_creates_has_step_edge_to_prompt_step(self) -> None:
        """After execution:start the run must have a HAS_STEP edge to the PromptStep."""
        worker, services = _make_worker_and_services()
        handlers = setup_handlers(services)

        await process_event(
            worker,
            "session:start",
            {"session_id": SESSION_ID, "timestamp": T0},
            handlers,
        )
        await process_event(
            worker,
            "prompt:submit",
            {"session_id": SESSION_ID, "timestamp": T1, "prompt": "Hello"},
            handlers,
        )
        await process_event(
            worker,
            "execution:start",
            {"session_id": SESSION_ID, "timestamp": T2},
            handlers,
        )

        run_id = make_node_id(SESSION_ID, "execution:start", T2)
        prompt_id = make_node_id(SESSION_ID, "prompt:submit", T1)

        # HAS_STEP edge: OrchestratorRun → PromptStep
        has_step_edge = await services.graph.get_edge(run_id, prompt_id)
        assert has_step_edge is not None
        assert has_step_edge["type"] == "HAS_STEP"

    async def test_provider_request_creates_assistant_step_with_has_step_edge(
        self,
    ) -> None:
        """After provider:request an AssistantStep+Step node and HAS_STEP edge must exist."""
        worker, services = _make_worker_and_services()
        handlers = setup_handlers(services)

        await process_event(
            worker,
            "session:start",
            {"session_id": SESSION_ID, "timestamp": T0},
            handlers,
        )
        await process_event(
            worker,
            "prompt:submit",
            {"session_id": SESSION_ID, "timestamp": T1, "prompt": "Hello"},
            handlers,
        )
        await process_event(
            worker,
            "execution:start",
            {"session_id": SESSION_ID, "timestamp": T2},
            handlers,
        )
        await process_event(
            worker,
            "provider:request",
            {"session_id": SESSION_ID, "timestamp": T3, "provider": "anthropic"},
            handlers,
        )

        run_id = make_node_id(SESSION_ID, "execution:start", T2)
        step_id = make_node_id(SESSION_ID, "provider:request", T3)

        step_node = await services.graph.get_node(step_id)
        assert step_node is not None
        assert "AssistantStep" in step_node["labels"]
        assert "Step" in step_node["labels"]

        # HAS_STEP edge: OrchestratorRun → AssistantStep
        has_step_edge = await services.graph.get_edge(run_id, step_id)
        assert has_step_edge is not None
        assert has_step_edge["type"] == "HAS_STEP"

    async def test_tool_pre_creates_tool_execution_with_triggered_edge(self) -> None:
        """After tool:pre a ToolExecution node and TRIGGERED edge from Step must exist."""
        worker, services = _make_worker_and_services()
        handlers = setup_handlers(services)
        tool_call_id = "call_integ_001"

        await process_event(
            worker,
            "session:start",
            {"session_id": SESSION_ID, "timestamp": T0},
            handlers,
        )
        await process_event(
            worker,
            "prompt:submit",
            {"session_id": SESSION_ID, "timestamp": T1, "prompt": "Hello"},
            handlers,
        )
        await process_event(
            worker,
            "execution:start",
            {"session_id": SESSION_ID, "timestamp": T2},
            handlers,
        )
        await process_event(
            worker,
            "provider:request",
            {"session_id": SESSION_ID, "timestamp": T3, "provider": "anthropic"},
            handlers,
        )
        await process_event(
            worker,
            "tool:pre",
            {
                "session_id": SESSION_ID,
                "timestamp": T4,
                "tool_call_id": tool_call_id,
                "tool_name": "bash",
            },
            handlers,
        )

        step_id = make_node_id(SESSION_ID, "provider:request", T3)
        te_id = make_node_id(SESSION_ID, "tool:pre", T4, disambiguator=tool_call_id)

        te_node = await services.graph.get_node(te_id)
        assert te_node is not None
        assert "ToolExecution" in te_node["labels"]
        assert te_node["status"] == "executing"

        # TRIGGERED edge: AssistantStep → ToolExecution
        triggered_edge = await services.graph.get_edge(step_id, te_id)
        assert triggered_edge is not None
        assert triggered_edge["type"] == "TRIGGERED"

    async def test_tool_post_completes_tool_execution(self) -> None:
        """After tool:post the ToolExecution node must have status='complete'."""
        worker, services = _make_worker_and_services()
        handlers = setup_handlers(services)
        tool_call_id = "call_integ_002"

        await process_event(
            worker,
            "session:start",
            {"session_id": SESSION_ID, "timestamp": T0},
            handlers,
        )
        await process_event(
            worker,
            "prompt:submit",
            {"session_id": SESSION_ID, "timestamp": T1, "prompt": "Hello"},
            handlers,
        )
        await process_event(
            worker,
            "execution:start",
            {"session_id": SESSION_ID, "timestamp": T2},
            handlers,
        )
        await process_event(
            worker,
            "provider:request",
            {"session_id": SESSION_ID, "timestamp": T3},
            handlers,
        )
        await process_event(
            worker,
            "tool:pre",
            {
                "session_id": SESSION_ID,
                "timestamp": T4,
                "tool_call_id": tool_call_id,
                "tool_name": "bash",
            },
            handlers,
        )
        await process_event(
            worker,
            "tool:post",
            {
                "session_id": SESSION_ID,
                "timestamp": T5,
                "tool_call_id": tool_call_id,
                "result": "exit code 0",
            },
            handlers,
        )

        te_id = make_node_id(SESSION_ID, "tool:pre", T4, disambiguator=tool_call_id)
        te_node = await services.graph.get_node(te_id)
        assert te_node is not None
        assert te_node["status"] == "complete"

    async def test_orchestrator_complete_closes_run_with_mapped_status(self) -> None:
        """After orchestrator:complete the OrchestratorRun must have status='complete' (mapped from 'success')."""
        worker, services = _make_worker_and_services()
        handlers = setup_handlers(services)

        await process_event(
            worker,
            "session:start",
            {"session_id": SESSION_ID, "timestamp": T0},
            handlers,
        )
        await process_event(
            worker,
            "prompt:submit",
            {"session_id": SESSION_ID, "timestamp": T1, "prompt": "Hello"},
            handlers,
        )
        await process_event(
            worker,
            "execution:start",
            {"session_id": SESSION_ID, "timestamp": T2},
            handlers,
        )
        await process_event(
            worker,
            "orchestrator:complete",
            {
                "session_id": SESSION_ID,
                "timestamp": T6,
                "status": "success",
                "turn_count": 2,
            },
            handlers,
        )

        run_id = make_node_id(SESSION_ID, "execution:start", T2)
        run_node = await services.graph.get_node(run_id)
        assert run_node is not None
        assert run_node["status"] == "complete"  # "success" maps to "complete"
        assert run_node.get("turn_count") == 2

    async def test_orchestrator_complete_clears_current_run_id(self) -> None:
        """After orchestrator:complete the session cursors must have current_run_id=None."""
        worker, services = _make_worker_and_services()
        handlers = setup_handlers(services)

        await process_event(
            worker,
            "session:start",
            {"session_id": SESSION_ID, "timestamp": T0},
            handlers,
        )
        await process_event(
            worker,
            "prompt:submit",
            {"session_id": SESSION_ID, "timestamp": T1, "prompt": "Hello"},
            handlers,
        )
        await process_event(
            worker,
            "execution:start",
            {"session_id": SESSION_ID, "timestamp": T2},
            handlers,
        )
        await process_event(
            worker,
            "orchestrator:complete",
            {"session_id": SESSION_ID, "timestamp": T6, "status": "success"},
            handlers,
        )

        cursors = services.get_cursors(SESSION_ID)
        assert cursors.current_run_id is None


# ===========================================================================
# TestWorkspaceSetCorrectly
# ===========================================================================


class TestWorkspaceSetCorrectly:
    """Workspace string is propagated to the graph store."""

    def test_workspace_propagated_to_graph_on_construction(self) -> None:
        """Services created with a workspace bind that workspace to their graph."""
        services = HookStateService(workspace="project-alpha")
        assert services.graph.workspace == "project-alpha"

    def test_custom_workspace_reflected_in_graph(self) -> None:
        """A custom workspace name is readable from the graph store."""
        services = HookStateService(workspace="my-custom-workspace")
        assert services.graph.workspace == "my-custom-workspace"

    async def test_workspace_unchanged_after_event_processing(self) -> None:
        """Processing events must not alter the graph workspace."""
        worker, services = _make_worker_and_services(workspace="stable-workspace")
        handlers = setup_handlers(services)

        await process_event(
            worker,
            "session:start",
            {"session_id": SESSION_ID, "timestamp": T0},
            handlers,
        )

        assert services.graph.workspace == "stable-workspace"


# ===========================================================================
# TestHandlerErrorIsolation
# ===========================================================================


class TestHandlerErrorIsolation:
    """Malformed events must not crash the pipeline.

    process_event wraps all handler logic in a broad try/except; even
    exceptions caused by bad data (e.g. an empty timestamp that fails
    datetime.fromisoformat) must be swallowed.
    """

    async def test_bad_timestamp_does_not_crash_pipeline(self) -> None:
        """An empty timestamp string causes make_node_id to raise, but
        process_event must catch the exception and not propagate it."""
        worker, services = _make_worker_and_services()
        handlers = setup_handlers(services)

        # prompt:submit with empty timestamp will crash make_node_id inside
        # OrchestratorRunHandler (after ensure_session_node creates the session).
        # process_event must catch and swallow the exception.
        data: dict[str, str] = {"session_id": SESSION_ID, "timestamp": ""}

        # Must NOT raise
        await process_event(worker, "prompt:submit", data, handlers)

    async def test_pipeline_continues_processing_after_bad_event(self) -> None:
        """After a bad event, subsequent well-formed events must still be processed."""
        worker, services = _make_worker_and_services()
        handlers = setup_handlers(services)

        # Bad event — must not raise
        await process_event(
            worker,
            "prompt:submit",
            {"session_id": SESSION_ID, "timestamp": ""},
            handlers,
        )

        # Good event immediately after — must succeed and mutate graph
        await process_event(
            worker,
            "session:start",
            {"session_id": SESSION_ID, "timestamp": T0},
            handlers,
        )

        # Session node must exist (created by the good event's ensure_session_node
        # or SessionHandler)
        node = await services.graph.get_node(SESSION_ID)
        assert node is not None


# ===========================================================================
# TestUnclaimedEventsFlowToDefault
# ===========================================================================


class TestUnclaimedEventsFlowToDefault:
    """Events not claimed by any entity handler flow to DefaultHandler.

    session:resume is intentionally excluded from SessionHandler.handled_events
    and must therefore be handled by DefaultHandler, which creates an
    Event+{DerivedLabel} node.
    """

    async def test_session_resume_creates_event_and_session_resume_node(self) -> None:
        """session:resume must produce an Event node with labels [Event, SessionResume]."""
        worker, services = _make_worker_and_services()
        handlers = setup_handlers(services)

        await process_event(
            worker,
            "session:start",
            {"session_id": SESSION_ID, "timestamp": T0},
            handlers,
        )
        await process_event(
            worker,
            "session:resume",
            {"session_id": SESSION_ID, "timestamp": T1},
            handlers,
        )

        event_node_id = make_node_id(SESSION_ID, "session:resume", T1)
        node = await services.graph.get_node(event_node_id)
        assert node is not None
        assert "Event" in node["labels"]
        assert "SessionResume" in node["labels"]

    async def test_session_resume_event_attached_to_session(self) -> None:
        """When no run is active, the Event node must be attached to the Session node."""
        worker, services = _make_worker_and_services()
        handlers = setup_handlers(services)

        await process_event(
            worker,
            "session:start",
            {"session_id": SESSION_ID, "timestamp": T0},
            handlers,
        )
        await process_event(
            worker,
            "session:resume",
            {"session_id": SESSION_ID, "timestamp": T1},
            handlers,
        )

        event_node_id = make_node_id(SESSION_ID, "session:resume", T1)
        # DefaultHandler attaches to session when no run is active
        edge = await services.graph.get_edge(SESSION_ID, event_node_id)
        assert edge is not None


# ===========================================================================
# TestSystemEventsCreateNodes
# ===========================================================================


class TestSystemEventsCreateNodes:
    """SystemEventHandler claims system events and creates :Event:{DerivedLabel} nodes.

    context:compaction, cancel:requested, and cancel:completed are owned by
    SystemEventHandler which persists a dedicated event node for each.  These
    tests verify the new contract: event nodes ARE created (as opposed to the
    old no-op behaviour that existed before Task 8).
    """

    async def test_context_compaction_creates_event_node(self) -> None:
        """context:compaction must create a ContextCompaction event node."""
        worker, services = _make_worker_and_services()
        handlers = setup_handlers(services)

        await process_event(
            worker,
            "context:compaction",
            {"session_id": SESSION_ID, "timestamp": T0},
            handlers,
        )

        event_node_id = make_node_id(SESSION_ID, "context:compaction", T0)
        event_node = await services.graph.get_node(event_node_id)
        assert event_node is not None
        assert "Event" in event_node["labels"]
        assert "ContextCompaction" in event_node["labels"]

    async def test_context_compaction_creates_session_and_event_nodes(self) -> None:
        """After context:compaction the graph must contain the Session stub and
        the ContextCompaction event node — exactly two nodes."""
        worker, services = _make_worker_and_services()
        handlers = setup_handlers(services)

        await process_event(
            worker,
            "context:compaction",
            {"session_id": SESSION_ID, "timestamp": T0},
            handlers,
        )

        # Session node created by ensure_session_node must exist
        session_node = await services.graph.get_node(SESSION_ID)
        assert session_node is not None

        # Event node must also exist — two nodes in total
        event_node_id = make_node_id(SESSION_ID, "context:compaction", T0)
        assert len(services.graph._nodes) == 2
        assert SESSION_ID in services.graph._nodes
        assert event_node_id in services.graph._nodes

    async def test_cancel_requested_creates_event_node(self) -> None:
        """cancel:requested must create a CancelRequested event node."""
        worker, services = _make_worker_and_services()
        handlers = setup_handlers(services)

        await process_event(
            worker,
            "cancel:requested",
            {"session_id": SESSION_ID, "timestamp": T0},
            handlers,
        )

        event_node_id = make_node_id(SESSION_ID, "cancel:requested", T0)
        event_node = await services.graph.get_node(event_node_id)
        assert event_node is not None
        assert "Event" in event_node["labels"]
        assert "CancelRequested" in event_node["labels"]


# ===========================================================================
# TestSessionEndWorkerCleanup
# ===========================================================================


class TestSessionEndWorkerCleanup:
    """session:end triggers worker removal and CompletedSession recording."""

    async def test_session_end_removes_worker_from_registry(self) -> None:
        """After session:end is drained, the worker is removed from the registry."""
        reg, worker = _make_registry_and_worker()

        with patch(
            "context_intelligence_server.registry.process_event",
            new_callable=AsyncMock,
        ):
            reg.start_drain(worker)
            await worker.queue.put(
                (
                    "session:start",
                    WORKSPACE,
                    {"session_id": SESSION_ID, "timestamp": T0},
                )
            )
            await worker.queue.put(
                (
                    "prompt:submit",
                    WORKSPACE,
                    {"session_id": SESSION_ID, "timestamp": T1, "prompt": "Hello"},
                )
            )
            await worker.queue.put(
                ("session:end", WORKSPACE, {"session_id": SESSION_ID, "timestamp": T6})
            )
            assert worker.task is not None
            await worker.task

        assert reg.active_count() == 0

    async def test_session_end_populates_completed_ring(self) -> None:
        """After session:end the completed ring holds one CompletedSession."""
        reg, worker = _make_registry_and_worker()

        with patch(
            "context_intelligence_server.registry.process_event",
            new_callable=AsyncMock,
        ):
            reg.start_drain(worker)
            await worker.queue.put(
                (
                    "session:start",
                    WORKSPACE,
                    {"session_id": SESSION_ID, "timestamp": T0},
                )
            )
            await worker.queue.put(
                ("session:end", WORKSPACE, {"session_id": SESSION_ID, "timestamp": T6})
            )
            assert worker.task is not None
            await worker.task

        completed = reg.completed_sessions()
        assert len(completed) == 1
        assert completed[0].session_id == SESSION_ID
        assert completed[0].events_processed > 0


# ===========================================================================
# TestStatusIncludesCompletedSessions
# ===========================================================================


class TestStatusIncludesCompletedSessions:
    """build_status_response includes completed sessions after drain."""

    async def test_status_has_completed_sessions_after_drain(self) -> None:
        """build_status_response lists the completed session after drain."""
        reg, worker = _make_registry_and_worker()

        with patch(
            "context_intelligence_server.registry.process_event",
            new_callable=AsyncMock,
        ):
            reg.start_drain(worker)
            await worker.queue.put(
                (
                    "session:start",
                    WORKSPACE,
                    {"session_id": SESSION_ID, "timestamp": T0},
                )
            )
            await worker.queue.put(
                ("session:end", WORKSPACE, {"session_id": SESSION_ID, "timestamp": T6})
            )
            assert worker.task is not None
            await worker.task

        response = build_status_response(reg, time.time() - 60)
        assert "completed_sessions" in response
        assert len(response["completed_sessions"]) == 1
        assert response["completed_sessions"][0]["session_id"] == SESSION_ID
        assert response["completed_sessions"][0]["events_processed"] > 0
