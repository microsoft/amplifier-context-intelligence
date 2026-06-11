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

import asyncio
import json
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
    services.graph.flush = AsyncMock()  # type: ignore[method-assign]
    worker = SessionWorker(
        session_id=session_id,
        workspace=workspace,
        services=services,
    )
    reg._register_for_test(worker)
    return reg, worker


def _line(event: str, workspace: str, data: dict[str, Any]) -> bytes:
    """Encode an appended event line exactly as POST /events stores it."""
    return json.dumps({"event": event, "workspace": workspace, "data": data}).encode(
        "utf-8"
    )


# ===========================================================================
# TestFullEventSequence
# ===========================================================================


class TestFullEventSequence:
    """Process a realistic session through the full pipeline and verify
    graph state at each step.

    The pipeline is: DefaultHandler (always) + five ordered enrichers:
    SessionHandler, OrchestratorRunHandler, IterationHandler,
    ContentBlockHandler, ToolCallHandler.  Cursor state (active_iteration_id,
    pending_tool_block_ids, execution_start_ts) is managed across handlers.
    ToolCallHandler creates E08/E09/E10 edges when the corresponding cursors
    are set.

    Event sequence:
        session:start → tool:pre → tool:post → session:end
    """

    # -----------------------------------------------------------------------
    # Sequence constants — scoped to this class to avoid shadowing the
    # module-level SESSION_ID / T0…T6 used by the other test classes.
    # -----------------------------------------------------------------------

    _SESSION_ID = "integ-session-001"
    _T0 = "2026-01-01T00:00:00.000000000+00:00"  # session:start
    _T1 = "2026-01-01T00:00:01.000000000+00:00"  # tool:pre
    _T2 = "2026-01-01T00:00:02.000000000+00:00"  # tool:post
    _T3 = "2026-01-01T00:00:03.000000000+00:00"  # session:end
    _TOOL_CALL_ID = "toolu_01AbcDef123"
    _PARALLEL_GROUP = "pg-001"

    async def test_session_start_creates_root_session_node(self) -> None:
        """After session:start the Session node must carry RootSession label
        and status='running'."""
        worker, services = _make_worker_and_services()
        handlers = setup_handlers(services)

        await process_event(
            worker,
            "session:start",
            {
                "session_id": self._SESSION_ID,
                "parent_id": None,
                "timestamp": self._T0,
            },
            handlers,
        )

        node = await services.graph.get_node(self._SESSION_ID)
        assert node is not None
        assert "RootSession" in node["labels"]
        assert "Session" in node["labels"]
        assert node["status"] == "running"

    async def test_session_start_creates_session_event_node(self) -> None:
        """After session:start DefaultHandler must create a SessionStart Event
        node and attach it to the Session via HAS_EVENT."""
        worker, services = _make_worker_and_services()
        handlers = setup_handlers(services)

        await process_event(
            worker,
            "session:start",
            {
                "session_id": self._SESSION_ID,
                "parent_id": None,
                "timestamp": self._T0,
            },
            handlers,
        )

        event_node_id = make_node_id(self._SESSION_ID, "session:start", self._T0)
        event_node = await services.graph.get_node(event_node_id)
        assert event_node is not None
        assert "SessionStartEvent" in event_node["labels"]
        assert "SessionEvent" in event_node["labels"]
        assert "Event" in event_node["labels"]

        # DefaultHandler attaches Event node to Session via HAS_EVENT
        edge = await services.graph.get_edge(self._SESSION_ID, event_node_id)
        assert edge is not None

    async def test_tool_pre_creates_tool_call_node(self) -> None:
        """After tool:pre ToolCallHandler must create a ToolCall node with the
        correct properties keyed by tool_call_id directly. No edges are created."""
        worker, services = _make_worker_and_services()
        handlers = setup_handlers(services)

        await process_event(
            worker,
            "session:start",
            {
                "session_id": self._SESSION_ID,
                "parent_id": None,
                "timestamp": self._T0,
            },
            handlers,
        )
        await process_event(
            worker,
            "tool:pre",
            {
                "session_id": self._SESSION_ID,
                "timestamp": self._T1,
                "tool_call_id": self._TOOL_CALL_ID,
                "tool_name": "delegate",
                "parallel_group_id": self._PARALLEL_GROUP,
            },
            handlers,
        )

        # Node ID is the tool_call_id directly (Phase A cleanup)
        tc_node = await services.graph.get_node(self._TOOL_CALL_ID)
        assert tc_node is not None
        assert "ToolCall" in tc_node["labels"]
        assert "SST_EVENT" in tc_node["labels"]
        assert tc_node["tool_name"] == "delegate"
        assert tc_node["tool_call_id"] == self._TOOL_CALL_ID
        assert tc_node["parallel_group_id"] == self._PARALLEL_GROUP

        # No HAS_TOOL_CALL edge created — no active_iteration_id cursor set in this sequence
        edge = await services.graph.get_edge(self._SESSION_ID, self._TOOL_CALL_ID)
        assert edge is None

    async def test_tool_pre_creates_event_node(self) -> None:
        """After tool:pre DefaultHandler must create a ToolPre Event node with
        a HAS_EVENT edge from the Session. ToolCallHandler creates E08/E09/E10
        edges only when cursors (active_iteration_id, pending_tool_block_ids,
        parallel_group_id) are set — none are set in this sequence.
        ToolCallHandler also creates a SOURCED_FROM edge from the ToolCall node
        to the ToolPreEvent node (data_layer_2 bridge) regardless of cursors."""
        worker, services = _make_worker_and_services()
        handlers = setup_handlers(services)

        await process_event(
            worker,
            "session:start",
            {
                "session_id": self._SESSION_ID,
                "parent_id": None,
                "timestamp": self._T0,
            },
            handlers,
        )
        await process_event(
            worker,
            "tool:pre",
            {
                "session_id": self._SESSION_ID,
                "timestamp": self._T1,
                "tool_call_id": self._TOOL_CALL_ID,
                "tool_name": "delegate",
                "parallel_group_id": self._PARALLEL_GROUP,
            },
            handlers,
        )

        event_node_id = make_node_id(
            self._SESSION_ID, "tool:pre", self._T1, self._TOOL_CALL_ID
        )
        event_node = await services.graph.get_node(event_node_id)
        assert event_node is not None
        assert "ToolPreEvent" in event_node["labels"]
        assert "ToolEvent" in event_node["labels"]
        assert "Event" in event_node["labels"]

        # HAS_EVENT edge: Session → Event (DefaultHandler)
        session_to_event = await services.graph.get_edge(
            self._SESSION_ID, event_node_id
        )
        assert session_to_event is not None

        # SOURCED_FROM edge: ToolCall → ToolPreEvent (ToolCallHandler data_layer_2 bridge)
        tc_to_event = await services.graph.get_edge(self._TOOL_CALL_ID, event_node_id)
        assert tc_to_event is not None
        assert tc_to_event["type"] == "SOURCED_FROM"

    async def test_tool_post_enriches_tool_call_node(self) -> None:
        """After tool:post the ToolCall node must have ended_at set to T2."""
        worker, services = _make_worker_and_services()
        handlers = setup_handlers(services)

        await process_event(
            worker,
            "session:start",
            {
                "session_id": self._SESSION_ID,
                "parent_id": None,
                "timestamp": self._T0,
            },
            handlers,
        )
        await process_event(
            worker,
            "tool:pre",
            {
                "session_id": self._SESSION_ID,
                "timestamp": self._T1,
                "tool_call_id": self._TOOL_CALL_ID,
                "tool_name": "delegate",
                "parallel_group_id": self._PARALLEL_GROUP,
            },
            handlers,
        )
        await process_event(
            worker,
            "tool:post",
            {
                "session_id": self._SESSION_ID,
                "timestamp": self._T2,
                "tool_call_id": self._TOOL_CALL_ID,
                "tool_name": "delegate",
                "parallel_group_id": self._PARALLEL_GROUP,
                "result": {"success": True, "output": "done"},
            },
            handlers,
        )

        # Node ID is the tool_call_id directly (Phase A cleanup)
        tc_node = await services.graph.get_node(self._TOOL_CALL_ID)
        assert tc_node is not None
        assert tc_node.get("ended_at") == self._T2

    async def test_session_end_sets_ended_at(self) -> None:
        """After session:end the Session node must have ended_at set."""
        worker, services = _make_worker_and_services()
        handlers = setup_handlers(services)

        await process_event(
            worker,
            "session:start",
            {
                "session_id": self._SESSION_ID,
                "parent_id": None,
                "timestamp": self._T0,
            },
            handlers,
        )
        await process_event(
            worker,
            "session:end",
            {
                "session_id": self._SESSION_ID,
                "parent_id": None,
                "timestamp": self._T3,
            },
            handlers,
        )

        node = await services.graph.get_node(self._SESSION_ID)
        assert node is not None
        assert "ended_at" in node


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
        assert "SessionResumeEvent" in node["labels"]

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
        assert "ContextCompactionEvent" in event_node["labels"]

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
        assert "CancelRequestedEvent" in event_node["labels"]


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
            await reg.queue_manager.append(
                SESSION_ID,
                _line(
                    "session:start",
                    WORKSPACE,
                    {"session_id": SESSION_ID, "timestamp": T0},
                ),
            )
            await reg.queue_manager.append(
                SESSION_ID,
                _line(
                    "prompt:submit",
                    WORKSPACE,
                    {"session_id": SESSION_ID, "timestamp": T1, "prompt": "Hello"},
                ),
            )
            await reg.queue_manager.append(
                SESSION_ID,
                _line(
                    "session:end",
                    WORKSPACE,
                    {"session_id": SESSION_ID, "timestamp": T6},
                ),
            )
            reg.start_drain(worker)
            assert worker.task is not None
            await asyncio.wait_for(worker.task, timeout=3.0)

        assert reg.active_count() == 0

    async def test_session_end_populates_completed_ring(self) -> None:
        """After session:end the completed ring holds one CompletedSession."""
        reg, worker = _make_registry_and_worker()

        with patch(
            "context_intelligence_server.registry.process_event",
            new_callable=AsyncMock,
        ):
            await reg.queue_manager.append(
                SESSION_ID,
                _line(
                    "session:start",
                    WORKSPACE,
                    {"session_id": SESSION_ID, "timestamp": T0},
                ),
            )
            await reg.queue_manager.append(
                SESSION_ID,
                _line(
                    "session:end",
                    WORKSPACE,
                    {"session_id": SESSION_ID, "timestamp": T6},
                ),
            )
            reg.start_drain(worker)
            assert worker.task is not None
            await asyncio.wait_for(worker.task, timeout=3.0)

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
            await reg.queue_manager.append(
                SESSION_ID,
                _line(
                    "session:start",
                    WORKSPACE,
                    {"session_id": SESSION_ID, "timestamp": T0},
                ),
            )
            await reg.queue_manager.append(
                SESSION_ID,
                _line(
                    "session:end",
                    WORKSPACE,
                    {"session_id": SESSION_ID, "timestamp": T6},
                ),
            )
            reg.start_drain(worker)
            assert worker.task is not None
            await asyncio.wait_for(worker.task, timeout=3.0)

        response = build_status_response(reg, time.time() - 60)
        assert "completed_sessions" in response
        assert len(response["completed_sessions"]) == 1
        assert response["completed_sessions"][0]["session_id"] == SESSION_ID
        assert response["completed_sessions"][0]["events_processed"] > 0
