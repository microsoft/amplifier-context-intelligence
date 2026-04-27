"""Integration tests — delegation and skill load via full 12-enricher pipeline.

Tests verify the complete event processing pipeline from raw events through
to graph state for data_layer_3 handlers, covering:
- TestDelegationViaFullPipeline: delegate lifecycle creates correct nodes/properties
- TestSkillLoadViaFullPipeline: skill load lifecycle creates correct nodes/edges

Uses the full setup_handlers() pipeline (8 Layer 2 + DelegationHandler +
SkillLoadHandler + 2 stubs = 12 enrichers total).
"""

from __future__ import annotations

import types
from typing import Any

from context_intelligence_server.pipeline import process_event, setup_handlers
from context_intelligence_server.services import HookStateService


# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------

WORKSPACE = "integ-test-l3-workspace"
SESSION_ID = "session-l3-001"
PARENT_SESSION_ID = "session-parent-l3-001"
SUB_SESSION_ID = "session-sub-l3-001"
TOOL_CALL_ID = "tc-l3-001"
AGENT_NAME = "code-agent"
SKILL_NAME = "bash-skill"

# Ordered timestamps
T0 = "2026-01-01T00:00:00.000000000+00:00"
T1 = "2026-01-01T00:00:01.000000000+00:00"
T2 = "2026-01-01T00:00:02.000000000+00:00"
T3 = "2026-01-01T00:00:03.000000000+00:00"


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


# ===========================================================================
# TestDelegationViaFullPipeline
# ===========================================================================


class TestDelegationViaFullPipeline:
    """delegate:agent_spawned and lifecycle events via the full 12-enricher pipeline."""

    async def test_agent_spawned_creates_delegation_node(self) -> None:
        """After delegate:agent_spawned via pipeline, Delegation node exists with
        Delegation + SST_EVENT labels and agent, context_depth, context_scope properties.
        """
        worker, services = _make_worker_and_services()
        handlers = setup_handlers(services)

        await process_event(
            worker,
            "delegate:agent_spawned",
            {
                "session_id": PARENT_SESSION_ID,
                "parent_session_id": PARENT_SESSION_ID,
                "sub_session_id": SUB_SESSION_ID,
                "tool_call_id": TOOL_CALL_ID,
                "agent": AGENT_NAME,
                "timestamp": T0,
                "context_depth": "all",
                "context_scope": "full",
            },
            handlers,
        )

        delegation_id = f"{PARENT_SESSION_ID}::delegation::{TOOL_CALL_ID}"
        node = await services.graph.get_node(delegation_id)
        assert node is not None, f"Delegation node must exist at '{delegation_id}'"
        assert "Delegation" in node["labels"]
        assert "SST_EVENT" in node["labels"]
        assert node["agent"] == AGENT_NAME
        assert node["context_depth"] == "all"
        assert node["context_scope"] == "full"

    async def test_agent_spawned_creates_agent_concept_node(self) -> None:
        """After delegate:agent_spawned via pipeline, Agent:SST_CONCEPT node exists
        at AGENT_NAME with Agent and SST_CONCEPT labels but NOT SST_EVENT label.
        """
        worker, services = _make_worker_and_services()
        handlers = setup_handlers(services)

        await process_event(
            worker,
            "delegate:agent_spawned",
            {
                "session_id": PARENT_SESSION_ID,
                "parent_session_id": PARENT_SESSION_ID,
                "sub_session_id": SUB_SESSION_ID,
                "tool_call_id": TOOL_CALL_ID,
                "agent": AGENT_NAME,
                "timestamp": T0,
            },
            handlers,
        )

        node = await services.graph.get_node(AGENT_NAME)
        assert node is not None, f"Agent node must exist at '{AGENT_NAME}'"
        assert "Agent" in node["labels"]
        assert "SST_CONCEPT" in node["labels"]
        assert "SST_EVENT" not in node["labels"], (
            "Agent concept node must NOT have SST_EVENT label"
        )

    async def test_full_delegation_lifecycle_spawned_to_completed(self) -> None:
        """Spawn at T0 then complete at T1; node has ended_at==T1 and success is True."""
        worker, services = _make_worker_and_services()
        handlers = setup_handlers(services)

        # Spawn at T0
        await process_event(
            worker,
            "delegate:agent_spawned",
            {
                "session_id": PARENT_SESSION_ID,
                "parent_session_id": PARENT_SESSION_ID,
                "sub_session_id": SUB_SESSION_ID,
                "tool_call_id": TOOL_CALL_ID,
                "agent": AGENT_NAME,
                "timestamp": T0,
            },
            handlers,
        )

        # Complete at T1
        await process_event(
            worker,
            "delegate:agent_completed",
            {
                "session_id": PARENT_SESSION_ID,
                "parent_session_id": PARENT_SESSION_ID,
                "tool_call_id": TOOL_CALL_ID,
                "timestamp": T1,
            },
            handlers,
        )

        delegation_id = f"{PARENT_SESSION_ID}::delegation::{TOOL_CALL_ID}"
        node = await services.graph.get_node(delegation_id)
        assert node is not None
        assert node["ended_at"] == T1
        assert node["success"] is True


# ===========================================================================
# TestSkillLoadViaFullPipeline
# ===========================================================================


class TestSkillLoadViaFullPipeline:
    """skill:loaded and skill:unloaded events via the full 12-enricher pipeline."""

    async def test_skill_loaded_during_iteration_creates_e05(self) -> None:
        """E05 (HAS_SKILL_LOAD/CONTAINS) edge exists from active_iteration_id to
        skill_load_id when skill:loaded is sent after provider:request sets the
        active_iteration_id cursor.

        If this test fails, verify that IterationHandler sets
        services.data_layer_2.active_iteration_id on provider:request — if None,
        E05 is skipped.
        """
        worker, services = _make_worker_and_services()
        handlers = setup_handlers(services)

        # session:start — create session node
        await process_event(
            worker,
            "session:start",
            {
                "session_id": SESSION_ID,
                "parent_id": None,
                "timestamp": T0,
            },
            handlers,
        )

        # execution:start — sets execution_start_ts cursor in OrchestratorRunHandler
        await process_event(
            worker,
            "execution:start",
            {
                "session_id": SESSION_ID,
                "timestamp": T1,
            },
            handlers,
        )

        # provider:request — IterationHandler sets active_iteration_id cursor
        await process_event(
            worker,
            "provider:request",
            {
                "session_id": SESSION_ID,
                "timestamp": T2,
            },
            handlers,
        )

        # skill:loaded — SkillLoadHandler uses active_iteration_id to create E05
        await process_event(
            worker,
            "skill:loaded",
            {
                "session_id": SESSION_ID,
                "skill_name": SKILL_NAME,
                "timestamp": T2,
                "content_length": 2048,
                "source": "workspace",
                "version": "1.0.0",
                "context": "inline",
                "disable_model_invocation": False,
                "user_invocable": True,
            },
            handlers,
        )

        # iteration_id = SESSION_ID::iteration::1 (first iteration)
        iteration_id = f"{SESSION_ID}::iteration::1"
        skill_load_id = f"{SESSION_ID}::skill::{SKILL_NAME}::{T2}"

        # Verify active_iteration_id was set by IterationHandler
        assert services.data_layer_2.active_iteration_id == iteration_id, (
            "IterationHandler must set active_iteration_id on provider:request"
        )

        # Verify E05 edge exists
        edge = await services.graph.get_edge(iteration_id, skill_load_id)
        assert edge is not None, (
            f"E05 HAS_SKILL_LOAD edge must exist: {iteration_id} -> {skill_load_id}"
        )
        assert edge["type"] == "HAS_SKILL_LOAD"
        assert edge["sst_semantic"] == "CONTAINS"

    async def test_skill_loaded_before_iteration_connects_to_session(
        self,
    ) -> None:
        """SkillLoad node exists and E05 connects to Session when skill:loaded is
        sent without a prior provider:request (OQ-L3-3 fix: Session is the fallback parent).
        """
        worker, services = _make_worker_and_services()
        handlers = setup_handlers(services)

        # session:start only — no execution:start or provider:request
        await process_event(
            worker,
            "session:start",
            {
                "session_id": SESSION_ID,
                "parent_id": None,
                "timestamp": T0,
            },
            handlers,
        )

        # skill:loaded without prior provider:request
        await process_event(
            worker,
            "skill:loaded",
            {
                "session_id": SESSION_ID,
                "skill_name": SKILL_NAME,
                "timestamp": T1,
                "content_length": 1024,
                "source": "workspace",
                "version": "1.0.0",
                "context": "inline",
                "disable_model_invocation": False,
                "user_invocable": False,
            },
            handlers,
        )

        skill_load_id = f"{SESSION_ID}::skill::{SKILL_NAME}::{T1}"

        # SkillLoad node must exist
        node = await services.graph.get_node(skill_load_id)
        assert node is not None, f"SkillLoad node must exist at '{skill_load_id}'"
        assert "SkillLoad" in node["labels"]

        # E05 must connect to Session (not float) — OQ-L3-3 fix
        edge = await services.graph.get_edge(SESSION_ID, skill_load_id)
        assert edge is not None, (
            "E05 HAS_SKILL_LOAD edge must exist from Session when no iteration is active "
            "(OQ-L3-3 fix: Session is the fallback parent)"
        )
        assert edge["type"] == "HAS_SKILL_LOAD"
        assert edge["sst_semantic"] == "CONTAINS"

    async def test_skill_unloaded_sets_ended_at_via_pipeline(self) -> None:
        """Load at T2 then unload at T3 via pipeline; node['ended_at'] == T3."""
        worker, services = _make_worker_and_services()
        handlers = setup_handlers(services)

        # skill:loaded at T2
        await process_event(
            worker,
            "skill:loaded",
            {
                "session_id": SESSION_ID,
                "skill_name": SKILL_NAME,
                "timestamp": T2,
                "content_length": 512,
                "source": "workspace",
                "version": "1.0.0",
                "context": "inline",
                "disable_model_invocation": False,
                "user_invocable": True,
            },
            handlers,
        )

        # skill:unloaded at T3
        await process_event(
            worker,
            "skill:unloaded",
            {
                "session_id": SESSION_ID,
                "skill_name": SKILL_NAME,
                "timestamp": T3,
            },
            handlers,
        )

        skill_load_id = f"{SESSION_ID}::skill::{SKILL_NAME}::{T2}"
        node = await services.graph.get_node(skill_load_id)
        assert node is not None
        assert node["ended_at"] == T3
