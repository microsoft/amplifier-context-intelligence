"""Integration test for turn chain — E14 (TRIGGERS) and E15 (ENABLES) across turns.

Verifies that the full E14/E15 turn chain works correctly through the complete
pipeline (setup_handlers + process_event) using DataLayer2State cursor sharing
between PromptHandler and OrchestratorRunHandler.

Event sequence (11 steps):
    prompt:submit P1 → execution:start → provider:request → llm:request →
    llm:response → execution:end → orchestrator:complete →
    prompt:submit P2 (E15: R1→P2) → execution:start (E14: P2→R2) →
    orchestrator:complete → prompt:submit P3 (E15: R2→P3)

Expected outcome — two possibilities:
1. ALL 6 PASS — The E14/E15 cursor logic is correct and the turn chain works
   through the full pipeline. Skip Task 12.
2. E15 tests FAIL — The cursor sharing between PromptHandler and
   OrchestratorRunHandler is broken. Proceed to Task 12.
"""

from __future__ import annotations

import types
from typing import Any

from context_intelligence_server.pipeline import process_event, setup_handlers
from context_intelligence_server.services import HookStateService


# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------

WORKSPACE = "turn-chain-test-workspace"
SESSION_ID = "session-turn-chain-001"

# Ordered timestamps for the full sequence
T1 = "2026-01-01T00:00:01.000000000+00:00"  # prompt:submit P1
T2 = "2026-01-01T00:00:02.000000000+00:00"  # execution:start R1
T3 = "2026-01-01T00:00:03.000000000+00:00"  # provider:request
T4 = "2026-01-01T00:00:04.000000000+00:00"  # llm:request
T5 = "2026-01-01T00:00:05.000000000+00:00"  # llm:response
T6 = "2026-01-01T00:00:06.000000000+00:00"  # execution:end
T7 = "2026-01-01T00:00:07.000000000+00:00"  # orchestrator:complete
T8 = "2026-01-01T00:00:08.000000000+00:00"  # prompt:submit P2  (E15: R1→P2)
T9 = "2026-01-01T00:00:09.000000000+00:00"  # execution:start R2 (E14: P2→R2)
T10 = "2026-01-01T00:00:10.000000000+00:00"  # orchestrator:complete R2
T11 = "2026-01-01T00:00:11.000000000+00:00"  # prompt:submit P3  (E15: R2→P3)

# Computed node IDs matching the handlers' key conventions
PROMPT_1_ID = f"{SESSION_ID}::prompt::{T1}"
RUN_1_ID = f"{SESSION_ID}::orch_run::{T2}"
PROMPT_2_ID = f"{SESSION_ID}::prompt::{T8}"
RUN_2_ID = f"{SESSION_ID}::orch_run::{T9}"
PROMPT_3_ID = f"{SESSION_ID}::prompt::{T11}"


# ---------------------------------------------------------------------------
# Full-sequence helper
# ---------------------------------------------------------------------------


async def _process_full_sequence() -> HookStateService:
    """Process the complete three-prompt, two-turn sequence through the full pipeline.

    Creates HookStateService(workspace=WORKSPACE), a SimpleNamespace worker,
    calls setup_handlers(services), and processes all 11 events through
    process_event.  Returns the services instance for graph assertions.
    """
    services = HookStateService(workspace=WORKSPACE)
    worker: Any = types.SimpleNamespace(services=services)
    handlers = setup_handlers(services)

    # Step 1: prompt:submit P1
    # → PromptHandler creates P1, sets last_prompt_id = P1
    # → No E15 (last_completed_orch_run_id is None)
    await process_event(
        worker,
        "prompt:submit",
        {
            "session_id": SESSION_ID,
            "timestamp": T1,
            "prompt": "First prompt",
        },
        handlers,
    )

    # Step 2: execution:start → OrchestratorRun R1
    # → OrchestratorRunHandler creates R1, E01 (Session→R1), SOURCED_FROM
    # → E14: P1 -[:TRIGGERS]-> R1  (last_prompt_id = P1 is set)
    # → Sets execution_start_ts = T2
    await process_event(
        worker,
        "execution:start",
        {
            "session_id": SESSION_ID,
            "timestamp": T2,
        },
        handlers,
    )

    # Step 3: provider:request → Iteration node
    # → IterationHandler creates Iteration, E06 (R1→Iteration), SOURCED_FROM
    await process_event(
        worker,
        "provider:request",
        {
            "session_id": SESSION_ID,
            "timestamp": T3,
        },
        handlers,
    )

    # Step 4: llm:request → enriches Iteration
    await process_event(
        worker,
        "llm:request",
        {
            "session_id": SESSION_ID,
            "timestamp": T4,
            "provider": "anthropic",
            "model": "claude-3-5-sonnet",
            "message_count": 1,
            "has_system": True,
        },
        handlers,
    )

    # Step 5: llm:response → enriches Iteration with usage
    await process_event(
        worker,
        "llm:response",
        {
            "session_id": SESSION_ID,
            "timestamp": T5,
            "usage": {"input_tokens": 100, "output_tokens": 50},
        },
        handlers,
    )

    # Step 6: execution:end → enriches R1 with ended_at / status
    await process_event(
        worker,
        "execution:end",
        {
            "session_id": SESSION_ID,
            "timestamp": T6,
            "status": "completed",
        },
        handlers,
    )

    # Step 7: orchestrator:complete
    # → OrchestratorRunHandler enriches R1 with name/turn_count/completed_at
    # → Creates Orchestrator concept node, E03 (Session→Orchestrator), SOURCED_FROM
    # → Sets last_completed_orch_run_id = R1
    # → Clears execution_start_ts = None
    await process_event(
        worker,
        "orchestrator:complete",
        {
            "session_id": SESSION_ID,
            "timestamp": T7,
            "orchestrator": "main-orchestrator",
            "turn_count": 1,
        },
        handlers,
    )

    # Step 8: prompt:submit P2
    # → PromptHandler creates P2, E05 (Session→P2), SOURCED_FROM
    # → E15: R1 -[:ENABLES]-> P2  (last_completed_orch_run_id = R1 is set)
    # → Clears last_completed_orch_run_id = None
    # → Sets last_prompt_id = P2
    await process_event(
        worker,
        "prompt:submit",
        {
            "session_id": SESSION_ID,
            "timestamp": T8,
            "prompt": "Second prompt",
        },
        handlers,
    )

    # Step 9: execution:start → OrchestratorRun R2
    # → OrchestratorRunHandler creates R2, E01 (Session→R2), SOURCED_FROM
    # → E14: P2 -[:TRIGGERS]-> R2  (last_prompt_id = P2 is set)
    # → Sets execution_start_ts = T9
    await process_event(
        worker,
        "execution:start",
        {
            "session_id": SESSION_ID,
            "timestamp": T9,
        },
        handlers,
    )

    # Step 10: orchestrator:complete (second turn)
    # → OrchestratorRunHandler enriches R2
    # → Sets last_completed_orch_run_id = R2
    # → Clears execution_start_ts = None
    await process_event(
        worker,
        "orchestrator:complete",
        {
            "session_id": SESSION_ID,
            "timestamp": T10,
            "orchestrator": "main-orchestrator",
            "turn_count": 1,
        },
        handlers,
    )

    # Step 11: prompt:submit P3
    # → PromptHandler creates P3, E05 (Session→P3), SOURCED_FROM
    # → E15: R2 -[:ENABLES]-> P3  (last_completed_orch_run_id = R2 is set)
    # → Clears last_completed_orch_run_id = None
    # → Sets last_prompt_id = P3
    await process_event(
        worker,
        "prompt:submit",
        {
            "session_id": SESSION_ID,
            "timestamp": T11,
            "prompt": "Third prompt",
        },
        handlers,
    )

    return services


# ===========================================================================
# TestTurnChainE14AndE15
# ===========================================================================


class TestTurnChainE14AndE15:
    """Integration tests for the E14/E15 turn chain across two turns.

    Processes a complete three-prompt, two-turn sequence through the full
    pipeline and verifies:
    - E14 TRIGGERS edges: Prompt -[:TRIGGERS {sst_semantic: 'LEADS_TO'}]-> OrchestratorRun
    - E15 ENABLES edges:  OrchestratorRun -[:ENABLES {sst_semantic: 'LEADS_TO'}]-> Prompt
    - Exact edge counts (2 TRIGGERS, 2 ENABLES) across the full sequence
    """

    async def test_e14_turn_1_prompt_triggers_run(self) -> None:
        """P1 -[:TRIGGERS {sst_semantic: 'LEADS_TO'}]-> R1."""
        services = await _process_full_sequence()
        edge = await services.graph.get_edge(PROMPT_1_ID, RUN_1_ID)
        assert edge is not None, (
            f"E14 TRIGGERS edge from '{PROMPT_1_ID}' to '{RUN_1_ID}' must exist. "
            f"last_prompt_id was set before execution:start so E14 must be created."
        )
        assert edge.get("type") == "TRIGGERS", (
            f"E14 edge must have type='TRIGGERS'. Got: {edge.get('type')!r}"
        )
        assert edge.get("sst_semantic") == "LEADS_TO", (
            f"E14 edge must have sst_semantic='LEADS_TO'. Got: {edge.get('sst_semantic')!r}"
        )

    async def test_e14_turn_2_prompt_triggers_run(self) -> None:
        """P2 -[:TRIGGERS {sst_semantic: 'LEADS_TO'}]-> R2."""
        services = await _process_full_sequence()
        edge = await services.graph.get_edge(PROMPT_2_ID, RUN_2_ID)
        assert edge is not None, (
            f"E14 TRIGGERS edge from '{PROMPT_2_ID}' to '{RUN_2_ID}' must exist. "
            f"last_prompt_id was set before execution:start so E14 must be created."
        )
        assert edge.get("type") == "TRIGGERS", (
            f"E14 edge must have type='TRIGGERS'. Got: {edge.get('type')!r}"
        )
        assert edge.get("sst_semantic") == "LEADS_TO", (
            f"E14 edge must have sst_semantic='LEADS_TO'. Got: {edge.get('sst_semantic')!r}"
        )

    async def test_e15_run_1_enables_prompt_2(self) -> None:
        """R1 -[:ENABLES {sst_semantic: 'LEADS_TO'}]-> P2."""
        services = await _process_full_sequence()
        edge = await services.graph.get_edge(RUN_1_ID, PROMPT_2_ID)
        assert edge is not None, (
            f"E15 ENABLES edge from '{RUN_1_ID}' to '{PROMPT_2_ID}' must exist. "
            f"orchestrator:complete sets last_completed_orch_run_id=R1; "
            f"prompt:submit P2 reads that cursor and creates E15."
        )
        assert edge.get("type") == "ENABLES", (
            f"E15 edge must have type='ENABLES'. Got: {edge.get('type')!r}"
        )
        assert edge.get("sst_semantic") == "LEADS_TO", (
            f"E15 edge must have sst_semantic='LEADS_TO'. Got: {edge.get('sst_semantic')!r}"
        )

    async def test_e15_run_2_enables_prompt_3(self) -> None:
        """R2 -[:ENABLES {sst_semantic: 'LEADS_TO'}]-> P3."""
        services = await _process_full_sequence()
        edge = await services.graph.get_edge(RUN_2_ID, PROMPT_3_ID)
        assert edge is not None, (
            f"E15 ENABLES edge from '{RUN_2_ID}' to '{PROMPT_3_ID}' must exist. "
            f"orchestrator:complete sets last_completed_orch_run_id=R2; "
            f"prompt:submit P3 reads that cursor and creates E15."
        )
        assert edge.get("type") == "ENABLES", (
            f"E15 edge must have type='ENABLES'. Got: {edge.get('type')!r}"
        )
        assert edge.get("sst_semantic") == "LEADS_TO", (
            f"E15 edge must have sst_semantic='LEADS_TO'. Got: {edge.get('sst_semantic')!r}"
        )

    async def test_total_triggers_count(self) -> None:
        """Exactly 2 TRIGGERS edges exist across the full sequence."""
        services = await _process_full_sequence()
        triggers_edges = [
            (src, dst)
            for (src, dst), edge_data in services.graph._edges.items()
            if edge_data.get("type") == "TRIGGERS"
        ]
        assert len(triggers_edges) == 2, (
            f"Expected exactly 2 TRIGGERS edges (one per turn). "
            f"Got {len(triggers_edges)}: {triggers_edges}"
        )

    async def test_total_enables_count(self) -> None:
        """Exactly 2 ENABLES edges exist across the full sequence."""
        services = await _process_full_sequence()
        enables_edges = [
            (src, dst)
            for (src, dst), edge_data in services.graph._edges.items()
            if edge_data.get("type") == "ENABLES"
        ]
        assert len(enables_edges) == 2, (
            f"Expected exactly 2 ENABLES edges (one per completed run). "
            f"Got {len(enables_edges)}: {enables_edges}"
        )
