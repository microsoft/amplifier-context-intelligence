"""Cross-handler per-session state for data_layer_2 enrichers."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class DataLayer2State:
    """Cross-handler per-session state for data_layer_2 enrichers.

    All fields are scalars — no session_id keying needed since each HookStateService
    is already per-session.
    """

    # OrchestratorRun identity
    execution_start_ts: str | None = None

    # Monotonic per-session run counter, incremented on each execution:start and
    # folded into orch_run_id. Two runs that share an identical execution_start_ts
    # (coarse clock, or a replayed execution:start) would otherwise collide on the
    # same orch_run_id and MERGE-overwrite each other's nodes. Lives here so the
    # durable cursor persists it across a worker rebuild.
    orch_run_seq: int = 0

    # The full orch_run_id of the active run, including the tiebreaker. Set at
    # execution:start and read back by execution:end/orchestrator:complete and
    # the Iteration/ContentBlock handlers, so every node in one run agrees on the
    # same id without any site recomputing it from execution_start_ts alone.
    active_orch_run_id: str | None = None

    # Iteration cursor read by ContentBlockHandler + ToolCallHandler
    active_iteration_id: str | None = None

    # Maps block.id → block_node_id for tool_call-type blocks (E09 correlation)
    pending_tool_block_ids: dict[str, str] = field(default_factory=dict)

    # E14 Prompt→OrchestratorRun turn-flow cursor
    last_prompt_id: str | None = None

    # E15 OrchestratorRun→Prompt turn-flow cursor
    last_completed_orch_run_id: str | None = None

    # Iteration counter — incremented on each provider:request; combined with
    # execution_start_ts (via IterationHandler) to compute the run-scoped
    # iteration_id '{session_id}::orch_run::{execution_start_ts}::{seq}::iteration::{iteration_count}'
    # (falls back to the bare '{session_id}::iteration::{iteration_count}' shape when no
    # orchestrator run is active). Scoped per-session, not reset per run: uniqueness across
    # runs comes from the orch_run_id prefix, not from this counter —
    # resetting it would collide with ContentBlockHandler's block_node_id derivation, which
    # keys solely off this counter's value, not the run.
    iteration_count: int = 0
