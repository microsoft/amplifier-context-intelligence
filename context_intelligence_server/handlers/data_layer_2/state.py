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

    # Iteration cursor read by ContentBlockHandler + ToolCallHandler
    active_iteration_id: str | None = None

    # Maps block.id → block_node_id for tool_call-type blocks (E09 correlation)
    pending_tool_block_ids: dict[str, str] = field(default_factory=dict)

    # E14 Prompt→OrchestratorRun turn-flow cursor
    last_prompt_id: str | None = None

    # E15 OrchestratorRun→Prompt turn-flow cursor
    last_completed_orch_run_id: str | None = None

    # Iteration counter — incremented on each provider:request; used to compute
    # iteration_id as '{session_id}::iteration::{iteration_count}'
    iteration_count: int = 0
