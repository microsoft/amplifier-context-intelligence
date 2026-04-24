"""Cross-handler per-session state for data_layer_3 enrichers."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class DataLayer3State:
    """Cross-handler per-session state for data_layer_3 enrichers.

    All fields are scalars or collections — no session_id keying needed since each
    HookStateService is already per-session.
    """

    # Stack of active RecipeRun IDs — push on recipe:start, pop on recipe:complete.
    # Supports arbitrary nesting of recipe runs.
    active_recipe_run_stack: list[str] = field(default_factory=list)

    # Scalar ID of the innermost active RecipeStep — used for E10 and E11 attribution.
    active_recipe_step_id: str | None = None
