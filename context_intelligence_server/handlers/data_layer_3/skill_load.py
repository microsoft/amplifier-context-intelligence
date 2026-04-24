"""SkillLoadHandler — correlates skill load events into SkillLoad nodes.

Each skill load lifecycle produces up to two events:
  skill:loaded   — skill was loaded into the agent's context
  skill:unloaded — skill was unloaded from the agent's context

This handler creates a single SkillLoad node per load instance (keyed by
'{session_id}::skill::{skill_name}::{timestamp}') with the SST_EVENT label,
and enriches it with ended_at on skill:unloaded. The E05 edge
(Iteration -[HAS_SKILL_LOAD]-> SkillLoad) is created when active_iteration_id
is set at load time.

Note: skills:discovered is a catalog event (no SkillLoad instance) and is
handled only by DefaultHandler.
"""

from __future__ import annotations

import logging
from typing import Any

from context_intelligence_server.protocol import HookResult
from context_intelligence_server.services import HookStateService
from context_intelligence_server.utils import make_node_id

logger = logging.getLogger(__name__)


class SkillLoadHandler:
    """Enricher handler for skill load lifecycle events.

    Correlates skill:loaded and skill:unloaded events into a single SkillLoad
    node per load instance. The SkillLoad node ID is a compound key:
    '{session_id}::skill::{skill_name}::{timestamp}'.

    Handler-local state:
        _active_skill_nodes: dict[str, str] — maps skill_name -> skill_load_id.
        Required because skill:unloaded only carries skill_name (no load
        timestamp), so the node_id cannot be reconstructed without the cache.
    """

    handled_events: frozenset[str] = frozenset({"skill:loaded", "skill:unloaded"})

    def __init__(self, services: HookStateService) -> None:
        self.services = services
        self._active_skill_nodes: dict[str, str] = {}

    async def __call__(self, event: str, data: dict[str, Any]) -> HookResult:
        """Handle a skill lifecycle event.

        Extracts session_id and skill_name; returns continue without mutations
        if either is missing.
        """
        session_id: str | None = data.get("session_id")
        skill_name: str | None = data.get("skill_name")

        if not session_id or not skill_name:
            return HookResult(action="continue")

        if event == "skill:loaded":
            await self._handle_loaded(session_id, skill_name, data)
        else:
            await self._handle_unloaded(session_id, skill_name, data)

        return HookResult(action="continue")

    async def _handle_loaded(
        self,
        session_id: str,
        skill_name: str,
        data: dict[str, Any],
    ) -> None:
        """Create SkillLoad node and conditional E05 edge.

        Creates:
        - SkillLoad:SST_EVENT node at compound ID with all 9 lifted fields
        - E05: Iteration -[HAS_SKILL_LOAD {sst_semantic: 'CONTAINS'}]-> SkillLoad
          (only when active_iteration_id is set; otherwise SkillLoad floats — OQ-L3-3)
        - Populates _active_skill_nodes[skill_name] = skill_load_id cache
        - SOURCED_FROM: SkillLoad -> data_layer_1 skill:loaded event
        """
        timestamp: str = data.get("timestamp", "")
        skill_load_id = f"{session_id}::skill::{skill_name}::{timestamp}"

        # Build SkillLoad:SST_EVENT node with all 9 lifted fields
        node_data: dict[str, Any] = {
            "labels": ["SkillLoad", "SST_EVENT"],
            "skill_name": skill_name,
            "started_at": timestamp,
            "content_length": data.get("content_length"),
            "source": data.get("source"),
            "version": data.get("version"),
            "context": data.get("context"),
            "disable_model_invocation": data.get("disable_model_invocation"),
            "user_invocable": data.get("user_invocable"),
            "auto_loaded": data.get("auto_loaded", False),
        }
        await self.services.graph.upsert_node(skill_load_id, node_data)

        # E05: Iteration -[HAS_SKILL_LOAD {sst_semantic: 'CONTAINS'}]-> SkillLoad
        # Only created when there is an active iteration cursor; otherwise floats (OQ-L3-3)
        iteration_id = self.services.data_layer_2.active_iteration_id
        if iteration_id is not None:
            await self.services.graph.upsert_edge(
                iteration_id,
                skill_load_id,
                {"type": "HAS_SKILL_LOAD", "sst_semantic": "CONTAINS"},
            )

        # Cache skill_name -> skill_load_id for use by skill:unloaded
        self._active_skill_nodes[skill_name] = skill_load_id

        # SOURCED_FROM bridge: SkillLoad -> data_layer_1 skill:loaded event node
        data_layer_1_node_id = make_node_id(
            session_id, "skill:loaded", timestamp, skill_name
        )
        await self.services.graph.upsert_edge(
            skill_load_id, data_layer_1_node_id, {"type": "SOURCED_FROM"}
        )

    async def _handle_unloaded(
        self,
        session_id: str,
        skill_name: str,
        data: dict[str, Any],
    ) -> None:
        """Enrich existing SkillLoad node with ended_at on skill:unloaded.

        Pops skill_name from the cache to get the skill_load_id; if absent,
        this is an orphan unload event (no matching loaded event) — no-op.

        Adds:
        - ended_at on the existing SkillLoad node
        - SOURCED_FROM: SkillLoad -> data_layer_1 skill:unloaded event
        """
        skill_load_id = self._active_skill_nodes.pop(skill_name, None)
        if skill_load_id is None:
            # Orphan unload event — no matching skill:loaded observed; no-op
            return

        timestamp: str = data.get("timestamp", "")

        # Upsert SkillLoad node with ended_at
        await self.services.graph.upsert_node(
            skill_load_id,
            {"labels": ["SkillLoad", "SST_EVENT"], "ended_at": timestamp},
        )

        # SOURCED_FROM bridge: SkillLoad -> data_layer_1 skill:unloaded event node
        data_layer_1_node_id = make_node_id(
            session_id, "skill:unloaded", timestamp, skill_name
        )
        await self.services.graph.upsert_edge(
            skill_load_id, data_layer_1_node_id, {"type": "SOURCED_FROM"}
        )
