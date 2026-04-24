"""Tests for SkillLoadHandler — skill:loaded with SkillLoad node, E05 edge, cache.

Covers (Task 7 classes):
1. TestSkillLoadHandlerHandledEvents — handled_events == frozenset({'skill:loaded','skill:unloaded'}),
   skills:discovered NOT in handled_events
2. TestSkillLoadedCreatesNode — node exists at compound ID, SkillLoad label, SST_EVENT label,
   all 9 lifted fields with correct values, auto_loaded defaults to False when absent,
   auto_loaded True when present
3. TestSkillLoadedSourcedFrom — SOURCED_FROM edge targets
   make_node_id(session_id, 'skill:loaded', timestamp, skill_name)
4. TestE05HasSkillLoadEdge — E05 created when active_iteration_id set (HAS_SKILL_LOAD/CONTAINS),
   no E05 when active_iteration_id is None, _active_skill_nodes cache populated after loaded
"""

from __future__ import annotations

from context_intelligence_server.handlers.data_layer_3.skill_load import (
    SkillLoadHandler,
)
from context_intelligence_server.services import HookStateService
from context_intelligence_server.utils import make_node_id


# ---------------------------------------------------------------------------
# 1. TestSkillLoadHandlerHandledEvents
# ---------------------------------------------------------------------------


class TestSkillLoadHandlerHandledEvents:
    """handled_events must be exactly frozenset({'skill:loaded', 'skill:unloaded'})."""

    def test_handled_events_equals_frozenset(self) -> None:
        """handled_events must be exactly frozenset({'skill:loaded', 'skill:unloaded'})."""
        assert SkillLoadHandler.handled_events == frozenset(
            {"skill:loaded", "skill:unloaded"}
        )

    def test_skills_discovered_not_in_handled_events(self) -> None:
        """skills:discovered must NOT be in handled_events (catalog event, not an instance)."""
        assert "skills:discovered" not in SkillLoadHandler.handled_events


# ---------------------------------------------------------------------------
# 2. TestSkillLoadedCreatesNode
# ---------------------------------------------------------------------------


class TestSkillLoadedCreatesNode:
    """skill:loaded creates SkillLoad:SST_EVENT node at compound ID with all 9 lifted fields."""

    def _make_loaded_data(
        self,
        *,
        session_id: str = "sess-001",
        skill_name: str = "python-standards",
        timestamp: str = "2026-01-01T00:00:00Z",
        include_auto_loaded: bool = False,
        auto_loaded_value: bool = False,
    ) -> dict:
        data: dict = {
            "session_id": session_id,
            "skill_name": skill_name,
            "timestamp": timestamp,
            "content_length": 4096,
            "source": "workspace",
            "version": "1.0.0",
            "context": "inline",
            "disable_model_invocation": False,
            "user_invocable": True,
        }
        if include_auto_loaded:
            data["auto_loaded"] = auto_loaded_value
        return data

    async def test_node_exists_at_compound_id(self, services: HookStateService) -> None:
        """SkillLoad node must exist at '{session_id}::skill::{skill_name}::{timestamp}'."""
        handler = SkillLoadHandler(services)
        data = self._make_loaded_data()
        await handler("skill:loaded", data)

        node_id = "sess-001::skill::python-standards::2026-01-01T00:00:00Z"
        node = await services.graph.get_node(node_id)
        assert node is not None, f"SkillLoad node must exist at '{node_id}'"

    async def test_node_has_skill_load_label(self, services: HookStateService) -> None:
        """SkillLoad node must have 'SkillLoad' in labels."""
        handler = SkillLoadHandler(services)
        data = self._make_loaded_data()
        await handler("skill:loaded", data)

        node_id = "sess-001::skill::python-standards::2026-01-01T00:00:00Z"
        node = await services.graph.get_node(node_id)
        assert node is not None
        assert "SkillLoad" in node["labels"]

    async def test_node_has_sst_event_label(self, services: HookStateService) -> None:
        """SkillLoad node must have 'SST_EVENT' in labels."""
        handler = SkillLoadHandler(services)
        data = self._make_loaded_data()
        await handler("skill:loaded", data)

        node_id = "sess-001::skill::python-standards::2026-01-01T00:00:00Z"
        node = await services.graph.get_node(node_id)
        assert node is not None
        assert "SST_EVENT" in node["labels"]

    async def test_node_has_all_lifted_fields_with_correct_values(
        self, services: HookStateService
    ) -> None:
        """SkillLoad node must carry all 9 lifted fields with correct values."""
        handler = SkillLoadHandler(services)
        data = self._make_loaded_data(
            session_id="sess-001",
            skill_name="python-standards",
            timestamp="2026-01-01T00:00:00Z",
            include_auto_loaded=False,
        )
        await handler("skill:loaded", data)

        node_id = "sess-001::skill::python-standards::2026-01-01T00:00:00Z"
        node = await services.graph.get_node(node_id)
        assert node is not None
        assert node["skill_name"] == "python-standards"
        assert node["started_at"] == "2026-01-01T00:00:00Z"
        assert node["content_length"] == 4096
        assert node["source"] == "workspace"
        assert node["version"] == "1.0.0"
        assert node["context"] == "inline"
        assert node["disable_model_invocation"] is False
        assert node["user_invocable"] is True
        assert node["auto_loaded"] is False

    async def test_auto_loaded_defaults_to_false_when_absent(
        self, services: HookStateService
    ) -> None:
        """auto_loaded must default to False when not present in event data."""
        handler = SkillLoadHandler(services)
        data = self._make_loaded_data(include_auto_loaded=False)
        # Confirm auto_loaded is truly absent from data
        assert "auto_loaded" not in data

        await handler("skill:loaded", data)

        node_id = "sess-001::skill::python-standards::2026-01-01T00:00:00Z"
        node = await services.graph.get_node(node_id)
        assert node is not None
        assert node["auto_loaded"] is False

    async def test_auto_loaded_true_when_present(
        self, services: HookStateService
    ) -> None:
        """auto_loaded must be True when explicitly present in event data."""
        handler = SkillLoadHandler(services)
        data = self._make_loaded_data(include_auto_loaded=True, auto_loaded_value=True)
        await handler("skill:loaded", data)

        node_id = "sess-001::skill::python-standards::2026-01-01T00:00:00Z"
        node = await services.graph.get_node(node_id)
        assert node is not None
        assert node["auto_loaded"] is True


# ---------------------------------------------------------------------------
# 3. TestSkillLoadedSourcedFrom
# ---------------------------------------------------------------------------


class TestSkillLoadedSourcedFrom:
    """SOURCED_FROM edge must target make_node_id(session_id, 'skill:loaded', timestamp, skill_name)."""

    async def test_sourced_from_uses_skill_name_as_disambiguator(
        self, services: HookStateService
    ) -> None:
        """SOURCED_FROM edge from SkillLoad node targets make_node_id with skill_name disambiguator."""
        handler = SkillLoadHandler(services)
        session_id = "sess-001"
        skill_name = "python-standards"
        timestamp = "2026-01-01T00:00:00Z"
        data = {
            "session_id": session_id,
            "skill_name": skill_name,
            "timestamp": timestamp,
            "content_length": 1024,
            "source": "workspace",
            "version": "1.0.0",
            "context": "inline",
            "disable_model_invocation": False,
            "user_invocable": True,
        }
        await handler("skill:loaded", data)

        skill_load_id = f"{session_id}::skill::{skill_name}::{timestamp}"
        expected_target = make_node_id(
            session_id, "skill:loaded", timestamp, skill_name
        )
        edge = await services.graph.get_edge(skill_load_id, expected_target)
        assert edge is not None, (
            "SOURCED_FROM edge must exist from SkillLoad node to data_layer_1 event node"
        )
        assert edge.get("type") == "SOURCED_FROM"


# ---------------------------------------------------------------------------
# 4. TestE05HasSkillLoadEdge
# ---------------------------------------------------------------------------


class TestE05HasSkillLoadEdge:
    """E05 HAS_SKILL_LOAD edge between Iteration and SkillLoad when active_iteration_id is set."""

    def _make_loaded_data(
        self, session_id: str = "sess-001", skill_name: str = "python-standards"
    ) -> dict:
        return {
            "session_id": session_id,
            "skill_name": skill_name,
            "timestamp": "2026-01-01T00:00:00Z",
            "content_length": 512,
            "source": "workspace",
            "version": "1.0.0",
            "context": "inline",
            "disable_model_invocation": False,
            "user_invocable": False,
        }

    async def test_e05_created_when_active_iteration_id_set(
        self, services: HookStateService
    ) -> None:
        """E05: iteration_id -[HAS_SKILL_LOAD {sst_semantic: 'CONTAINS'}]-> skill_load_id when cursor set."""
        iteration_id = "sess-001::iteration::1"
        services.data_layer_2.active_iteration_id = iteration_id
        handler = SkillLoadHandler(services)
        data = self._make_loaded_data()
        await handler("skill:loaded", data)

        skill_load_id = "sess-001::skill::python-standards::2026-01-01T00:00:00Z"
        edge = await services.graph.get_edge(iteration_id, skill_load_id)
        assert edge is not None, "E05 edge (Iteration -> SkillLoad) must exist"
        assert edge.get("type") == "HAS_SKILL_LOAD"
        assert edge.get("sst_semantic") == "CONTAINS"

    async def test_no_e05_when_active_iteration_id_is_none(
        self, services: HookStateService
    ) -> None:
        """No E05 edge when active_iteration_id is None — SkillLoad floats (OQ-L3-3)."""
        assert services.data_layer_2.active_iteration_id is None
        handler = SkillLoadHandler(services)
        data = self._make_loaded_data()
        await handler("skill:loaded", data)

        has_skill_load_edges = [
            edge
            for edge in services.graph._edges.values()
            if edge.get("type") == "HAS_SKILL_LOAD"
        ]
        assert len(has_skill_load_edges) == 0, (
            "No E05 HAS_SKILL_LOAD edge must be created when active_iteration_id is None"
        )

    async def test_active_skill_nodes_cache_populated_after_loaded(
        self, services: HookStateService
    ) -> None:
        """_active_skill_nodes[skill_name] must be set to skill_load_id after skill:loaded."""
        handler = SkillLoadHandler(services)
        data = self._make_loaded_data()
        await handler("skill:loaded", data)

        skill_load_id = "sess-001::skill::python-standards::2026-01-01T00:00:00Z"
        assert handler._active_skill_nodes.get("python-standards") == skill_load_id
