"""Tier 3 — Neo4j-backed integration tests for data_layer_2.

These tests validate that SST labels, relationship types, and sst_semantic
properties round-trip correctly through Neo4jGraphStore into a real Neo4j
instance. They address the test double gap where GraphState (in-memory)
cannot verify Neo4j-specific behaviors like multi-label indexing, Cypher
relationship TYPE matching, and property persistence.

Every test class is marked with @pytest.mark.neo4j and requires a live
Neo4j container (provided by neo4j_container fixture).

Run explicitly:
    uv run pytest tests/neo4j/ -v -m neo4j
"""

from __future__ import annotations

from typing import Any

import pytest

from context_intelligence_server.handlers.data_layer_2.orchestrator_run import (
    OrchestratorRunHandler,
)
from context_intelligence_server.handlers.data_layer_2.prompt import PromptHandler
from context_intelligence_server.handlers.data_layer_2.session import SessionHandler
from context_intelligence_server.handlers.data_layer_2.tool_call import ToolCallHandler


# ---------------------------------------------------------------------------
# 1. TestNeo4jSessionSST
# ---------------------------------------------------------------------------


@pytest.mark.neo4j
class TestNeo4jSessionSST:
    """Session node stores :SST_EVENT label and is queryable in real Neo4j."""

    async def test_session_node_has_sst_event_label(self, neo4j_services: Any) -> None:
        """Session:SST_EVENT node created by session:start is queryable via MATCH."""
        handler = SessionHandler(neo4j_services)
        await handler(
            "session:start",
            {"session_id": "neo4j-test-s1", "timestamp": "2026-01-01T00:00:00Z"},
        )
        # Flush buffered writes to Neo4j
        await neo4j_services.graph.flush()

        # Query Neo4j directly via the store's own query mechanism
        records = await neo4j_services.graph.execute_query(
            "MATCH (s:Session:SST_EVENT {session_id: $sid}) RETURN s.session_id AS sid",
            {"sid": "neo4j-test-s1"},
            workspace="*",
        )
        assert len(records) > 0, "Session:SST_EVENT node must be queryable"
        assert records[0]["sid"] == "neo4j-test-s1"


# ---------------------------------------------------------------------------
# 2. TestNeo4jForkedEdge
# ---------------------------------------------------------------------------


@pytest.mark.neo4j
class TestNeo4jForkedEdge:
    """FORKED edge has sst_semantic = 'LEADS_TO' property in real Neo4j."""

    async def test_forked_edge_sst_semantic(self, neo4j_services: Any) -> None:
        """session:fork must create FORKED edge with sst_semantic 'LEADS_TO' in Neo4j."""
        # Ensure parent session exists
        await neo4j_services.ensure_session_node("neo4j-parent-s1", {})
        handler = SessionHandler(neo4j_services)
        await handler(
            "session:fork",
            {
                "session_id": "neo4j-fork-s1",
                "parent_id": "neo4j-parent-s1",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        await neo4j_services.graph.flush()

        records = await neo4j_services.graph.execute_query(
            "MATCH (p:Session)-[r:FORKED]->(f:Session) "
            "WHERE f.session_id = $sid "
            "RETURN r.sst_semantic AS sem",
            {"sid": "neo4j-fork-s1"},
            workspace="*",
        )
        assert len(records) > 0, "FORKED edge must exist"
        assert records[0]["sem"] == "LEADS_TO"


# ---------------------------------------------------------------------------
# 3. TestNeo4jToolCallNodeId
# ---------------------------------------------------------------------------


@pytest.mark.neo4j
class TestNeo4jToolCallNodeId:
    """ToolCall node stored with native tool_call_id UUID, queryable."""

    async def test_tool_call_node_queryable_by_tool_call_id(
        self, neo4j_services: Any
    ) -> None:
        """ToolCall node must be queryable by tool_call_id property in Neo4j."""
        handler = ToolCallHandler(neo4j_services)
        await handler(
            "tool:pre",
            {
                "session_id": "neo4j-test-s1",
                "timestamp": "2026-01-01T00:00:00Z",
                "tool_call_id": "tc-uuid-001",
                "tool_name": "bash",
            },
        )
        await neo4j_services.graph.flush()

        records = await neo4j_services.graph.execute_query(
            "MATCH (t:ToolCall:SST_EVENT {tool_call_id: $tcid}) "
            "RETURN t.tool_name AS name",
            {"tcid": "tc-uuid-001"},
            workspace="*",
        )
        assert len(records) > 0, "ToolCall node must be queryable by tool_call_id"
        assert records[0]["name"] == "bash"


# ---------------------------------------------------------------------------
# 4. TestNeo4jMountPlanNode
# ---------------------------------------------------------------------------


@pytest.mark.neo4j
class TestNeo4jMountPlanNode:
    """MountPlan:SST_THING node created and queryable after session:fork."""

    async def test_mount_plan_node_queryable(self, neo4j_services: Any) -> None:
        """session:fork must create MountPlan:SST_THING node queryable in Neo4j."""
        await neo4j_services.ensure_session_node("neo4j-parent-s2", {})
        handler = SessionHandler(neo4j_services)
        await handler(
            "session:fork",
            {
                "session_id": "neo4j-fork-s2",
                "parent_id": "neo4j-parent-s2",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        await neo4j_services.graph.flush()

        records = await neo4j_services.graph.execute_query(
            "MATCH (s:Session)-[:HAS_PART]->(m:MountPlan:SST_THING) "
            "WHERE s.session_id = $sid "
            "RETURN m",
            {"sid": "neo4j-fork-s2"},
            workspace="*",
        )
        assert len(records) > 0, "MountPlan:SST_THING node must be queryable after fork"


# ---------------------------------------------------------------------------
# 5. TestNeo4jTurnFlowChain
# ---------------------------------------------------------------------------


@pytest.mark.neo4j
class TestNeo4jTurnFlowChain:
    """Full turn chain: Prompt -> TRIGGERS -> OrchestratorRun -> ENABLES -> Prompt."""

    async def test_full_turn_chain(self, neo4j_services: Any) -> None:
        """Complete turn chain must be queryable in Neo4j.

        Sequence:
        1. prompt:submit (Prompt_1) — creates node + E05, sets last_prompt_id
        2. execution:start — creates OrchestratorRun_1 + E01 + E14 (TRIGGERS)
        3. orchestrator:complete — sets last_completed_orch_run_id
        4. prompt:submit (Prompt_2) — creates node + E05 + E15 (ENABLES)
        """
        sid = "neo4j-turn-s1"
        await neo4j_services.ensure_session_node(sid, {})

        prompt_handler = PromptHandler(neo4j_services)
        orch_handler = OrchestratorRunHandler(neo4j_services)

        # 1. First prompt
        await prompt_handler(
            "prompt:submit",
            {"session_id": sid, "timestamp": "t1", "prompt": "hello"},
        )

        # 2. Execution start (should create E14: Prompt_1 -> OrchestratorRun_1)
        await orch_handler(
            "execution:start",
            {"session_id": sid, "timestamp": "t2"},
        )

        # 3. Orchestrator complete (sets last_completed_orch_run_id)
        await orch_handler(
            "orchestrator:complete",
            {
                "session_id": sid,
                "timestamp": "t3",
                "orchestrator": "default",
                "turn_count": 1,
            },
        )

        # 4. Second prompt (should create E15: OrchestratorRun_1 -> Prompt_2)
        await prompt_handler(
            "prompt:submit",
            {"session_id": sid, "timestamp": "t4", "prompt": "next"},
        )

        await neo4j_services.graph.flush()

        # Verify E14: Prompt_1 -[:TRIGGERS]-> OrchestratorRun_1
        e14_records = await neo4j_services.graph.execute_query(
            "MATCH (p:Prompt)-[r:TRIGGERS]->(o:OrchestratorRun) "
            "WHERE p.session_id = $sid "
            "RETURN r.sst_semantic AS sem",
            {"sid": sid},
            workspace="*",
        )
        assert len(e14_records) > 0, "E14 TRIGGERS edge must exist in Neo4j"
        assert e14_records[0]["sem"] == "LEADS_TO"

        # Verify E15: OrchestratorRun_1 -[:ENABLES]-> Prompt_2
        e15_records = await neo4j_services.graph.execute_query(
            "MATCH (o:OrchestratorRun)-[r:ENABLES]->(p:Prompt) "
            "WHERE o.session_id = $sid "
            "RETURN r.sst_semantic AS sem",
            {"sid": sid},
            workspace="*",
        )
        assert len(e15_records) > 0, "E15 ENABLES edge must exist in Neo4j"
        assert e15_records[0]["sem"] == "LEADS_TO"
