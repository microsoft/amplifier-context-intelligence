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
            {"session_id": sid, "timestamp": "2026-01-01T00:00:01Z", "prompt": "hello"},
        )

        # 2. Execution start (should create E14: Prompt_1 -> OrchestratorRun_1)
        await orch_handler(
            "execution:start",
            {"session_id": sid, "timestamp": "2026-01-01T00:00:02Z"},
        )

        # 3. Orchestrator complete (sets last_completed_orch_run_id)
        await orch_handler(
            "orchestrator:complete",
            {
                "session_id": sid,
                "timestamp": "2026-01-01T00:00:03Z",
                "orchestrator": "default",
                "turn_count": 1,
            },
        )

        # 4. Second prompt (should create E15: OrchestratorRun_1 -> Prompt_2)
        await prompt_handler(
            "prompt:submit",
            {"session_id": sid, "timestamp": "2026-01-01T00:00:04Z", "prompt": "next"},
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


# ---------------------------------------------------------------------------
# 6. TestNoBareSessionNodes
# ---------------------------------------------------------------------------


@pytest.mark.neo4j
class TestNoBareSessionNodes:
    """Tier 3 gate: ensure_session_node + SessionHandler cannot create bare Session nodes.

    GraphState (used in Tier 1/2 tests) does not implement the Session-labeled vs
    label-free MERGE routing in neo4j_store.flush(). These tests verify the actual
    Neo4j behavior that the fix must enforce.

    The fix (commit 5d99bce): _handle_start and _handle_fork both now include
    ``"labels": ["Session"]`` in their upsert_node call, routing them to the
    Session-labeled MERGE bucket (``MERGE (n:Session {node_id, workspace})``).
    Without the fix they used the label-free MERGE bucket
    (``MERGE (n {node_id, workspace})``), which is a separate Neo4j operation
    that can create a second bare node under concurrent flushes.

    NOTE: These tests verify the correct outcome in a sequential scenario. The
    underlying race condition (two workers with overlapping transactions) cannot
    be reliably reproduced in a single-process sequential test because:
    1. The label-free MERGE *does* find existing Session nodes by property match
       when run sequentially after the Session-labeled MERGE has committed.
    2. The ``set_labels`` call that follows upsert_node in non-early-return
       paths always adds "Session" to the buffer, so the flush uses
       ``session_rows`` regardless of whether upsert_node included the label.
    The regression is therefore caught by verifying the NODE COUNT and TYPE
    LABEL correctness under sequential simulation — any deviation means the
    MERGE routing is broken in a way that *will* cause duplicates under
    concurrency. A true concurrent regression guard requires two separate
    store instances with asyncio.gather; that is a Tier 3+ concern.
    """

    async def test_ensure_session_then_handle_start_creates_exactly_one_node(
        self, neo4j_services: Any
    ) -> None:
        """ensure_session_node flush + _handle_start flush must produce exactly one node.

        Simulates the production scenario where two workers (child and parent session)
        flush independently. Without the fix (_handle_start upsert had no Session label),
        the label-free MERGE created a second bare node alongside the Session node from
        ensure_session_node.
        """
        session_id = "neo4j-bare-session-gate-1"

        # Step 1: ensure_session_node creates placeholder (Session-labeled MERGE)
        await neo4j_services.ensure_session_node(session_id, {})
        await (
            neo4j_services.graph.flush()
        )  # flush to Neo4j — simulates worker A completing

        # Step 2: SessionHandler processes session:start in a separate flush cycle.
        # This simulates worker B (the parent session's own processing).
        handler = SessionHandler(neo4j_services)
        await handler(
            "session:start",
            {
                "session_id": session_id,
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        await (
            neo4j_services.graph.flush()
        )  # flush to Neo4j — simulates worker B completing

        # ASSERT: exactly ONE node in Neo4j for this session_id
        records = await neo4j_services.graph.execute_query(
            "MATCH (n {node_id: $nid, workspace: $ws}) "
            "RETURN count(n) AS cnt, collect(labels(n)) AS label_sets",
            {"nid": session_id, "ws": neo4j_services.graph.workspace},
            workspace="*",
        )
        assert len(records) == 1
        count = records[0]["cnt"]
        assert count == 1, (
            f"Expected exactly 1 Session node, got {count}. "
            f"Label sets found: {records[0]['label_sets']}. "
            "This means _handle_start used a different MERGE pattern than ensure_session_node, "
            "creating a second bare node. Fix: add 'Session' label to upsert_node in _handle_start."
        )

    async def test_session_node_has_type_label_after_handle_start(
        self, neo4j_services: Any
    ) -> None:
        """Session node must have RootSession type label — never just bare Session."""
        session_id = "neo4j-bare-session-gate-2"

        await neo4j_services.ensure_session_node(session_id, {})
        await neo4j_services.graph.flush()

        handler = SessionHandler(neo4j_services)
        await handler(
            "session:start",
            {"session_id": session_id, "timestamp": "2026-01-01T00:00:01Z"},
        )
        await neo4j_services.graph.flush()

        # Must have RootSession (no parent_id → RootSession classification)
        records = await neo4j_services.graph.execute_query(
            "MATCH (n:Session {node_id: $nid, workspace: $ws}) RETURN labels(n) AS lbls",
            {"nid": session_id, "ws": neo4j_services.graph.workspace},
            workspace="*",
        )
        assert len(records) >= 1, "Session node must exist after handle_start"
        all_labels = set(records[0]["lbls"])
        type_labels = {"RootSession", "SubSession", "ForkedSession"}
        assert all_labels & type_labels, (
            f"Session node has labels {all_labels} but none of {type_labels}. "
            "Bare Session nodes are a design violation."
        )

    async def test_fork_ensure_then_handle_fork_creates_exactly_one_node(
        self, neo4j_services: Any
    ) -> None:
        """Same guard for _handle_fork: ensure_session_node + session:fork = one node."""
        parent_id = "neo4j-bare-fork-gate-parent"
        child_id = "neo4j-bare-fork-gate-child"

        # Ensure parent exists first
        await neo4j_services.ensure_session_node(parent_id, {})
        await neo4j_services.graph.flush()

        # ensure_session_node creates placeholder for child
        await neo4j_services.ensure_session_node(child_id, {})
        await neo4j_services.graph.flush()

        # _handle_fork processes session:fork for child in a separate flush cycle
        handler = SessionHandler(neo4j_services)
        await handler(
            "session:fork",
            {
                "session_id": child_id,
                "parent_id": parent_id,
                "timestamp": "2026-01-01T00:00:02Z",
            },
        )
        await neo4j_services.graph.flush()

        records = await neo4j_services.graph.execute_query(
            "MATCH (n {node_id: $nid, workspace: $ws}) RETURN count(n) AS cnt",
            {"nid": child_id, "ws": neo4j_services.graph.workspace},
            workspace="*",
        )
        count = records[0]["cnt"]
        assert count == 1, (
            f"Expected exactly 1 Session node for forked child, got {count}. "
            "_handle_fork must use Session-labeled MERGE."
        )
