"""E2E reproduction for issue #484: `agent` dropped on the child-first ordering.

Runs against an ISOLATED, throwaway Neo4j container (the ``neo4j_container``
fixture in ``tests/neo4j/conftest.py`` — random ports, ``remove=True``, torn
down after the session). NEVER touches the production/shared store.

The race
--------
A spawned sub-session's ``agent`` name arrives on the PARENT's
``delegate:agent_spawned`` event (top-level ``agent``). The CHILD's own
``session:start`` carries no top-level ``agent``. Both are processed through
``ensure_session_node`` (pipeline step 2). ``ensure_session_node``'s "node
already exists" (Tier-2) branch historically upserted only
``{labels, status, session_id}`` — so when the CHILD's ``session:start`` created
the node first, the PARENT's later ``delegate:agent_spawned`` (which DOES carry
``{"agent": ...}``) hit the Tier-2 branch and its ``agent`` was silently dropped,
leaving ``:Session.agent`` permanently empty.

This test drives the real handlers across two independent ``Neo4jGraphStore``
instances sharing one Neo4j (exactly the two-drainer condition), forcing the
CHILD-first ordering deterministically, then asserts ``agent`` is persisted.

RED before the services.py fix, GREEN after.

Run: uv run pytest tests/neo4j/test_agent_field_ordering_race.py -v -m neo4j
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest

from context_intelligence_server.handlers.data_layer_2.session import SessionHandler
from context_intelligence_server.handlers.data_layer_3.delegation import (
    DelegationHandler,
)
from context_intelligence_server.neo4j_store import (
    Neo4jGraphStore,
    ensure_neo4j_schema,
)
from context_intelligence_server.services import HookStateService

pytestmark = pytest.mark.neo4j


async def _neo4j_agent(store: Neo4jGraphStore, node_id: str) -> str | None:
    """Read the `agent` property of a node straight from Neo4j (not the buffer)."""
    rows = await store.execute_query(
        "MATCH (n) WHERE n.node_id = $id AND n.workspace = $workspace "
        "RETURN n.agent AS agent",
        {"id": node_id, "workspace": store.workspace},
        workspace="*",
    )
    return rows[0]["agent"] if rows else None


def _ts(n: int = 0) -> str:
    return f"2026-01-01T00:{n:02d}:00Z"


@pytest.mark.neo4j
class TestAgentFieldChildFirstOrdering:
    """#484: agent must survive when the child's session:start lands first."""

    async def test_child_start_before_parent_spawn_persists_agent(
        self, neo4j_container: dict[str, Any]
    ) -> None:
        """CHILD session:start creates the node first (no agent); the PARENT's
        later delegate:agent_spawned must still persist `agent` onto it."""
        auth = (neo4j_container["user"], neo4j_container["password"])
        bolt = neo4j_container["bolt_url"]
        ws = f"test-agent-484-{uuid.uuid4().hex[:8]}"

        from neo4j import AsyncGraphDatabase

        driver = AsyncGraphDatabase.driver(bolt, auth=auth)
        await ensure_neo4j_schema(driver)
        await driver.close()

        parent_id = f"parent-{uuid.uuid4().hex[:8]}"
        child_id = f"child-{uuid.uuid4().hex[:8]}"
        tool_call_id = f"tc-{uuid.uuid4().hex[:8]}"
        expected_agent = "foundation:git-ops"

        # --- CHILD's drainer resources ---
        child_store = Neo4jGraphStore(uri=bolt, auth=auth, workspace=ws)
        child_services = HookStateService(workspace=ws, graph_store=child_store)
        session_handler_child = SessionHandler(child_services)

        # --- PARENT's drainer resources ---
        parent_store = Neo4jGraphStore(uri=bolt, auth=auth, workspace=ws)
        parent_services = HookStateService(workspace=ws, graph_store=parent_store)
        await parent_services.ensure_session_node(parent_id, {})
        delegation_handler = DelegationHandler(parent_services)

        # Step 1 — CHILD's own session:start creates the sub-session node FIRST,
        # carrying NO top-level agent (agent name is only nested in metadata).
        await session_handler_child(
            "session:start",
            {
                "session_id": child_id,
                "parent_id": parent_id,
                "timestamp": _ts(1),
                "metadata": {"agent_name": expected_agent},
            },
        )
        await child_store.flush()

        # Precondition: node exists in Neo4j but has no agent yet.
        assert await _neo4j_agent(child_store, child_id) is None

        # Step 2 — PARENT's delegate:agent_spawned arrives LATER, carrying the
        # top-level agent. This is the only event that supplies `agent` to
        # ensure_session_node, and it now hits the Tier-2 existing-node branch.
        await delegation_handler(
            "delegate:agent_spawned",
            {
                "session_id": parent_id,
                "parent_session_id": parent_id,
                "sub_session_id": child_id,
                "agent": expected_agent,
                "tool_call_id": tool_call_id,
                "timestamp": _ts(0),
            },
        )
        await parent_store.flush()

        # Assert — the sub-session node carries the agent end-to-end in Neo4j.
        verify_store = Neo4jGraphStore(uri=bolt, auth=auth, workspace=ws)
        try:
            actual = await _neo4j_agent(verify_store, child_id)
            assert actual == expected_agent, (
                f"#484 REPRODUCED: sub-session {child_id} has agent={actual!r}, "
                f"expected {expected_agent!r}. The parent's delegate:agent_spawned "
                f"agent value was dropped by ensure_session_node's Tier-2 branch."
            )
        finally:
            await verify_store.close()
            await child_store.close()
            await parent_store.close()
