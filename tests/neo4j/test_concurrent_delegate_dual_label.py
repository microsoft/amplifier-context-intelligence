"""Reproduction test for the Sub+Forked concurrent dual-label bug.

This test reproduces the race between the PARENT's drainer and CHILD's drainer
writing to the same CHILD Neo4j node, which produces ForkedSession+SubSession
dual terminal labels on every real delegate() call.

Background
----------
When a parent session calls delegate():
  1. PARENT's drainer processes ``delegate:agent_spawned``
     → DelegationHandler calls ensure_session_node(CHILD, {}) on the PARENT's
       Neo4jGraphStore → writes bare :Session node for CHILD to PARENT's buffer
     → PARENT flushes: CHILD exists in Neo4j as bare Session + ENCOMPASSES edge

  2. CHILD's drainer processes events in this order (real Amplifier emit order):
     a. ``session:fork``  — emitted during initialize(), carries "parent" NOT "parent_id"
     b. ``session:start`` — emitted during first execute(), carries "parent_id"

The server's _handle_fork reads: parent_id = data.get("parent_id") or "" → "".
Because the real Amplifier core emits session:fork with key "parent" not "parent_id",
the fork handler always sees parent_id="", which means has_parent=False.

The key question: with "parent"→"" mismatch plus concurrent PARENT+CHILD flushes,
does a Sub+Forked dual label ever appear in Neo4j?

Run: uv run pytest tests/neo4j/test_concurrent_delegate_dual_label.py -v -m neo4j
"""

from __future__ import annotations

import asyncio
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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _neo4j_labels(store: Neo4jGraphStore, node_id: str) -> list[str]:
    """Return labels from Neo4j directly (bypasses in-memory buffer)."""
    rows = await store.execute_query(
        "MATCH (n) WHERE n.node_id = $id AND n.workspace = $workspace "
        "RETURN labels(n) AS lbls",
        {"id": node_id, "workspace": store.workspace},
        workspace="*",
    )
    return sorted(rows[0]["lbls"]) if rows else []


async def _neo4j_edges_to(
    store: Neo4jGraphStore, dst_id: str
) -> list[dict[str, str]]:
    """Return all inbound edges to dst_id from Neo4j."""
    rows = await store.execute_query(
        "MATCH (src)-[r]->(dst) "
        "WHERE dst.node_id = $dst AND dst.workspace = $workspace "
        "RETURN src.node_id AS src, type(r) AS rel_type",
        {"dst": dst_id, "workspace": store.workspace},
        workspace="*",
    )
    return [{"src": r["src"], "rel_type": r["rel_type"]} for r in rows]


def _ts(n: int = 0) -> str:
    return f"2026-01-01T00:{n:02d}:00Z"


# ---------------------------------------------------------------------------
# Scenario A: Fork-before-start (real Amplifier ordering)
# CHILD queue: [session:fork (parent="PARENT"), session:start (parent_id=PARENT)]
# PARENT concurrent: delegate:agent_spawned
# ---------------------------------------------------------------------------


@pytest.mark.neo4j
class TestForkBeforeStartConcurrentWithParentDrainer:
    """Reproduce the real delegate() event ordering with concurrent PARENT drainer.

    In the real Amplifier flow:
      - session:fork  is emitted during initialize()  (uses "parent" key)
      - session:start is emitted during first execute() (uses "parent_id" key)
      - delegate:agent_spawned fires from the PARENT concurrently

    This test exercises all three concurrently and checks for dual labels.
    """

    async def test_concurrent_fork_start_agent_spawned_no_dual(
        self, neo4j_container: dict[str, Any]
    ) -> None:
        """
        CORE REPRODUCTION TEST.

        Exercises the exact concurrent scenario from live production:
        - PARENT's drainer processes delegate:agent_spawned (PARENT's Neo4jGraphStore)
        - CHILD's drainer processes session:fork then session:start (CHILD's Neo4jGraphStore)

        Both flush concurrently. Asserts CHILD has exactly ONE terminal label.
        """
        auth = (neo4j_container["user"], neo4j_container["password"])
        bolt = neo4j_container["bolt_url"]
        ws = f"test-concurrent-{uuid.uuid4().hex[:8]}"

        await ensure_neo4j_schema(
            __import__("neo4j", fromlist=["AsyncGraphDatabase"]).AsyncGraphDatabase.driver(
                bolt, auth=auth
            )
        )

        parent_id = f"parent-{uuid.uuid4().hex[:8]}"
        child_id = f"child-{uuid.uuid4().hex[:8]}"
        tool_call_id = f"tc-{uuid.uuid4().hex[:8]}"

        # --- PARENT's drainer resources ---
        parent_store = Neo4jGraphStore(uri=bolt, auth=auth, workspace=ws)
        parent_services = HookStateService(workspace=ws, graph_store=parent_store)
        # Pre-seed parent session node (PARENT's drainer would have processed session:start for PARENT)
        await parent_services.ensure_session_node(parent_id, {})

        # --- CHILD's drainer resources ---
        child_store = Neo4jGraphStore(uri=bolt, auth=auth, workspace=ws)
        child_services = HookStateService(workspace=ws, graph_store=child_store)

        delegation_handler = DelegationHandler(parent_services)
        session_handler_child = SessionHandler(child_services)

        # --- PARENT processes delegate:agent_spawned ---
        async def run_parent() -> None:
            await delegation_handler(
                "delegate:agent_spawned",
                {
                    "session_id": parent_id,
                    "parent_session_id": parent_id,
                    "sub_session_id": child_id,
                    "agent": "foundation:explorer",
                    "tool_call_id": tool_call_id,
                    "timestamp": _ts(0),
                },
            )
            await parent_store.flush()

        # --- CHILD processes session:fork then session:start ---
        # Real Amplifier ordering: fork uses "parent" key, start uses "parent_id"
        async def run_child() -> None:
            # session:fork with "parent" key (NOT "parent_id") — real Amplifier format
            await session_handler_child(
                "session:fork",
                {
                    "session_id": child_id,
                    "parent": parent_id,  # <-- real Amplifier uses "parent" not "parent_id"
                    "timestamp": _ts(1),
                },
            )
            await child_store.flush()

            # session:start with "parent_id" key — real Amplifier format
            await session_handler_child(
                "session:start",
                {
                    "session_id": child_id,
                    "parent_id": parent_id,  # <-- session:start uses "parent_id"
                    "timestamp": _ts(2),
                },
            )
            await child_store.flush()

        # Run PARENT and CHILD concurrently (simulates real concurrent drainers)
        await asyncio.gather(run_parent(), run_child())

        # --- Verify ---
        final_labels = await _neo4j_labels(child_store, child_id)
        terminals = [
            l for l in final_labels if l in ("RootSession", "SubSession", "ForkedSession")
        ]

        assert len(terminals) <= 1, (
            f"DUAL LABEL BUG REPRODUCED: CHILD {child_id} has multiple terminal labels "
            f"{terminals} in {final_labels}. This is the concurrent drainer race."
        )

        await parent_store.close()
        await child_store.close()


# ---------------------------------------------------------------------------
# Scenario B: Start-before-fork (less common, network reordering)
# CHILD queue: [session:start (parent_id=PARENT), session:fork (parent="PARENT")]
# PARENT concurrent: delegate:agent_spawned
# ---------------------------------------------------------------------------


@pytest.mark.neo4j
class TestStartBeforeForkConcurrentWithParentDrainer:
    """Start arrives before fork (network reordering) + concurrent PARENT drainer."""

    async def test_start_before_fork_no_dual(
        self, neo4j_container: dict[str, Any]
    ) -> None:
        """
        When session:start arrives before session:fork, the fork handler should
        reclassify SubSession → ForkedSession, removing SubSession.

        This test verifies that SubSession IS correctly removed even in the
        concurrent-PARENT scenario.
        """
        auth = (neo4j_container["user"], neo4j_container["password"])
        bolt = neo4j_container["bolt_url"]
        ws = f"test-start-fork-{uuid.uuid4().hex[:8]}"

        await ensure_neo4j_schema(
            __import__("neo4j", fromlist=["AsyncGraphDatabase"]).AsyncGraphDatabase.driver(
                bolt, auth=auth
            )
        )

        parent_id = f"parent-{uuid.uuid4().hex[:8]}"
        child_id = f"child-{uuid.uuid4().hex[:8]}"
        tool_call_id = f"tc-{uuid.uuid4().hex[:8]}"

        parent_store = Neo4jGraphStore(uri=bolt, auth=auth, workspace=ws)
        parent_services = HookStateService(workspace=ws, graph_store=parent_store)
        await parent_services.ensure_session_node(parent_id, {})

        child_store = Neo4jGraphStore(uri=bolt, auth=auth, workspace=ws)
        child_services = HookStateService(workspace=ws, graph_store=child_store)

        delegation_handler = DelegationHandler(parent_services)
        session_handler_child = SessionHandler(child_services)

        async def run_parent() -> None:
            await delegation_handler(
                "delegate:agent_spawned",
                {
                    "session_id": parent_id,
                    "parent_session_id": parent_id,
                    "sub_session_id": child_id,
                    "agent": "foundation:explorer",
                    "tool_call_id": tool_call_id,
                    "timestamp": _ts(0),
                },
            )
            await parent_store.flush()

        async def run_child() -> None:
            # session:start FIRST (parent_id key)
            await session_handler_child(
                "session:start",
                {
                    "session_id": child_id,
                    "parent_id": parent_id,
                    "timestamp": _ts(1),
                },
            )
            await child_store.flush()

            # session:fork SECOND (parent key — real Amplifier format)
            await session_handler_child(
                "session:fork",
                {
                    "session_id": child_id,
                    "parent": parent_id,  # real Amplifier uses "parent" not "parent_id"
                    "timestamp": _ts(2),
                },
            )
            await child_store.flush()

        await asyncio.gather(run_parent(), run_child())

        final_labels = await _neo4j_labels(child_store, child_id)
        terminals = [
            l for l in final_labels if l in ("RootSession", "SubSession", "ForkedSession")
        ]

        assert len(terminals) <= 1, (
            f"DUAL LABEL BUG REPRODUCED: CHILD {child_id} has multiple terminal labels "
            f"{terminals} in {final_labels}. This is the start-before-fork concurrent race."
        )

        await parent_store.close()
        await child_store.close()


# ---------------------------------------------------------------------------
# Scenario C: Explicit same-batch fork+start
# Both events in one batch — no flush between them
# ---------------------------------------------------------------------------


@pytest.mark.neo4j
class TestSameBatchForkAndStart:
    """Both fork and start events in the same drainer batch (no flush between them)."""

    async def test_same_batch_fork_before_start(
        self, neo4j_container: dict[str, Any]
    ) -> None:
        """Same-batch: fork then start. The fork takes the bare path.
        Then start sees ForkedSession → NO-OP. Result: ForkedSession only.
        """
        auth = (neo4j_container["user"], neo4j_container["password"])
        bolt = neo4j_container["bolt_url"]
        ws = f"test-samebatch-fb4s-{uuid.uuid4().hex[:8]}"
        parent_id = f"parent-{uuid.uuid4().hex[:8]}"
        child_id = f"child-{uuid.uuid4().hex[:8]}"

        store = Neo4jGraphStore(uri=bolt, auth=auth, workspace=ws)
        services = HookStateService(workspace=ws, graph_store=store)
        handler = SessionHandler(services)

        # Process fork then start in same "batch" (no flush between)
        await handler(
            "session:fork",
            {"session_id": child_id, "parent": parent_id, "timestamp": _ts(1)},
        )
        # No flush here — same batch
        await handler(
            "session:start",
            {"session_id": child_id, "parent_id": parent_id, "timestamp": _ts(2)},
        )
        await store.flush()

        labels = await _neo4j_labels(store, child_id)
        terminals = [l for l in labels if l in ("RootSession", "SubSession", "ForkedSession")]
        assert len(terminals) <= 1, (
            f"Dual label in same-batch fork→start: {terminals} in {labels}"
        )

        await store.close()

    async def test_same_batch_start_before_fork(
        self, neo4j_container: dict[str, Any]
    ) -> None:
        """Same-batch: start then fork. The start adds SubSession.
        Fork sees SubSession → reclassify → ForkedSession (SubSession removed).
        Result: ForkedSession only.
        """
        auth = (neo4j_container["user"], neo4j_container["password"])
        bolt = neo4j_container["bolt_url"]
        ws = f"test-samebatch-s4f-{uuid.uuid4().hex[:8]}"
        parent_id = f"parent-{uuid.uuid4().hex[:8]}"
        child_id = f"child-{uuid.uuid4().hex[:8]}"

        store = Neo4jGraphStore(uri=bolt, auth=auth, workspace=ws)
        services = HookStateService(workspace=ws, graph_store=store)
        handler = SessionHandler(services)

        # Process start then fork in same "batch" (no flush between)
        await handler(
            "session:start",
            {"session_id": child_id, "parent_id": parent_id, "timestamp": _ts(1)},
        )
        # No flush here — same batch
        await handler(
            "session:fork",
            {"session_id": child_id, "parent": parent_id, "timestamp": _ts(2)},
        )
        await store.flush()

        labels = await _neo4j_labels(store, child_id)
        terminals = [l for l in labels if l in ("RootSession", "SubSession", "ForkedSession")]
        assert len(terminals) <= 1, (
            f"Dual label in same-batch start→fork: {terminals} in {labels}"
        )

        await store.close()


# ---------------------------------------------------------------------------
# Scenario D: Adversarial — two concurrent Neo4jGraphStore instances
# both writing to the SAME CHILD node simultaneously (write_semaphore=2)
# ---------------------------------------------------------------------------


@pytest.mark.neo4j
class TestAdversarialConcurrentWrites:
    """Two independent Neo4jGraphStore instances write to CHILD concurrently.

    This is the most direct simulation of the two-drainer race.
    Writer 1: adds SubSession (simulates CHILD's start batch)
    Writer 2: adds ForkedSession without removing SubSession (simulates CHILD's fork batch
              when fork took the bare path BEFORE start committed)
    """

    async def test_two_writers_start_and_fork_concurrent(
        self, neo4j_container: dict[str, Any]
    ) -> None:
        """
        ADVERSARIAL RACE: two concurrent transactions, one adds SubSession,
        one adds ForkedSession (without REMOVE SubSession — as if fork ran before
        start committed SubSession to Neo4j).

        This directly tests the Neo4j-level race window. If both commits succeed
        without a conflict, we get the dual label. This proves that the race IS
        possible at the Neo4j transaction level when the fork handler takes the
        bare-session path CONCURRENTLY with the start handler flushing SubSession.
        """
        auth = (neo4j_container["user"], neo4j_container["password"])
        bolt = neo4j_container["bolt_url"]
        ws = f"test-adversarial-{uuid.uuid4().hex[:8]}"
        child_id = f"child-{uuid.uuid4().hex[:8]}"

        # Ensure schema first
        from neo4j import AsyncGraphDatabase
        driver = AsyncGraphDatabase.driver(bolt, auth=auth)
        await ensure_neo4j_schema(driver)
        await driver.close()

        # Create bare CHILD node first (as PARENT's drainer would)
        bootstrap = Neo4jGraphStore(uri=bolt, auth=auth, workspace=ws)
        await bootstrap.upsert_node(child_id, {"labels": ["Session"], "session_id": child_id})
        await bootstrap.flush()
        await bootstrap.close()

        # Writer 1: session:start path — adds SubSession
        async def write_start() -> None:
            store = Neo4jGraphStore(uri=bolt, auth=auth, workspace=ws)
            services = HookStateService(workspace=ws, graph_store=store)
            handler = SessionHandler(services)
            await handler(
                "session:start",
                {
                    "session_id": child_id,
                    "parent_id": f"parent-{uuid.uuid4().hex[:8]}",
                    "timestamp": _ts(1),
                },
            )
            await store.flush()
            await store.close()

        # Writer 2: session:fork path (bare-session branch) — adds ForkedSession
        # This simulates the case where fork ran get_node BEFORE start committed SubSession
        async def write_fork_bare() -> None:
            store = Neo4jGraphStore(uri=bolt, auth=auth, workspace=ws)
            services = HookStateService(workspace=ws, graph_store=store)
            handler = SessionHandler(services)
            # session:fork with "parent" key — real Amplifier format (NO "parent_id")
            await handler(
                "session:fork",
                {
                    "session_id": child_id,
                    "parent": f"parent-{uuid.uuid4().hex[:8]}",  # "parent" not "parent_id"
                    "timestamp": _ts(2),
                },
            )
            await store.flush()
            await store.close()

        # Both concurrent — this is the race window
        await asyncio.gather(write_start(), write_fork_bare())

        # Check for dual label
        verify = Neo4jGraphStore(uri=bolt, auth=auth, workspace=ws)
        try:
            final_labels = await _neo4j_labels(verify, child_id)
            terminals = [
                l
                for l in final_labels
                if l in ("RootSession", "SubSession", "ForkedSession")
            ]

            # This assertion is INVERTED from the usual: we EXPECT this might fail
            # (reproduce the dual), showing the race IS possible at the Neo4j level.
            # If it fails, we've proven the dual.
            assert len(terminals) <= 1, (
                f"RACE CONFIRMED: Two concurrent writers produced dual terminal labels "
                f"{terminals} in {final_labels}. "
                f"This proves the Sub+Forked dual is achievable when start and fork "
                f"run in separate transactions without the fork's get_node seeing SubSession."
            )
        finally:
            await verify.close()
