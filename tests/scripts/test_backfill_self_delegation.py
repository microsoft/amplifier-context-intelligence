"""Unit tests for the pure transform in scripts/backfill_self_delegation.py.

These tests exercise the report/write-agnostic core (``compute_flag`` and
``compute_updates``) against in-memory ``GraphState`` fixtures, with the
resolver walk bound to the SAME ``resolve_self_agent`` the live ingestion path
uses (single logic home). No Neo4j required — the live-Neo4j proof lives in
tests/neo4j/test_backfill_self_delegation.py.

Coverage:
- Brick 1 flag three-branch rule, incl. the council-caught ``agent IS NULL -> False``.
- Brick 2 resolved_agent recompute mapping: self->real (parent Delegation),
  self->root, self->forked, self->unresolved, incomplete-colabel->root.
- Idempotency: a plan applied to the rows yields an empty second plan.
"""

from __future__ import annotations

from typing import Any

from context_intelligence_server.handlers.data_layer_3.delegation import (
    resolve_self_agent,
)
from context_intelligence_server.services import GraphState
from scripts.backfill_self_delegation import (
    Plan,
    compute_flag,
    compute_updates,
)

WS = "test-workspace"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _bind_resolver(graph: GraphState):
    """Return an async resolve_fn bound to resolve_self_agent over *graph*."""

    async def _resolve(parent_session_id: str) -> str:
        return await resolve_self_agent(graph, parent_session_id, WS)

    return _resolve


async def _seed_parent_delegation(
    graph: GraphState, node_id: str, sub_session_id: str, agent: str
) -> None:
    """Seed a parent Delegation node (the resolver looks it up by sub_session_id)."""
    await graph.upsert_node(
        node_id,
        {
            "labels": ["Delegation", "SST_EVENT"],
            "agent": agent,
            "parent_session_id": "root-ps",
            "sub_session_id": sub_session_id,
        },
    )


async def _seed_session(graph: GraphState, node_id: str, labels: list[str]) -> None:
    await graph.upsert_node(node_id, {"labels": labels})


def _apply_plan_to_rows(rows: list[dict[str, Any]], plan: Plan) -> list[dict[str, Any]]:
    """Simulate applying the plan back onto the scanned rows (for idempotency)."""
    by_id = {u.node_id: u for u in plan.updates}
    out: list[dict[str, Any]] = []
    for row in rows:
        new = dict(row)
        upd = by_id.get(row["node_id"])
        if upd is not None:
            if upd.flag_changed:
                new["is_self_delegation"] = upd.new_flag
            if upd.resolved_changed:
                new["resolved_agent"] = upd.new_resolved
        out.append(new)
    return out


# ---------------------------------------------------------------------------
# Brick 1 — flag three-branch rule (pure)
# ---------------------------------------------------------------------------


class TestComputeFlag:
    def test_self_is_true(self) -> None:
        assert compute_flag("self") is True

    def test_named_agent_is_false(self) -> None:
        assert compute_flag("foundation:explorer") is False

    def test_null_agent_is_false(self) -> None:
        # The council-caught branch: agent IS NULL -> False, never left null.
        assert compute_flag(None) is False


# ---------------------------------------------------------------------------
# Brick 1 — flag changes via compute_updates
# ---------------------------------------------------------------------------


class TestFlagBackfillViaComputeUpdates:
    async def test_null_flag_self_row_becomes_true(self) -> None:
        graph = GraphState(workspace=WS)
        # self row whose parent is a genuine root, so resolved recompute is stable.
        await _seed_session(graph, "ps-root", ["Session", "RootSession"])
        rows = [
            {
                "node_id": "d1",
                "agent": "self",
                "parent_session_id": "ps-root",
                "is_self_delegation": None,  # historical null
                "resolved_agent": "root",  # already correct so only flag changes
            }
        ]
        plan = await compute_updates(rows, _bind_resolver(graph))
        assert plan.pending == 1
        assert plan.flag_to_true == 1
        assert plan.updates[0].flag_changed is True
        assert plan.updates[0].new_flag is True

    async def test_null_flag_named_row_becomes_false(self) -> None:
        graph = GraphState(workspace=WS)
        rows = [
            {
                "node_id": "d2",
                "agent": "foundation:explorer",
                "parent_session_id": "ps1",
                "is_self_delegation": None,  # historical null
                "resolved_agent": "foundation:explorer",
            }
        ]
        plan = await compute_updates(rows, _bind_resolver(graph))
        assert plan.pending == 1
        assert plan.flag_to_false == 1
        assert plan.flag_null_to_false == 1
        # Non-self node: resolved_agent must NOT be touched.
        assert plan.updates[0].resolved_changed is False

    async def test_null_flag_null_agent_becomes_false(self) -> None:
        graph = GraphState(workspace=WS)
        rows = [
            {
                "node_id": "d3",
                "agent": None,  # agent IS NULL
                "parent_session_id": "ps1",
                "is_self_delegation": None,
                "resolved_agent": None,
            }
        ]
        plan = await compute_updates(rows, _bind_resolver(graph))
        assert plan.pending == 1
        assert plan.flag_to_false == 1
        assert plan.flag_null_to_false == 1
        assert plan.updates[0].new_flag is False


# ---------------------------------------------------------------------------
# Brick 2 — resolved_agent recompute mapping via compute_updates
# ---------------------------------------------------------------------------


class TestResolvedRecomputeMapping:
    async def test_self_resolves_to_real_agent_from_parent_delegation(self) -> None:
        graph = GraphState(workspace=WS)
        # Parent Delegation with sub_session_id == the self row's parent_session_id.
        await _seed_parent_delegation(graph, "dparent", "ps-mid", "foundation:explorer")
        rows = [
            {
                "node_id": "dself",
                "agent": "self",
                "parent_session_id": "ps-mid",
                "is_self_delegation": True,  # flag already correct
                "resolved_agent": "root-agent",  # the legacy sentinel (wrong)
            }
        ]
        plan = await compute_updates(rows, _bind_resolver(graph))
        assert plan.pending == 1
        assert plan.updates[0].new_resolved == "foundation:explorer"
        assert plan.updates[0].resolved_changed is True
        # The "wins" bucket: root-agent -> real agent.
        assert plan.resolved_wins == [("dself", "foundation:explorer")]

    async def test_self_resolves_to_root(self) -> None:
        graph = GraphState(workspace=WS)
        await _seed_session(graph, "ps-root", ["Session", "RootSession"])
        rows = [
            {
                "node_id": "dself",
                "agent": "self",
                "parent_session_id": "ps-root",
                "is_self_delegation": True,
                "resolved_agent": "root-agent",
            }
        ]
        plan = await compute_updates(rows, _bind_resolver(graph))
        assert plan.updates[0].new_resolved == "root"
        assert plan.resolved_to_root == 1

    async def test_self_resolves_to_forked(self) -> None:
        graph = GraphState(workspace=WS)
        await _seed_session(graph, "ps-forked", ["Session", "ForkedSession"])
        rows = [
            {
                "node_id": "dself",
                "agent": "self",
                "parent_session_id": "ps-forked",
                "is_self_delegation": True,
                "resolved_agent": "root-agent",
            }
        ]
        plan = await compute_updates(rows, _bind_resolver(graph))
        assert plan.updates[0].new_resolved == "forked"
        assert plan.resolved_to_forked == 1

    async def test_self_resolves_to_unresolved_when_parent_missing(self) -> None:
        graph = GraphState(workspace=WS)
        rows = [
            {
                "node_id": "dself",
                "agent": "self",
                "parent_session_id": "ps-missing",
                "is_self_delegation": True,
                "resolved_agent": "root-agent",
            }
        ]
        plan = await compute_updates(rows, _bind_resolver(graph))
        assert plan.updates[0].new_resolved == "unresolved"
        assert plan.resolved_to_unresolved == 1

    async def test_incomplete_colabel_resolves_to_root_not_unresolved(self) -> None:
        graph = GraphState(workspace=WS)
        # RootSession co-labeled IncompleteSession must still resolve to root.
        await _seed_session(
            graph, "ps-root-inc", ["Session", "RootSession", "IncompleteSession"]
        )
        rows = [
            {
                "node_id": "dself",
                "agent": "self",
                "parent_session_id": "ps-root-inc",
                "is_self_delegation": True,
                "resolved_agent": "root-agent",
            }
        ]
        plan = await compute_updates(rows, _bind_resolver(graph))
        assert plan.updates[0].new_resolved == "root"
        assert plan.resolved_to_root == 1
        assert plan.resolved_to_unresolved == 0


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------


class TestIdempotency:
    async def test_already_correct_rows_stage_nothing(self) -> None:
        graph = GraphState(workspace=WS)
        await _seed_session(graph, "ps-root", ["Session", "RootSession"])
        rows = [
            {
                "node_id": "dself",
                "agent": "self",
                "parent_session_id": "ps-root",
                "is_self_delegation": True,  # correct
                "resolved_agent": "root",  # correct
            },
            {
                "node_id": "dnamed",
                "agent": "foundation:explorer",
                "parent_session_id": "ps1",
                "is_self_delegation": False,  # correct
                "resolved_agent": "foundation:explorer",
            },
        ]
        plan = await compute_updates(rows, _bind_resolver(graph))
        assert plan.pending == 0

    async def test_applying_plan_yields_empty_second_plan(self) -> None:
        graph = GraphState(workspace=WS)
        await _seed_parent_delegation(graph, "dparent", "ps-mid", "foundation:explorer")
        await _seed_session(graph, "ps-root", ["Session", "RootSession"])
        rows = [
            # self -> real agent, flag null
            {
                "node_id": "dself-real",
                "agent": "self",
                "parent_session_id": "ps-mid",
                "is_self_delegation": None,
                "resolved_agent": "root-agent",
            },
            # self -> root, flag correct
            {
                "node_id": "dself-root",
                "agent": "self",
                "parent_session_id": "ps-root",
                "is_self_delegation": True,
                "resolved_agent": "root-agent",
            },
            # named, flag null
            {
                "node_id": "dnamed",
                "agent": "foundation:explorer",
                "parent_session_id": "ps1",
                "is_self_delegation": None,
                "resolved_agent": "foundation:explorer",
            },
        ]
        plan1 = await compute_updates(rows, _bind_resolver(graph))
        assert plan1.pending == 3

        rows2 = _apply_plan_to_rows(rows, plan1)
        plan2 = await compute_updates(rows2, _bind_resolver(graph))
        assert plan2.pending == 0, "second plan must be empty (idempotent)"
