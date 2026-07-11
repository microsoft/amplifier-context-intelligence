"""DelegationHandler — correlates delegate events into Delegation nodes.

Each agent delegation lifecycle produces up to five events:
  delegate:agent_spawned    — agent spawn started
  delegate:agent_completed  — agent completed successfully
  delegate:agent_resumed    — agent resumed from a prior session
  delegate:agent_cancelled  — agent was cancelled
  delegate:error            — agent failed with an error

This handler creates a single Delegation node per delegation (keyed by
'{parent_session_id}::delegation::{tool_call_id or sub_session_id}') with the SST_EVENT label,
Note: tool_call_id may be empty string in some Amplifier versions; sub_session_id is used
as fallback key in that case.
and creates an Agent:SST_CONCEPT node per unique agent name (MERGE semantics,
no SOURCED_FROM edge). Phase B semantic edges (E01, E02, E03, E04, E10) are
created by _handle_spawned when the corresponding data is present.
"""

from __future__ import annotations

import logging
from typing import Any

from context_intelligence_server.protocol import HookResult
from context_intelligence_server.services import HookStateService
from context_intelligence_server.utils import make_node_id

logger = logging.getLogger(__name__)

# Max hops the self-delegation resolver will walk up the parent-Delegation
# chain before giving up and returning "unresolved". Live graph data (2477
# Delegation nodes, 1071 self-delegations) shows a max self->self chain depth
# of 1 and zero cycles, so 8 is safety-margin insurance, not an expected path.
_MAX_SELF_DELEGATION_DEPTH = 8


def _discriminate_root_vs_unresolved(parent_session: dict[str, Any] | None) -> str:
    """Root-vs-unresolved discriminator, applied when no parent Delegation is found.

    A lookup miss alone cannot distinguish "genuine root/fork, no spawning
    Delegation exists" from "spawning Delegation not flushed yet" (ingestion
    ordering is not guaranteed -- the spawning record lives in the parent
    session's stream and the self-delegation in the child's; they drain
    concurrently). This resolves the ambiguity from the parent Session node's
    TERMINAL label:

    - ``RootSession``   -> "root"   (correct, terminal: no spawning agent exists)
    - ``ForkedSession`` -> "forked" (correct, terminal: a fork's origin is a
      FORK/HAS_FORK edge, not an agent Delegation, so "no spawner Delegation"
      is expected here, not a race-miss)
    - anything else (parent Session missing, or no terminal label present)
      -> "unresolved" (fails loud; monitored; never mistaken for a real answer)

    CRITICAL: branches on the TERMINAL label ONLY, never on
    ``IncompleteSession``. Live graph data shows ``IncompleteSession``
    co-labels a terminal label ~41% of the time (a session can reach
    session:end with session:start/fork permanently missed, out of order --
    see SessionHandler._handle_end). Treating ``IncompleteSession`` as a
    discriminator would mis-flag hundreds of genuine root/forked sessions as
    unresolved.
    """
    labels: list[str] = (parent_session or {}).get("labels", [])
    if "RootSession" in labels:
        return "root"
    if "ForkedSession" in labels:
        return "forked"
    return "unresolved"


async def resolve_self_agent(
    graph: Any,
    parent_session_id: str,
    workspace: str,
    *,
    max_depth: int = _MAX_SELF_DELEGATION_DEPTH,
) -> str:
    """Resolve the real agent behind a 'self' delegation (single logic home).

    This is the ONE canonical implementation of the self-delegation walk. Both
    the ingestion handler (``DelegationHandler._resolve_self_agent``) and the
    backfill migration (``scripts/backfill_self_delegation.py``) call it, so the
    forward-write path and the historical-repair path can never diverge.

    *graph* is any object exposing the ``GraphStore`` reads the walk needs:
    ``find_delegation_by_sub_session(sub_session_id, workspace)`` and
    ``get_node(node_id)``. Note ``get_node`` scopes by the store's own
    ``workspace`` property, so callers reusing one store across workspaces must
    set ``graph.workspace`` before calling.

    Walks the parent-Delegation chain -- keyed by sub_session_id -- to the
    nearest non-self ancestor:
    1. Look up Delegation D where D.sub_session_id == parent_session_id.
    2. If found and D.agent != "self" -> return D.agent.
    3. If found and D.agent == "self" -> recurse using D's own
       parent_session_id (chained self-delegation), guarded by a visited-set +
       max-depth cap.
    4. If NOT found -> apply the root-vs-unresolved discriminator
       (_discriminate_root_vs_unresolved): a lookup miss alone cannot
       distinguish "genuine root/fork, no spawning Delegation exists" from
       "spawning Delegation not flushed yet" (ingestion ordering is not
       guaranteed -- the spawning record lives in the parent session's stream
       and the self-delegation in the child's; they drain concurrently).

    Returns one of: a real agent name | "root" | "forked" | "unresolved".
    """
    visited: set[str] = set()
    current_session_id = parent_session_id

    for _ in range(max_depth):
        if current_session_id in visited:
            return "unresolved"  # cycle guard
        visited.add(current_session_id)

        parent_delegation = await graph.find_delegation_by_sub_session(
            current_session_id, workspace
        )
        if parent_delegation is None:
            parent_session = await graph.get_node(current_session_id)
            return _discriminate_root_vs_unresolved(parent_session)

        parent_agent = parent_delegation.get("agent")
        if parent_agent != "self":
            return parent_agent or "unresolved"

        # Chained self-delegation (self -> self -> ...): walk to the
        # nearest non-self ancestor via the parent Delegation's own parent.
        next_session_id = parent_delegation.get("parent_session_id")
        if not next_session_id:
            return "unresolved"
        current_session_id = next_session_id

    return "unresolved"  # depth-cap guard (never expected to be reached --
    # live data shows max self-chain depth is 1; this is safety margin)


class DelegationHandler:
    """Enricher handler for delegate lifecycle events.

    Correlates delegate:agent_spawned, delegate:agent_completed,
    delegate:agent_resumed, delegate:agent_cancelled, and delegate:error
    events into a single Delegation node per delegation invocation.
    The Delegation node ID is a compound key:
    '{parent_session_id}::delegation::{tool_call_id}'.
    """

    handled_events: frozenset[str] = frozenset(
        {
            "delegate:agent_spawned",
            "delegate:agent_completed",
            "delegate:agent_resumed",
            "delegate:agent_cancelled",
            "delegate:error",
        }
    )

    def __init__(self, services: HookStateService) -> None:
        self.services = services
        self._parallel_groups: dict[str, list[str]] = {}

    async def __call__(self, event: str, data: dict[str, Any]) -> HookResult:
        """Handle a delegate lifecycle event.

        Extracts parent_session_id and tool_call_id (or sub_session_id fallback).
        Returns continue without mutations only if parent_session_id is missing.

        Note: tool_call_id may be an empty string in some Amplifier versions;
        sub_session_id is used as a stable fallback key in that case so that
        Delegation nodes are always created for any spawned agent.
        """
        parent_session_id: str | None = data.get("parent_session_id")
        if not parent_session_id:
            return HookResult(action="continue")

        # tool_call_id is empty string in some Amplifier versions; fall back to
        # sub_session_id to keep the Delegation node ID stable and unique.
        raw_tool_call_id: str = data.get("tool_call_id") or ""
        sub_session_id_for_fallback: str = data.get("sub_session_id") or ""
        tool_call_id: str = raw_tool_call_id or sub_session_id_for_fallback
        if not tool_call_id:
            return HookResult(action="continue")

        if event == "delegate:agent_spawned":
            await self._handle_spawned(parent_session_id, tool_call_id, data)
        else:
            await self._handle_lifecycle(parent_session_id, tool_call_id, event, data)

        return HookResult(action="continue")

    async def _handle_spawned(
        self,
        parent_session_id: str,
        tool_call_id: str,
        data: dict[str, Any],
    ) -> None:
        """Create Delegation node and all spawn-path edges.

        Creates:
        - Delegation:SST_EVENT node at compound ID
        - Agent:SST_CONCEPT node keyed by agent name (MERGE, no SOURCED_FROM)
        - ensures sub_session_id Session node exists
        - E01: Session(sub) -[:HAS_AGENT {sst_semantic: 'EXPRESSES'}]-> Agent
        - E02: Delegation -[:ENCOMPASSES {sst_semantic: 'CONTAINS'}]-> Session(sub)
        - E03: ToolCall -[:TRIGGERED {sst_semantic: 'LEADS_TO'}]-> Delegation
        - E04: PARALLEL_AGENT edges within parallel_group_id (if present)
        - E10: active_recipe_step_id -[:TRIGGERED]-> Delegation (if cursor set)
        - SOURCED_FROM: Delegation -> data_layer_1 delegate:agent_spawned event
        """
        timestamp: str = data.get("timestamp", "")
        agent: str = data.get("agent", "")
        sub_session_id: str = data.get("sub_session_id", "")
        parallel_group_id: str | None = data.get("parallel_group_id")
        context_depth: str | None = data.get("context_depth")
        context_scope: str | None = data.get("context_scope")
        model_role: str | None = data.get("model_role")

        delegation_id = f"{parent_session_id}::delegation::{tool_call_id}"
        is_self_delegation = agent == "self"

        resolved_agent: str = agent
        if is_self_delegation:
            resolved_agent = await self._resolve_self_agent(
                parent_session_id, self.services.graph.workspace
            )

        # Create the Delegation:SST_EVENT node
        node_data: dict[str, Any] = {
            "labels": ["Delegation", "SST_EVENT"],
            "agent": agent,
            "parent_session_id": parent_session_id,
            "sub_session_id": sub_session_id,
            "tool_call_id": tool_call_id,
            "started_at": timestamp,
            # Written unconditionally (True AND False) so the property is never
            # left null for non-self delegations. Previously this was only ever
            # written True inside the agent=="self" branch, so upsert_node's
            # merge semantics left it null for every non-self delegation --
            # indistinguishable from "unwritten" under Cypher's null=false
            # comparison (WHERE is_self_delegation=false matched zero rows).
            "is_self_delegation": is_self_delegation,
        }
        if parallel_group_id is not None:
            node_data["parallel_group_id"] = parallel_group_id
        if context_depth is not None:
            node_data["context_depth"] = context_depth
        if context_scope is not None:
            node_data["context_scope"] = context_scope
        if model_role is not None:
            node_data["model_role"] = model_role
        if is_self_delegation:
            # Additive field only: node_data["agent"] above stays the literal
            # "self" from the raw event. resolved_agent never overrides it.
            node_data["resolved_agent"] = resolved_agent

        await self.services.graph.upsert_node(delegation_id, node_data)

        # Create Agent:SST_CONCEPT node — MERGE semantics, NO SOURCED_FROM edge
        # Agent concept node — use resolved_agent, not raw agent
        await self.services.graph.upsert_node(
            resolved_agent,
            {
                "labels": ["Agent", "SST_CONCEPT"],
                "agent": resolved_agent,
            },
        )

        # Ensure sub-session node exists
        await self.services.ensure_session_node(sub_session_id, {"agent": agent})

        # E01: Session(sub) -[:HAS_AGENT {sst_semantic: 'EXPRESSES'}]-> Agent — use resolved_agent
        await self.services.graph.upsert_edge(
            sub_session_id,
            resolved_agent,
            {"type": "HAS_AGENT", "sst_semantic": "EXPRESSES"},
        )

        # E02: Delegation -[:ENCOMPASSES {sst_semantic: 'CONTAINS'}]-> Session(sub)
        await self.services.graph.upsert_edge(
            delegation_id,
            sub_session_id,
            {"type": "ENCOMPASSES", "sst_semantic": "CONTAINS"},
        )

        # E03: ToolCall(tool_call_id) -[:TRIGGERED {sst_semantic: 'LEADS_TO'}]-> Delegation
        await self.services.graph.upsert_edge(
            tool_call_id,
            delegation_id,
            {"type": "TRIGGERED", "sst_semantic": "LEADS_TO"},
        )

        # E04: parallel_group handling
        if parallel_group_id is not None:
            group = self._parallel_groups.setdefault(parallel_group_id, [])
            for prior_delegation_id in group:
                await self.services.graph.upsert_edge(
                    delegation_id,
                    prior_delegation_id,
                    {"type": "PARALLEL_AGENT", "sst_semantic": "NEAR"},
                )
            group.append(delegation_id)

        # E10: active_recipe_step_id -> delegation (if cursor set)
        active_step_id = self.services.data_layer_3.active_recipe_step_id
        if active_step_id is not None:
            await self.services.graph.upsert_edge(
                active_step_id,
                delegation_id,
                {"type": "TRIGGERED", "sst_semantic": "LEADS_TO"},
            )

        # SOURCED_FROM bridge: Delegation -> data_layer_1 delegate:agent_spawned event
        data_layer_1_node_id = make_node_id(
            parent_session_id, "delegate:agent_spawned", timestamp, tool_call_id
        )
        await self.services.graph.upsert_edge(
            delegation_id, data_layer_1_node_id, {"type": "SOURCED_FROM"}
        )

    async def _resolve_self_agent(self, parent_session_id: str, workspace: str) -> str:
        """Resolve the real agent behind a 'self' delegation (additive field only).

        Thin delegator to the module-level :func:`resolve_self_agent`, the single
        logic home shared with the backfill migration. ``node_data["agent"]``
        stays the literal ``"self"`` from the raw event; this only computes the
        additive ``resolved_agent`` field.
        """
        return await resolve_self_agent(
            self.services.graph, parent_session_id, workspace
        )

    async def _handle_lifecycle(
        self,
        parent_session_id: str,
        tool_call_id: str,
        event: str,
        data: dict[str, Any],
    ) -> None:
        """Enrich existing Delegation node with lifecycle event properties.

        Sets appropriate enrichment properties based on event type and
        adds a SOURCED_FROM edge to the corresponding data_layer_1 event node.
        """
        timestamp: str = data.get("timestamp", "")
        delegation_id = f"{parent_session_id}::delegation::{tool_call_id}"

        enrichment: dict[str, Any]
        if event == "delegate:agent_completed":
            enrichment = {"ended_at": timestamp, "success": True}
        elif event == "delegate:agent_resumed":
            enrichment = {"resumed_at": timestamp}
        elif event == "delegate:agent_cancelled":
            enrichment = {"cancelled_at": timestamp}
        else:  # delegate:error
            enrichment = {
                "ended_at": timestamp,
                "success": False,
                "error": data.get("error"),
            }

        node_data: dict[str, Any] = {
            "labels": ["Delegation", "SST_EVENT"],
            **enrichment,
        }
        await self.services.graph.upsert_node(delegation_id, node_data)

        # SOURCED_FROM bridge: Delegation -> data_layer_1 event node
        data_layer_1_node_id = make_node_id(
            parent_session_id, event, timestamp, delegation_id
        )
        await self.services.graph.upsert_edge(
            delegation_id, data_layer_1_node_id, {"type": "SOURCED_FROM"}
        )
