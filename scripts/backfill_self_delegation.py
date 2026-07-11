#!/usr/bin/env python
"""Maintenance script: one-off backfill of historical :Delegation nodes to the
new self-delegation resolver contract.

NOT product code -- integration-tested in
tests/neo4j/test_backfill_self_delegation.py and unit-tested (pure transform)
in tests/scripts/test_backfill_self_delegation.py. Never execute this file as
part of the normal application lifecycle.

WHAT IT FIXES
-------------
Two historical defects, both now fixed in the live ingestion path
(handlers/data_layer_3/delegation.py) but still baked into rows written before
the upgrade:

* Brick 1 -- ``is_self_delegation`` was written ONLY inside the
  ``agent == "self"`` branch, and only as ``True``. Every non-self delegation
  was left with the property NULL (never ``False``), so
  ``WHERE is_self_delegation = false`` matched zero rows. This backfill sets
  the flag on EVERY Delegation node, with the three-branch rule below.

* Brick 2 -- ``resolved_agent`` for a self-delegation was read off the parent
  SESSION node (which structurally never carries an ``agent`` property), so it
  fell back to the constant ``"root-agent"`` 100% of the time. This backfill
  RECOMPUTES it via the SAME resolver the live path now uses
  (``resolve_self_agent`` in delegation.py -- the single logic home), reading
  the parent DELEGATION node.

SINGLE LOGIC HOME
-----------------
The resolution walk is NOT reimplemented here. This script imports and calls
``context_intelligence_server.handlers.data_layer_3.delegation.resolve_self_agent``
so the backfill and the forward-write path can never diverge.

POST-DEPLOY GATE
----------------
Run this script ONLY AFTER upgrading the ingestion server to the version that
contains the self-delegation fix, and against a FULLY-FLUSHED graph (no live
drain in flight). Running it earlier races the ordering window the resolver's
``unresolved`` sentinel exists to catch, and would backfill values that a later
flush could still improve.

Recommended sequence (dry-run is READ-ONLY):

    # 1. Point at the deployed Neo4j (same env vars the server uses), e.g.:
    AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J_URL=bolt://localhost:7687 \\
    AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J_PASSWORD=... \\
    uv run python scripts/backfill_self_delegation.py --dry-run

    # 2. If it reports pending changes (exit 1), apply with a snapshot:
    uv run python scripts/backfill_self_delegation.py --apply --snapshot snap.json

    # 3. Re-run --dry-run -> must report 0 pending, exit 0 (idempotency).

Or via docker compose: ``docker compose exec <server> uv run python
scripts/backfill_self_delegation.py --dry-run``.

Compute rules (per Delegation node)
-----------------------------------
* Brick 1 flag (three explicit branches -- the NULL branch is the one the
  council caught):
    - ``agent == 'self'``                    -> ``True``
    - ``agent`` non-null and ``!= 'self'``   -> ``False``
    - ``agent IS NULL``                      -> ``False``
* Brick 2 resolved_agent: recomputed ONLY for ``agent == 'self'`` nodes via
  ``resolve_self_agent``. Non-self nodes never touch ``resolved_agent``.

IDEMPOTENCY
-----------
A write is staged ONLY when a computed value DIFFERS from the value already
stored. Re-running ``--apply`` on an already-backfilled graph stages zero
writes. ``--dry-run`` is read-only and exits 0 on a clean graph.

Exit codes
----------
0  Clean: dry-run found zero pending changes; OR --apply completed and the
   post-apply re-plan found zero pending changes remaining.
1  Attention: dry-run found pending changes (gate not clear); OR --apply left
   residual pending changes.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import dataclass, field
from typing import Any

from context_intelligence_server.config import get_settings
from context_intelligence_server.handlers.data_layer_3.delegation import (
    resolve_self_agent,
)
from context_intelligence_server.neo4j_store import Neo4jGraphStore

BATCH_SIZE = 1000

# Sentinel replaced by the recompute in the historical rows. Used only for
# reporting (the "wins" bucket: root-agent -> a real agent name).
_LEGACY_SENTINEL = "root-agent"


# ---------------------------------------------------------------------------
# Pure transform (testable in isolation)
# ---------------------------------------------------------------------------


def compute_flag(agent: str | None) -> bool:
    """Brick 1 three-branch flag rule.

    * ``agent == 'self'``                  -> True
    * ``agent`` non-null and ``!= 'self'`` -> False
    * ``agent IS NULL``                    -> False (the council-caught branch)
    """
    return agent == "self"


@dataclass
class NodeUpdate:
    """A single staged write for one Delegation node.

    ``flag_changed`` / ``resolved_changed`` keep the flag and resolved_agent as
    SEPARATE concerns: a non-self node only ever gets its flag updated, never
    its ``resolved_agent`` (which is left exactly as-is).
    """

    node_id: str
    new_flag: bool
    flag_changed: bool
    # resolved_agent is only computed/changed for self-delegations
    old_resolved: str | None = None
    new_resolved: str | None = None
    resolved_changed: bool = False


@dataclass
class Plan:
    """The full set of staged updates plus reporting buckets."""

    updates: list[NodeUpdate] = field(default_factory=list)
    scanned: int = 0
    # Flag buckets
    flag_to_true: int = 0
    flag_to_false: int = 0
    flag_null_to_false: int = 0
    # resolved_agent buckets (self nodes only)
    resolved_wins: list[tuple[str, str]] = field(default_factory=list)  # (id, new)
    resolved_to_root: int = 0
    resolved_to_forked: int = 0
    resolved_to_unresolved: int = 0
    resolved_other: int = 0  # any other real-name change not from the sentinel

    @property
    def pending(self) -> int:
        """Number of nodes with at least one staged write."""
        return len(self.updates)


async def compute_updates(
    rows: list[dict[str, Any]],
    resolve_fn: Any,
) -> Plan:
    """Pure transform: turn scanned Delegation rows into an idempotent Plan.

    This is the testable heart of the backfill -- it depends ONLY on the row
    dicts and an injected ``resolve_fn`` (an async callable
    ``(parent_session_id: str) -> str``), never on a live store or Cypher. The
    scan/write/report layers stay thin around it, and it can be unit-tested
    against in-memory ``GraphState`` fixtures (with ``resolve_fn`` bound to the
    shared ``resolve_self_agent`` walk over that GraphState).

    Each *row* is expected to carry: ``node_id``, ``agent``,
    ``parent_session_id``, ``is_self_delegation`` (current), ``resolved_agent``
    (current). A write is staged ONLY when a computed value differs from the
    stored value (idempotency).
    """
    plan = Plan()

    for row in rows:
        plan.scanned += 1
        node_id = row["node_id"]
        agent = row.get("agent")
        current_flag = row.get("is_self_delegation")
        current_resolved = row.get("resolved_agent")

        # --- Brick 1: flag (three branches) ---
        new_flag = compute_flag(agent)
        flag_changed = current_flag != new_flag

        # --- Brick 2: resolved_agent (self nodes only) ---
        new_resolved: str | None = None
        resolved_changed = False
        if agent == "self":
            new_resolved = await resolve_fn(row["parent_session_id"])
            resolved_changed = current_resolved != new_resolved

        if not flag_changed and not resolved_changed:
            continue  # already correct -> stage nothing (idempotent)

        plan.updates.append(
            NodeUpdate(
                node_id=node_id,
                new_flag=new_flag,
                flag_changed=flag_changed,
                old_resolved=current_resolved,
                new_resolved=new_resolved,
                resolved_changed=resolved_changed,
            )
        )

        # --- reporting buckets ---
        if flag_changed:
            if new_flag:
                plan.flag_to_true += 1
            else:
                plan.flag_to_false += 1
                if current_flag is None:
                    plan.flag_null_to_false += 1
        if resolved_changed:
            if new_resolved == "root":
                plan.resolved_to_root += 1
            elif new_resolved == "forked":
                plan.resolved_to_forked += 1
            elif new_resolved == "unresolved":
                plan.resolved_to_unresolved += 1
            elif current_resolved == _LEGACY_SENTINEL:
                plan.resolved_wins.append((node_id, new_resolved or ""))
            else:
                plan.resolved_other += 1

    return plan


async def plan_workspace(store: Any, workspace: str) -> Plan:
    """Scan every :Delegation node in *workspace* and stage idempotent updates.

    Thin wrapper: reads the rows via Cypher, binds ``resolve_fn`` to the shared
    ``resolve_self_agent`` walk (the single logic home), and delegates the
    actual planning to the pure :func:`compute_updates`. Performs only reads.
    """
    rows = await store.execute_query(
        "MATCH (d:Delegation {workspace: $ws}) "
        "RETURN d.node_id AS node_id, d.agent AS agent, "
        "d.parent_session_id AS parent_session_id, "
        "d.is_self_delegation AS is_self_delegation, "
        "d.resolved_agent AS resolved_agent",
        {"ws": workspace},
        workspace=workspace,
    )

    async def _resolve(parent_session_id: str) -> str:
        return await resolve_self_agent(store, parent_session_id, workspace)

    return await compute_updates(rows, _resolve)


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def _print_examples(label: str, items: list[str], limit: int = 5) -> None:
    if not items:
        return
    print(f"    {label} (showing up to {limit}):")
    for ex in items[:limit]:
        print(f"      - {ex}")


def print_plan(workspace: str, plan: Plan) -> None:
    """Human-readable dry-run / apply summary for one workspace."""
    print(f"WORKSPACE {workspace!r} -- scanned {plan.scanned} Delegation node(s)")
    print(f"  pending node writes : {plan.pending}")
    print("  Brick 1 flag changes:")
    print(f"    -> true          : {plan.flag_to_true}")
    print(
        f"    -> false         : {plan.flag_to_false} "
        f"(of which null->false: {plan.flag_null_to_false})"
    )
    print("  Brick 2 resolved_agent changes (self-delegations only):")
    print(f"    root-agent -> real agent (the wins): {len(plan.resolved_wins)}")
    _print_examples(
        "examples",
        [f"{nid}  =>  {new}" for nid, new in plan.resolved_wins],
    )
    print(f"    -> root          : {plan.resolved_to_root}")
    print(f"    -> forked        : {plan.resolved_to_forked}")
    print(f"    -> unresolved    : {plan.resolved_to_unresolved}")
    if plan.resolved_other:
        print(f"    -> other change  : {plan.resolved_other}")
    # a few concrete flag example node_ids
    flag_examples = [u.node_id for u in plan.updates if u.flag_changed]
    _print_examples("flag-change examples", flag_examples)
    print()


# ---------------------------------------------------------------------------
# Snapshot / restore
# ---------------------------------------------------------------------------


async def snapshot_updates(store: Any, plan: Plan) -> list[dict[str, Any]]:
    """Capture the PRIOR (node_id, is_self_delegation, resolved_agent) for every
    node the plan will touch, so --restore can roll the apply back.

    Reads the current stored values (post-flush the buffer is empty, so this is
    a real read of persisted state).
    """
    snap: list[dict[str, Any]] = []
    ids = [u.node_id for u in plan.updates]
    for start in range(0, len(ids), BATCH_SIZE):
        chunk = ids[start : start + BATCH_SIZE]
        rows = await store.execute_query(
            "UNWIND $ids AS nid "
            "MATCH (d:Delegation {node_id: nid, workspace: $ws}) "
            "RETURN d.node_id AS node_id, "
            "d.is_self_delegation AS is_self_delegation, "
            "d.resolved_agent AS resolved_agent",
            {"ids": chunk, "ws": store.workspace},
            workspace=store.workspace,
        )
        for row in rows:
            snap.append(
                {
                    "node_id": row["node_id"],
                    "is_self_delegation": row.get("is_self_delegation"),
                    "resolved_agent": row.get("resolved_agent"),
                }
            )
    return snap


async def apply_plan(store: Any, plan: Plan) -> tuple[int, int]:
    """Write the staged updates back to Neo4j in batches.

    Flag and resolved_agent are kept as SEPARATE concerns:
    * every staged node gets ``is_self_delegation`` set (flag), and
    * ONLY nodes whose resolved_agent changed also get ``resolved_agent`` set
      (a non-self node therefore only ever updates its flag).

    Returns ``(flag_writes, resolved_writes)``.
    """
    flag_rows = [
        {"id": u.node_id, "flag": u.new_flag} for u in plan.updates if u.flag_changed
    ]
    resolved_rows = [
        {"id": u.node_id, "resolved": u.new_resolved}
        for u in plan.updates
        if u.resolved_changed
    ]

    for start in range(0, len(flag_rows), BATCH_SIZE):
        chunk = flag_rows[start : start + BATCH_SIZE]
        await store.execute_query(
            "UNWIND $rows AS row "
            "MATCH (d:Delegation {node_id: row.id, workspace: $ws}) "
            "SET d.is_self_delegation = row.flag",
            {"rows": chunk, "ws": store.workspace},
            workspace=store.workspace,
        )

    for start in range(0, len(resolved_rows), BATCH_SIZE):
        chunk = resolved_rows[start : start + BATCH_SIZE]
        await store.execute_query(
            "UNWIND $rows AS row "
            "MATCH (d:Delegation {node_id: row.id, workspace: $ws}) "
            "SET d.resolved_agent = row.resolved",
            {"rows": chunk, "ws": store.workspace},
            workspace=store.workspace,
        )

    return len(flag_rows), len(resolved_rows)


async def restore_snapshot(
    store: Any, workspace: str, snap: list[dict[str, Any]]
) -> int:
    """Restore is_self_delegation + resolved_agent from a snapshot JSON.

    A NULL prior value is restored by REMOVEing the property (Cypher cannot
    SET a property to null), reproducing the pre-apply state faithfully.
    """
    restored = 0
    for start in range(0, len(snap), BATCH_SIZE):
        chunk = snap[start : start + BATCH_SIZE]
        # Split rows by whether each prior value was null, so we can SET real
        # values and REMOVE the ones that were absent.
        for row in chunk:
            sets: list[str] = []
            params: dict[str, Any] = {"id": row["node_id"], "ws": workspace}
            if row.get("is_self_delegation") is None:
                sets.append("REMOVE d.is_self_delegation")
            else:
                sets.append("SET d.is_self_delegation = $flag")
                params["flag"] = row["is_self_delegation"]
            if row.get("resolved_agent") is None:
                sets.append("REMOVE d.resolved_agent")
            else:
                sets.append("SET d.resolved_agent = $resolved")
                params["resolved"] = row["resolved_agent"]
            await store.execute_query(
                "MATCH (d:Delegation {node_id: $id, workspace: $ws}) " + " ".join(sets),
                params,
                workspace=workspace,
            )
            restored += 1
    return restored


# ---------------------------------------------------------------------------
# Workspace discovery
# ---------------------------------------------------------------------------


async def discover_workspaces(store: Any) -> list[str]:
    """Return every distinct workspace that owns at least one :Delegation node.

    Uses ``workspace="*"`` so the scan is NOT scoped to the store's own
    workspace -- the backfill must see all of them.
    """
    rows = await store.execute_query(
        "MATCH (d:Delegation) WHERE d.workspace IS NOT NULL "
        "RETURN DISTINCT d.workspace AS ws ORDER BY ws",
        {},
        workspace="*",
    )
    return [row["ws"] for row in rows]


# ---------------------------------------------------------------------------
# Async orchestration
# ---------------------------------------------------------------------------


async def _run(args: argparse.Namespace, store: Any) -> int:
    if args.restore:
        with open(args.restore) as fh:
            snap_doc = json.load(fh)
        workspace = snap_doc.get("workspace", args.workspace or "default")
        store.workspace = workspace
        restored = await restore_snapshot(store, workspace, snap_doc.get("nodes", []))
        print(f"Restored {restored} Delegation node(s) in workspace {workspace!r}.")
        return 0

    # Determine target workspaces.
    if args.workspace:
        workspaces = [args.workspace]
    else:
        workspaces = await discover_workspaces(store)
    if not workspaces:
        print("No :Delegation nodes found in any workspace. Nothing to do.")
        return 0

    total_pending = 0
    all_snapshots: list[dict[str, Any]] = []
    snapshot_workspace: str | None = None

    for ws in workspaces:
        store.workspace = ws  # scope get_node() correctly for the resolver
        plan = await plan_workspace(store, ws)
        print_plan(ws, plan)
        total_pending += plan.pending

        if args.apply and plan.pending:
            snap = await snapshot_updates(store, plan)
            all_snapshots.extend(snap)
            snapshot_workspace = ws if snapshot_workspace is None else "*multi*"
            flag_writes, resolved_writes = await apply_plan(store, plan)
            print(
                f"  APPLIED workspace {ws!r}: {flag_writes} flag write(s), "
                f"{resolved_writes} resolved_agent write(s)."
            )

    if args.apply:
        if args.snapshot:
            with open(args.snapshot, "w") as fh:
                json.dump(
                    {"workspace": snapshot_workspace, "nodes": all_snapshots},
                    fh,
                    indent=2,
                )
            print(
                f"Snapshot of {len(all_snapshots)} node(s) written to {args.snapshot}"
            )

        # Re-plan to prove idempotency: a correct apply leaves zero pending.
        residual = 0
        for ws in workspaces:
            store.workspace = ws
            replan = await plan_workspace(store, ws)
            residual += replan.pending
        if residual:
            print(f"WARNING: {residual} pending change(s) remain after apply.")
            return 1
        print("BACKFILL COMPLETE -- re-plan found 0 pending changes (idempotent).")
        return 0

    # dry-run
    if total_pending:
        print(
            f"DRY RUN -- {total_pending} pending change(s) across "
            f"{len(workspaces)} workspace(s). Re-run with --apply."
        )
        return 1
    print("DRY RUN -- 0 pending changes. Graph already matches the resolver.")
    return 0


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> int:
    """Parse arguments, connect to Neo4j, and run the requested mode."""
    parser = argparse.ArgumentParser(
        prog="backfill_self_delegation.py",
        description=(
            "One-off maintenance tool: backfill historical :Delegation nodes to "
            "the self-delegation resolver contract (Brick 1 boolean flag + Brick "
            "2 recomputed resolved_agent). Run ONLY AFTER upgrading the ingestion "
            "server to the fixed version, against a fully-flushed graph. "
            "Idempotent; --dry-run is read-only. Exit 0 = clean, 1 = pending."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Read-only report of pending flag/resolved_agent changes. Exits 1 if "
            "any changes are pending (signals the backfill gate is not clear); "
            "0 on an already-correct graph."
        ),
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Snapshot, then write staged updates, then re-plan to prove idempotency.",
    )
    parser.add_argument(
        "--snapshot",
        metavar="PATH",
        default=None,
        help="Write pre-apply snapshot JSON to PATH (used with --apply).",
    )
    parser.add_argument(
        "--restore",
        metavar="PATH",
        default=None,
        help="Restore is_self_delegation/resolved_agent from a snapshot JSON file.",
    )
    parser.add_argument(
        "--workspace",
        default=None,
        help=(
            "Restrict to a single Neo4j workspace. Default: discover and process "
            "every workspace that owns :Delegation nodes."
        ),
    )
    parser.add_argument(
        "--neo4j-url",
        metavar="URL",
        default=None,
        help="Neo4j Bolt URL (overrides server-config.yaml / env var).",
    )
    parser.add_argument(
        "--neo4j-user",
        metavar="USER",
        default=None,
        help="Neo4j username (overrides server-config.yaml / env var).",
    )
    parser.add_argument(
        "--neo4j-password",
        metavar="PW",
        default=None,
        help="Neo4j password (overrides server-config.yaml / env var).",
    )

    args = parser.parse_args()

    if not (args.dry_run or args.apply or args.restore):
        parser.error("One of --dry-run, --apply, or --restore is required.")

    settings = get_settings()
    admin = settings.resolve_neo4j_admin()
    url = args.neo4j_url or admin.url
    user = args.neo4j_user or admin.username
    password = args.neo4j_password or admin.password
    auth = (user, password) if password else admin.auth

    print(f"Connecting to Neo4j at {url} as {user}\n")
    store = Neo4jGraphStore(uri=url, auth=auth)
    try:
        return asyncio.run(_run(args, store))
    finally:
        asyncio.run(store.close())


if __name__ == "__main__":
    sys.exit(main())
