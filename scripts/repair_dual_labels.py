#!/usr/bin/env python
"""Maintenance script: one-off repair of dual-labelled session nodes.

NOT product code — integration-tested in
tests/neo4j/test_repair_dual_labels.py.  Never execute this file as part of
the normal application lifecycle.

POST-DEPLOY GATE
----------------
Run this script ONLY after the Phase 1 read-path fix is deployed AND
verified on live.  Running it before Phase 1 is deployed races fresh
corruption: new nodes can arrive with BOTH labels while this script is
removing them, resulting in an incomplete heal and possible re-corruption.

Heal contract
-------------
For every node carrying BOTH :SubSession AND :ForkedSession labels:

* Remove :SubSession, keep :ForkedSession.
* Delete any incoming HAS_SUBSESSION edge into the node.
* Keep any incoming FORKED edge.
* All other labels, properties, and relationships are left unchanged.

Modes
-----
--dry-run
    Read-only report: counts how many nodes WOULD be healed and lists their
    session_ids.  Does NOT mutate the database.  Exits 1 if any nodes found
    (blocks deployment gate).

--apply
    Take an in-memory snapshot of dual-labelled nodes and edges (optionally
    written to disk via --snapshot PATH), then heal all dual-labelled nodes,
    then verify that zero dual-labelled nodes remain.

--snapshot PATH
    Write the pre-apply snapshot JSON to PATH.  Only meaningful when used
    together with --apply.

--restore PATH
    Read a snapshot JSON written by a previous --apply --snapshot run and
    restore the labels and relationships it describes.  Use to roll back a
    bad --apply run.

Idempotency
-----------
The predicate for every mutation is "the node still carries BOTH labels".
Re-running --apply on an already-healed database matches zero nodes and
makes zero changes — completely safe to re-run.

Exit codes
----------
0  Success: dry-run found zero nodes that would heal; OR --apply completed
   and the post-apply verification found zero dual-labelled nodes remaining.
1  Attention needed: dry-run found nodes that WOULD be healed; OR --apply
   left residual dual-labelled nodes after the repair.
"""

from __future__ import annotations

import argparse
import json
import sys

from neo4j import GraphDatabase

from context_intelligence_server.config import get_settings


# ---------------------------------------------------------------------------
# Read-only helpers
# ---------------------------------------------------------------------------


def count_dual(session, workspace: str) -> dict:
    """Return a read-only summary of dual-labelled nodes in *workspace*.

    Executes two read-only Cypher queries:
    1. Collects the node_ids of every node with both :SubSession and
       :ForkedSession labels in the given workspace.
    2. Counts the HAS_SUBSESSION edges pointing at those nodes.

    Returns:
        {
            "node_count": int,
            "edge_count": int,
            "session_ids": list[str],
        }
    """
    result = session.run(
        "MATCH (n:SubSession:ForkedSession {workspace:$ws}) RETURN n.node_id AS sid",
        ws=workspace,
    )
    session_ids = [record["sid"] for record in result]

    result2 = session.run(
        "MATCH (p)-[r:HAS_SUBSESSION]->(n:SubSession:ForkedSession {workspace:$ws})"
        " RETURN count(r) AS n",
        ws=workspace,
    )
    edge_count = result2.single()["n"]

    return {
        "node_count": len(session_ids),
        "edge_count": edge_count,
        "session_ids": session_ids,
    }


def run_dry_run(session, workspace: str) -> int:
    """Print a dry-run report and return the number of nodes that would heal.

    Does NOT mutate the database.

    Returns:
        int: the number of dual-labelled nodes found (0 means nothing to do).
    """
    report = count_dual(session, workspace)
    print("DRY RUN — nodes that WOULD be healed:\n")
    print(f"  node_count : {report['node_count']}")
    print(f"  edge_count : {report['edge_count']}")
    if report["session_ids"]:
        print("  session_ids:")
        for sid in report["session_ids"]:
            print(f"    - {sid}")
    else:
        print("  session_ids: (none)")
    print()
    return report["node_count"]


# ---------------------------------------------------------------------------
# Mutating operations — defined in Tasks 5 / 6.
# Referenced here so main() can dispatch to them; name resolution is deferred
# to call time, so Python does not complain about forward references.
# ---------------------------------------------------------------------------


# snapshot(session, workspace) -> dict          -- Task 5
# apply_repair(session, workspace) -> int       -- Task 5
# restore_snapshot(session, snap) -> None       -- Task 6


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> int:
    """Parse arguments, connect to Neo4j, and run the requested mode."""
    parser = argparse.ArgumentParser(
        prog="repair_dual_labels.py",
        description=(
            "One-off maintenance tool: heal dual-labelled session nodes "
            "(:SubSession AND :ForkedSession) down to :ForkedSession only. "
            "POST-DEPLOY GATE — run only after Phase 1 is live and verified. "
            "Exit 0 = success / no work needed; 1 = nodes found or residual."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Read-only report of nodes that would be healed. "
            "Exits 1 if any nodes found (signals deployment gate is not clear)."
        ),
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Snapshot, then heal all dual-labelled nodes, then verify.",
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
        help="Restore labels and relationships from a snapshot JSON file.",
    )
    parser.add_argument(
        "--workspace",
        default="default",
        help="Neo4j workspace to scope the repair (default: 'default').",
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

    settings = get_settings()
    url = args.neo4j_url or settings.neo4j_url
    user = args.neo4j_user or settings.neo4j_user
    password = args.neo4j_password or settings.neo4j_password

    driver = GraphDatabase.driver(url, auth=(user, password))
    try:
        with driver.session() as neo4j_session:
            if args.restore:
                with open(args.restore) as fh:
                    snap = json.load(fh)
                restore_snapshot(neo4j_session, snap)
                return 0

            if args.dry_run:
                pending = run_dry_run(neo4j_session, args.workspace)
                return 1 if pending else 0

            if args.apply:
                snap = snapshot(neo4j_session, args.workspace)
                if args.snapshot:
                    with open(args.snapshot, "w") as fh:
                        json.dump(snap, fh, indent=2)
                healed = apply_repair(neo4j_session, args.workspace)
                print(f"Healed {healed} node(s).")
                residual = count_dual(neo4j_session, args.workspace)
                if residual["node_count"]:
                    print(
                        f"WARNING: {residual['node_count']} dual-labelled node(s) remain."
                    )
                    return 1
                print("REPAIR COMPLETE")
                return 0

            parser.error("One of --dry-run, --apply, or --restore is required.")
            return 1  # unreachable; satisfies type checkers
    finally:
        driver.close()


if __name__ == "__main__":
    sys.exit(main())
