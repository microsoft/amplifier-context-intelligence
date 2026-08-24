#!/usr/bin/env python
"""Maintenance tool: TAG (non-destructive) legacy Iteration nodes that were
confirmed MERGEd across >=2 distinct OrchestratorRuns before the run-scoped-id fix.

**NOT product code -- no unit tests.**  This is a standalone, one-off
maintenance script.  The regression that PREVENTS new corruption is covered
by ``tests/handlers/data_layer_2/test_iteration.py``
(``TestIterationRunScopingP21``); this script only deals with historical data
already in a live graph.

Background
---------------------------------------
Before the run-scoped-id fix, ``Iteration.node_id`` was the bare shape
``{session_id}::iteration::{N}``, with ``N`` a per-*session* (not per-*run*)
counter. Because the counter could restart (e.g. drainer restart/replay),
two DIFFERENT ``OrchestratorRun``s could reuse the same ``N`` and MERGE onto
the SAME bare-id Iteration node, clobbering its usage/message properties
(last-write-wins). After the run-scoped-id fix, new Iteration node_ids are run-scoped:
``{session_id}::orch_run::{ts}::iteration::{N}`` -- these can never collide
across runs and are excluded from consideration by this script entirely.

A **bare-id node_id does NOT mean the node is corrupt.** A design review +
live-data verification established that only nodes whose bare id is reached
by ``HAS_PART`` from **two or more distinct** ``OrchestratorRun`` nodes are
provably corrupt (pooled). The rest of the bare-id population (the large
majority -- ~93-95% of bare-id nodes in live data) are clean, single-run
nodes created before the fix and must NOT be touched: a run=1 bare-id node is
exactly what a healthy pre-fix Iteration looked like.

What this script does
----------------------
This script TAGS -- and only tags -- the confirmed-corrupt subset with a
non-destructive marker property ``data_quality = 'legacy_pooled_pre_fix'``.
It never deletes or restructures anything. The destructive cleanup (e.g.
splitting or deleting pooled nodes) is a SEPARATE, gated follow-up and is
explicitly OUT OF SCOPE here.

CONFIRMED-CORRUPT SELECTOR (the ONLY nodes this script ever touches)::

    MATCH (run:OrchestratorRun)-[:HAS_PART]->(i:Iteration)
    WHERE NOT i.node_id CONTAINS '::orch_run::'
    WITH i, count(DISTINCT run) AS runs
    WHERE runs >= 2

Selector-sanity note (does not over-claim)
-------------------------------------------
``runs >= 2`` is a **confirmed lower bound** on corruption, not an exhaustive
enumeration of it. A bare-id node with exactly one surviving ``HAS_PART``
parent (``runs == 1``) could -- in principle -- have been pooled across MORE
runs whose ``OrchestratorRun`` node or edge was later pruned/expired, leaving
only one parent behind; such a node would be indistinguishable, from current
graph state, from a genuinely single-run node. This script deliberately does
**not** attempt to catch that case: it tags only what is provably corroborated
by graph structure today (>=2 live distinct parents), and explicitly leaves
``runs == 1`` (``confirmed_clean_single_run``) and ``runs == 0``
(``no_run_edge`` -- no ``HAS_PART`` parent survives at all, so corruption
cannot be confirmed or denied) untouched.

Nodes this script explicitly does NOT touch
--------------------------------------------
* ``runs == 1`` -- confirmed clean single-run bare-id node. Leave alone.
* ``runs == 0`` -- no surviving ``OrchestratorRun`` parent edge at all, so
  pooling cannot be confirmed. Leave alone (see selector-sanity note above).
* Any run-scoped node (``node_id`` contains ``'::orch_run::'``) -- these are
  post-fix and structurally cannot collide across runs.

DEPLOYMENT GATE
---------------
This script is **not a deployment gate**. It performs a single additive,
idempotent ``SET`` of one marker property on a provably-corrupt subset of
historical nodes; it does not race the run-scoped-id write-path fix (which
governs only newly-created Iteration nodes) and carries no risk of
mixed-type or mixed-shape state. It may be run at any time, before or after
the run-scoped-id fix is deployed to a given server, and re-run as often as
desired (see Idempotency below). Running it is a maintenance convenience,
not a precondition for shipping the run-scoped-id fix.

Connection
----------
Connection details come from :func:`context_intelligence_server.config.get_settings`.
Resolution order (highest first):

1. Environment variables with prefix ``AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_``.
2. ``server-config.yaml`` in the current working directory.
3. Built-in defaults.

To target a DTU, override env vars::

    AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J_URL=bolt://localhost:7688 \\
    AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J_PASSWORD=testpassword \\
    uv run python scripts/tag_legacy_pooled_iterations.py --dry-run

This script is standalone: it connects to Neo4j directly via the ``neo4j``
driver and does NOT import or start the FastAPI application.

Idempotency
-----------
The write query only touches nodes where
``i.data_quality IS NULL OR i.data_quality <> 'legacy_pooled_pre_fix'``, so a
second ``--apply`` run against an already-tagged graph matches zero rows and
performs zero writes. The tag itself is never removed by this script (it is
a permanent, non-destructive marker); un-tagging is out of scope.

Modes
-----
--dry-run (default)
    Read-only report: total bare-id Iteration count, broken down into
    ``confirmed_corrupt`` (runs >= 2), ``confirmed_clean_single_run``
    (runs == 1), ``no_run_edge`` (runs == 0), and how many of the
    ``confirmed_corrupt`` set are NOT yet tagged (i.e. would be written by
    ``--apply``). Writes NOTHING. Always exits 0 (this is a health-check /
    reporting mode, not a gate).

--apply
    ``SET i.data_quality = 'legacy_pooled_pre_fix'`` on the confirmed-corrupt
    set, batched via ``CALL { ... } IN TRANSACTIONS OF N ROWS``. Only writes
    rows not already carrying the tag (idempotent). Prints the number of rows
    tagged, then re-runs the untagged-count verification query and reports
    it (must be 0 after a successful apply).

Exit codes
----------
* 0 -- success (dry-run report printed; or apply completed and verification
  found zero confirmed-corrupt nodes remaining untagged).
* 1 -- apply completed but verification found untagged confirmed-corrupt
  nodes remaining (should not happen; signals a bug or concurrent writer).
"""

from __future__ import annotations

import argparse
import sys

from context_intelligence_server.config import get_settings
from neo4j import GraphDatabase

DEFAULT_BATCH_SIZE = 500

TAG_VALUE = "legacy_pooled_pre_fix"

# ---------------------------------------------------------------------------
# Selector -- the ONLY nodes this script ever touches.
# Shared verbatim (as a fragment) across classify/apply/verify so there is
# exactly one place that defines "confirmed corrupt".
# ---------------------------------------------------------------------------
_CONFIRMED_CORRUPT_MATCH = (
    "MATCH (run:OrchestratorRun)-[:HAS_PART]->(i:Iteration) "
    "WHERE NOT i.node_id CONTAINS '::orch_run::' "
    "WITH i, count(DISTINCT run) AS runs "
    "WHERE runs >= 2"
)

_UNTAGGED_GUARD = (
    "WITH DISTINCT i WHERE i.data_quality IS NULL OR i.data_quality <> $tag_value"
)


# ---------------------------------------------------------------------------
# Read-only helpers
# ---------------------------------------------------------------------------


def classify(session) -> dict[str, int]:
    """Return bucketed counts over ALL bare-id Iteration nodes. No writes.

    Buckets:
    - total: every bare-id (pre-fix-shaped) Iteration node
    - confirmed_corrupt: runs >= 2 (the ONLY set this script will ever tag)
    - confirmed_clean_single_run: runs == 1 (leave alone)
    - no_run_edge: runs == 0, i.e. no surviving OrchestratorRun HAS_PART
      parent at all (leave alone -- corruption cannot be confirmed)
    - confirmed_corrupt_untagged: of confirmed_corrupt, how many are NOT yet
      carrying the tag (i.e. how many --apply would write)
    """
    result = session.run(
        "MATCH (i:Iteration) "
        "WHERE NOT i.node_id CONTAINS '::orch_run::' "
        "OPTIONAL MATCH (run:OrchestratorRun)-[:HAS_PART]->(i) "
        "WITH i, count(DISTINCT run) AS runs "
        "RETURN "
        "  count(i) AS total, "
        "  sum(CASE WHEN runs >= 2 THEN 1 ELSE 0 END) AS confirmed_corrupt, "
        "  sum(CASE WHEN runs = 1 THEN 1 ELSE 0 END) AS confirmed_clean_single_run, "
        "  sum(CASE WHEN runs = 0 THEN 1 ELSE 0 END) AS no_run_edge"
    )
    row = result.single()
    counts = {
        "total": row["total"] or 0,
        "confirmed_corrupt": row["confirmed_corrupt"] or 0,
        "confirmed_clean_single_run": row["confirmed_clean_single_run"] or 0,
        "no_run_edge": row["no_run_edge"] or 0,
    }
    counts["confirmed_corrupt_untagged"] = count_untagged(session)
    return counts


def count_untagged(session) -> int:
    """Return the count of confirmed-corrupt Iteration nodes NOT yet tagged.

    Standalone re-runnable verification query: after a successful --apply
    this must return 0.
    """
    result = session.run(
        f"{_CONFIRMED_CORRUPT_MATCH} {_UNTAGGED_GUARD} RETURN count(i) AS untagged",
        tag_value=TAG_VALUE,
    )
    return result.single()["untagged"] or 0


# ---------------------------------------------------------------------------
# Mutating operation
# ---------------------------------------------------------------------------


def tag_confirmed_corrupt(session, batch_size: int) -> int:
    """SET data_quality='legacy_pooled_pre_fix' on the confirmed-corrupt,
    not-yet-tagged subset, batched via CALL { ... } IN TRANSACTIONS OF N ROWS.

    Idempotent: rows already carrying the tag are excluded by the guard
    before the CALL, so re-running matches (and writes) zero rows.

    Returns the number of rows tagged.
    """
    result = session.run(
        f"{_CONFIRMED_CORRUPT_MATCH} {_UNTAGGED_GUARD} "
        "CALL { "
        "  WITH i "
        "  SET i.data_quality = $tag_value "
        "} IN TRANSACTIONS OF $batch_size ROWS "
        "RETURN count(i) AS tagged",
        tag_value=TAG_VALUE,
        batch_size=batch_size,
    )
    return result.single()["tagged"] or 0


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def run_dry_run(session) -> int:
    """Print classification report. Always returns 0 (health-check, not a gate)."""
    counts = classify(session)
    print("DRY RUN -- confirmed-corrupt Iteration nodes (TAG ONLY, no writes):\n")
    print(f"{'Bucket':<35} {'Count':>10}")
    print("-" * 46)
    print(f"{'total (all bare-id Iterations)':<35} {counts['total']:>10}")
    print(f"{'confirmed_corrupt (runs >= 2)':<35} {counts['confirmed_corrupt']:>10}")
    print(
        f"{'confirmed_clean_single_run (runs == 1)':<35} "
        f"{counts['confirmed_clean_single_run']:>10}"
    )
    print(f"{'no_run_edge (runs == 0)':<35} {counts['no_run_edge']:>10}")
    print("-" * 46)
    print(
        f"{'confirmed_corrupt NOT yet tagged':<35} "
        f"{counts['confirmed_corrupt_untagged']:>10}"
    )
    print(
        f"\n  --apply would tag {counts['confirmed_corrupt_untagged']} node(s). "
        f"{counts['confirmed_clean_single_run'] + counts['no_run_edge']} bare-id "
        "node(s) are left untouched (not confirmed corrupt).\n"
    )
    return 0


def run_apply(session, batch_size: int) -> int:
    """Tag the confirmed-corrupt set, then verify zero remain untagged.

    Returns the process exit code (0 = verified clean, 1 = residual untagged).
    """
    print("APPLY -- tagging confirmed-corrupt Iteration nodes:\n")
    tagged = tag_confirmed_corrupt(session, batch_size)
    print(f"  Tagged {tagged} node(s) with data_quality='{TAG_VALUE}'.\n")

    remaining = count_untagged(session)
    print("VERIFICATION -- confirmed-corrupt nodes still missing the tag:\n")
    print(f"  remaining untagged: {remaining}")
    if remaining:
        print(
            "\nAPPLY INCOMPLETE -- "
            f"{remaining} confirmed-corrupt node(s) remain untagged after apply. "
            "This should not happen; investigate for a concurrent writer or a bug."
        )
        return 1
    print("\nAPPLY COMPLETE -- all confirmed-corrupt nodes are tagged.")
    return 0


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> int:
    """Parse arguments, connect to Neo4j, and run dry-run or apply."""
    parser = argparse.ArgumentParser(
        prog="tag_legacy_pooled_iterations.py",
        description=(
            "Maintenance tool: non-destructively TAG (data_quality="
            f"'{TAG_VALUE}') legacy bare-id Iteration nodes confirmed pooled "
            "across >=2 distinct OrchestratorRuns before the run-scoped-id fix. Never "
            "touches run-scoped nodes, single-run bare-id nodes, or bare-id "
            "nodes with no surviving run parent. The destructive cleanup is "
            "a separate, gated follow-up -- out of scope here."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Report bucketed counts (confirmed_corrupt / "
            "confirmed_clean_single_run / no_run_edge) and how many "
            "confirmed-corrupt nodes would be tagged. Writes nothing. "
            "This is the default mode."
        ),
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help=(
            "Tag the confirmed-corrupt set (batched, idempotent), then "
            "verify zero remain untagged."
        ),
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        metavar="N",
        help=f"Rows per transaction for --apply (default: {DEFAULT_BATCH_SIZE}).",
    )
    parser.add_argument(
        "--neo4j-url",
        metavar="URL",
        default=None,
        help="Neo4j Bolt URL (overrides server-config.yaml / env var)",
    )
    parser.add_argument(
        "--neo4j-user",
        metavar="USER",
        default=None,
        help="Neo4j username (overrides server-config.yaml / env var)",
    )
    parser.add_argument(
        "--neo4j-password",
        metavar="PW",
        default=None,
        help="Neo4j password (overrides server-config.yaml / env var)",
    )
    args = parser.parse_args()

    if args.apply and args.dry_run:
        parser.error("--dry-run and --apply are mutually exclusive")

    settings = get_settings()
    neo4j_url = args.neo4j_url or settings.neo4j_url
    neo4j_user = args.neo4j_user or settings.neo4j_user
    neo4j_password = args.neo4j_password or settings.neo4j_password

    print(f"Connecting to Neo4j at {neo4j_url} as {neo4j_user}\n")
    driver = GraphDatabase.driver(neo4j_url, auth=(neo4j_user, neo4j_password))
    try:
        with driver.session() as neo_session:
            if args.apply:
                return run_apply(neo_session, args.batch_size)
            # --dry-run is the default: no writes unless --apply is explicit.
            return run_dry_run(neo_session)
    finally:
        driver.close()


if __name__ == "__main__":
    sys.exit(main())
