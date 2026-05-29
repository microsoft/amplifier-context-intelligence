#!/usr/bin/env python
"""Maintenance tool: one-off conversion of legacy ISO-string temporal properties
on a Neo4j server to native ZONED DATETIME.

**NOT product code — no unit tests.**  Validation is the DTU 3-step sequence
described in Task 5.

DEPLOYMENT GATE
---------------
Run this script against the live server **BEFORE** deploying Phase 1 code.
Deploying code before migration leaves the database in a mixed-type state
(some nodes with STRING, others with ZONED DATETIME) which causes query errors.

Correct order:

1. Run ``migrate-dates.py`` (this script, exit code 0).
2. Confirm that the DTU verification tests pass.
3. Deploy Phase 1 server code.

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
    uv run python scripts/migrate-dates.py

Idempotency
-----------
Every conversion is guarded by ``valueType(n.prop) = 'STRING NOT NULL'`` so
re-running against an already-migrated database is safe (zero rows matched,
zero rows touched).

Exit codes
----------
* 0 — Zero remaining STRING temporal values.
* 1 — Remaining STRING temporal values exist, **or** ``--dry-run`` found values
  that WOULD convert (blocks deployment).
"""

from __future__ import annotations

import argparse
import sys

from neo4j import GraphDatabase

from context_intelligence_server.config import get_settings

BATCH_SIZE = 1000

# ---------------------------------------------------------------------------
# (label, property) pairs for node temporal properties.
# This list is intentionally NOT derived from TEMPORAL_PROPS — the registry
# holds property *names* only for the flush path; migration needs (label,
# property) pairs for per-label scans.
# ---------------------------------------------------------------------------
NODE_TEMPORAL: list[tuple[str, str]] = [
    ("Session", "started_at"),
    ("Session", "ended_at"),
    ("Session", "last_updated"),
    ("OrchestratorRun", "started_at"),
    ("OrchestratorRun", "ended_at"),
    ("OrchestratorRun", "completed_at"),
    ("Iteration", "started_at"),
    ("ContentBlock", "started_at"),
    ("ContentBlock", "ended_at"),
    ("ToolCall", "started_at"),
    ("ToolCall", "ended_at"),
    ("Delegation", "started_at"),
    ("Delegation", "ended_at"),
    ("Delegation", "resumed_at"),
    ("Delegation", "cancelled_at"),
    ("RecipeRun", "started_at"),
    ("RecipeRun", "ended_at"),
    ("RecipeRun", "last_loop_iteration_at"),
    ("RecipeRun", "loop_completed_at"),
    ("RecipeStep", "started_at"),
    ("Prompt", "occurred_at"),
    ("Cancellation", "occurred_at"),
    ("ContextCompaction", "occurred_at"),
    ("SkillLoad", "started_at"),
    ("SkillLoad", "ended_at"),
    ("Event", "occurred_at"),
]

# ---------------------------------------------------------------------------
# (relationship_type, property) pairs for edge temporal properties.
# ---------------------------------------------------------------------------
EDGE_TEMPORAL: list[tuple[str, str]] = [
    ("HAS_EVENT", "occurred_at"),
    ("HAS_SUBSESSION", "occurred_at"),
    ("FORKED", "occurred_at"),
]


# ---------------------------------------------------------------------------
# Helpers — count pending
# ---------------------------------------------------------------------------


def _count_pending_node(session, label: str, prop: str) -> int:
    """Return the number of nodes with a STRING value for *prop*."""
    query = (
        f"MATCH (n:{label}) "
        f"WHERE n.{prop} IS NOT NULL AND n.{prop} <> '' "
        f"AND valueType(n.{prop}) = 'STRING NOT NULL' "
        f"RETURN count(n) AS pending"
    )
    result = session.run(query)
    return result.single()["pending"]


def _count_pending_edge(session, rel: str, prop: str) -> int:
    """Return the number of relationships with a STRING value for *prop*."""
    query = (
        f"MATCH ()-[r:{rel}]->() "
        f"WHERE r.{prop} IS NOT NULL AND r.{prop} <> '' "
        f"AND valueType(r.{prop}) = 'STRING NOT NULL' "
        f"RETURN count(r) AS pending"
    )
    result = session.run(query)
    return result.single()["pending"]


# ---------------------------------------------------------------------------
# Helpers — migrate
# ---------------------------------------------------------------------------


def _migrate_node(session, label: str, prop: str) -> int:
    """Convert STRING *prop* to ZONED DATETIME on all *label* nodes.

    Processes in batches of :data:`BATCH_SIZE`.  Returns total converted count.
    """
    total = 0
    while True:
        query = (
            f"MATCH (n:{label}) "
            f"WHERE n.{prop} IS NOT NULL AND n.{prop} <> '' "
            f"AND valueType(n.{prop}) = 'STRING NOT NULL' "
            f"WITH n LIMIT {BATCH_SIZE} "
            f"SET n.{prop} = datetime(n.{prop}) "
            f"RETURN count(n) AS updated"
        )
        result = session.run(query)
        updated = result.single()["updated"]
        total += updated
        if updated == 0:
            break
    return total


def _migrate_edge(session, rel: str, prop: str) -> int:
    """Convert STRING *prop* to ZONED DATETIME on all *rel* relationships.

    Processes in batches of :data:`BATCH_SIZE`.  Returns total converted count.
    """
    total = 0
    while True:
        query = (
            f"MATCH ()-[r:{rel}]->() "
            f"WHERE r.{prop} IS NOT NULL AND r.{prop} <> '' "
            f"AND valueType(r.{prop}) = 'STRING NOT NULL' "
            f"WITH r LIMIT {BATCH_SIZE} "
            f"SET r.{prop} = datetime(r.{prop}) "
            f"RETURN count(r) AS updated"
        )
        result = session.run(query)
        updated = result.single()["updated"]
        total += updated
        if updated == 0:
            break
    return total


# ---------------------------------------------------------------------------
# High-level operations
# ---------------------------------------------------------------------------


def run_dry_run(session) -> int:
    """Print pending counts table and return total pending."""
    print("DRY RUN — values that WOULD be converted:\n")
    print(f"{'Label/Rel':<30} {'Property':<25} {'Pending':>10}")
    print("-" * 67)
    total = 0
    for label, prop in NODE_TEMPORAL:
        count = _count_pending_node(session, label, prop)
        if count:
            print(f"{label:<30} {prop:<25} {count:>10}")
            total += count
    for rel, prop in EDGE_TEMPORAL:
        count = _count_pending_edge(session, rel, prop)
        if count:
            print(f"{rel:<30} {prop:<25} {count:>10}")
            total += count
    print("-" * 67)
    print(f"{'TOTAL':<56} {total:>10}\n")
    return total


def run_migration(session) -> None:
    """Convert all STRING temporal properties to ZONED DATETIME."""
    print("MIGRATION — converting STRING → ZONED DATETIME:\n")
    print(f"{'Label/Rel':<30} {'Property':<25} {'Converted':>10}")
    print("-" * 67)
    for label, prop in NODE_TEMPORAL:
        converted = _migrate_node(session, label, prop)
        if converted:
            print(f"{label:<30} {prop:<25} {converted:>10}")
    for rel, prop in EDGE_TEMPORAL:
        converted = _migrate_edge(session, rel, prop)
        if converted:
            print(f"{rel:<30} {prop:<25} {converted:>10}")
    print("-" * 67)
    print()


def run_verification(session) -> int:
    """Print verification table (OK/FAIL per row) and return total remaining."""
    print("VERIFICATION — remaining STRING temporal values:\n")
    print(f"{'Label/Rel':<30} {'Property':<25} {'Remaining':>10} {'Status':>8}")
    print("-" * 76)
    total = 0
    for label, prop in NODE_TEMPORAL:
        remaining = _count_pending_node(session, label, prop)
        status = "OK" if remaining == 0 else "FAIL"
        print(f"{label:<30} {prop:<25} {remaining:>10} {status:>8}")
        total += remaining
    for rel, prop in EDGE_TEMPORAL:
        remaining = _count_pending_edge(session, rel, prop)
        status = "OK" if remaining == 0 else "FAIL"
        print(f"{rel:<30} {prop:<25} {remaining:>10} {status:>8}")
        total += remaining
    print("-" * 76)
    print(f"{'TOTAL':<56} {total:>10}\n")
    return total


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> int:
    """Parse arguments, connect to Neo4j, and run migration or dry-run."""
    parser = argparse.ArgumentParser(
        prog="migrate-dates.py",
        description=(
            "One-off maintenance tool: convert legacy ISO-string temporal "
            "properties on a Neo4j server to native ZONED DATETIME. "
            "Run BEFORE deploying Phase 1 code. "
            "Exit 0 = complete, 1 = incomplete."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Report how many values WOULD be converted without making changes. "
            "Exits 1 if any pending values exist (blocks deployment)."
        ),
    )
    args = parser.parse_args()

    settings = get_settings()
    driver = GraphDatabase.driver(
        settings.neo4j_url,
        auth=(settings.neo4j_user, settings.neo4j_password),
    )
    try:
        with driver.session() as neo_session:
            if args.dry_run:
                pending = run_dry_run(neo_session)
                return 1 if pending else 0

            run_migration(neo_session)
            remaining = run_verification(neo_session)
            if remaining:
                print("MIGRATION INCOMPLETE")
                return 1
            print("MIGRATION COMPLETE")
            return 0
    finally:
        driver.close()


if __name__ == "__main__":
    sys.exit(main())
