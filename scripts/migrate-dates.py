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
Every conversion is guarded by ``valueType(x.prop) = 'STRING NOT NULL'`` so
re-running against an already-migrated database is safe (zero rows matched,
zero rows touched).

Malformed values (per-row tolerance)
------------------------------------
Values are parsed **per row** with the SAME parser the write path uses
(``datetime.fromisoformat`` — see ``neo4j_store._convert_temporal_props``), and
the parsed Python ``datetime`` objects are written back via ``UNWIND``.  This is
deliberate: it means the migration converts *exactly* the set of strings the
live write path would convert, and no more.

A value that cannot be parsed (an invalid calendar date such as
``'2026-02-30T00:00:00Z'`` or outright garbage) is **not** silently skipped and
does **not** abort the batch.  It is QUARANTINED: left unchanged, reported
loudly (label/rel, property, elementId, raw value), and counted against the exit
code.  Good rows sharing a batch with a bad row still convert — the previous
``SET x.prop = datetime(x.prop)`` form rolled the whole batch back on the first
unparseable value, taking valid rows down with it.

Timezone note (parity vs. single-type goal)
--------------------------------------------
``datetime.fromisoformat`` (Python 3.11+) accepts offset-less inputs
(``'2026-06-22 10:00:00'``, date-only ``'2026-06-22'``).  Those parse to a
*naive* datetime, which the driver stores as **LOCAL DATETIME** — a different
type from the **ZONED DATETIME** produced by offset-aware strings.  This matches
the live write path exactly (parity is the point), and the real event source
always emits offset-aware RFC 3339 (``+00:00``), so production data converges on
ZONED DATETIME.  But if offset-less legacy strings ever exist they will heal to
LOCAL DATETIME, not ZONED DATETIME — still a single-type-per-value outcome, but
not uniform across the column.  Such values are surfaced by the write path's own
WARNING logs and can be found with
``MATCH (x) WHERE valueType(x.<prop>) = 'LOCAL DATETIME NOT NULL' RETURN x`` if a
uniformity guarantee is ever required.

Exit codes
----------
* 0 — Zero remaining STRING temporal values.
* 1 — Remaining STRING temporal values exist (unparseable / quarantined), **or**
  ``--dry-run`` found values that WOULD convert or could not be parsed
  (blocks deployment).
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime

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
# Parsing — parity with the write path
# ---------------------------------------------------------------------------


def _parse_iso(value: str) -> datetime | None:
    """Parse an ISO-8601 string to a Python ``datetime`` or return ``None``.

    Uses ``datetime.fromisoformat`` — the SAME parser as the live write path
    (``neo4j_store._convert_temporal_props``), so this migration converts
    exactly the strings a fresh write would convert.  Python 3.11+ accepts a
    trailing ``Z`` as a synonym for ``+00:00``.  Returns ``None`` on any
    ``ValueError`` so the caller can quarantine the value instead of crashing.
    """
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Match-clause builders (single entity bound to variable ``x``)
# ---------------------------------------------------------------------------


def _node_match(label: str) -> str:
    return f"MATCH (x:{label})"


def _edge_match(rel: str) -> str:
    return f"MATCH ()-[x:{rel}]->()"


# ---------------------------------------------------------------------------
# Helpers — classify (read-only, used by --dry-run)
# ---------------------------------------------------------------------------


def _classify(session, match: str, prop: str) -> tuple[int, dict[str, str]]:
    """Count convertible STRING values and collect unparseable ones. No writes.

    Returns ``(convertible_count, quarantine)`` where *quarantine* maps
    ``elementId -> raw string value`` for every value that fails ``_parse_iso``.
    Paginates with SKIP/ORDER BY (safe: dry-run makes no writes, so the STRING
    set is stable).
    """
    convertible = 0
    quarantine: dict[str, str] = {}
    offset = 0
    while True:
        rows = session.run(
            f"{match} "
            f"WHERE x.{prop} IS NOT NULL AND x.{prop} <> '' "
            f"AND valueType(x.{prop}) = 'STRING NOT NULL' "
            f"RETURN elementId(x) AS eid, x.{prop} AS val "
            f"ORDER BY eid SKIP $skip LIMIT {BATCH_SIZE}",
            skip=offset,
        ).data()
        if not rows:
            break
        offset += len(rows)
        for row in rows:
            if _parse_iso(row["val"]) is None:
                quarantine[row["eid"]] = row["val"]
            else:
                convertible += 1
    return convertible, quarantine


# ---------------------------------------------------------------------------
# Helpers — migrate (per-row tolerant, quarantines unparseable values)
# ---------------------------------------------------------------------------


def _migrate(session, match: str, prop: str) -> tuple[int, dict[str, str]]:
    """Convert STRING *prop* to native datetime, one batch at a time.

    Parseable values are converted (parsed in Python, written back via UNWIND);
    unparseable values are quarantined (left unchanged, recorded and returned).

    Returns ``(converted_count, quarantine)``.

    Termination: each batch either converts rows (which then no longer match the
    ``valueType = 'STRING NOT NULL'`` guard) or adds them to *quarantine* (which
    is excluded via ``NOT elementId(x) IN $seen``).  Either way the pending set
    strictly shrinks, so the loop always ends.  ``$seen`` only ever holds
    quarantined ids (expected to be tiny), so the exclusion stays cheap.
    """
    converted = 0
    quarantine: dict[str, str] = {}
    while True:
        rows = session.run(
            f"{match} "
            f"WHERE x.{prop} IS NOT NULL AND x.{prop} <> '' "
            f"AND valueType(x.{prop}) = 'STRING NOT NULL' "
            f"AND NOT elementId(x) IN $seen "
            f"RETURN elementId(x) AS eid, x.{prop} AS val "
            f"LIMIT {BATCH_SIZE}",
            seen=list(quarantine.keys()),
        ).data()
        if not rows:
            break
        good: list[dict[str, object]] = []
        for row in rows:
            parsed = _parse_iso(row["val"])
            if parsed is None:
                quarantine[row["eid"]] = row["val"]
            else:
                good.append({"eid": row["eid"], "dt": parsed})
        if good:
            session.run(
                f"UNWIND $rows AS row {match} "
                f"WHERE elementId(x) = row.eid "
                f"SET x.{prop} = row.dt",
                rows=good,
            )
            converted += len(good)
        # Loop continues: converted rows drop out of the guard, quarantined rows
        # are excluded via $seen — guaranteeing forward progress every batch.
    return converted, quarantine


# ---------------------------------------------------------------------------
# Quarantine reporting
# ---------------------------------------------------------------------------


def _print_quarantine(rows: list[tuple[str, str, str, str]]) -> None:
    """Loudly report unparseable values that were left as STRING.

    *rows* is a list of ``(label_or_rel, property, elementId, raw_value)``.
    """
    print("QUARANTINE — UNPARSEABLE temporal strings (left unchanged, NOT migrated):\n")
    for where, prop, eid, val in rows:
        print(f"  ! {where}.{prop}  elementId={eid}  value={val!r}")
    print(
        f"\n  {len(rows)} value(s) could not be parsed as ISO-8601 "
        "(datetime.fromisoformat) and remain STRING.\n"
        "  These must be inspected/repaired by hand — until then any recency\n"
        "  query on that property stays mixed-type. This script converted every\n"
        "  parseable value regardless; no good row was blocked by these.\n"
    )


# ---------------------------------------------------------------------------
# High-level operations
# ---------------------------------------------------------------------------


def run_dry_run(session) -> int:
    """Print convertible/unparseable counts and return total pending."""
    print("DRY RUN — values that WOULD be converted:\n")
    print(f"{'Label/Rel':<30} {'Property':<25} {'Convertible':>12} {'Unparseable':>12}")
    print("-" * 81)
    total_convertible = 0
    quarantined: list[tuple[str, str, str, str]] = []
    for label, prop in NODE_TEMPORAL:
        convertible, quarantine = _classify(session, _node_match(label), prop)
        if convertible or quarantine:
            print(f"{label:<30} {prop:<25} {convertible:>12} {len(quarantine):>12}")
        total_convertible += convertible
        quarantined.extend((label, prop, eid, val) for eid, val in quarantine.items())
    for rel, prop in EDGE_TEMPORAL:
        convertible, quarantine = _classify(session, _edge_match(rel), prop)
        if convertible or quarantine:
            print(f"{rel:<30} {prop:<25} {convertible:>12} {len(quarantine):>12}")
        total_convertible += convertible
        quarantined.extend((rel, prop, eid, val) for eid, val in quarantine.items())
    print("-" * 81)
    print(f"{'TOTAL':<56} {total_convertible:>12} {len(quarantined):>12}\n")
    if quarantined:
        _print_quarantine(quarantined)
    return total_convertible + len(quarantined)


def run_migration(session) -> list[tuple[str, str, str, str]]:
    """Convert all parseable STRING temporal properties to native datetime.

    Returns the list of quarantined ``(label_or_rel, property, elementId,
    raw_value)`` tuples (empty when everything converted cleanly).
    """
    print("MIGRATION — converting STRING → native datetime:\n")
    print(f"{'Label/Rel':<30} {'Property':<25} {'Converted':>10}")
    print("-" * 67)
    quarantined: list[tuple[str, str, str, str]] = []
    for label, prop in NODE_TEMPORAL:
        converted, quarantine = _migrate(session, _node_match(label), prop)
        if converted:
            print(f"{label:<30} {prop:<25} {converted:>10}")
        quarantined.extend((label, prop, eid, val) for eid, val in quarantine.items())
    for rel, prop in EDGE_TEMPORAL:
        converted, quarantine = _migrate(session, _edge_match(rel), prop)
        if converted:
            print(f"{rel:<30} {prop:<25} {converted:>10}")
        quarantined.extend((rel, prop, eid, val) for eid, val in quarantine.items())
    print("-" * 67)
    print()
    if quarantined:
        _print_quarantine(quarantined)
    return quarantined


def run_verification(session) -> int:
    """Print verification table (OK/FAIL per row) and return total remaining.

    After a run, any 'remaining' STRING value is by definition one the parser
    rejected (i.e. quarantined) — every parseable value has been converted.
    """
    print("VERIFICATION — remaining STRING temporal values:\n")
    print(f"{'Label/Rel':<30} {'Property':<25} {'Remaining':>10} {'Status':>8}")
    print("-" * 76)
    total = 0
    for label, prop in NODE_TEMPORAL:
        remaining = _count_remaining(session, _node_match(label), prop)
        status = "OK" if remaining == 0 else "FAIL"
        print(f"{label:<30} {prop:<25} {remaining:>10} {status:>8}")
        total += remaining
    for rel, prop in EDGE_TEMPORAL:
        remaining = _count_remaining(session, _edge_match(rel), prop)
        status = "OK" if remaining == 0 else "FAIL"
        print(f"{rel:<30} {prop:<25} {remaining:>10} {status:>8}")
        total += remaining
    print("-" * 76)
    print(f"{'TOTAL':<56} {total:>10}\n")
    return total


def _count_remaining(session, match: str, prop: str) -> int:
    """Return the number of entities still holding a STRING value for *prop*."""
    result = session.run(
        f"{match} "
        f"WHERE x.{prop} IS NOT NULL AND x.{prop} <> '' "
        f"AND valueType(x.{prop}) = 'STRING NOT NULL' "
        f"RETURN count(x) AS pending"
    )
    return result.single()["pending"]


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
            "Unparseable values are quarantined and reported, never crash the "
            "run. Exit 0 = complete, 1 = incomplete/pending."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Report how many values WOULD be converted (and how many are "
            "unparseable) without making changes. Exits 1 if any pending values "
            "exist (blocks deployment). Doubles as a drift health-check: on a "
            "healthy graph it prints zero and exits 0."
        ),
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

    settings = get_settings()
    neo4j_url = args.neo4j_url or settings.neo4j_url
    neo4j_user = args.neo4j_user or settings.neo4j_user
    neo4j_password = args.neo4j_password or settings.neo4j_password

    print(f"Connecting to Neo4j at {neo4j_url} as {neo4j_user}\n")
    driver = GraphDatabase.driver(neo4j_url, auth=(neo4j_user, neo4j_password))
    try:
        with driver.session() as neo_session:
            if args.dry_run:
                pending = run_dry_run(neo_session)
                return 1 if pending else 0

            run_migration(neo_session)
            remaining = run_verification(neo_session)
            if remaining:
                print(
                    "MIGRATION INCOMPLETE — "
                    f"{remaining} value(s) remain STRING (unparseable; see "
                    "QUARANTINE above)."
                )
                return 1
            print("MIGRATION COMPLETE")
            return 0
    finally:
        driver.close()


if __name__ == "__main__":
    sys.exit(main())
