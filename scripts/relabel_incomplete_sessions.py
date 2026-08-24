#!/usr/bin/env python
"""Maintenance script: one-off backfill removing the stale IncompleteSession
false-positive marker from historical Session nodes (the one-off backfill
half of the IncompleteSession relabel fix).

NOT product code -- integration-tested in
tests/neo4j/test_relabel_incomplete_sessions.py.  Never execute this file as
part of the normal application lifecycle.

Background
----------
:IncompleteSession is stamped at session:end only when a Session node has no
type label yet.  Forked sub-sessions drain in independent, concurrently
draining per-session queues, so a child's session:end can be processed
BEFORE its session:fork/session:start -- stamping the marker before the real
terminal type arrives.  The heal-forward classify() fix
(SessionLabelStateMachine.classify(), see handlers/data_layer_2/session.py)
heals this FORWARD: every start/fork transition now strips a stale
IncompleteSession marker the moment it is processed.  That fix handles
new/future events.  This script is the one-off backfill for nodes that were
ALREADY mislabeled before the classify() fix shipped and will never see
another start/fork event to heal them forward.

POST-DEPLOY GATE
----------------
Run this script ONLY after the heal-forward classify() fix is deployed AND
verified live.  Running --apply before that fix is deployed races fresh
mislabeling: new out-of-order nodes can still be stamped IncompleteSession by
the old code while this script is removing the stale marker from historical
nodes, so a subsequent read could see the marker reappear on a node this
script "fixed." The heal-forward fix must be live first so the false-positive
population stops growing before this one-off cleans up the existing backlog.

Selector direction correction (found while writing the Neo4j integration
tests for this script -- NOT what an earlier draft's literal Cypher assumed)
--------------------------------------------------------------------------
An earlier draft's Cypher fragments write the linked-event check as
``(s)<-[:SOURCED_FROM]-(:SessionStartEvent)`` -- i.e. SessionStartEvent as
the edge SOURCE, Session as the TARGET.  Verifying against the shipping
write path shows the real graph is the other way around:

* ``SessionHandler._handle_start``/``_handle_fork``
  (handlers/data_layer_2/session.py) call
  ``graph.upsert_edge(session_id, data_layer_1_node_id, {"type": "SOURCED_FROM"})``
  -- ``session_id`` is the FIRST (src) argument.
* ``Neo4jGraphStore._edge_merge_cypher`` (neo4j_store.py) emits
  ``MERGE (src)-[r:{edge_type}]->(dst)`` with src/dst bound to the first/second
  ``upsert_edge`` arguments respectively.
* Every other SOURCED_FROM bridge in the codebase (ToolCall -> ToolPreEvent,
  Prompt -> PromptSubmitEvent, etc. -- see tests/handlers/data_layer_1/test_default.py
  and tests/integration/test_event_pipeline.py) follows the same convention:
  the domain entity is the edge SOURCE, the data_layer_1 event node is the
  TARGET.

So the real graph shape is ``(Session)-[:SOURCED_FROM]->(SessionStartEvent)``,
NOT the reverse.  This script's selectors use the code-verified direction
``(s)-[:SOURCED_FROM]->(:SessionStartEvent)`` /
``(s)-[:SOURCED_FROM]->(:SessionForkEvent)`` throughout.  A selector built on
the earlier draft's literal (reversed) arrow would never match a single real
node and would silently do nothing.

Concrete execution shape
-------------------------------
A single-statement, server-side batched REMOVE using
``CALL { ... } IN TRANSACTIONS OF $batch_size ROWS`` (the pattern proven in
scripts/tag_legacy_pooled_iterations.py -- NOT the per-node Python loop of
scripts/repair_dual_labels.py).  The false-positive selector
(_FALSE_POSITIVE_MATCH below) is the ONE module constant reused by the
diagnostic, count, sample, collect, and apply queries -- one definition of
"false positive."  ``apply_relabel()`` additionally collects the node_ids it
actually touched (for the printed "removed N node(s)" report) via ``RETURN
collect(s.node_id)`` after the batched CALL block -- the same "outer
variable survives the per-batch commits" technique
``tag_legacy_pooled_iterations.py`` uses for its ``RETURN count(i)``, just
projecting ids instead of a count.  This is a REPORTING count only: the
undo-log (below) is sourced from a separate, EARLIER, read-only collection --
see below for why.

Read-only reconciliation diagnostic, REQUIRED before --apply
--------------------------------------------------------------------
The selector's core assumption -- "a linked SessionStartEvent/SessionForkEvent
implies the terminal label is already set" -- holds for today's handler code
but is UNVERIFIED for historical nodes written before this fix existed.  So
--apply is hard-gated on a read-only diagnostic (``diagnostic_report()``)
that this script always runs first:

* ``linked_but_untyped`` -- IncompleteSession nodes with a linked start/fork
  event but NO terminal label.  This is the assumption's blind spot.  If this
  is > 0, the assumption is FALSIFIED on this DB and --apply REFUSES (exit 1,
  no write), printing the count and up to 20 sample node_ids.  The operator
  must reconcile (or narrow the selector to the terminal-label clause only)
  before re-running --apply.
* ``typed_but_unlinked`` -- IncompleteSession nodes with a terminal label but
  no linked start/fork event at all.  Informational only; does not gate.

(The POST-DEPLOY GATE banner above is the third hardening measure this script
applies.)

Lightweight rollback
---------------------------
--apply writes a plain JSON undo-log (--undo-log PATH, default a timestamped
path in the current directory) BEFORE it mutates the graph.  The log is
sourced from ``collect_false_positive_ids()`` -- a read-only, pre-mutation
collection of the FULL false-positive candidate set (same _FALSE_POSITIVE_MATCH
selector as everything else) -- NOT from the ids ``apply_relabel()`` reports
as touched.  This ordering matters: ``apply_relabel()``'s batched
``CALL { ... } IN TRANSACTIONS OF N ROWS`` COMMITS PER BATCH, so a mid-run
crash can leave some REMOVEs already durable in Neo4j.  Writing the undo-log
from the pre-mutation candidate set means that even a crash after batch 1 of
N still leaves a COMPLETE undo-log on disk -- there is no auditability
window where a committed removal has no undo record anywhere.  See
``run_apply()`` for the inline safety argument for why logging the (superset)
candidate set instead of the (subset) touched set is safe.  The file's
header records the Neo4j host and an ISO-8601 generation timestamp.
--restore PATH re-adds :IncompleteSession to exactly those node_ids via the
same batched ``CALL { ... } IN TRANSACTIONS OF N ROWS`` shape.  No
edge/temporal snapshot is needed: this is one non-destructive label REMOVE,
fully recomputable from the node_id list alone.

Acceptance check
------------------------
Both --dry-run and --apply print a population summary before (and --apply
also after) any write: total :IncompleteSession count, and the
genuine-incomplete count (:IncompleteSession AND NOT linked to any
start/fork event at all -- the true, un-mislabeled data-loss population this
script must never touch).  This lets the operator see the population move.

"Only once" guarantees (both hold, no version machinery)
----------------------------------------------------------
1. Out-of-band -- standalone script, run manually post-upgrade; NEVER at
   server startup, and NOT triggered by any product code path.
2. Idempotent -- the false-positive selector's WHERE clause leads with
   ``s:IncompleteSession``, so once a node's marker is removed it no longer
   matches; re-running --apply against an already-healed graph matches (and
   touches) zero rows.  Exactly-once degrades safely to at-least-once.

Leaves untouched the genuine ~0.5% (no linked start/fork event at all --
these carry no evidence that the labeling race, rather than a real data-loss
event, produced the marker).

SCHEMA_VERSION is unchanged (this is a data backfill, not a schema change).

Modes
-----
--dry-run (default)
    Read-only report: population summary, the reconciliation diagnostic
    (informational here), and the count + a bounded sample (first 20
    node_ids) of what --apply would touch.  Writes NOTHING.  Always exits 0
    (health-check, not a gate).

--apply
    Runs the reconciliation diagnostic first.  If ``linked_but_untyped`` > 0,
    REFUSES (exit 1, no write).  Otherwise runs the batched REMOVE
    (``apply_relabel``), writes the undo-log, and prints the before/after
    population summary.

--restore PATH
    Reads an undo-log written by a previous --apply and re-adds
    :IncompleteSession to exactly those node_ids.

Connection
----------
Connection details come from :func:`context_intelligence_server.config.get_settings`.
Resolution order (highest first):

1. Environment variables with prefix ``AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_``.
2. ``server-config.yaml`` in the current working directory.
3. Built-in defaults.

--neo4j-url/--neo4j-user/--neo4j-password override the resolved settings.
This script is standalone: it connects to Neo4j directly via the ``neo4j``
driver and does NOT import or start the FastAPI application.

Exit codes
----------
* 0 -- --dry-run (always); --apply completed (whether or not any node
  needed healing); --restore completed.
* 1 -- --apply refused by the reconciliation-diagnostic gate
  (``linked_but_untyped`` > 0); or --restore could not read the given
  undo-log file.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

from context_intelligence_server.config import get_settings
from neo4j import GraphDatabase

DEFAULT_BATCH_SIZE = 500
DEFAULT_SAMPLE_LIMIT = 20

# ---------------------------------------------------------------------------
# Selector fragments -- the ONE place each predicate is defined.  Every
# diagnostic / count / sample / apply query below is built from these same
# fragments, so there is exactly one definition of "has a terminal label" and
# "has a linked start/fork event."  Both fragments are only ever embedded
# inside a WHERE clause (standard Cypher pattern-predicate usage) -- never
# used as a bare expression inside RETURN/CASE.
# ---------------------------------------------------------------------------

# A Session node carries a real terminal type.
_HAS_TERMINAL_LABEL = "(s:RootSession OR s:SubSession OR s:ForkedSession)"

# A Session node has its own linked SessionStartEvent/SessionForkEvent.
# Direction verified against the shipping write path -- see "Selector
# direction correction" in the module docstring above.
_HAS_LINKED_START_OR_FORK = (
    "( (s)-[:SOURCED_FROM]->(:SessionStartEvent) "
    "OR (s)-[:SOURCED_FROM]->(:SessionForkEvent) )"
)

# The ONE false-positive selector.  An IncompleteSession node is a
# false positive if it already carries a real terminal label, OR has its own
# linked start/fork event.  This is the exact set --apply removes the stale
# marker from.  ``WHERE s:IncompleteSession`` leads the predicate, which is
# also what makes the operation idempotent: once removed, a node no longer
# matches (see "Only once guarantees" above).
_FALSE_POSITIVE_MATCH = (
    "MATCH (s:Session) WHERE s:IncompleteSession "
    f"AND ( {_HAS_TERMINAL_LABEL} OR {_HAS_LINKED_START_OR_FORK} )"
)


# ---------------------------------------------------------------------------
# Read-only helpers
# ---------------------------------------------------------------------------


def population_summary(session) -> dict[str, int]:
    """Acceptance check: total IncompleteSession count + genuine-incomplete count.

    genuine_incomplete = IncompleteSession AND NOT linked to any
    SessionStartEvent/SessionForkEvent at all -- the true data-loss
    population this fix must never touch.
    """
    total = session.run(
        "MATCH (s:Session) WHERE s:IncompleteSession RETURN count(s) AS n"
    ).single()["n"]

    genuine = session.run(
        "MATCH (s:Session) WHERE s:IncompleteSession "
        f"AND NOT {_HAS_LINKED_START_OR_FORK} "
        "RETURN count(s) AS n"
    ).single()["n"]

    return {"total": total or 0, "genuine_incomplete": genuine or 0}


def diagnostic_report(session) -> dict[str, Any]:
    """Read-only reconciliation diagnostic.  No writes.

    Returns:
        {
            "linked_but_untyped": int,
            "linked_but_untyped_samples": list[str],  # up to 20 node_ids
            "typed_but_unlinked": int,
        }

    linked_but_untyped is the assumption's blind spot: an IncompleteSession
    node with a linked start/fork event but NO terminal label yet.  If this
    is > 0 anywhere in the graph, the selector's core assumption is
    FALSIFIED on this DB and --apply must refuse.

    typed_but_unlinked is informational only (never gates --apply): an
    IncompleteSession node with a terminal label but no linked start/fork
    event at all.
    """
    linked_but_untyped_rows = session.run(
        "MATCH (s:Session) WHERE s:IncompleteSession "
        f"AND {_HAS_LINKED_START_OR_FORK} AND NOT {_HAS_TERMINAL_LABEL} "
        "RETURN s.node_id AS node_id"
    )
    linked_but_untyped_ids = [row["node_id"] for row in linked_but_untyped_rows]

    typed_but_unlinked = (
        session.run(
            "MATCH (s:Session) WHERE s:IncompleteSession "
            f"AND {_HAS_TERMINAL_LABEL} AND NOT {_HAS_LINKED_START_OR_FORK} "
            "RETURN count(s) AS n"
        ).single()["n"]
        or 0
    )

    return {
        "linked_but_untyped": len(linked_but_untyped_ids),
        "linked_but_untyped_samples": linked_but_untyped_ids[:DEFAULT_SAMPLE_LIMIT],
        "typed_but_unlinked": typed_but_unlinked,
    }


def count_false_positives(session) -> int:
    """Count of IncompleteSession nodes the false-positive selector matches
    (i.e. how many --apply would touch)."""
    return (
        session.run(f"{_FALSE_POSITIVE_MATCH} RETURN count(s) AS n").single()["n"] or 0
    )


def sample_false_positives(session, limit: int = DEFAULT_SAMPLE_LIMIT) -> list[str]:
    """Bounded sample of node_ids the false-positive selector matches.

    ``limit`` is always an internal int constant (never user-controlled
    text), so it is safe to interpolate directly into the LIMIT clause.
    """
    rows = session.run(
        f"{_FALSE_POSITIVE_MATCH} RETURN s.node_id AS node_id LIMIT {limit}"
    )
    return [row["node_id"] for row in rows]


def collect_false_positive_ids(session) -> list[str]:
    """Read-only collection of the FULL (unbounded) set of node_ids the
    false-positive selector matches -- i.e. every candidate --apply would
    touch.

    This is the pre-mutation read ``run_apply`` uses to source the
    undo-log BEFORE calling ``apply_relabel`` (see "undo-log-before-mutation"
    in ``run_apply``'s docstring for the ordering rationale). Unlike
    ``sample_false_positives`` (bounded by ``DEFAULT_SAMPLE_LIMIT``, for
    human-readable reporting), this has no LIMIT -- it must capture every
    candidate so the undo-log is complete even if a subsequent crash
    prevents ``apply_relabel`` from reporting its own touched set.
    """
    result = session.run(f"{_FALSE_POSITIVE_MATCH} RETURN collect(s.node_id) AS ids")
    return list(result.single()["ids"] or [])


# ---------------------------------------------------------------------------
# Mutating operations
# ---------------------------------------------------------------------------


def apply_relabel(session, batch_size: int = DEFAULT_BATCH_SIZE) -> list[str]:
    """The single-statement, server-side batched REMOVE.

    Strips :IncompleteSession from every node matched by
    _FALSE_POSITIVE_MATCH, batched via
    ``CALL { ... } IN TRANSACTIONS OF $batch_size ROWS`` (the pattern in
    scripts/tag_legacy_pooled_iterations.py).  Returns the node_ids actually
    touched -- needed for the undo-log.

    Idempotent: because the outer MATCH re-requires ``s:IncompleteSession``,
    a node healed by a previous run no longer matches; re-running this
    function against an already-healed graph returns [].

    This function performs NO gate check.  Callers that must honor the
    reconciliation gate use ``run_apply()``, not this function directly.
    """
    result = session.run(
        f"{_FALSE_POSITIVE_MATCH} "
        "CALL { WITH s REMOVE s:IncompleteSession } IN TRANSACTIONS OF $batch_size ROWS "
        "RETURN collect(s.node_id) AS touched_ids",
        batch_size=batch_size,
    )
    return list(result.single()["touched_ids"] or [])


def restore_ids(
    session, node_ids: list[str], batch_size: int = DEFAULT_BATCH_SIZE
) -> int:
    """--restore: re-add :IncompleteSession to exactly the given node_ids.

    Uses the same batched ``CALL { ... } IN TRANSACTIONS OF N ROWS`` shape as
    apply_relabel.  Returns the number of nodes actually restored (node_ids
    that still exist in the graph).
    """
    result = session.run(
        "UNWIND $node_ids AS nid "
        "MATCH (s:Session {node_id: nid}) "
        "CALL { WITH s SET s:IncompleteSession } IN TRANSACTIONS OF $batch_size ROWS "
        "RETURN count(s) AS restored",
        node_ids=node_ids,
        batch_size=batch_size,
    )
    return result.single()["restored"] or 0


# ---------------------------------------------------------------------------
# Undo-log
# ---------------------------------------------------------------------------


def _neo4j_host(url: str) -> str:
    """Return a host-only fragment of a Neo4j URL for the undo-log header."""
    return urlparse(url).hostname or url


def default_undo_log_path() -> str:
    """Default timestamped undo-log path, used when --undo-log is omitted."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"relabel_incomplete_sessions_undo_{ts}.json"


def write_undo_log(path: str, neo4j_url: str, node_ids: list[str]) -> None:
    """Write an undo-log: header (Neo4j host + ISO timestamp) + touched node_ids."""
    payload = {
        "neo4j_host": _neo4j_host(neo4j_url),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "node_ids": node_ids,
    }
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)


def read_undo_log(path: str) -> dict[str, Any]:
    """Read an undo-log written by write_undo_log."""
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


# ---------------------------------------------------------------------------
# Reporting / orchestration
# ---------------------------------------------------------------------------


def run_dry_run(session) -> int:
    """Read-only report.  Writes nothing.  Always returns 0 (health-check,
    not a gate)."""
    before = population_summary(session)
    print("DRY RUN -- relabel_incomplete_sessions.py (no writes)\n")
    print("POPULATION SUMMARY:")
    print(f"  total IncompleteSession      : {before['total']}")
    print(f"  genuine_incomplete (no link) : {before['genuine_incomplete']}\n")

    diag = diagnostic_report(session)
    print("RECONCILIATION DIAGNOSTIC (gates --apply; informational here):")
    print(f"  linked_but_untyped : {diag['linked_but_untyped']}")
    print(f"  typed_but_unlinked : {diag['typed_but_unlinked']} (informational only)")
    if diag["linked_but_untyped"]:
        print("\n  --apply would REFUSE (see module docstring). Sample node_ids:")
        for node_id in diag["linked_but_untyped_samples"]:
            print(f"    - {node_id}")
    print()

    would_touch = count_false_positives(session)
    sample = sample_false_positives(session)
    print(
        f"WOULD APPLY -- {would_touch} node(s) would have :IncompleteSession removed."
    )
    if sample:
        print(f"Sample node_ids (up to {DEFAULT_SAMPLE_LIMIT}):")
        for node_id in sample:
            print(f"  - {node_id}")
    return 0


def run_apply(session, batch_size: int, undo_log_path: str, neo4j_url: str) -> int:
    """Gated --apply: diagnostic first, write only if the gate is clear.

    1. Print the BEFORE population summary.
    2. Run the reconciliation diagnostic.  If linked_but_untyped > 0,
       REFUSE (no write): print the count and up to 20 sample node_ids,
       return 1.
    3. Otherwise: collect the FULL false-positive candidate set (read-only),
       write the undo-log from THAT set, THEN run apply_relabel() to
       mutate, print the AFTER population summary, return 0.

    undo-log-before-mutation
    -----------------------------------
    Step 3 writes the undo-log BEFORE calling apply_relabel(), not after.
    apply_relabel()'s batched ``CALL { ... } IN TRANSACTIONS OF N ROWS``
    COMMITS PER BATCH -- each batch's REMOVE is durable in Neo4j the moment
    that batch completes, well before apply_relabel() returns. If the
    process crashed mid-run under the OLD ordering (mutate first, log
    after), some REMOVEs would already be committed with NO undo record
    anywhere -- an auditability hole. Logging the pre-mutation candidate set
    first closes that window: even a crash after the very first batch still
    leaves a COMPLETE undo-log on disk.

    Why logging the (pre-mutation) candidate set instead of apply_relabel's
    (post-mutation) touched set is safe:
      - candidate_ids is collected via the exact same _FALSE_POSITIVE_MATCH
        selector apply_relabel() uses, moments before apply_relabel() runs
        -- so candidate_ids is a SUPERSET of (on a clean run, EQUAL to)
        whatever apply_relabel() actually removes.
      - restore_ids() (the --restore consumer of this log) is idempotent
        over a superset: it SETs :IncompleteSession on each node_id that
        still exists. For a node that WAS removed, this correctly restores
        the marker. For a candidate that -- for any reason -- was NOT
        removed (still carries :IncompleteSession), the SET is a no-op: the
        label is already present. So restoring from the candidate superset
        is always safe -- it never incorrectly re-adds a marker that
        shouldn't exist, and it never fails to restore a node that WAS
        removed.
      - On a clean run (the common case, and the only case this script's
        idempotent selector allows in practice) candidate_ids == the ids
        apply_relabel() reports touched, so the printed "removed N node(s)"
        count is UNCHANGED by this reordering -- only the undo-log's source
        and its write timing move earlier.
    """
    before = population_summary(session)
    print("POPULATION SUMMARY (before):")
    print(f"  total IncompleteSession      : {before['total']}")
    print(f"  genuine_incomplete (no link) : {before['genuine_incomplete']}\n")

    diag = diagnostic_report(session)
    print("RECONCILIATION DIAGNOSTIC:")
    print(f"  linked_but_untyped : {diag['linked_but_untyped']}")
    print(f"  typed_but_unlinked : {diag['typed_but_unlinked']} (informational only)\n")

    if diag["linked_but_untyped"] > 0:
        print(
            "APPLY REFUSED -- the selector's assumption (a linked "
            "SessionStartEvent/SessionForkEvent implies the terminal label is "
            f"already set) does NOT hold on this DB: {diag['linked_but_untyped']} "
            "node(s) are linked but carry no terminal label.\n"
        )
        print("Sample node_ids (up to 20):")
        for node_id in diag["linked_but_untyped_samples"]:
            print(f"  - {node_id}")
        print(
            "\nReconcile these nodes (or narrow the selector to the "
            "terminal-label clause only) before re-running --apply."
        )
        return 1

    # Read-only pre-mutation capture, THEN write the undo-log, THEN mutate.
    # See the "undo-log-before-mutation" note in this function's
    # docstring for why this order (and logging the candidate superset
    # rather than apply_relabel's touched subset) is safe.
    candidate_ids = collect_false_positive_ids(session)
    write_undo_log(undo_log_path, neo4j_url, candidate_ids)
    print(
        f"Undo log written to {undo_log_path} ({len(candidate_ids)} node_id(s), "
        "pre-mutation candidate set).\n"
    )

    touched_ids = apply_relabel(session, batch_size)
    print(f"APPLIED -- removed :IncompleteSession from {len(touched_ids)} node(s).\n")

    after = population_summary(session)
    print("POPULATION SUMMARY (after):")
    print(f"  total IncompleteSession      : {after['total']}")
    print(f"  genuine_incomplete (no link) : {after['genuine_incomplete']}")
    return 0


def run_restore(session, path: str, batch_size: int) -> int:
    """--restore: re-add :IncompleteSession to exactly the ids in *path*."""
    try:
        snap = read_undo_log(path)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"RESTORE FAILED -- could not read undo-log {path}: {exc}")
        return 1

    node_ids = snap.get("node_ids", [])
    restored = restore_ids(session, node_ids, batch_size)
    print(
        f"RESTORED -- re-added :IncompleteSession to {restored}/{len(node_ids)} "
        f"node(s) from {path} (generated_at={snap.get('generated_at')})."
    )
    return 0


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> int:
    """Parse arguments, connect to Neo4j, and run the requested mode."""
    parser = argparse.ArgumentParser(
        prog="relabel_incomplete_sessions.py",
        description=(
            "One-off maintenance tool (the backfill half of the IncompleteSession "
            "relabel fix): remove the stale :IncompleteSession false-positive "
            "marker from historical Session nodes that already carry a real "
            "terminal label or a linked SessionStartEvent/SessionForkEvent. "
            "POST-DEPLOY GATE -- run only after the heal-forward classify() fix "
            "is deployed and verified live. --apply is itself gated on a "
            "read-only reconciliation diagnostic; see module docstring."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Read-only report: population summary, reconciliation diagnostic, and the "
            "count + bounded sample of what --apply would touch. Writes "
            "nothing. This is the default mode."
        ),
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help=(
            "Run the reconciliation diagnostic; if clear, remove :IncompleteSession from "
            "the false-positive set (batched, idempotent), write an "
            "undo-log, and print the before/after population summary."
        ),
    )
    parser.add_argument(
        "--restore",
        metavar="PATH",
        default=None,
        help="Re-add :IncompleteSession to exactly the node_ids in an undo-log file.",
    )
    parser.add_argument(
        "--undo-log",
        metavar="PATH",
        default=None,
        help=(
            "Path to write the undo-log (used with --apply). Defaults to "
            "a timestamped path in the current directory."
        ),
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        metavar="N",
        help=f"Rows per transaction for --apply/--restore (default: {DEFAULT_BATCH_SIZE}).",
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

    modes_selected = sum(bool(m) for m in (args.dry_run, args.apply, args.restore))
    if modes_selected > 1:
        parser.error("--dry-run, --apply, and --restore are mutually exclusive")

    settings = get_settings()
    neo4j_url = args.neo4j_url or settings.neo4j_url
    neo4j_user = args.neo4j_user or settings.neo4j_user
    neo4j_password = args.neo4j_password or settings.neo4j_password

    print(f"Connecting to Neo4j at {neo4j_url} as {neo4j_user}\n")
    driver = GraphDatabase.driver(neo4j_url, auth=(neo4j_user, neo4j_password))
    try:
        with driver.session() as neo4j_session:
            if args.restore:
                return run_restore(neo4j_session, args.restore, args.batch_size)
            if args.apply:
                undo_log_path = args.undo_log or default_undo_log_path()
                return run_apply(
                    neo4j_session, args.batch_size, undo_log_path, neo4j_url
                )
            # --dry-run is the default: no writes unless --apply is explicit.
            return run_dry_run(neo4j_session)
    finally:
        driver.close()


if __name__ == "__main__":
    sys.exit(main())
