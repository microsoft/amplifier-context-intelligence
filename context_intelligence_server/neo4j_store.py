"""Neo4j implementation of GraphStore and QueryableStore protocols.

Provides a workspace-scoped, buffered graph store backed by Neo4j.
Writes are buffered in memory and persisted via UNWIND-based batch Cypher on flush.
Both ``GraphStore`` and ``QueryableStore`` protocols are satisfied.

Canonical workspace naming is used throughout; all scoping is done via the
``workspace`` attribute exclusively.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime
from typing import Any, Generator, LiteralString, cast

from neo4j import AsyncGraphDatabase, unit_of_work as _unit_of_work
from neo4j.exceptions import DriverError, Neo4jError

_LOG = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Cypher identifier validation
# ---------------------------------------------------------------------------

_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_DEFAULT_EDGE_TYPE = "RELATED"

# ---------------------------------------------------------------------------
# Temporal property registry
# ---------------------------------------------------------------------------
# Every property name in this set is stored as a Neo4j ZONED DATETIME.
#
# On WRITE: _convert_temporal_props() parses ISO strings to Python datetime;
#   the Neo4j driver then writes those datetime objects as ZONED DATETIME nodes.
# On READ:  _normalize_temporal() converts neo4j.time.DateTime back to Python
#   datetime objects.
#
# IMPORTANT: neo4j.time types must NEVER leave this module.
#   services.py, pipeline.py, and handlers deal in Python stdlib types only.
#
# RULE: add a name here whenever you add a temporal property to any handler.
#   Forgetting means the value lands as a plain string silently — no error,
#   no warning, just wrong data.
#
# Note: last_updated is the one field that does NOT follow the *_at convention
#   and is listed deliberately.  Do NOT replace this explicit set with a suffix
#   heuristic — a heuristic would silently miss last_updated.
#
# FUTURE: when the first duration-typed property arrives, convert this frozenset
#   to TEMPORAL_PROPERTIES: dict[str, type] and add neo4j.time.Duration to the
#   _sanitize_properties allow-list at that point — not before.
TEMPORAL_PROPS: frozenset[str] = frozenset(
    {
        "started_at",
        "ended_at",
        "occurred_at",
        "completed_at",
        "last_updated",  # only temporal field NOT ending in _at
        "resumed_at",
        "cancelled_at",
        "last_loop_iteration_at",
        "loop_completed_at",
    }
)

# ---------------------------------------------------------------------------
# Terminal-label lattice
# ---------------------------------------------------------------------------
# The three terminal Session-type labels form a strict lattice:
#   ForkedSession > SubSession > RootSession
#
# _LATTICE_NORMALIZATION is appended (in-line) to any Cypher SET statement
# that adds at least one terminal label.  Running the normalization inside the
# SAME Cypher statement as the SET ensures the node write-lock is still held
# when the CASE WHEN conditions are evaluated, so the later-committing writer
# in a concurrent two-drainer race always normalises to exactly one terminal.
#
# Race-safety guarantee (READ_COMMITTED):
#   The naive "WHERE NOT n:ForkedSession … SET n:SubSession" pattern evaluates
#   the guard at MATCH time and can re-introduce a dual if a SubSession SET
#   commits after a concurrent ForkedSession SET.  By placing the REMOVE
#   AFTER the SET (separated only by WITH n), the lock is already held before
#   the CASE WHEN is evaluated, so no interleaving can escape normalisation.
#
# Invariants:
#   • n:ForkedSession  → REMOVE n:SubSession, REMOVE n:RootSession
#   • n:SubSession     → REMOVE n:RootSession  (only when ForkedSession absent)
_TERMINAL_LABELS: frozenset[str] = frozenset(
    {"ForkedSession", "SubSession", "RootSession"}
)
_LATTICE_NORMALIZATION = (
    " WITH n"
    " FOREACH (_ IN CASE WHEN n:ForkedSession THEN [1] ELSE [] END |"
    " REMOVE n:SubSession REMOVE n:RootSession)"
    " WITH n"
    " FOREACH (_ IN CASE WHEN n:SubSession THEN [1] ELSE [] END |"
    " REMOVE n:RootSession)"
)

# Universal node label.  EVERY node (Session and non-Session alike) carries
# :Node, and there is a composite :Node(node_id, workspace) index.  This lets
# the non-Session node MERGE seek the index instead of doing an AllNodesScan
# over the whole graph (the 1.3M-node drain stall).  :Node is an *additional*
# label only — it is never the sole identity of a node and never replaces a
# type label, so it preserves the original label-free MERGE semantics: identity
# is still (node_id, workspace), independent of which type labels a node has
# accumulated across flush cycles.
_UNIVERSAL_NODE_LABEL = "Node"

# The non-Session node MERGE, extracted so tests can assert its query plan
# (NodeIndexSeek, never AllNodesScan).  MERGE on the universal :Node label so
# the composite :Node(node_id, workspace) index backs the lookup.
_NODE_MERGE_CYPHER = (
    "UNWIND $rows AS row "
    f"MERGE (n:{_UNIVERSAL_NODE_LABEL} "
    "{node_id: row.node_id, workspace: row.props.workspace}) "
    "SET n += row.props"
)

# Single-node lookup-by-identity MATCH prefix, seeking the universal :Node
# label so the composite :Node(node_id, workspace) index backs the lookup
# (NodeIndexSeek) instead of an AllNodesScan.  Shared by every label-write
# query in _write_batch (the per-node label SET and the label-patch add/remove)
# so they are DRY and the tests can assert their query plan against the exact
# production string.
_NODE_MATCH_BY_ID = (
    f"MATCH (n:{_UNIVERSAL_NODE_LABEL} {{node_id: $node_id, workspace: $workspace}})"
)


def _edge_merge_cypher(edge_type: str) -> str:
    """Return the UNWIND edge-MERGE query for *edge_type* — self-healing endpoints.

    src/dst are ``MERGE``d (not ``MATCH``ed) on the universal :Node label, then the
    relationship is ``MERGE``d.  ``MERGE`` on the composite :Node(node_id, workspace)
    key uses a NodeIndexSeek (idx_node_universal / the :Node uniqueness constraint),
    never an AllNodesScan, so the 1.3M-node index-seek behaviour from #19 is kept.

    Why MERGE and not MATCH: the old ``MATCH (src) MATCH (dst)`` was an inner join
    that SILENTLY dropped the edge whenever either endpoint was not yet committed
    (the HAS_SUBSESSION parent-absent race — but pervasive: SOURCED_FROM and ~20
    other edge types legitimately write edges to not-yet-committed endpoints).
    ``MERGE`` creates a bare ``:Node`` placeholder for an absent endpoint instead
    of dropping the edge; the placeholder converges with the later typed write
    because every node writer keys identity on ``:Node`` (Session/Event/etc. add
    their type label via ``SET``), backed by the :Node(node_id, workspace)
    uniqueness constraint so concurrent MERGEs stay atomic.  Never-silent without
    aborting the legitimate eventual-consistency ingest.

    *edge_type* MUST already be validated via ``_validate_identifier`` by the
    caller (it is interpolated into the Cypher, so it cannot be user input).
    """
    return (
        "UNWIND $rows AS row "
        f"MERGE (src:{_UNIVERSAL_NODE_LABEL} "
        "{node_id: row.src_id, workspace: $workspace}) "
        f"MERGE (dst:{_UNIVERSAL_NODE_LABEL} "
        "{node_id: row.dst_id, workspace: $workspace}) "
        f"MERGE (src)-[r:{edge_type}]->(dst) "
        "SET r += row.props"
    )


# Batch size for the one-time :Node backfill migration (CALL ... IN
# TRANSACTIONS OF N ROWS).  A literal int is required by Cypher for the batch
# size, so this constant is interpolated (never user-supplied).
_NODE_BACKFILL_BATCH = 10_000


def _validate_identifier(name: str, kind: str) -> None:
    """Raise ``ValueError`` if *name* is not a safe Neo4j label / relationship-type identifier.

    Accepts only ``[A-Za-z_][A-Za-z0-9_]*`` — i.e. no spaces, hyphens, parentheses,
    or other characters that could be exploited for Cypher injection.

    Args:
        name: The identifier string to validate.
        kind: Human-readable category used in the error message (e.g. ``"label"``).

    Raises:
        ValueError: If *name* contains unsafe characters.
    """
    if not _SAFE_IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid Neo4j {kind} identifier: {name!r}")


def _convert_temporal_props(props: dict[str, Any]) -> None:
    """Parse ISO-8601 strings to Python ``datetime`` objects for registered temporal properties.

    Mutates *props* in place; returns ``None``.

    Rules:
    - Iterates over ``TEMPORAL_PROPS``; skips keys absent from *props*.
    - Skips values that are not ``str`` or are empty strings (already ``datetime``
      or other type, or empty string handled downstream by ``_sanitize_properties``).
    - Parses non-empty strings via ``datetime.fromisoformat`` (Python 3.11+
      accepts trailing ``Z`` as a synonym for ``+00:00``).
    - On ``ValueError``, logs a WARNING including the key and value, leaves the
      value unchanged, and does NOT raise (non-fatal).

    Must be called as a statement before ``_sanitize_properties``:
        ``_convert_temporal_props(props)``
        ``props = Neo4jGraphStore._sanitize_properties(props)``
    Never chain: ``props = _convert_temporal_props(props)``  — returns ``None``.
    """
    for key in TEMPORAL_PROPS:
        if key not in props:
            continue
        value = props[key]
        if not isinstance(value, str) or value == "":
            continue
        try:
            props[key] = datetime.fromisoformat(value)
        except ValueError:
            _LOG.warning(
                "_convert_temporal_props: could not parse %r=%r as ISO 8601 datetime; "
                "leaving value unchanged",
                key,
                value,
            )


def _normalize_temporal(value: Any) -> Any:
    """Convert neo4j.time.DateTime to Python datetime via .to_native().

    The Neo4j driver returns temporal properties as neo4j.time.DateTime;
    convert to Python datetime via .to_native() so no neo4j.time type ever
    leaves this module; all other values returned unchanged.

    This is the read-path half of the type boundary,
    _convert_temporal_props is the write-path half.

    Rationale: getattr (not isinstance against neo4j.time.DateTime) avoids
    importing the driver's temporal class at module top-level and transparently
    handles Date, Time, and DateTime (all expose .to_native()); plain
    Python/JSON values have no .to_native() and pass straight through.
    """
    to_native = getattr(value, "to_native", None)
    if callable(to_native):
        return to_native()
    return value


# ---------------------------------------------------------------------------
# Benign schema-error allow-list for constraint creation.
# ---------------------------------------------------------------------------
# These four codes are the ONLY ones treated as benign when CREATE CONSTRAINT
# fails — benign concurrent-create-race / already-exists codes (incl.
# DeadlockDetected from a concurrent CREATE CONSTRAINT race). Every one is kept
# deliberately, and none can mask data corruption:
#
#   * Neo.ClientError.Schema.EquivalentSchemaRuleAlreadyExists
#   * Neo.TransientError.Transaction.DeadlockDetected
#   * Neo.ClientError.Schema.IndexWithNameAlreadyExists
#   * Neo.ClientError.Schema.ConstraintWithNameAlreadyExists
#
# Rationale:
#   - Real duplicate data NEVER surfaces as one of these — it surfaces as a
#     DIFFERENT code, Neo.ClientError.Schema.ConstraintCreationFailed, which is
#     deliberately NOT in this set and is surfaced at ERROR.
#   - IndexWithNameAlreadyExists is the actual production-observed symptom of a
#     concurrent-schema race (a backing index already present).
#   - DeadlockDetected is NOT an "already exists" signal: it is the empirically
#     verified CONCURRENT-CREATE RACE loser — a second worker issuing the same
#     CREATE CONSTRAINT at the same time loses the deadlock. It is benign because
#     the winner creates the constraint.
#   - EquivalentSchemaRuleAlreadyExists and DeadlockDetected were reproduced by
#     an empirical spike on neo4j:5.26.22-community / driver 6.1.0.
#
# Note: this benign-code set is NOT shared with _create_index above, which keys
# only off EquivalentSchemaRuleAlreadyExists. What the constraint blocks below
# and _create_index DO share (after widening _create_index) is the DriverError
# tolerance: a non-Neo4jError connectivity DriverError is logged and swallowed
# rather than re-raised into the flush path.
_BENIGN_SCHEMA_CODES: frozenset[str] = frozenset(
    {
        "Neo.ClientError.Schema.EquivalentSchemaRuleAlreadyExists",
        "Neo.TransientError.Transaction.DeadlockDetected",
        "Neo.ClientError.Schema.IndexWithNameAlreadyExists",
        "Neo.ClientError.Schema.ConstraintWithNameAlreadyExists",
    }
)


async def ensure_neo4j_schema(
    driver: Any,
    database: str = "neo4j",
    workspace: str = "",
) -> bool:
    """Create Neo4j indexes and constraints idempotently.

    Intended to be called **once at server startup** (before any requests are
    accepted) so that the uniqueness constraint on Session nodes is active before
    concurrent ``flush()`` transactions execute ``MERGE``.

    Runs a deduplication pass *first* so that any pre-existing duplicate Session
    and Event nodes (from a previous run without the constraint) do not block
    constraint creation.

    Args:
        driver:    An ``AsyncDriver`` instance created via
                   ``AsyncGraphDatabase.driver(...)``.
        database:  Target Neo4j database name (default: ``"neo4j"``).
        workspace: Reserved for future workspace-scoped schema; currently unused.

    Returns:
        ``True`` iff the schema is **fully established** — every index and
        uniqueness constraint was created (or already exists via a benign race).
        ``False`` if any index/constraint could not be created (e.g. Neo4j was
        unreachable and the connectivity error was swallowed to avoid
        dead-lettering real events). Callers use this to decide whether to retry
        schema init on a later flush rather than latching a half-built schema and
        leaving the uniqueness constraint permanently absent. The best-effort
        Step-1 dedup pass does not affect the return value.
    """
    async with driver.session(database=database) as session:
        # ------------------------------------------------------------------
        # Step 1: deduplicate any pre-existing duplicate (node_id, workspace)
        # nodes.  This MUST run before constraint creation so a dirty graph does
        # not cause the CREATE CONSTRAINT statements (Session, Event, and the
        # universal :Node constraint in Step 6) to fail.
        #
        # Three passes:
        #   1a. GLOBAL by (node_id, workspace) across EVERY label — required for
        #       the Step-6 :Node uniqueness constraint, which spans all node
        #       types (Session/Event/OrchestratorRun/Step/ToolExecution/...).
        #       Keeps the RICHEST node (most labels) so a fully-typed node always
        #       wins over a bare :Node placeholder; DETACH DELETE the rest.
        #       Dups here come from the #19 dead-backfill bug (an indexed
        #       MERGE (n:Node {...}) duplicated legacy untagged nodes).
        #   1b/1c. Session / Event specifically — kept for clarity and because
        #       their per-label uniqueness constraints (Steps 3/4) still apply.
        #       (Redundant after 1a, but cheap and explicit.)
        # All passes are best-effort (non-fatal) and do not affect the return.
        # ------------------------------------------------------------------
        try:
            await session.run(
                "MATCH (n) "
                "WITH n.node_id AS nid, n.workspace AS ws, collect(n) AS nodes "
                "WHERE size(nodes) > 1 "
                # keep the node carrying the most labels (typed > bare placeholder)
                "WITH nodes, reduce(best = nodes[0], x IN nodes | "
                "CASE WHEN size(labels(x)) > size(labels(best)) THEN x ELSE best END) "
                "AS keep "
                "UNWIND [x IN nodes WHERE x <> keep] AS duplicate "
                "DETACH DELETE duplicate"
            )
            await session.run(
                "MATCH (s:Session) "
                "WITH s.node_id AS nid, s.workspace AS ws, collect(s) AS nodes "
                "WHERE size(nodes) > 1 "
                "UNWIND tail(nodes) AS duplicate "
                "DETACH DELETE duplicate"
            )
            await session.run(
                "MATCH (e:Event) "
                "WITH e.node_id AS nid, e.workspace AS ws, collect(e) AS nodes "
                "WHERE size(nodes) > 1 "
                "UNWIND tail(nodes) AS duplicate "
                "DETACH DELETE duplicate"
            )
        except Exception as exc:  # noqa: BLE001
            _LOG.warning(
                "ensure_neo4j_schema: deduplication query failed (non-fatal): %s", exc
            )

        # ------------------------------------------------------------------
        # Step 2: create node_id indexes (idempotent via IF NOT EXISTS).
        # ``CREATE INDEX ... IF NOT EXISTS`` is idempotent for serial callers,
        # but two callers racing on a fresh database can both pass the existence
        # check and then collide, surfacing
        # ``Neo.ClientError.Schema.EquivalentSchemaRuleAlreadyExists``. Because
        # multiple independent stores auto-run this on their first concurrent
        # flush (each store has its own ``_schema_initialized`` flag), that race
        # is reachable. The colliding rule is exactly the one we wanted, so the
        # error is benign — swallow it (mirroring the constraint handling below)
        # while re-raising any other schema error.
        # ------------------------------------------------------------------
        async def _create_index(statement: str) -> bool:
            try:
                await session.run(statement)
            except (Neo4jError, DriverError) as exc:
                if (
                    isinstance(exc, Neo4jError)
                    and exc.code
                    == "Neo.ClientError.Schema.EquivalentSchemaRuleAlreadyExists"
                ):
                    _LOG.debug(
                        "ensure_neo4j_schema: index already exists (concurrent "
                        "create race, benign): %s",
                        statement,
                    )
                    # The index exists / will exist via the race winner: established.
                    return True
                elif isinstance(exc, Neo4jError):
                    # A dangerous Neo4jError code we do not recognise as benign.
                    raise
                else:
                    # A connectivity DriverError that is NOT a Neo4jError
                    # (ServiceUnavailable / SessionExpired). It has no meaningful
                    # .code for our allow-list. Crucially, we continue rather than
                    # re-raise: index creation runs on the flush path via
                    # _ensure_schema (BEFORE the constraint steps), and a re-raise
                    # would be counted as a flush failure and could dead-letter real
                    # events. This mirrors the DriverError tolerance in the
                    # constraint blocks below.
                    _LOG.error(
                        "ensure_neo4j_schema: could not create index (connectivity "
                        "error); continuing without it: %s",
                        exc,
                    )
                    # Index NOT created: report failure so the caller does not latch.
                    return False
            # session.run succeeded: the index is established.
            return True

        # Track whether every index/constraint was established. We attempt them
        # ALL each run (no short-circuit) so a single transient failure does not
        # skip the rest — but the aggregate latches only when nothing was missed.
        fully_established = True

        for label in (
            "Session",
            "OrchestratorRun",
            "Step",
            "ToolExecution",
            "Event",
        ):
            fully_established = (
                await _create_index(
                    f"CREATE INDEX IF NOT EXISTS FOR (n:{label}) ON (n.node_id)"
                )
                and fully_established
            )
        fully_established = (
            await _create_index(
                "CREATE INDEX idx_session_workspace IF NOT EXISTS "
                "FOR (n:Session) ON (n.workspace)"
            )
            and fully_established
        )

        # NOTE: the composite :Node(node_id, workspace) lookup index that backs the
        # universal-label node MERGE (NodeIndexSeek, not AllNodesScan — the
        # 1.3M-node drain-stall fix) is now provided by the :Node UNIQUENESS
        # CONSTRAINT created in Step 6 below (a uniqueness constraint carries its
        # own backing range index).  A standalone ``CREATE INDEX idx_node_universal``
        # here would CONFLICT with that constraint ("a constraint cannot be created
        # until the index has been dropped"), so it is intentionally NOT created
        # here; Step 6 drops any legacy idx_node_universal first, then creates the
        # constraint.

        # ------------------------------------------------------------------
        # Steps 3 & 4: uniqueness constraints on Session then Event nodes.
        # Both must run AFTER deduplication (step 1) so pre-existing duplicates
        # do not cause constraint creation to fail. Combined with MERGE (n ...)
        # in flush(), they make concurrent MERGEs atomic and prevent duplicate
        # Session/Event nodes under load. The two constraints are identical in
        # shape, so both route through the single _create_constraint helper.
        # ------------------------------------------------------------------
        async def _create_constraint(session: Any, name: str, statement: str) -> bool:
            """Create a uniqueness constraint, tolerating benign races and
            connectivity errors (never re-raise into the flush path).

            Benign already-exists / concurrent-schema-race codes (the
            ``_BENIGN_SCHEMA_CODES`` allow-list) are swallowed at DEBUG. Anything
            else — either a dangerous Neo4jError code (e.g. ConstraintCreationFailed)
            or a connectivity DriverError that is NOT a Neo4jError (ServiceUnavailable
            / SessionExpired, which has no meaningful ``.code`` for our allow-list) —
            is reported generically at ERROR. Crucially, we continue rather than
            re-raise: this runs on the flush path via _ensure_schema, and a re-raise
            would be counted as a flush failure and could dead-letter real events.

            Returns ``True`` when the constraint is established (the statement
            succeeded, or a benign code means the winner created/will create it) and
            ``False`` on the ERROR-and-continue path (constraint NOT created) so the
            caller can decide whether to retry rather than latch a half-built schema.
            """
            try:
                await session.run(statement)
            except (Neo4jError, DriverError) as exc:
                if isinstance(exc, Neo4jError) and exc.code in _BENIGN_SCHEMA_CODES:
                    _LOG.debug(
                        "ensure_neo4j_schema: %s uniqueness constraint already "
                        "present (benign concurrent-schema race, code=%s)",
                        name,
                        exc.code,
                    )
                    # The constraint exists / will exist via the race winner.
                    return True
                else:
                    code = exc.code if isinstance(exc, Neo4jError) else None
                    _LOG.error(
                        "ensure_neo4j_schema: could not create %s uniqueness "
                        "constraint (code=%s); continuing without it — duplicate %s "
                        "data may be present: %s",
                        name,
                        code,
                        name,
                        exc,
                    )
                    # Constraint NOT created: report failure so the caller retries.
                    return False
            # session.run succeeded: the constraint is established.
            return True

        fully_established = (
            await _create_constraint(
                session,
                "Session",
                "CREATE CONSTRAINT session_node_id_workspace_unique IF NOT EXISTS "
                "FOR (n:Session) REQUIRE (n.node_id, n.workspace) IS UNIQUE",
            )
            and fully_established
        )
        fully_established = (
            await _create_constraint(
                session,
                "Event",
                "CREATE CONSTRAINT event_node_id_workspace_unique IF NOT EXISTS "
                "FOR (n:Event) REQUIRE (n.node_id, n.workspace) IS UNIQUE",
            )
            and fully_established
        )

        # ------------------------------------------------------------------
        # Step 5: backfill the universal :Node label onto pre-existing nodes.
        # The non-Session node MERGE targets (n:Node {node_id, workspace}) so it
        # can use the composite :Node(node_id, workspace) index.  Nodes written
        # before this label existed have NO :Node label, so the indexed MERGE
        # would CREATE a duplicate of them rather than match them.  Tag every
        # such node first.
        #
        # This MUST run BEFORE the :Node uniqueness constraint (Step 6): the
        # constraint cannot be created over a graph that still has untagged nodes
        # that would become duplicate (node_id, workspace) :Node pairs, and the
        # re-keyed Session/edge MERGEs on :Node must find pre-existing nodes
        # rather than fork their identity.
        #
        # Batched + idempotent: CALL { ... } IN TRANSACTIONS commits in chunks
        # (so 1.3M nodes don't blow the per-transaction memory cap), and the
        # WHERE NOT n:Node guard makes re-runs converge to a no-op once the
        # graph is fully tagged.  ensure_neo4j_schema is awaited at the top of
        # _flush_body, so this runs before any flush MERGEs on :Node.
        # The batch size must be a literal in Cypher, so it is interpolated from
        # the in-process int constant (never user input).
        #
        # NOTE (was a latent bug): this block previously sat after an early
        # ``return fully_established`` and was therefore DEAD CODE — the backfill
        # never ran, so legacy untagged nodes silently duplicated on re-write.
        try:
            await session.run(
                cast(
                    LiteralString,
                    f"MATCH (n) WHERE NOT n:{_UNIVERSAL_NODE_LABEL} "
                    f"CALL {{ WITH n SET n:{_UNIVERSAL_NODE_LABEL} }} "
                    f"IN TRANSACTIONS OF {int(_NODE_BACKFILL_BATCH)} ROWS",
                )
            )
        except Exception as exc:  # noqa: BLE001
            _LOG.warning(
                "ensure_neo4j_schema: :Node backfill did not complete "
                "(non-fatal, retried next startup): %s",
                exc,
            )

        # Backfill verification (observability): the migration's "is it done?"
        # signal.  A non-zero remaining-untagged count means the universal :Node
        # identity is NOT yet established graph-wide, so the Step-6 constraint may
        # fail and the re-keyed writers can still fork a legacy node's identity.
        # Surfaced LOUD (never-silent) so an incomplete migration is visible in
        # logs rather than discovered via duplicate nodes.
        try:
            rec = await (
                await session.run(
                    f"MATCH (n) WHERE NOT n:{_UNIVERSAL_NODE_LABEL} "
                    "RETURN count(n) AS remaining"
                )
            ).single()
            remaining = int(rec["remaining"]) if rec is not None else 0
            if remaining:
                _LOG.warning(
                    "ensure_neo4j_schema: :Node backfill incomplete — %d node(s) "
                    "still lack the :Node label; the universal-identity migration "
                    "is NOT complete (re-runs next schema init). Do not rely on the "
                    ":Node uniqueness constraint until this reaches 0.",
                    remaining,
                )
            else:
                _LOG.info(
                    "ensure_neo4j_schema: :Node backfill complete (0 untagged nodes)."
                )
        except Exception as exc:  # noqa: BLE001 - observability only, never break init
            _LOG.debug(
                "ensure_neo4j_schema: backfill verification count skipped (benign): %s",
                exc,
            )

        # ------------------------------------------------------------------
        # Step 6: universal :Node identity uniqueness constraint.
        # Every node writer now keys identity on :Node (Session/Event/etc. add
        # their type label via SET), and the cross-session edge writer MERGEs
        # bare :Node endpoint placeholders.  This constraint is the atomicity
        # guard that keeps concurrent MERGEs on (node_id, workspace) from
        # creating divergent duplicate nodes — the role the :Session constraint
        # used to play for the Session MERGE.  Created AFTER the Step-5 backfill
        # so no pre-existing untagged duplicate blocks it.
        #
        # OPERATIONAL GATE (prod, tracked separately): on the live 1.3M-node
        # graph, run + verify the backfill completes (count `WHERE NOT n:Node`
        # -> 0) before relying on this constraint, and do not enable the re-keyed
        # writers if the constraint is absent.  In the test/fresh-DB path the
        # backfill is a no-op and this constraint is created up-front.
        #
        # A uniqueness constraint carries its OWN backing range index, and Neo4j
        # refuses to create it while a standalone index on the same (label,
        # properties) exists ("a constraint cannot be created until the index has
        # been dropped").  #19 shipped a plain `idx_node_universal` index on
        # :Node(node_id, workspace); drop it first (IF EXISTS, idempotent) so the
        # constraint can take over the seek role.  The Step-5 backfill + Step-1
        # dedup ran first, so the graph satisfies uniqueness before we drop the
        # index and create the constraint.
        # ------------------------------------------------------------------
        try:
            await session.run("DROP INDEX idx_node_universal IF EXISTS")
        except (Neo4jError, DriverError) as exc:  # pragma: no cover - tolerate
            _LOG.debug(
                "ensure_neo4j_schema: DROP INDEX idx_node_universal skipped "
                "(benign): %s",
                exc,
            )
        fully_established = (
            await _create_constraint(
                session,
                "Node",
                "CREATE CONSTRAINT node_node_id_workspace_unique IF NOT EXISTS "
                "FOR (n:Node) REQUIRE (n.node_id, n.workspace) IS UNIQUE",
            )
            and fully_established
        )

        return fully_established


def _serialized_row_size(value: Any) -> int:
    """Return a cheap conservative proxy for the serialized byte size of *value*.

    Uses ``json.dumps(value, default=str)`` so that non-JSON-serialisable values
    (e.g. ``datetime`` objects) are converted via ``str()`` rather than raising.
    This measures the *serialized* form — not ``len()`` on a dict/list, which
    returns the number of keys/elements and is blind to fat nested payloads such
    as large ``messages`` arrays or ``context_snapshot`` dicts.

    Not an exact wire-byte count (no encoding overhead, no Neo4j framing), but a
    consistent, crash-free lower bound suitable for chunk-size decisions.
    """
    try:
        return len(json.dumps(value, default=str))
    except (TypeError, ValueError):
        return len(str(value))


def _chunk_dict(
    snapshot: dict[Any, Any],
    max_rows: int,
    max_bytes: int,
) -> Generator[dict[Any, Any], None, None]:
    """Yield sub-dicts of *snapshot* bounded by *max_rows* and *max_bytes*.

    Dual-bound slicing: a chunk is emitted when either the row count OR the
    accumulated byte size would be exceeded by the next row.  A single row that
    is larger than *max_bytes* is always yielded alone (one-row floor), so the
    generator never loops forever on an oversized row.

    An empty *snapshot* yields nothing.
    """
    chunk: dict[Any, Any] = {}
    cur_rows = 0
    cur_bytes = 0
    for key, value in snapshot.items():
        size = _serialized_row_size(value)
        if chunk and (cur_rows + 1 > max_rows or cur_bytes + size > max_bytes):
            yield chunk
            chunk = {}
            cur_rows = 0
            cur_bytes = 0
        chunk[key] = value
        cur_rows += 1
        cur_bytes += size
    if chunk:
        yield chunk


def _chunk_list(
    snapshot: list[Any],
    max_rows: int,
    max_bytes: int,
) -> Generator[list[Any], None, None]:
    """Yield sub-lists of *snapshot* bounded by *max_rows* and *max_bytes*.

    Identical dual-bound logic to :func:`_chunk_dict` but operates on an
    ordered list rather than a mapping.  A single oversized element is always
    yielded alone (one-row floor).

    An empty *snapshot* yields nothing.
    """
    chunk: list[Any] = []
    cur_rows = 0
    cur_bytes = 0
    for item in snapshot:
        size = _serialized_row_size(item)
        if chunk and (cur_rows + 1 > max_rows or cur_bytes + size > max_bytes):
            yield chunk
            chunk = []
            cur_rows = 0
            cur_bytes = 0
        chunk.append(item)
        cur_rows += 1
        cur_bytes += size
    if chunk:
        yield chunk


async def _write_batch(
    tx: Any,
    node_snapshot: dict[str, dict[str, Any]],
    edge_snapshot: dict[tuple[str, str], dict[str, Any]],
    patch_snapshot: list[dict[str, Any]],
    workspace: str,
) -> None:
    """Execute one buffered batch of node, label, and edge writes on ``tx``.

    Operates on the supplied managed-transaction object ``tx``; it never opens
    or commits a transaction itself, so the same coroutine can run inside either
    a raw ``begin_transaction()`` block or a driver-managed ``execute_write``.

    Idempotency rule #1: the batch is captured by the caller *before* this
    coroutine is invoked (the snapshots are passed as parameters and never read
    from a mutable buffer here), so this is a pure function of its parameters.
    The driver may therefore safely re-run it on a transient error
    (``TransientError`` / ``DeadlockDetected``) without corrupting state.

    Every Cypher result is consumed (``await res.consume()``) rather than
    returned, so this coroutine never leaks a raw ``Result`` object across the
    transaction boundary.
    """
    # ---- nodes ---- (Session nodes use label-aware MERGE + uniqueness constraint)
    session_rows: list[dict[str, Any]] = []  # have Session label
    other_rows: list[dict[str, Any]] = []  # everything else
    label_assignments: list[dict[str, Any]] = []

    for node_id, data in node_snapshot.items():
        labels: list[str] = data.get("labels", [])
        node_props = {k: v for k, v in data.items() if k != "labels"}
        _convert_temporal_props(node_props)  # ISO str -> datetime, in place
        props = Neo4jGraphStore._sanitize_properties(node_props)
        props["workspace"] = workspace
        row: dict[str, Any] = {"node_id": node_id, "props": props}

        if "Session" in labels:
            session_rows.append(row)
        else:
            other_rows.append(row)

        if labels:
            for label in labels:
                _validate_identifier(label, "label")
            label_assignments.append(
                {"node_id": node_id, "labels": sorted(set(labels))}
            )

    # Session nodes: MERGE by the universal :Node identity (atomic under concurrency
    # via the :Node(node_id, workspace) uniqueness constraint); the :Session type
    # label is added via SET.
    # Lock-order hygiene: sort by stable key so every writer acquires node locks in one
    # global order, preventing the out-of-order lock cycle that causes deadlocks.
    session_rows.sort(key=lambda r: r["node_id"])
    if session_rows:
        # MERGE key is :Node (NOT :Session) so a bare :Node placeholder created by a
        # cross-session edge write (_edge_merge_cypher MERGEs its endpoints) CONVERGES
        # with this typed write instead of splitting into two nodes.  Identity is
        # purely (node_id, workspace) on :Node — every node carries :Node, and the
        # :Node uniqueness constraint (ensure_neo4j_schema) makes concurrent MERGEs
        # atomic, the role the :Session constraint used to play for this MERGE.
        res = await tx.run(
            "UNWIND $rows AS row "
            f"MERGE (n:{_UNIVERSAL_NODE_LABEL} "
            "{node_id: row.node_id, workspace: row.props.workspace}) "
            "SET n += row.props, n:Session",
            rows=session_rows,
        )
        await res.consume()

    # Non-session nodes: MERGE on the universal :Node label so the composite
    # :Node(node_id, workspace) index backs the lookup (NodeIndexSeek) instead
    # of an AllNodesScan over the entire graph.  Identity is still purely
    # (node_id, workspace) — :Node is on every node, so this matches the exact
    # same set the old label-free MERGE did, without splitting a node whose type
    # labels change across flush cycles.
    other_rows.sort(key=lambda r: r["node_id"])
    if other_rows:
        res = await tx.run(
            cast(LiteralString, _NODE_MERGE_CYPHER),
            rows=other_rows,
        )
        await res.consume()

    # Set all labels for labeled nodes (primary + extra in one SET per node).
    # For Session nodes, Session label is already set by the MERGE above — this
    # adds any additional type labels (RootSession, SubSession, ForkedSession, etc.)
    # Lattice normalisation: whenever a terminal label is included in the SET, the
    # _LATTICE_NORMALIZATION suffix is appended to the SAME Cypher statement so the
    # node write-lock is held before the CASE WHEN conditions are evaluated.  This
    # prevents the concurrent-writer dual-label race (see _LATTICE_NORMALIZATION).
    label_assignments.sort(key=lambda i: i["node_id"])
    for item in label_assignments:
        labels_str = ":".join(item["labels"])
        # Seek via :Node (idx_node_universal) instead of an AllNodesScan.
        cypher = f"{_NODE_MATCH_BY_ID} SET n:{labels_str}"
        if set(item["labels"]) & _TERMINAL_LABELS:
            cypher += _LATTICE_NORMALIZATION
        res = await tx.run(
            cast(LiteralString, cypher),
            node_id=item["node_id"],
            workspace=workspace,
        )
        await res.consume()

    # ---- label patches (must run AFTER node writes — nodes must exist in Neo4j before MATCH) ----
    for lp in patch_snapshot:
        pid = lp["node_id"]
        for label in lp.get("remove", []):
            _validate_identifier(label, "label")
            res = await tx.run(
                cast(
                    LiteralString,
                    f"{_NODE_MATCH_BY_ID} REMOVE n:{label}",
                ),
                node_id=pid,
                workspace=workspace,
            )
            await res.consume()
        for label in lp.get("add", []):
            _validate_identifier(label, "label")
            # Lattice normalisation for patch-adds of terminal labels: same lock-first
            # guarantee — append the normalization to the SET statement.
            cypher = f"{_NODE_MATCH_BY_ID} SET n:{label}"
            if label in _TERMINAL_LABELS:
                cypher += _LATTICE_NORMALIZATION
            res = await tx.run(
                cast(LiteralString, cypher),
                node_id=pid,
                workspace=workspace,
            )
            await res.consume()

    # ---- edges ----
    edge_groups: dict[str, list[dict[str, Any]]] = {}
    for (src_id, dst_id), data in edge_snapshot.items():
        edge_type: str = data.get("type", _DEFAULT_EDGE_TYPE)
        edge_props = {k: v for k, v in data.items() if k != "type"}
        _convert_temporal_props(edge_props)  # ISO str -> datetime, in place
        props = Neo4jGraphStore._sanitize_properties(edge_props)
        props["workspace"] = workspace
        # Store src_id/dst_id on the relationship so the
        # get_edge() fallback query (WHERE r.src_id = $src_id
        # AND r.dst_id = $dst_id) can locate it after a flush.
        props["src_id"] = src_id
        props["dst_id"] = dst_id
        row = {"src_id": src_id, "dst_id": dst_id, "props": props}
        edge_groups.setdefault(edge_type, []).append(row)

    for edge_type, rows in edge_groups.items():
        _validate_identifier(edge_type, "edge_type")
        edge_merge_query = _edge_merge_cypher(edge_type)
        rows.sort(key=lambda r: (r["src_id"], r["dst_id"]))
        res = await tx.run(
            edge_merge_query,  # type: ignore[arg-type]
            rows=rows,
            workspace=workspace,
        )
        await res.consume()


class Neo4jGraphStore:
    """Neo4j-backed implementation of the GraphStore and QueryableStore protocols.

    Writes are buffered in memory (``_node_buffer``, ``_edge_buffer``) and flushed
    to Neo4j via UNWIND-based batch Cypher. Workspace-scoped; schema indexes are
    created idempotently on first flush.
    """

    def __init__(
        self,
        uri: str,
        auth: tuple | None = None,
        database: str = "neo4j",
        workspace: str | None = None,
        flush_chunk_rows: int = 100,
        flush_chunk_bytes: int = 4_194_304,
        neo4j_lock_timeout: float | None = None,
    ) -> None:
        """Initialise the store and create the async Neo4j driver.

        Args:
            uri:               Bolt/neo4j URI, e.g. ``bolt://localhost:7687``.
            auth:              ``(username, password)`` tuple, or ``None`` for no-auth.
            database:          Target Neo4j database name (default: ``"neo4j"``).
            workspace:         Workspace to scope writes to.  ``None`` resolves to
                               ``"default"`` via the ``workspace`` property.
            neo4j_lock_timeout: Server-side transaction timeout in seconds
                               (Layer B — fail loud).  When set, every
                               ``execute_write`` call is wrapped with
                               ``unit_of_work(timeout=neo4j_lock_timeout)``
                               so a blocked flush raises ``Neo4jError``
                               instead of parking forever.  ``None`` disables
                               the timeout (default: no per-transaction limit).
                               Also sets ``connection_acquisition_timeout`` on
                               the driver to the same value so pool-exhaustion
                               failures also surface quickly.
        """
        # Explicit auto-retry budget for transient errors (e.g. deadlocks) so the
        # managed-transaction retry window is deliberate and reviewable rather than
        # relying on the driver default implicitly. 30.0s is a working default;
        # design Open Question #3 — verify driver 6.1.0 backoff constants before tuning.
        driver_kwargs: dict[str, Any] = {"max_transaction_retry_time": 30.0}
        if neo4j_lock_timeout is not None and neo4j_lock_timeout > 0:
            driver_kwargs["connection_acquisition_timeout"] = neo4j_lock_timeout
        self._driver = AsyncGraphDatabase.driver(uri, auth=auth, **driver_kwargs)
        self._database = database
        self._workspace = workspace
        self._node_buffer: dict[str, dict[str, Any]] = {}
        self._edge_buffer: dict[tuple, dict[str, Any]] = {}
        self._label_patches: list[dict[str, Any]] = []
        self._schema_initialized: bool = False
        self._closed: bool = False
        self._flush_lock: asyncio.Lock = asyncio.Lock()
        self._flush_chunk_rows: int = max(1, flush_chunk_rows)
        self._flush_chunk_bytes: int = max(1, flush_chunk_bytes)
        self._neo4j_lock_timeout: float | None = (
            neo4j_lock_timeout
            if neo4j_lock_timeout and neo4j_lock_timeout > 0
            else None
        )

    # ------------------------------------------------------------------
    # workspace property
    # ------------------------------------------------------------------

    @property
    def workspace(self) -> str:
        """Return the current workspace, defaulting to ``'default'`` when unset."""
        return self._workspace if self._workspace is not None else "default"

    @workspace.setter
    def workspace(self, value: str | None) -> None:
        """Set the workspace (``None`` will resolve to ``'default'`` on read)."""
        self._workspace = value

    # ------------------------------------------------------------------
    # supported_dialects property
    # ------------------------------------------------------------------

    @property
    def supported_dialects(self) -> frozenset[str]:
        """Return the set of supported query dialects."""
        return frozenset({"cypher"})

    # ------------------------------------------------------------------
    # Buffer operations
    # ------------------------------------------------------------------

    async def upsert_node(self, node_id: str, data: dict[str, Any]) -> None:
        """Buffer a node upsert (no I/O).

        If the node already exists in the buffer:
        - ``labels`` lists are *unioned* (no duplicates, original order preserved).
        - All other properties are merged with ``dict.update`` semantics
          (later call wins on conflict).

        If the node does not exist, it is stored as a shallow copy of *data*.
        """
        if node_id not in self._node_buffer:
            self._node_buffer[node_id] = {}

        existing = self._node_buffer[node_id]

        # Union labels, preserving insertion order and avoiding duplicates
        if "labels" in data:
            existing_labels: list = list(existing.get("labels", []))
            for label in data["labels"]:
                if label not in existing_labels:
                    existing_labels.append(label)
            existing["labels"] = existing_labels

        # Update all other properties (labels handled above, skip to avoid overwrite)
        for key, value in data.items():
            if key != "labels":
                existing[key] = value

    async def upsert_edge(self, src_id: str, dst_id: str, data: dict[str, Any]) -> None:
        """Buffer an edge upsert (no I/O).

        If the edge already exists in the buffer, properties are merged via
        ``dict.update`` semantics.  Otherwise the edge is stored as a new entry.
        """
        key = (src_id, dst_id)
        if key not in self._edge_buffer:
            self._edge_buffer[key] = {}
        self._edge_buffer[key].update(data)

    async def get_node(self, node_id: str) -> dict[str, Any] | None:
        """Return node data, checking the in-memory buffer first.

        If the node is found in the buffer, a *shallow copy* is returned so
        that callers cannot accidentally mutate the buffered state.

        If not found in the buffer, falls back to a Neo4j query.  Returns
        ``None`` if the node is absent from both.
        """
        if node_id in self._node_buffer:
            return dict(self._node_buffer[node_id])

        # Neo4j fallback
        try:
            result = await self._driver.execute_query(
                "MATCH (n) WHERE n.node_id = $id AND n.workspace = $workspace "
                "RETURN properties(n) AS props, labels(n) AS lbls",
                {"id": node_id, "workspace": self.workspace},
                database_=self._database,
            )
            records = result.records
            if records:
                node = {
                    k: _normalize_temporal(v)
                    for k, v in dict(records[0]["props"]).items()
                }
                # Merge true node labels so handlers see current_type across a
                # flush boundary, matching the in-buffer get_node shape.
                node["labels"] = list(records[0]["lbls"])
                return node
        except Neo4jError:
            pass

        return None

    async def get_edge(self, src_id: str, dst_id: str) -> dict[str, Any] | None:
        """Return edge data, checking the in-memory buffer first.

        If the edge is found in the buffer, a *shallow copy* is returned.

        If not found in the buffer, falls back to a Neo4j query.  Returns
        ``None`` if the edge is absent from both.
        """
        key = (src_id, dst_id)
        if key in self._edge_buffer:
            return dict(self._edge_buffer[key])

        # Neo4j fallback
        try:
            result = await self._driver.execute_query(
                "MATCH ()-[r]->() "
                "WHERE r.src_id = $src_id AND r.dst_id = $dst_id "
                "AND r.workspace = $workspace "
                "RETURN properties(r) AS props",
                {"src_id": src_id, "dst_id": dst_id, "workspace": self.workspace},
                database_=self._database,
            )
            records = result.records
            if records:
                return {
                    k: _normalize_temporal(v)
                    for k, v in dict(records[0]["props"]).items()
                }
        except Neo4jError:
            pass

        return None

    def remove_edge(self, src_id: str, dst_id: str) -> None:
        """Remove a buffered edge from the edge buffer.

        No-op if the edge does not exist in the buffer.
        Note: edges already flushed to Neo4j are handled separately by the
        ownership integrity checker via explicit Cypher DELETE queries.
        """
        self._edge_buffer.pop((src_id, dst_id), None)

    async def set_labels(
        self, node_id: str, remove_labels: list[str], add_labels: list[str]
    ) -> None:
        """Buffer a label patch and immediately update _node_buffer.

        Two-phase effect:
        1. _node_buffer updated immediately — so get_node() reflects the change
           within the same flush cycle. Essential for fork guard and state machine
           logic that reads labels between handler calls.
        2. Patch queued in _label_patches — applied to Neo4j at flush time via
           explicit REMOVE/SET Cypher statements AFTER node writes.

        Each label in remove_labels is removed via REMOVE n:Label Cypher.
        Each label in add_labels is set via SET n:Label Cypher.
        """
        # Phase 1: update in-memory buffer immediately (same as GraphState.set_labels)
        if node_id not in self._node_buffer:
            self._node_buffer[node_id] = {}
        existing = self._node_buffer[node_id]
        current = set(existing.get("labels", []))
        existing["labels"] = sorted((current - set(remove_labels)) | set(add_labels))

        # Phase 2: queue patch for Neo4j flush
        self._label_patches.append(
            {
                "node_id": node_id,
                "remove": list(remove_labels),
                "add": list(add_labels),
            }
        )

    # ------------------------------------------------------------------
    # Persistence methods
    # ------------------------------------------------------------------

    async def flush(self) -> None:
        """Persist all buffered writes to Neo4j using UNWIND-based batch Cypher.

        Serializes callers via ``_flush_lock`` so that at most one Neo4j
        transaction is open at any time for this store.  Without the lock a
        concurrent caller could open a second transaction on the same nodes,
        which Neo4j resolves as a deadlock.

        Optimistically snapshots and clears buffers before writing.  On any
        failure the transaction is rolled back and the buffers are restored
        (merging any writes that arrived during the flush attempt).

        All nodes are merged by (node_id, workspace) identity — label is never
        part of the MERGE key.  This prevents duplicate nodes when the same
        session is written with different type labels across flush cycles
        (e.g. bare Session created by a child worker, then RootSession written
        by the parent worker's session:end flush).
        """
        async with self._flush_lock:
            await self._flush_body()

    async def _flush_body(self) -> None:
        """Inner flush implementation — must only be called while _flush_lock is held.

        Phased, dual-bounded, per-chunk-committed coordinator.  Splits each buffer
        into row/byte-bounded chunks and commits each chunk in its own
        execute_write (independent Neo4j transaction), eliminating the memory-cap
        OOM caused by sending the entire buffer in a single transaction.

        Phase order: nodes → label patches → edges.  This ordering preserves the
        invariant that label patches can only reference nodes already committed to
        Neo4j, and edges can only reference nodes already committed.

        On any chunk failure the full snapshot is merged back into the live buffers
        (restoring the entire un-flushed batch for the next attempt) and the
        exception is re-raised so the caller's offset never advances on a partial
        flush.
        """
        if not self._node_buffer and not self._edge_buffer and not self._label_patches:
            return  # early exit — nothing to write

        # Snapshot and clear buffers optimistically
        node_snapshot = self._node_buffer
        edge_snapshot = self._edge_buffer
        self._node_buffer = {}
        self._edge_buffer = {}
        patch_snapshot = self._label_patches
        self._label_patches = []

        rows = self._flush_chunk_rows
        byts = self._flush_chunk_bytes

        # Layer A — global lock-acquisition order.
        # Sort both snapshots by key BEFORE chunking so every concurrent flush
        # visits node_ids and (src_id, dst_id) pairs in the same monotonically
        # increasing order.  A single consistent order rules out circular
        # wait-for cycles across transactions: if every flush processes key X
        # before key Y, no flush can ever hold Y while waiting for X.
        # (Cost: O(N log N) on snapshot keys, always negligible next to I/O.)
        node_snapshot_sorted = dict(sorted(node_snapshot.items()))
        edge_snapshot_sorted = dict(sorted(edge_snapshot.items()))

        # Layer B — fail loud via finite transaction timeout.
        # Wrap _write_batch with unit_of_work(timeout=) so the Neo4j server
        # aborts a transaction that cannot acquire locks within the configured
        # window.  Without this, db.lock.acquisition.timeout=0 (server default)
        # parks the coroutine forever, exhausting write_semaphore permits and
        # stalling the entire drain pipeline.
        if self._neo4j_lock_timeout is not None:
            _bounded_write = _unit_of_work(timeout=self._neo4j_lock_timeout)(
                _write_batch
            )
        else:
            _bounded_write = _write_batch

        success = False
        try:
            await self._ensure_schema()

            # Phase 1: nodes — each chunk is its own execute_write (independent commit).
            # Do NOT wrap multiple chunks in one explicit transaction — that would
            # re-collapse the memory bound and defeat the chunking.
            for chunk in _chunk_dict(node_snapshot_sorted, rows, byts):
                async with self._driver.session(database=self._database) as db_session:
                    await db_session.execute_write(
                        _bounded_write, chunk, {}, [], self.workspace
                    )

            # Phase 2: label patches — each chunk is its own execute_write.
            for chunk in _chunk_list(patch_snapshot, rows, byts):
                async with self._driver.session(database=self._database) as db_session:
                    await db_session.execute_write(
                        _bounded_write, {}, {}, chunk, self.workspace
                    )

            # Phase 3: edges — each chunk is its own execute_write.
            for chunk in _chunk_dict(edge_snapshot_sorted, rows, byts):
                async with self._driver.session(database=self._database) as db_session:
                    await db_session.execute_write(
                        _bounded_write, {}, chunk, [], self.workspace
                    )

            success = True
        except Exception:
            _LOG.error(
                "flush_chunk_failed workspace=%s nodes=%d edges=%d patches=%d;"
                " restoring full snapshot and re-raising",
                self.workspace,
                len(node_snapshot),
                len(edge_snapshot),
                len(patch_snapshot),
            )
            raise
        finally:
            if not success:
                # Restore buffers: snapshot base + any new writes since flush started
                merged_nodes = dict(node_snapshot)
                merged_nodes.update(self._node_buffer)
                self._node_buffer = merged_nodes
                merged_edges = dict(edge_snapshot)
                merged_edges.update(self._edge_buffer)
                self._edge_buffer = merged_edges
                self._label_patches = patch_snapshot + self._label_patches

    def discard_buffer(self) -> None:
        """Drop all buffered writes without persisting them (no driver I/O).

        Clears ``_node_buffer``, ``_edge_buffer`` and ``_label_patches`` in
        memory.  Unlike ``flush``, this performs no Neo4j transaction and never
        restores buffers.  It is the dead-letter primitive used to isolate a
        poison line: a failed write's nodes/edges/label-patches stay resident in
        the buffers (``_flush_body`` restores them on failure), so without a
        discard primitive a dead-lettered line would remain resident and the
        next good line's flush would re-include it — cascading dead-letters of
        otherwise-good events.

        Safety note (LOW severity): this method is intentionally NOT guarded by
        ``_flush_lock``.  That is safe ONLY because the drainer is the sole flush
        trigger (Task 6); if another concurrent flush path is ever reintroduced,
        this method must take ``_flush_lock`` to avoid racing buffer mutation.
        """
        self._node_buffer = {}
        self._edge_buffer = {}
        self._label_patches = []

    async def _ensure_schema(self) -> None:
        """Create Neo4j indexes and constraints idempotently (latches once fully established).

        Safety-net for contexts where the FastAPI lifespan is not active (e.g. tests,
        CLI tools, or direct store use).  The primary schema-initialization path is
        ``ensure_neo4j_schema()`` called from the lifespan handler *before* the server
        starts accepting requests, which guarantees the uniqueness constraint is active
        before any concurrent ``flush()`` transactions execute ``MERGE``.

        The ``_schema_initialized`` flag latches True ONLY when the schema is fully
        established.  If ``ensure_neo4j_schema`` could not create every index/constraint
        (e.g. Neo4j was unreachable at init and the connectivity error was swallowed to
        avoid dead-lettering real events), the flag stays False so the NEXT flush retries
        schema init — self-healing once Neo4j is reachable and the dedup pass has cleared
        any duplicates.  This closes the "constraint created once, never retried"
        data-integrity gap, where a missing uniqueness constraint would otherwise let
        concurrent MERGE accrue duplicate Session/Event nodes until process restart.
        """
        if self._schema_initialized:
            return

        fully_established = await ensure_neo4j_schema(self._driver, self._database)
        if fully_established:
            self._schema_initialized = True
        # else: leave the flag False so the NEXT flush retries schema init (self-heals
        # once Neo4j is reachable / duplicates are cleared by the dedup pass).

    async def close(self) -> None:
        """Flush pending writes, await any background task, and close the driver.

        Handles event-loop mismatch gracefully when closing the driver from a
        different loop context.  Sets ``_closed`` on completion.
        """
        # Final flush to persist remaining buffer contents
        try:
            await self.flush()
        except Exception:
            _LOG.exception(
                "Final flush failed during close; buffered writes may be lost"
            )

        # Close the driver, ignoring event-loop mismatch errors
        try:
            await self._driver.close()
        except RuntimeError:
            pass

        self._closed = True

    async def execute_query(
        self,
        query: str,
        params: dict[str, Any] | None = None,
        dialect: str = "cypher",
        workspace: str | None = None,
    ) -> list[dict[str, Any]]:
        """Execute a Cypher query against Neo4j.

        Args:
            query:     Cypher query string.
            params:    Optional query parameters.
            dialect:   Must be ``"cypher"``; raises ``ValueError`` otherwise.
            workspace: Scope results to this workspace.  ``None`` uses the
                       store's own workspace.  ``"*"`` disables workspace
                       filtering.

        Returns:
            List of result row dicts.

        Raises:
            ValueError: If *dialect* is not in ``supported_dialects``.
        """
        if dialect not in self.supported_dialects:
            raise ValueError(
                f"Unsupported dialect: {dialect!r}. Supported: {self.supported_dialects}"
            )

        effective_workspace = workspace if workspace is not None else self.workspace
        query_params: dict[str, Any] = dict(params) if params else {}

        if effective_workspace != "*":
            query_params["workspace"] = effective_workspace

        async with self._driver.session(database=self._database) as session:
            result = await session.run(query, query_params)  # type: ignore[arg-type]
            data = await result.data()
            # Normalizes only top-level record values; nested temporal values (rare)
            # must be normalized by callers.
            return [
                {k: _normalize_temporal(v) for k, v in dict(record).items()}
                for record in data
            ]

    # ------------------------------------------------------------------
    # Static helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _sanitize_properties(props: dict[str, Any]) -> dict[str, Any]:
        """Convert a properties dict to Neo4j-compatible types.

        Rules:
        - ``None`` values are *skipped* (not included in output).
        - ``str``, ``int``, ``float``, ``bool``, ``datetime`` are kept as-is.
        - ``list`` whose items are all primitives (str/int/float/bool) is kept.
        - ``list`` containing non-primitive items is JSON-serialised to a string.
        - ``dict`` values are JSON-serialised to a string.
        - Everything else is converted via ``str()``.
        """
        result: dict[str, Any] = {}
        for key, value in props.items():
            if value is None:
                continue
            if isinstance(value, (str, int, float, bool, datetime)):
                # Never write empty-string timestamps — SET n += row.props would
                # overwrite a previously valid started_at on the existing node.
                if isinstance(value, str) and value == "" and key.endswith("_at"):
                    continue
                result[key] = value
            elif isinstance(value, list):
                if all(isinstance(item, (str, int, float, bool)) for item in value):
                    result[key] = value
                else:
                    result[key] = json.dumps(value)
            elif isinstance(value, dict):
                result[key] = json.dumps(value)
            else:
                result[key] = str(value)
        return result
