# Changelog

All notable changes to the Context Intelligence Server are recorded here,
newest first. This is a human-readable log. A `SCHEMA_VERSION` baseline data
point now exists (see below), but the *handling* machinery that would consume
it -- a versioned migration manifest and an automated upgrade/rollback runner
-- is deliberately deferred to a separate design.

## Unreleased -- data-quality fixes (I5, I5b, I1), schema_version baseline + legacy-tagging script

- **I5b -- durable handler cursor (fixes the I5 regression on worker rebuild).**
  The run cursor I5's `Iteration.node_id` depends on (`execution_start_ts`,
  `iteration_count`, and the rest of `DataLayer2State` / `DataLayer3State`) was
  in-memory only, so any worker rebuild mid-session -- a process restart with an
  undrained tail, or a stale-session reap -- reset it and regressed node_ids to
  the pre-I5 bare shape, re-pooling Iteration nodes and dropping run edges. The
  cursor is now persisted **atomically with the queue offset** (folded into the
  `<key>.offset` record as a single JSON object written via `os.replace`) and
  restored on every worker (re)creation in `drain_worker`, covering both the
  crash-recovery and stale-reap paths. Legacy bare-integer `.offset` files are
  read transparently. A dead-lettered line's in-memory mutations are rolled back
  so they never enter the committed cursor (no phantom cursor pointing at a
  discarded node). Persisting the full DL2/DL3 cursor also preserves the E09
  (tool-call), E14/E15 (prompt-flow) and E10/E11 (recipe) edges across a rebuild.
  (`queue_manager.py`, `services.py`, `registry.py`)

- **`Iteration.iteration_scope` additive property (`"run" | "unscoped"`).**
  Every Iteration node now carries `iteration_scope`, stamped at all three
  upsert sites (`provider:request`, `llm:request`, `llm:response`), so a
  run-scoped node and a legitimately unscoped one (e.g. a `loop-basic` session
  with no `execution:start`) are distinguishable in-graph rather than only by
  node_id shape. Additive and forward-only: historical nodes are `null`; the
  node_id shape is unchanged. (`handlers/data_layer_2/iteration.py`)

- **`SCHEMA_VERSION` baseline data points (integer `1`).** The server now
  declares the graph schema version it expects (`SCHEMA_VERSION` alongside
  `SERVER_VERSION`, surfaced on `/version`) and records the version the data
  store was initialised at as a singleton `(:SchemaMeta {id:'singleton'})` node
  written **create-if-absent only** (`ON CREATE SET schema_version, last_updated`;
  never overwritten) inside `ensure_neo4j_schema` as an O(1) MERGE. This lands
  only the *data points* for a future upgrade design -- there is deliberately
  **no** comparison, migration, or reconciliation logic in this change.
  (`status.py`, `routers/version.py`, `neo4j_store.py`)

- **I5 -- run-scoped `Iteration.node_id` (also fixes I3).**
  `Iteration.node_id` is now composed with the active orchestrator-run
  identifier: `{session_id}::orch_run::{ts}::iteration::{N}`, using the same
  run disambiguator `OrchestratorRun` already tracks. Previously the id was
  the bare `{session_id}::iteration::{N}`, with `N` a per-*session* (not
  per-*run*) counter -- so a counter reset (e.g. drainer restart/replay)
  could reproduce a prior run's `N` under a new run and MERGE two runs'
  Iteration nodes onto the same node, clobbering `usage_input` /
  `usage_output` / `usage_cache_write` / `message_count` (last-write-wins).
  This also fixes I3 (`usage_cache_write` junk) as a direct consequence.
  **Forward-only**: already-merged historical Iteration nodes are not
  retroactively split by this fix; see the tagging script below for the
  non-destructive way to mark the confirmed-corrupt subset.
  (`context_intelligence_server/handlers/data_layer_2/iteration.py`)

- **I1 -- `working_dir` lifted to a root `Session` property
  (populate-if-missing).** `EventRequest` gains an optional top-level
  `working_dir`; `post_events` lifts it into the event data so it rides the
  existing pipeline into `ensure_session_node`, which sets
  `Session.working_dir`. Fills the property from any event that carries a
  non-empty `working_dir` -- including re-imports of pre-existing sessions --
  and never clobbers an already-set value (idempotent). **Forward-only**:
  sessions with no `working_dir`-bearing event stay `null`.
  (`context_intelligence_server/main.py`, `models.py`, `services.py`)

- **`scripts/tag_legacy_pooled_iterations.py` (new maintenance script).**
  Non-destructive tagging tool for historical (pre-I5) bare-id `Iteration`
  nodes. A bare `node_id` alone does NOT mean a node is corrupt -- only
  nodes MERGEd across **two or more** distinct `OrchestratorRun`s (via
  `HAS_PART`) are confirmed corrupt; live-data verification found this is a
  small minority (~4%) of bare-id nodes. The script tags only that
  confirmed-corrupt subset with `data_quality = 'legacy_pooled_pre_fix'`
  (idempotent, batched via `CALL { ... } IN TRANSACTIONS OF N ROWS`) and
  explicitly leaves single-run and no-run-edge bare-id nodes untouched. The
  destructive cleanup (splitting/deleting pooled nodes) is a **separate,
  gated follow-up** -- not part of this change.
