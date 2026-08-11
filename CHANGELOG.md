# Changelog

All notable changes to the Context Intelligence Server are recorded here,
newest first. This is a human-readable log, not a schema-migration ledger --
that machinery (`SCHEMA_VERSION`, a versioned migration manifest, an
automated upgrade runner) is deferred to a separate spike branch.

## Unreleased -- data-quality fixes (I5, I1) + legacy-tagging script

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
