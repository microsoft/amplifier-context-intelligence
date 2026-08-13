# Changelog

All notable changes to the Context Intelligence Server are recorded here,
newest first. This is a human-readable log. A `SCHEMA_VERSION` baseline data
point now exists (see below), but the *handling* machinery that would consume
it -- a versioned migration manifest and an automated upgrade/rollback runner
-- is deliberately deferred to a separate design.

## 6.8.0 -- Phase-2 review remediation: maintenance mode, retry-loop dedup fix, blob-reclaim/working_dir integrity

**Not schema-affecting.** No stored node/edge shape changes; `SCHEMA_VERSION`
(`status.py`) stays `1`. See `migrations/manifest.yaml` for the one
structural-rectification entry this release adds, and the README
["Upgrading"](README.md#upgrading) section for the operator recovery path.

> ⚠️ **A deployment with pre-existing un-migrated/duplicate `:Node` data will
> now boot into MAINTENANCE MODE** (ingest + query return `503`) instead of
> the "degraded but still writing" state 6.7.1 shipped. This is the fix, not
> a regression -- 6.7.1's degraded mode let concurrent writes manufacture
> *new* duplicates while un-migrated. If `GET /status` reports
> `mode=maintenance` or `mode=degraded` after upgrading, rectify **once**,
> out-of-band, via either:
> - `python migrations/run.py --apply` (local / VM / direct Neo4j access), or
> - `POST /admin/maintenance` (cloud / ACA, where the private Neo4j is not
>   directly reachable) -- poll `GET /admin/maintenance` to completion.
>
> The server then self-clears to healthy with **no restart**. A
> healthy/already-migrated graph boots under 6.8.0 with zero behavior
> change. Use `context-intelligence-upload` to backfill any events that
> could not be ingested during a maintenance window.

- **Maintenance mode -- gate ingest + query on degraded schema, never
  latched (B-1).** The `6.7.1` deploy-safe-boot signal computed
  `schema_health` once at boot and gated nothing; all write paths (boot
  recovery, `POST /events`, dead-letter replay) wrote unconditionally even
  while the `:Node` uniqueness constraint was absent, silently manufacturing
  *new* duplicates. The gate now lives at the single drain-loop chokepoint
  (before every batch's `read_batch`/commit), reads a **live, TTL-cached
  re-probe** of constraint presence (never latched -- a graph repaired
  out-of-band self-clears without a restart), and returns a structured
  `503` (`Retry-After` + JSON reason) for both ingest and the query/`cypher`
  surface. `/status` and `/version` stay up throughout and advertise the
  mode (`healthy` / `maintenance` / `degraded` / `unknown`,
  `maintenance_started_at`, `maintenance_elapsed_seconds`). Because the gate
  sits upstream of the queue-offset commit, a refused write never advances
  its offset -- kill-9 mid-refuse -> restart -> repair -> exactly-once
  redelivery, by construction. Entry/exit is logged
  (`maintenance_entered` / `maintenance_completed`).
  (`context_intelligence_server/maintenance.py`, `registry.py`,
  `routers/admin.py`, `main.py`)
- **`POST` / `GET /admin/maintenance` -- the cloud unblocker.** Assess +
  advertise + gate alone deadlocks on ACA: an un-migrated graph behind a
  private Neo4j has no operator who can run a script. `POST
  /admin/maintenance` triggers the rectification (same `run_repair` the
  standalone script calls -- one shared logic home, `maintenance_ops.py`)
  over the network, with atomic (CAS) single-flight, a promptly-returning
  `202` (never blocks for the op's duration), and no write-bypass (the op
  never consults the gate -- it just isn't gated in the first place, using
  the admin driver directly). `GET /admin/maintenance` reports
  `state`/`run_id`/`started_at`/`records_affected`/error for polling to
  completion. Both routes are on the maintenance-gate allow-list (with
  `/status`/`/version`) so they cannot 503 themselves out of existence.
- **`max_delete` cap-inversion fixed (B-3).** `POST
  /admin/blob-reclaim`'s `max_delete` had no floor validator; a
  fat-fingered `0` or negative value inverted the cap into an
  effectively-unbounded delete. Now `Field(ge=1)` rejects `<1` at the
  schema boundary with a `422`; the existing apply-mode `422` on `None` is
  unchanged. (`routers/admin.py`)
- **Retry-loop duplicate `Iteration` nodes fixed on the common path
  (B-2).** The `I5` run-scoped `Iteration.node_id` fix stopped duplicates
  on the normal path, but the flush **retry** branch mutated
  `iteration_count` before the write and never restored it between
  attempts -- a transient-fail-then-succeed retry could commit under two
  different counter values (`::iteration::1` and `::iteration::2` both
  landing). The existing `snapshot_cursor`/`restore_cursor` guard (already
  used by the post-budget path) now also wraps the common retry branch:
  snapshot once per batch, restore to pre-batch state before every replay
  attempt. (`registry.py`)
- **`working_dir` integrity.** Blank/whitespace-only `working_dir` is now
  rejected by the same validator style `workspace` already uses
  (`models.py`). The "never overwritten" guarantee is now enforced at the
  Cypher level (`ON CREATE SET` / `coalesce`), closing a cross-replica
  same-session race that could previously last-write-wins clobber a good
  value from Python-only discipline. (`neo4j_store.py`)
- **Blob-carrier allowlist tripwire (W-5).** The hardcoded
  `_BLOB_REF_CARRIER_PROPERTIES` allowlist used by blob-reclaim's
  reference scan is now a single source of truth shared with the mint
  site (`blob_processor.py`), with a fail-closed runtime tripwire so a
  future blob-ref-carrying property added elsewhere and forgotten in the
  allowlist cannot let reclaim silently misclassify a live blob as an
  orphan.
- **Docs auth-fold.** `README.md` / `docs/local-development.md` previously
  documented running the server via `main:app` (the bare FastAPI app, no
  bearer-auth middleware). Corrected to `main:asgi_app` (the
  middleware-wrapped entrypoint that enforces auth on `/admin/*` and data
  routes), with a tripwire test.
- Version bump: `6.7.1` -> `6.8.0` (minor -- new `/admin/maintenance`
  endpoint and gate behavior change on degraded graphs; no schema change).

See `docs/plans/2026-08-13-review-remediation-plan.md` and
`docs/plans/2026-08-13-ws3-implementation-spec.md` (workspace root, not
shipped) for the full remediation writeup and council verdicts.

## 6.7.1 -- Deploy-safe boot: server never crash-loops on un-migrated/unreachable/degraded graph state

**Incident:** deploying a restart crash-looped the server against a graph with
a single legacy node lacking the `:Node` label -- `RuntimeError: ... Cold
start refuses to boot ... Run: doctor --fix`, `systemd Restart=always` ->
hard outage, unrecoverable on Azure Container Apps (the private graph is
unreachable from outside and `doctor --fix` cannot be run to break the loop).

- **Boot never raises on graph/data state (B1).** The entire lifespan
  startup sequence (schema DDL, the untagged-node probe, the SchemaMeta
  baseline, and queue crash-recovery/reconcile) is now wrapped in ONE
  try/except boundary. Whichever of the ~11 startup raise sites fails
  (unreachable Neo4j, a `TransientError` during the ACA cold-start race,
  credential rotation, a corrupt per-session `.offset`/dead-letter, a
  genuine `:Node` constraint data conflict, ...) is logged LOUDLY
  (`startup_degraded`) and boot proceeds to serve requests. Cold start now
  calls `ensure_neo4j_schema(..., fail_on_data_conflict=False)` -- the same
  default the mid-flight flush path already used -- and the untagged-node
  probe no longer raises on a positive count. Only `run_repair`/
  `doctor --fix` still fails closed on a genuine post-repair conflict.
- **Crash recovery is defensive per-session (B6).** A corrupt queue for one
  session is quarantined (logged, skipped) instead of aborting the whole
  respawn loop; the B1 boundary is the backstop for anything this doesn't
  catch.
- **Degraded mode never loses the write-path index (B2).** The `:Node`
  uniqueness constraint is now attempted BEFORE any `DROP INDEX
  idx_node_universal` (previously unconditional and first, which could
  leave the graph with no index at all if the constraint then failed on
  duplicates -- regressing the 25-30s `AllNodesScan` stall PR #67 removed).
  The standalone index is dropped ONLY after the constraint succeeds. If
  the constraint cannot be created, a fallback `idx_node_universal` index
  is (re-)created so writes keep a `NodeIndexSeek` -- degraded mode now
  costs atomicity only, never the seek. A one-time drop-and-retry recovers
  the constraint automatically once the underlying conflict is fixed (so a
  prior degraded boot's own fallback index can never permanently lock the
  graph out of the constraint). Healthy graphs (constraint already
  present): zero behavior change.
- **Tri-state migration health on `GET /status` (B3/B7).** New fields
  `schema_health` (`"healthy"` / `"degraded"` / `"unknown"`),
  `untagged_nodes` (int|null), `schema_checked_at` (ISO-8601, computed once
  at boot), and `degraded_reason` (string|null). A probe failure reports
  `"unknown"`, never coerced to `"healthy"`. **This signal reflects
  data-migration state, not process liveness, and MUST NOT be wired to a
  Kubernetes/ACA liveness or readiness probe** -- doing so would recreate
  the exact crash-loop this fix removes, one layer up. `GET /version` is
  unchanged except for the version bump below.
- **Accepted tradeoff:** while degraded (constraint absent), concurrent
  writes to the same `(node_id, workspace)` can create a NEW duplicate --
  this is a ratchet, not bounded, so degraded mode is loud (ERROR-logged)
  rather than silent. Remediation is out-of-band `doctor --fix` (reachable
  deployments) or the in-place-fix capability tracked separately for ACA.
- Version bump: `6.7.0` -> `6.7.1`.

See `docs/plans/2026-08-12-deploy-safe-boot-spec.md` (workspace root, not
shipped) for the full incident writeup and council amendment.

## Unreleased -- data-quality fixes (I5, I5b, I1, IncompleteSession), schema_version baseline + maintenance scripts

> ⚠️ **ACTION REQUIRED ON UPGRADE IF LEGACY DATA IS PRESENT.**
> This release changes `IncompleteSession` labeling. New events self-heal, but
> **historical graphs carry ~52.8% stale `IncompleteSession` markers (~99% false
> positives) that are NOT corrected automatically.** A one-off, out-of-band
> reconciliation must be run **once** after this server is deployed and verified:
>
> 1. Deploy this release; confirm the server is up (Part 1 heal-forward is live).
> 2. Read-only check + preview (safe, writes nothing):
>    `python3 scripts/relabel_incomplete_sessions.py --dry-run`
> 3. If the built-in reconciliation diagnostic is clean, apply once:
>    `python3 scripts/relabel_incomplete_sessions.py --apply`
>    (`--apply` self-refuses if the diagnostic finds unexpected data; idempotent —
>    safe to re-run; writes an undo-log for `--restore`.)
>
> The maintenance scripts now ship **inside the Docker image** under `/app/scripts/`,
> so on a VM/ACI deployment run them via `docker exec <container> python3
> scripts/relabel_incomplete_sessions.py --dry-run`. Migrations are **never** run at
> server startup. Fresh/empty graphs need no action.

- **IncompleteSession mislabeling -- heal-forward + one-off backfill.**
  `IncompleteSession` was a Neo4j label written once at `session:end` when the
  Session node had no type label yet, and never revised. Forked sub-sessions drain
  in independent per-session queues with no cross-session ordering, so a child's
  `session:end` is routinely processed before its `session:fork`/`session:start` ->
  `classify()` saw no type -> stamped `IncompleteSession`; the later `fork`/`start`
  added the real terminal but never cleared the stale marker (live bisect: ~52.8%
  of sessions labeled, ~99.4% false positives -- the node carries its own linked
  start/fork event; genuine loss ~0.5%).
  - **Heal-forward (code):** `classify()` now strips `IncompleteSession` on every
    `start`/`fork` transition via a single `_heal_forward()` normalizer; the `end`
    branch is unchanged (still the real signal for the genuine ~0.5%). Reuses the
    existing `set_labels` remove path -- no store/Cypher change. Order-independent.
  - **Backfill (data rectification, run once):** `scripts/relabel_incomplete_sessions.py`
    -- standalone, out-of-band, idempotent. Clears `IncompleteSession` only from
    provably-false-positive nodes (real terminal type OR a linked
    `SessionStartEvent`/`SessionForkEvent`, following the
    `(Session)-[:SOURCED_FROM]->(Event)` direction), leaving the genuine ~0.5%
    untouched. `--apply` is hard-gated behind a read-only reconciliation diagnostic
    (refuses if any linked-but-untyped nodes exist), batched via
    `CALL {} IN TRANSACTIONS`, with a touched-id undo-log + `--restore` and a
    before/after population summary. **SCHEMA_VERSION unchanged** (no bump; handling
    deferred). See the upgrade notice above.

- **Maintenance/migration scripts now ship in the Docker image.** `scripts/` is
  `COPY`'d into the runtime image (`/app/scripts/`) so out-of-band data-rectification
  tools (`relabel_incomplete_sessions.py`, `tag_legacy_pooled_iterations.py`,
  `repair_dual_labels.py`, ...) can be run against a live cloud deployment via
  `docker exec ... python3 scripts/<name>.py`. Previously these existed only in the
  source tree and were unreachable from a running container. Invoke with `python3`
  (the runtime image has no `python` alias).

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
