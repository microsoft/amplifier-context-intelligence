# Changelog

All notable changes to the Context Intelligence Server are recorded here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [7.4.0]

### Added

- **Orphaned-blob garbage collection.** New `POST /admin/blobs/reclaim`
  endpoint reclaims blob-store artifacts no longer referenced by the graph. It
  is protocol-only — enumeration via `BlobStore.scan()`, deletion via the
  fenced `BlobStore.delete(uri, if_unmodified=ref)` compare-and-delete — so it
  never touches a filesystem path, glob, or `os.unlink`, and never reaches the
  queue / identity / lease stores or graph data. Safety gates: a graph-wide
  reference scan over the blob-carrier allowlist, a not-live / durable
  `is_fully_drained` session gate, a hard `min_age_minutes` floor (>= 15), a
  destructive-apply single-flight (409 on overlap), a required `max_delete`
  blast-radius cap, and **`dry_run=true` by default** (a preview that deletes
  nothing). One structured audit line per delete; blob contents are never
  logged, only the `ci-blob://` URI.
- **Blob-carrier allowlist** (`BLOB_REF_CARRIER_PROPERTIES` in
  `blob_processor`) — the single source of truth for which graph properties may
  carry a `ci-blob://` reference, validated at import and enforced at the mint
  site (`assert_carrier_registered`) so an unregistered carrier fails loud
  rather than becoming a silent reclaim-GC hole. The reclaim reference-scan
  Cypher is generated directly from this tuple, so the two can never drift.

## [7.3.0]

### Added

- **`lease_store` package.** Writer-lease persistence now lives behind a
  backend-neutral `LeaseStore` protocol (`protocol` + `filesystem` + `factory`),
  the fourth storage backend alongside `blob_store`, `queue_manager`, and
  `identity_store`. The writer-lease detector keeps all policy (staleness,
  conflict, the bounded single-thread I/O executor) and reaches the lease only
  through the store, so the same detector runs unchanged against any backend.
- **`QueueManager.session_keys()`.** A backend-neutral way to enumerate every
  persisted session key. Boot reclaim sweeps the queue through this method
  instead of globbing the queue directory, so the sweep works unchanged against
  any queue backend.
- **Storage-boundary guard test.** A standing AST tripwire asserts no module
  outside the four storage backend packages performs a storage-artifact file
  operation (glob/unlink/scandir) or reads a storage root path; it now also
  catches a raw `queues_dir` glob/path-join.

### Changed

- **`queues_dir` removed from the `QueueManager` Protocol.** A caller enumerates
  sessions via `session_keys()` and never learns the on-disk layout. The single
  sanctioned exception (`registry.queues_dir_path`, used by the WriterLease boot
  detector) resolves the directory straight from settings. The `Batch`
  docstring now states its offsets are opaque queue-produced cursors, matching
  `Record`'s contract.

## [7.2.0]

### Added

- **Schema-version observability.** The server now records the graph data-model
  version it wrote and reports drift on `/status`. A `SCHEMA_VERSION` constant
  is the compiled model version; at startup the server writes a single
  `:SchemaMeta{id:'singleton'}.schema_version` baseline (create-if-absent, off
  the per-worker flush path). `GET /status` returns three fields:
  `schema_version` (compiled), `graph_schema_version` (stored, or `null` when
  the graph is unreachable or never baselined), and `schema_version_current`
  (`true` in sync, `false` on mismatch, `null` when unknown). Advisory only —
  reported, never gated or migrated; `/status` never raises on a read failure.

## [7.1.0]

### Added

- **`working_dir` end-to-end on the Session node.** `EventRequest` accepts an
  optional top-level `working_dir` envelope field (rejected only when
  blank/whitespace-only); the events endpoint lifts it into the event data, and
  `ensure_session_node` writes it onto the Session node. Populate-if-missing: a
  node created before `working_dir` was known is backfilled by the first
  subsequent event that carries a non-empty value. An already-set value is
  never overwritten — the Neo4j write uses
  `coalesce(n.working_dir, row.working_dir)` rather than last-write-wins.

### Fixed

- **`agent` persists across the delivery-order race.** The agent name for a
  spawned sub-session arrives only on the parent's `delegate:agent_spawned`
  event, but the child's own `session:start` can create the Session node first.
  `ensure_session_node` now backfills `agent` on the existing-node branch with
  the same populate-if-missing rule as `working_dir`, so it is no longer
  silently dropped (which left `:Session.agent` empty and undercounted
  `WHERE s.agent = ...` queries).
- **IncompleteSession heal-forward.** A stale `IncompleteSession` marker —
  stamped when a session's `session:end` drained before its `session:start`/
  `session:fork` — is now stripped the moment the real start/fork is processed,
  leaving only the correct terminal label.

## [7.0.0]

### Changed (breaking)

- **`QueueManager.commit()` now takes a required `cursor` argument** —
  `commit(session_id, new_offset, cursor)`. The committed byte offset and the
  cross-handler cursor are written in one atomic offset record so they can
  never skew. This is a breaking change to the `QueueManager` Protocol that any
  alternate backend must implement; all in-repo callers are updated. A legacy
  bare-integer `.offset` written by an older build still reads its committed
  position correctly, so an in-place upgrade is safe.
- **`QueueManager` Protocol gains `read_cursor`** as a member every backend
  must provide: it returns the persisted cursor a rebuilt worker resumes from.
  Log-structured-queue details stay off the Protocol -- dead-line
  reconciliation is a filesystem-backend concern (a broker backend has no
  leading-dead-line window), so it is reached through the concrete backend, not
  the Protocol.

### Fixed

- The durable cursor now survives every offset-mutation path that keeps the
  session alive, not just `commit()`: idle compaction and the boot
  `RESET_OFFSET` reclaim preserve a non-empty cursor instead of wiping it with
  a bare offset write. Finalize's `delete_drained` still drops it, which is
  terminal cleanup, not loss: the session is over.
- Every write to a session's `.offset` now happens through one writer that
  takes the session's file lock itself and stages through a per-write temp
  file. Previously the lock was the caller's job, and the dead-letter
  reconcile reached by `read_batch` took none -- a commit racing it was
  silently rolled back to the offset the reconcile had read (measured: 248 of
  300 concurrent runs), and the shared temp name let one writer publish
  another's half-written record.
- A corrupt/unparseable `.offset` file now quarantines the one affected drain
  worker (logged, closed, deregistered) instead of crash-looping it; other
  sessions keep draining. Every read in the drain loop is covered, including
  the idle dry-exit recheck and the `session:end` tail drain, which previously
  escaped to the supervisor.
- Cross-handler run-id resolution is consistent across the orchestrator-run,
  iteration, and content-block handlers, so a partial cursor after a worker
  rebuild no longer drops the `HAS_PART` edge or orphans ContentBlock nodes.
- `restore_cursor` deep-copies mutable cursor fields, so an in-place mutation on
  a later retry attempt can no longer corrupt the pre-batch baseline the next
  rollback restores.
- A crash-then-respawn mid-isolation no longer re-dead-letters an
  already-dead-lettered record: the drain worker reconciles the session's
  leading dead lines on every (re)spawn, not only at boot.
- Finalizing a session retires its dead letters out of the session's own name
  instead of leaving them in place. A later session reusing the id no longer
  reconciles against the previous session's dead payloads and commits past
  events it never processed. The payloads are retained, still reported by
  `GET /queues/dead-letter/{key}`, and still expire on their own schedule.
- The Session node survives an exhausted-batch isolation: discarding the failed
  batch's buffer now also invalidates the seen-session cache, so the isolated
  re-dispatch re-issues the node instead of early-returning.

## [6.7.3]

### Added

- Durable per-record cursor persisted atomically with the committed offset, so
  a rebuilt drain worker resumes its cross-handler counters instead of
  re-minting node ids. Retry-dedup and the run-id tiebreaker keep a re-delivered
  batch idempotent.
