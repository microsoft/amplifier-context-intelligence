# Changelog

All notable changes to the Context Intelligence Server are recorded here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [6.8.0]

### Changed (breaking)

- **`QueueManager.commit()` now takes a required `cursor` argument** —
  `commit(session_id, new_offset, cursor)`. The committed byte offset and the
  cross-handler cursor are written in one atomic offset record so they can
  never skew. This is a breaking change to the `QueueManager` Protocol that any
  alternate backend must implement; all in-repo callers are updated. A legacy
  bare-integer `.offset` written by an older build still reads its committed
  position correctly, so an in-place upgrade is safe. The minor version bump
  reflects the breaking Protocol surface change.
- **`QueueManager` Protocol gains `read_cursor`, `is_fully_drained`, and
  `reconcile_dead`** as members every backend must provide. `read_cursor`
  returns the persisted cursor for a rebuilt worker; `is_fully_drained` reports
  whether a session has undrained log data; `reconcile_dead` advances one
  session's committed offset past its leading already-dead lines.

### Fixed

- The durable cursor now survives every offset-mutation path, not just
  `commit()`: idle compaction, the boot `RESET_OFFSET` reclaim, and finalize's
  `delete_drained` preserve a non-empty cursor instead of wiping it with a bare
  offset write.
- `commit()` writes under the same per-key file lock as compaction, closing a
  race where a concurrent commit during compaction could be silently erased.
- A corrupt/unparseable `.offset` file now quarantines the one affected drain
  worker (logged, closed, deregistered) instead of crash-looping it; other
  sessions keep draining.
- Cross-handler run-id resolution is consistent across the orchestrator-run,
  iteration, and content-block handlers, so a partial cursor after a worker
  rebuild no longer drops the `HAS_PART` edge or orphans ContentBlock nodes.
- `restore_cursor` deep-copies mutable cursor fields, so an in-place mutation on
  a later retry attempt can no longer corrupt the pre-batch baseline the next
  rollback restores.
- A crash-then-respawn mid-isolation no longer re-dead-letters an
  already-dead-lettered record: the drain worker reconciles the session's
  leading dead lines on every (re)spawn, not only at boot.
- The Session node survives an exhausted-batch isolation: discarding the failed
  batch's buffer now also invalidates the seen-session cache, so the isolated
  re-dispatch re-issues the node instead of early-returning.

## [6.7.3]

### Added

- Durable per-record cursor persisted atomically with the committed offset, so
  a rebuilt drain worker resumes its cross-handler counters instead of
  re-minting node ids. Retry-dedup and the run-id tiebreaker keep a re-delivered
  batch idempotent.
