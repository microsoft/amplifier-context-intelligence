# Changelog

All notable changes to the Context Intelligence Server are recorded here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
