# Native recovery

Native recovery is opt-in. Set `recovery.enabled: true`, configure an active
operator lease at `recovery.guard_lease_path`, and use `POST /recovery/events`.
When recovery paths are omitted, receipt, claim, lease, and recovery-queue
paths are derived beside the configured live `queues_path`; constructing a
server with recovery disabled creates none of that storage.

Each request includes a normal event envelope plus an opaque `origin`:

```json
{
  "session_id": "session-id",
  "ordinal": 0,
  "source_line_sha256": "64 lowercase hex characters",
  "source_stream_sha256": "64 lowercase hex characters"
}
```

The server stores origin hashes, receipt state, actor, workspace, and the
event envelope. It does not accept or retain a source path. Receipt admission
first enters a durable outbox and is reconciled idempotently into the separate
queue after an interruption. Retrying the same origin and payload is safe and
returns `202` with status `duplicate`; a conflicting origin or ordinal returns
`409`. Only one recovery receipt is admitted at a time; unavailable capacity
or a missing, malformed, expired, or denying lease returns `429` with
`Retry-After`.

The operator lease is an object with `allow: true`, a future epoch
`expires_at`, and a nonempty opaque `lease_id`. Issue a fresh unique
`lease_id` for every one-record admission. The server consumes that ID
durably with the receipt, so reusing it returns the same non-disclosing `429`
outcome even after the original record has been written or the server has
restarted. Leases without `lease_id` are denied.

In `access_control_mode: scoped`, recovery requires the contributor's
`recovery:write` capability and workspace grant. Session ownership is claimed
atomically by contributor and workspace; a conflict is returned as a
non-disclosing `409`. Recovery records use their own durable queue and run
through the normal event handlers and graph idempotency path.

Administrators can view aggregate receipt states, pause or resume recovery,
and return one quarantined record to the recovery queue at
`/admin/recovery/status`, `/admin/recovery/pause`, `/admin/recovery/resume`,
and `/admin/recovery/retry-quarantined`. Receipt identities are never
contributor-visible.