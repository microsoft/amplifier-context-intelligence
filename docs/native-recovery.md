# Native recovery

Native recovery is opt-in. Set `recovery.enabled: true`, generate one random
32-byte source capability per native capture, then obtain a permit from
`POST /recovery/admissions` before calling `POST /recovery/events`. Send the
raw, unpadded base64url capability only in `X-Recovery-Source`; it is never
part of JSON, a payload digest, a receipt, an outbox record, or a response.
When
recovery paths are omitted, receipt, claim, lease, and recovery-queue paths
are derived beside the configured live `queues_path`; constructing a server
with recovery disabled creates none of that storage.

The admission request includes the target workspace, a path-free `source`
descriptor, a per-event `origin`, and
the SHA-256 of the canonical forthcoming recovery event payload (the compact,
sorted JSON representation of exactly `event`, `workspace`, `data`, and a
non-null `idempotency_key`, if supplied; `origin` and null optional fields are
omitted). `working_dir` keys at the envelope or any nested payload depth are
intentionally omitted before this digest is calculated, persisted, or
delivered. A new candidate returns `201` with `status: "permit"`, an opaque
`permit`, and epoch `expires_at`; send that permit only in the
`X-Recovery-Permit` header on the matching event request. An exact retry by
the same source-capability holder, contributor, and workspace returns `200`
with `status: "duplicate"` and no permit. All other unavailable or
non-matching cases return a non-disclosing `429` with `Retry-After`. Both
requests carry:

```json
{
  "source": {
    "protocol": "native-recovery-source-v1",
    "session_id": "session-id",
    "source_stream_sha256": "64 lowercase hex characters",
    "source_sha256": "64 lowercase hex characters",
    "record_count": 42
  },
  "origin": {
    "ordinal": 0,
    "source_line_sha256": "64 lowercase hex characters"
  }
}
```

The server domain-separates and hashes the source capability into a stored
`source_handle`. Receipt and queue identity is `(source_handle, ordinal)`;
the immutable descriptor has no path. A capability is therefore its own
namespace: a caller that lacks another capture's capability cannot query or
learn that capture's receipt state by guessing public descriptor values.
Receipt admission first enters a durable outbox and is reconciled idempotently
into the separate queue after an interruption. An exact admission retry is a
durable acknowledgement, so a client can advance its cursor after losing the
event request's `202`; it does not consume capacity or confer event-write
authority. A new record needs a matching, unexpired, unconsumed permit bound
to the server generation, authenticated contributor, workspace, source handle,
descriptor digest, exact origin, and payload digest.
For a holder of that same source capability, a changed descriptor or a changed
line/payload at one ordinal creates hash-only durable conflict metadata and
returns `409 Recovery source conflict`; it never reaches the queue. Missing or
invalid source capabilities and every permit failure return the same
non-disclosing `429` with
`Retry-After`. Only one recovery receipt or outstanding permit is admitted at
a time. Permit issuance and consumption both recheck receipt identity while
holding the admission lock, so a receipt that appears between them leaves the
permit unconsumed and returns that same `429`.

Both `source.session_id` and `data.session_id` must be identical safe bounded
queue tokens. Empty values, `.`/`..`, path separators, control characters, and
values longer than one queue filename component are rejected with a
non-reflecting `400` before any permit, receipt, source, or spool mutation.

At permit issue and permit consume the server directly checks its durable live
queue while serializing normal live enqueue with recovery admission. It does
not use `/status` telemetry for this decision. A concurrent live enqueue that
gets the gate first makes recovery unavailable; a recovery consume that gets
it first can admit only that one recovery record before live work blocks
further recovery. Permits exist only in server memory and are therefore
invalid after a restart; previously durable receipts remain recoverable.

For a temporary upgrade transition only, an existing deployment can set
`recovery.permit_required: false` and retain `recovery.guard_lease_path`.
That compatibility mode accepts the former public-origin lease client and
cannot provide the source-capability privacy boundary or the server-side
atomic live-first guarantee. New deployments must use source capabilities.

In `access_control_mode: scoped`, source-capability recovery requires both the
contributor's `recovery:write` and `live:write` capabilities for the target
workspace. A source capability namespaces a capture; it is not authority to
inject events, and recovery never expands live-ingest access. Session ownership
is claimed atomically by contributor and workspace; an unavailable or conflicting claim
is returned as the same non-disclosing `429`. Recovery records use their own
durable queue and run through the normal event handlers and graph idempotency
path.

Administrators can view aggregate receipt states, pause or resume recovery,
and return one quarantined record to the recovery queue at
`/admin/recovery/status`, `/admin/recovery/pause`, `/admin/recovery/resume`,
and `/admin/recovery/retry-quarantined`. Receipt identities are never
contributor-visible.