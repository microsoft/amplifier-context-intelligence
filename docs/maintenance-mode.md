# Maintenance Mode

Runtime, no-restart gate that refuses ingest and query while the graph's
identity constraint is absent or a repair operation is in flight — the
live counterpart to the cold-start refusal described in the [README's
"Cold Start No Longer Auto-Migrates"](../README.md) section. All state
lives in one seam: `context_intelligence_server/maintenance.py`'s
`MaintenanceCoordinator` (module singleton `coordinator`). The HTTP gate
(`maintenance_gate_middleware`), the ingest drain loop
(`registry.drain_worker`), and `GET /status` all read from this one
coordinator, so they can never disagree about whether the server is in
maintenance.

## Why this exists

`node_node_id_workspace_unique` is the `:Node` uniqueness constraint that
makes every write path's `MERGE` safe. If that constraint is missing —
because the graph has duplicate legacy nodes it conflicts with — the
schema is in a state where **both writes and reads are unsafe**: a write
can silently create a duplicate node instead of merging onto the existing
one, and a read (`/cypher`) can return results computed against that
duplicated, inconsistent shape. Likewise, while an operator-triggered
repair (dedup + `:Node` backfill + constraint creation) is actually
executing, the graph is mid-mutation — reads and writes during that
window would race the repair.

Maintenance mode is the server's answer to both cases: refuse the request
with a `503` and a `Retry-After` header instead of serving a request
against schema-unsafe or mid-repair data.

## The mode state machine

`MaintenanceCoordinator._derive_mode()` is the **one** place mode is
computed; every caller (`gate_closed()`, `status()`) goes through it, so a
mode transition is always caught regardless of which surface polls first.
`MaintenanceMode` is one of four values:

| Mode | When | Gates ingest/query? |
|---|---|---|
| `healthy` | Constraint present, no op running, boot-time untagged-node count is 0 | No |
| `degraded` | Constraint present, no op running, but the **boot-time** untagged-node probe found `> 0` nodes lacking the `:Node` label | No |
| `unknown` | The live constraint probe could not run (Neo4j unreachable, credentials rejected, etc.) | No — an unreachable graph can't serve writes/reads anyway, so gating here would only buy an outage with no safety benefit |
| `maintenance` | **Either** a maintenance op is currently running (`op.state == "running"`), **or** the live probe found the `:Node` constraint absent | **Yes** — this is the only mode `gate_closed()` treats as closed |

Exact derivation order (`maintenance.py:267-291`), evaluated in this
priority:

1. `op.state == "running"` → `mode="maintenance"`, `reason="maintenance operation in progress"`
2. else `constraint_present is False` → `mode="maintenance"`, `reason=":Node uniqueness constraint absent -- migration required"`
3. else `constraint_present is None` → `mode="unknown"`, `reason="constraint probe could not determine graph state"`
4. else boot-time `untagged_nodes > 0` → `mode="degraded"`, `reason="{n} node(s) lacking the :Node label"`
5. else → `mode="healthy"`, `reason=None`

`gate_closed()` is exactly `mode == "maintenance"` — `degraded` and
`unknown` never gate (`maintenance.py:295-303`).

### The live probe

The constraint check is a cheap catalog read, not a data scan:

```cypher
SHOW CONSTRAINTS YIELD name WHERE name = 'node_node_id_workspace_unique' RETURN count(*) AS c
```

It is **TTL-cached and single-flight** (`maintenance_probe_ttl_seconds`,
default `5.0` seconds): the fast path never takes a lock, and concurrent
callers that see an expired cache collapse into exactly one live probe.
This is what makes maintenance mode **self-clearing with no server
restart** — once the constraint is repaired out-of-band, the next probe
(within the TTL window) sees `constraint_present=True` and the mode flips
back on its own.

## What happens while gated

While `mode == "maintenance"`:

- **Ingest is gated in the drain loop, not at spawn.** The check is the
  first statement inside `drain_worker`'s `while True:` loop
  (`registry.py:373-388`), before `read_batch`. This placement (rather
  than refusing at `get_or_create → start_drain`) is deliberate: gating
  *spawn* would be a **latch** — refuse once, never retried — the exact
  defect class this feature exists to prevent. Drainers still spawn
  during maintenance; they idle in a `_GATED_POLL_INTERVAL` sleep loop
  instead. Because everything downstream of the gate check (`read_batch`,
  `process`, `flush`, `commit`) is unreachable while gated, the on-disk
  offset **never advances** during a gated window — a crash mid-maintenance
  and restart replays from the last successfully-flushed offset, exactly
  once.
- **`POST /events`** (including dead-letter `?replay=true` re-ingestion,
  since it flows through the same drain loop) is refused via the HTTP
  gate below before it can even reach the queue-append step.
- **`POST /cypher`** is refused via the same HTTP gate.
- **`GET /status` and `GET /version` stay up.** They are explicitly on the
  allow-list (see below) — this is how an operator/automation observes
  maintenance mode in the first place.

### The structured 503

`maintenance_gate_middleware` (registered on `app` itself, via
`app.middleware("http")(maintenance_gate_middleware)` at `main.py:466`)
runs on every request whose path is not on the allow-list. When the
coordinator reports `mode == "maintenance"`, it returns the **one**
producer of this response, `maintenance_response()` — deliberately not a
FastAPI `HTTPException` (which would render `{"detail": ...}`, the wrong
contract here):

```json
{
  "status": "maintenance",
  "reason": ":Node uniqueness constraint absent -- migration required",
  "retry_after": 30,
  "schema_health": "degraded",
  "maintenance_started_at": "2026-08-13T12:00:00.123456+00:00"
}
```

with an HTTP header `Retry-After: 30` (the numeric value mirrors the
`retry_after` field). `retry_after` is sourced from
`maintenance_retry_after_seconds` (default `30`).

`reason` is the same human-readable string from the mode derivation above
(e.g. `"maintenance operation in progress"` when an op is running).
`schema_health` in this body is a **coarser, two-state** field than the
one on `/status` (below) — it is `"unknown"` only when the live probe
returned `None`, and `"degraded"` for every other case that reaches this
response, **including** the "op running, constraint actually present"
case. Operators should not read `"degraded"` here as "the constraint is
absent" — check `reason` for the actual cause.

## `GET /status` fields

`GET /status` is unauthenticated and always reachable (it's on the
allow-list). The maintenance-relevant fields, set in `main.py`'s
`get_status` handler (`main.py:897-933`):

| Field | Source | Meaning |
|---|---|---|
| `mode` | live, via `coordinator.status()` | `"healthy"` \| `"degraded"` \| `"unknown"` \| `"maintenance"` — de-latched: reflects the current probe, not a boot-time snapshot |
| `maintenance_started_at` | live | ISO-8601 UTC timestamp the **current** maintenance window opened; `null` when not in maintenance |
| `maintenance_elapsed_seconds` | live | Seconds since that window opened; `null` when not in maintenance |
| `schema_health` | live (computed independently in `main.py`, a **3-state** version, not reused from the 503's 2-state ternary) | `"unknown"` if the probe returned `None`; `"degraded"` if the constraint is absent **or** the boot-time untagged count was `> 0`; else `"healthy"` |
| `untagged_nodes` | **boot-time snapshot**, not live | Count of `:Node`-label-missing nodes at the last server boot. Documented as boot-time — it is not a gate input; the live gate/mode signal is the constraint probe above |
| `schema_checked_at` | `datetime.now(UTC)` at the moment this `/status` call is served | Not the exact instant the constraint probe last ran — the probe result may be up to `maintenance_probe_ttl_seconds` stale |
| `degraded_reason` | **boot-time snapshot**, set once by `_record_schema_health` at startup | Human-readable cause captured at boot (e.g. `":Node uniqueness constraint absent (data conflict)"`, `"N node(s) lacking the :Node label"`, or the probe-unreachable message); **not** recomputed on every call |

Example (in maintenance, constraint absent):

```json
{
  "mode": "maintenance",
  "maintenance_started_at": "2026-08-13T12:00:00.123456+00:00",
  "maintenance_elapsed_seconds": 42.7,
  "schema_health": "degraded",
  "untagged_nodes": 0,
  "schema_checked_at": "2026-08-13T12:00:42.831112+00:00",
  "degraded_reason": null
}
```

(`degraded_reason` is `null` here deliberately — it is a boot-time field,
and this example assumes the constraint was present at boot and only went
absent later; a boot-time degradation would populate it.)

Example (healthy):

```json
{
  "mode": "healthy",
  "maintenance_started_at": null,
  "maintenance_elapsed_seconds": null,
  "schema_health": "healthy",
  "untagged_nodes": 0,
  "schema_checked_at": "2026-08-13T12:05:00.001112+00:00",
  "degraded_reason": null
}
```

**Note:** `/status` does not surface the coordinator's internal op record
(`run_id` / `state` / `records_affected` / `error`). That detail is
deliberately kept off the unauthenticated `/status` surface and lives on
the admin-authenticated [`GET /admin/maintenance`](#postget-adminmaintenance--the-live-endpoint-contract)
instead, plus the transition log lines (below).

## Allow-list — blast radius

`MAINTENANCE_ALLOW_LIST` (`maintenance.py:100-102`) is an **allow-list,
not a deny-list**, matched on exact path (`request.url.path in
MAINTENANCE_ALLOW_LIST`) — deliberately, so any route added to the app
later is blocked-by-default rather than silently exempt:

```python
MAINTENANCE_ALLOW_LIST: frozenset[str] = frozenset(
    {"/status", "/version", "/admin/maintenance", "/docs", "/openapi.json"}
)
```

| Stays reachable during maintenance | Returns `503` during maintenance |
|---|---|
| `GET /status` | `POST /events` (including dead-letter replay) |
| `GET /version` | `POST /cypher` |
| `POST`/`GET /admin/maintenance` (live -- the one `/admin/*` route reachable during maintenance; see [the endpoint contract below](#postget-adminmaintenance--the-live-endpoint-contract)) | `GET /blobs/{session_id}`, `GET /blobs/{session_id}/{key}` |
| `GET /docs`, `GET /openapi.json` (Swagger UI / OpenAPI schema) | `GET /queues/dead-letter` and the other `/queues/*` routes |
| | `GET /admin/identities`, `GET /admin/keys`, `POST /admin/blobs/reclaim`, and every other `/admin/*` route |

This is **intentional**, not a bug: a blob-reclaim scan
(`POST /admin/blobs/reclaim`) walks the graph to decide which blobs are
still referenced, and a mid-dedup or mid-backfill graph would misclassify
live blobs as orphaned. Every other `/admin/*` route and all data-plane
routes are unsafe against a schema-unsafe or mid-repair graph for the
same underlying reason described in [Why this exists](#why-this-exists).

The middleware is registered on `app` itself (`main.py:466`), **not** the
auth-wrapped `asgi_app` — so it cannot be bypassed by the bare
`main:app` entrypoint. `BearerTokenMiddleware` wraps `app`, so
authentication still runs first; an unauthenticated request 401s before
it ever reaches this gate. A startup assertion,
`_assert_maintenance_endpoint_allow_listed()` (`main.py:542-559`), fails
loud if `/admin/maintenance`, `/status`, or `/version` are ever missing
from the allow-list — this prevents `/admin/maintenance` from ever
502-ing itself out of existence.

## Log events to grep for

| Event | Level | Emitted by | Fields |
|---|---|---|---|
| `maintenance_entered` | INFO | `maintenance.py` (`_handle_transition`) | `reason`, `run_id`, `trigger` (`"op"` or `"constraint"`) |
| `maintenance_completed` | INFO | `maintenance.py` (`_handle_transition`) | `reason`, `run_id`, `duration_seconds` |
| `maintenance_probe_failed` | WARNING | `maintenance.py` (`_run_probe`) | `error` — the live probe raised |
| `maintenance_finish_op_run_id_mismatch` | WARNING | `maintenance.py` (`finish_op`) | `expected`, `got` — a stale/foreign completion signal was ignored |
| `maintenance_quiesce` | INFO | `maintenance_ops.py` (`run_maintenance_operation`) | `run_id`, `seconds` |
| `maintenance_op_succeeded` | INFO | `maintenance_ops.py` | `run_id`, `records_affected` |
| `maintenance_op_failed` | ERROR (exception) | `maintenance_ops.py` | `run_id` (+ traceback) |
| `schema_degraded` | ERROR | `main.py` (`_record_schema_health`, boot only) | reason string |
| `startup_degraded` | ERROR (exception) | `main.py` (`lifespan`, boot only) | the exception that made the whole startup boundary degrade |

`_handle_transition` logs each open↔closed transition **exactly once**:
it runs with no `await` in its body, so two concurrent callers observing
the same transition can never both log it.

The last three of the `maintenance_ops.py` events
(`maintenance_quiesce` / `maintenance_op_succeeded` / `maintenance_op_failed`)
fire on every `POST /admin/maintenance` call, since that endpoint schedules
`run_maintenance_operation()` as its background task (see below). They do
**not** fire for `migrations/run.py --apply` or `doctor --fix` — both call
`neo4j_store.run_repair()` directly and never go through
`run_maintenance_operation` (or the coordinator's `try_begin_op`/`finish_op`
bookkeeping). Their effect is still observable via the constraint-probe
transition lines (`maintenance_entered` / `maintenance_completed`) once the
next live probe notices the restored constraint.

## Clearing maintenance

The coordinator self-clears the moment its live probe sees the constraint
restored — no restart required, on any of the channels below — but
something has to actually restore it first. Two supported channels exist
today, both ultimately calling the same `neo4j_store.run_repair` (dedup →
`:Node` backfill → constraint create):

- **`POST /admin/maintenance`** — the network-reachable channel (e.g.
  cloud/ACA deployments where the private Neo4j is not directly
  reachable). Admin-authenticated HTTP call; schedules
  `maintenance_ops.run_maintenance_operation()` as a background task,
  which wraps `run_repair` with a quiesce sleep and reports the outcome
  back through the coordinator (`try_begin_op`/`finish_op`). Poll
  `GET /admin/maintenance` for progress. See
  [the endpoint contract below](#postget-adminmaintenance--the-live-endpoint-contract)
  for the full request/response shape.
- **`migrations/run.py --apply`** — the local/VM/direct-Neo4j channel
  (`migrations/run.py`), for operators who can reach Neo4j directly.
  Standalone script, run out-of-band from the server process; it calls
  `run_repair` directly with no coordinator/HTTP/admin-auth involvement.
  `migrations/run.py --status` gives a read-only report first (constraint
  presence, untagged/duplicate counts) without writing anything.

Both channels run the identical repair logic — which one to use is purely
a matter of which one a given deployment can reach. `doctor --fix`
(`context-intelligence-server doctor --fix`) is an older, still-available
entry point to the same `run_repair` function (`doctor.py`); it remains an
equivalent low-level tool, but it is not the primary lever documented here
for clearing a *live* maintenance window — it predates this endpoint and
was originally documented (see the [README](../README.md)) for the
cold-start-refuses-to-boot case.

Once `run_repair` successfully re-creates the
`node_node_id_workspace_unique` constraint — via any of the three tools
above — the **next** probe (at most `maintenance_probe_ttl_seconds` later,
whether triggered by an ingest drainer's gate check, a `GET /status` poll,
or a `GET /admin/maintenance` poll) observes `constraint_present=True` and
`mode` flips back to `healthy`/`degraded` on its own. No server restart is
required.

If the gate is closed because an **op is running** (`op.state ==
"running"`) rather than because the constraint is absent, that sub-state
clears only via `coordinator.finish_op(...)`, which
`run_maintenance_operation` calls automatically once the background task
completes (success or failure) — see the endpoint contract below.
`migrations/run.py --apply` and `doctor --fix` never go through the
coordinator, so they can neither open nor close the "op running"
sub-state; they only affect the constraint-presence sub-state.

## `POST`/`GET /admin/maintenance` — the live endpoint contract

Both routes are defined in `context_intelligence_server/routers/admin.py`
and are on `maintenance.MAINTENANCE_ALLOW_LIST`, so they stay reachable
even while the gate is closed (enforced structurally at startup by
`main._assert_maintenance_endpoint_allow_listed()` — otherwise the
endpoint would 503 at exactly the moment it exists to unblock).

- **`POST /admin/maintenance`** — admin-authenticated (`require_admin`,
  same 401/403 matrix as every other `/admin/*` route). Single-flight:
  `coordinator.try_begin_op()` is a synchronous compare-and-swap with no
  `await` between check and set, so two concurrent POSTs can never both
  start an op. On winning the CAS, it schedules
  `maintenance_ops.run_maintenance_operation()` as a background
  `asyncio.Task` (retained via `coordinator.retain_task(...)` so it can't
  be garbage-collected mid-run) and returns **promptly** — it does not
  await the op's duration (quiesce sleep + the O(graph-size) dedup pass):
  - **202** on winning the CAS: `{"run_id": "...", "state": "running", "started_at": "..."}`
  - **409** if an op is already running: `{"detail": "maintenance operation already running", "run_id": "<current run's id>", "state": "running"}`
  - Re-running on an already-clean graph is a genuine re-scan, not a
    short-circuit: each `POST` performs a real `run_repair` call and gets
    a fresh `run_id`/`completed_at`, even when `records_affected` comes
    back `0`.
- **`GET /admin/maintenance`** — admin-authenticated, same matrix. Returns:

  ```json
  {
    "mode": "maintenance",
    "state": "running",
    "run_id": "3f9e...",
    "started_at": "2026-08-13T12:00:00.123456+00:00",
    "completed_at": null,
    "elapsed_seconds": 4.2,
    "records_affected": null,
    "error": null
  }
  ```

  | Field | Meaning |
  |---|---|
  | `mode` | The coordinator's current mode (`"healthy"` \| `"degraded"` \| `"unknown"` \| `"maintenance"`) — the same value `/status` reports |
  | `state` | The op record's state: `"unknown"` \| `"running"` \| `"succeeded"` \| `"failed"`. `"unknown"` with every other field `null` means no op has run yet in this process |
  | `run_id` | The op's id (uuid4 hex), `null` if none has run |
  | `started_at` / `completed_at` | ISO-8601 UTC timestamps for the op; `completed_at` is `null` while `state == "running"` |
  | `elapsed_seconds` | Seconds since the **current maintenance window** opened (the same value as `/status`'s `maintenance_elapsed_seconds`) — not strictly the op's own runtime; `null` when not in maintenance |
  | `records_affected` | `duplicates_removed + nodes_tagged` from the last completed `run_repair` call, `null` until one completes |
  | `error` | Human-readable exception string if the last op failed, else `null` |

Neither route is exposed in `/openapi.json` or `/docs` (the whole
`/admin` router is mounted with `include_in_schema=False`) — this affects
schema visibility only, not routing or auth.

## Do not wire this to a liveness/readiness probe

The same written prohibition that applies to boot-time `schema_health`
(see the [Azure deployment guide](azure-deployment.md)) applies here,
verbatim, per the code comments in both `main.py` (`_record_schema_health`
docstring and the `/status` handler) and `maintenance.py`:

> `schema_health`/`mode` MUST NOT be wired to a Kubernetes/ACA liveness or
> readiness probe. Doing so would recreate the exact crash-loop the
> deploy-safe-boot fix removes, one layer up.

`mode`, `schema_health`, `maintenance_started_at`, and
`maintenance_elapsed_seconds` are **data-migration signals** for an
operator or automation to *read* — not a gate to wire into container
orchestration health checks. `/status` (and `/version`) stay reachable
and return `200` throughout a maintenance window specifically so a plain
HTTP-200 liveness check against either of those paths keeps working
un-interrupted; use that for liveness if one is needed, and treat the
maintenance fields as a separate, human/automation-facing signal.
