# Context Intelligence Server

An event-driven telemetry platform for [Amplifier](https://github.com/microsoft/amplifier) sessions. Captures session events as structured data and builds a property graph in Neo4j.

## How It Works

```
Amplifier CLI sessions
       |
       |  hook-context-intelligence (thin forwarder)
       |  POST /events {event, workspace, data}  ->  202 Accepted (persist-then-202)
       v
+------------------------------------------+        +----------------------+
| Ingestion Server (:8000)                 |        | Neo4j                |
| - Durable per-session append-log (queue) |        | :7687  bolt/driver   |
| - Async drainer -> batched Neo4j flush   | bolt   | :7474  browser UI    |
|   under a global write semaphore         |------->| Property graph       |
| - Retry + dead-letter + crash recovery   |        | 5 node / 8 edge types|
| - Blob storage (large payloads to disk)  |        +----------------------+
| - Dashboard + API docs + Cypher proxy    |
+------------------------------------------+
```

`POST /events` appends the raw event to a durable per-session append-log and returns `202`
immediately; an async single drainer per session processes batches and flushes them to Neo4j
under a global write semaphore, retrying transient/deadlock failures and dead-lettering poison
events. See [docs/architecture/05-durable-ingest-queue.png](docs/architecture/05-durable-ingest-queue.png)
for the full ingest/drain flow.

---

## Neo4j Plugins (APOC + GDS)

The server needs Neo4j 5.x reachable over Bolt with the **APOC** procedures
available (and **GDS** for graph-analytics features). For enabling these plugins
on a local, non-Docker Neo4j install, see
[docs/local-development.md](docs/local-development.md) §1. Hosted **Neo4j AuraDB**
already has APOC Core preinstalled.

---

## First-Run Setup (Standalone)

There is **no `init` subcommand.** The quickest path is
`python scripts/prime-local-config.py` (see
[docs/local-development.md](docs/local-development.md) §2), which generates an API
token and writes a ready-to-use `server-config.yaml`. To do it by hand instead,
write `server-config.yaml` yourself (copy `server-config.example.yaml` and edit —
see [Running Locally](#running-locally) below) and add authentication.

For auth, choose one of:

- **Legacy single key** — set `api_key: "<your-secret>"` in the config. Clients
  send it as `Authorization: Bearer <your-secret>`.
- **Per-contributor keystore** — generate a token, derive its SHA-256 digest, and
  add an `api_keys` entry. Send the **raw token** to the client.

  ```bash
  TOKEN=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))")
  python3 -c "import hashlib,sys; print(hashlib.sha256(sys.argv[1].encode()).hexdigest())" "$TOKEN"
  echo "raw token (give to client): $TOKEN"
  ```

- **Microsoft Entra JWT** (`auth_mode=entra`) — clients authenticate with Azure AD
  bearer tokens instead of pre-shared keys; the server validates the JWT and maps the
  token's `oid` to a contributor. See [docs/entra-auth-setup.md](docs/entra-auth-setup.md).

The server verifies every request by hashing the presented token (`sha256(token)`)
and matching the digest. The full guide — adding/revoking/rotating peers, the
empty-`{}` hard-error rule, and the raw-token-vs-digest guardrail — is in
[docs/managing-api-keys.md](docs/managing-api-keys.md). Use the client token as
`context_intelligence_api_key` in your bundle config.

> First-run bootstrap: `python scripts/prime-local-config.py` generates an
> `api_keys` keystore and prints the token once — no manual steps. See
> [docs/local-development.md](docs/local-development.md).

> **Local static run with the web dashboard + runtime admin API?** For a
> copy-paste path that strings install → Neo4j → `server-config.yaml`
> (`api_keys` + `admin_api_key` + `web_ui_enabled: true`) → run → verify → use
> `/admin/*`, see the **"Local quickstart — static mode with web UI + admin"**
> section in [docs/service-setup.md](docs/service-setup.md).

---

## Running Locally

Run the server as a plain Python process — this is the primary run path. The full
walkthrough (installing Neo4j 5.x + APOC/GDS without Docker, priming keys, and the
"ask Amplifier to set it up" prompts) is in
[docs/local-development.md](docs/local-development.md). The three essentials:

**1. Install and run Neo4j 5.x locally** with the **APOC** procedures (and **GDS**
for graph-analytics features), reachable at `bolt://localhost:7687`. See
[docs/local-development.md](docs/local-development.md) §1 (Neo4j Desktop or a
package/tarball install).

> Use the `bolt://` scheme, **not** `neo4j://` — the routing scheme expects a
> cluster and fails against a Community single-node install.

**2. Prime API keys and config** — a helper generates an API token (printed once)
and writes a ready-to-use `server-config.yaml` plus a local
`./.context-intelligence-data/` tree:

```bash
python scripts/prime-local-config.py --neo4j-password '<neo4j-password>'
```

**3. Run the server** against that config:

```bash
export AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE="$(pwd)/server-config.yaml"
uv sync
uv run uvicorn context_intelligence_server.main:app --host 127.0.0.1 --port 8000
```

Open [http://localhost:8000](http://localhost:8000) to confirm the server is
running. Configuration resolution is `env > server-config.yaml > built-in
defaults`; the generated config overrides the container-oriented defaults for
local use. To configure the server by hand instead, copy
`server-config.example.yaml` to `server-config.yaml` and edit it — the full
settings reference is in [Configuration](#configuration) below.

---

## Running as a System Service

To run the server as an auto-starting background service on Linux (systemd)
or macOS (launchd), see [docs/service-setup.md](docs/service-setup.md).

---

## Deploying to Azure

See [docs/azure-deployment.md](docs/azure-deployment.md) for a full guide to deploying as an Azure Container App with automatic HTTPS, persistent storage, and Neo4j on AuraDB.

---

## Sharing with Trusted Peers

To let a few trusted people on **other networks** send their sessions to your server
— privately, exposing only the `/events` endpoint and nothing else — see
[docs/remote-access-sharing.md](docs/remote-access-sharing.md). It covers loopback
binding, exposing a single path over Tailscale, and per-peer access scoping
(including a common ACL pitfall that silently grants too much). Hand your peers
[docs/peer-onboarding.md](docs/peer-onboarding.md) to get them connected.

---

## Network Access and Security

The server defaults to `server_host: 0.0.0.0`, which binds on **all network interfaces** — loopback, LAN, and any container bridges (Docker, Incus). This is intentional: worker containers and DTU containers use the host's bridge gateway IP to reach the server, and they cannot reach `127.0.0.1` (the host's loopback) from inside a container. On a single-user development machine behind a NAT router or firewall, binding to 0.0.0.0 is safe. If you deploy the server in an environment where port 8000 (or your configured `server_port`) is reachable from untrusted networks, restrict access with a firewall rule to trusted source IPs. Use `server_host: 127.0.0.1` only when you are certain no container processes will ever need to reach the server.

---

## Feeding Events into the Server

The server receives events from [amplifier-bundle-context-intelligence](https://github.com/microsoft/amplifier-bundle-context-intelligence) — a thin-forwarder hook that captures every Amplifier session event and dispatches it to the server over HTTP.

### Install the bundle

```bash
amplifier bundle add git+https://github.com/microsoft/amplifier-bundle-context-intelligence@main --name context-intelligence --app
```

The `--app` flag makes the bundle always active across all sessions — no need to run `amplifier bundle use`.

### Configure the server URL

Add the hook configuration to `~/.amplifier/settings.yaml`:

```yaml
overrides:
  hook-context-intelligence:
    config:
      context_intelligence_server_url: "http://localhost:8000"
```

### How it works

When `context_intelligence_server_url` is configured, the hook:

1. Writes every event to local JSONL (always, regardless of server)
2. Fire-and-forgets `POST /events` to the server for each event (5s timeout, failures logged as warnings)
3. Registers `blob_list` and `blob_dump` tools for querying server-stored blobs

The local JSONL is the durable record. The server dispatch is best-effort and never blocks the Amplifier session. If the server is down, the session continues unaffected.

### Hook settings

All settings live in `~/.amplifier/settings.yaml` under `overrides.hook-context-intelligence.config`:

| Setting | Default | Description |
|---------|---------|-------------|
| `context_intelligence_server_url` | *(empty — disabled)* | Server URL to forward events to |
| `context_intelligence_api_key` | *(empty)* | Raw bearer token for server auth. The server verifies it by computing `sha256(token)` and matching the digest in its keystore (a legacy `api_key`, or an `api_keys` entry). Send this raw token to clients; never the digest. |
| `workspace` | *(auto-resolved)* | Workspace scope for graph data |

---

## Authentication and identity resolution

Every data request is authenticated by a single ASGI gate (`BearerTokenMiddleware`,
`auth.py`) that runs before any route. One `auth_mode` setting selects the active
resolver:

- **`static`** (default) — pre-shared bearer tokens. The server hashes the
  presented token (`sha256`) and looks the digest up in the keystore to get the
  contributor id. See [docs/managing-api-keys.md](docs/managing-api-keys.md).
- **`entra`** — Microsoft Entra JWTs (RS256, validated against the tenant JWKS,
  with audience / issuer / `tid` checks), then a **dual-path** discriminator on
  the token's `scp`/`idtyp` claims:
  - **User (delegated)** — `scp` present: must contain `access_as_user`, then the
    token's `oid` maps to a contributor via `entra_identities`. *Unchanged.*
  - **Service (app / managed-identity)** — `scp` absent: authorized by an Entra
    **App Role** alone — `Contributor` (write + read) or `Reader` (read-only:
    `POST /cypher`, `GET /blobs/*`). `created_by` is `service_identities[oid]` if
    mapped, else the stable `appid`.

  See [docs/entra-auth-setup.md](docs/entra-auth-setup.md) for the canonical model.

The matched contributor id is stamped onto the graph as the write-once `created_by`
provenance field. A missing/invalid credential is a **401**; a valid credential
that lacks the needed binding or role is a **403** — a delegated user whose `oid`
is unmapped, or a service token with no qualifying App Role (its 403 body names the
`appid`/`oid` and the required roles). **Behavior change (M2):** a token with no
`scp` and no qualifying role now returns **403** (previously **401**). Health and
(in full-web mode) the dashboard/docs paths are exempt; in API-only mode
(`web_ui_enabled=false`) the exempt set shrinks to `{/status, /version}`.

**Admin authority** is a separate layer that gates the `/admin/*` identity-map
endpoints: in static mode a dedicated `admin_api_key` (recognized by the
middleware, distinct from data keys); in entra mode the App Role named by
`entra_admin_role` (default `IdentityAdmin`) in the token's `roles` claim. Regular
data keys/tokens get **403** on `/admin/*`; if the admin credential for the active
mode is unconfigured, `/admin/*` returns **503**.

### How identity resolution stays fresh — no cache

There is **no cache and no TTL** on the identity map. Each map is an **in-process
live dictionary** backed by a durable JSON file on the `/data` volume
(`api_keys_store_path` / `entra_identities_store_path`). The resolver holds a
*reference* to that live dict, so a runtime `/admin/*` `PUT`/`DELETE` is visible on
the **very next request** — no restart, no redeploy. Writes use a
**write-file-then-swap-memory** commit (atomic file replace first, then the
in-process update) so the file is never behind memory; a corrupt store file
**fails closed** to an empty map (loud log, never a crash-loop). This is safe and
simple because the pilot runs a **single replica** (`maxReplicas=1`, single-writer
drainer): there is no second process to go stale. A future read-tier (M3) that adds
replicas would introduce a short TTL/poll re-read; that does not exist today.

Full runtime onboarding/offboarding runbook and the `/admin/*` API:
[docs/identity-management.md](docs/identity-management.md).

---

## API Reference

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/events` | Ingest a session event (returns 202 immediately) |
| `GET` | `/status` | Server health, active sessions, completed history, error counts, `neo4j_connected`, `neo4j_query_connected` (reflects the read/cypher_query driver's connection health), `neo4j_url`, `neo4j_browser_url` |
| `GET` | `/` | Landing page with navigation cards |
| `GET` | `/dashboard` | Live monitoring dashboard |
| `GET` | `/docs` | Swagger API docs |
| `GET` | `/logs/stream` | Server-Sent Events — live structured log tail |
| `GET` | `/blobs/{session_id}` | List all blob URIs for a session |
| `GET` | `/blobs/{session_id}/{key}` | Retrieve a stored blob |
| `POST` | `/cypher` | Proxy a Cypher query to Neo4j |
| `GET` | `/queues/dead-letter` | List dead-letter queues — `worker_key`, `item_count`, `last_error`, `last_ts` (requires `Authorization: Bearer`) |
| `POST` | `/queues/dead-letter/{worker_key}/replay` | Re-enqueue a worker's dead-letter records then purge; returns count re-enqueued (requires `Authorization: Bearer`) |
| `POST` | `/queues/dead-letter/{worker_key}/purge` | Permanently delete a worker's dead-letter records; returns count purged (requires `Authorization: Bearer`) |
| `PUT`/`DELETE`/`GET` | `/admin/identities[/{oid}]` | Runtime entra identity-map CRUD (`auth_mode=entra`) — admin authority required. See [docs/identity-management.md](docs/identity-management.md) |
| `PUT`/`DELETE`/`GET` | `/admin/keys[/{sha256hash}]` | Runtime static API-key map CRUD (`auth_mode=static`) — admin authority required. See [docs/identity-management.md](docs/identity-management.md) |

### Event payload

```json
{
  "event": "tool:pre",
  "workspace": "my-project",
  "data": {
    "session_id": "abc-123",
    "timestamp": "2026-03-14T12:00:00+00:00",
    "tool_name": "bash",
    "tool_call_id": "tc-001"
  }
}
```

### Cypher proxy

```json
{
  "query": "MATCH (s:Session {workspace: $workspace}) RETURN s.node_id, s.status",
  "params": {},
  "workspace": "my-project"
}
```

Use `"workspace": "*"` to query across all workspaces.

---

## Configuration

### Settings resolution order

Values are resolved with this priority (highest first):

1. **Environment variables** — `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_*`
2. **YAML configuration file** — `server-config.yaml` in the working directory, or the path in `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE`
3. **Built-in defaults**

### All settings

| Environment variable | YAML key | Default | Description |
|----------------------|----------|---------|-------------|
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE` | *(env only)* | `server-config.yaml` | Path to the YAML config file |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_API_KEY` | `api_key` | *(empty — auth disabled)* | Legacy single bearer token (folds to contributor id `owner`). When set, all API endpoints except `/status` and static routes require `Authorization: Bearer <value>`. The server verifies a request by computing `sha256(token)` and matching it. Coexists with `api_keys`. See [docs/managing-api-keys.md](docs/managing-api-keys.md). |
| *(YAML only)* | `api_keys` | *(empty — disabled)* | Per-contributor keystore: a map of `sha256_hex(token) -> {id: <contributor>}`. The server stores only digests; the peer sends the **raw** token and the server hashes it to look up the contributor. `api_keys: {}` (empty map) is a **hard startup error** — omit or use `null` to disable auth. The matched `id` is recorded as `created_by` on graph nodes. See [docs/managing-api-keys.md](docs/managing-api-keys.md). |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_AUTH_MODE` | `auth_mode` | `static` | Selects the active resolver: `static` (sha256 keystore, the `api_key`/`api_keys` path above) or `entra` (Microsoft Entra JWT validation via JWKS). Exactly one mode is active. `entra` requires the three fields below. See [docs/entra-auth-setup.md](docs/entra-auth-setup.md). |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_AZURE_CLIENT_ID` | `azure_client_id` | *(empty)* | App Registration (client) GUID. **Required when `auth_mode=entra`** (startup refuses otherwise). See [docs/entra-auth-setup.md](docs/entra-auth-setup.md). |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_AZURE_TENANT_ID` | `azure_tenant_id` | *(empty)* | Azure AD tenant GUID. **Required when `auth_mode=entra`**. See [docs/entra-auth-setup.md](docs/entra-auth-setup.md). |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_ENTRA_IDENTITIES` (JSON) | `entra_identities` | *(empty)* | Identity map `oid -> {id: <contributor>}` for the **user (delegated)** path (oids are Azure Object IDs — **PII**, never commit real values). **Required (non-empty) when `auth_mode=entra`**; the matched `id` is recorded as `created_by`. See [docs/entra-auth-setup.md](docs/entra-auth-setup.md). |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_SERVICE_IDENTITIES` (JSON) | `service_identities` | *(empty)* | **Entra service path — optional.** `oid -> {id: <contributor>}` map giving a **friendly `created_by`** name to a service principal / managed identity. **Not an auth gate** (App Roles authorize; see below) and **never required** for boot. Unmapped services still authorize, with `created_by` = `appid`. No runtime CRUD — edit config and redeploy. See [docs/entra-auth-setup.md](docs/entra-auth-setup.md). |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_SERVICE_DATA_ROLE` | `service_data_role` | `Contributor` | **Entra service path.** App Role name whose presence in an app token's `roles` claim grants service **write + read**. `""`/`null` disables the service write path. See [docs/entra-auth-setup.md](docs/entra-auth-setup.md). |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_READER_ROLE` | `reader_role` | `Reader` | **Entra service path.** App Role name granting service **read-only** access (`POST /cypher`, `GET /blobs/*`). `""`/`null` disables read-only app-token gating. See [docs/entra-auth-setup.md](docs/entra-auth-setup.md). |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_ADMIN_API_KEY` | `admin_api_key` | *(empty — admin API disabled)* | **Static-mode admin credential** — separate from the data `api_keys`; it is the only key allowed to call the `/admin/*` identity-map endpoints. Sent as a bearer token; the middleware recognizes it before the data keystore lookup. Empty → admin API returns `503`; regular data keys get `403` on `/admin/*`. Cannot be deleted/shadowed via the API. See [docs/identity-management.md](docs/identity-management.md). |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_ENTRA_ADMIN_ROLE` | `entra_admin_role` | `IdentityAdmin` | **Entra-mode admin authority** — the App Role name that must appear in a token's `roles` claim (never `groups`) to call `/admin/*`. Created/assigned in the App Registration. Empty/`null` → admin API returns `503`. See [docs/identity-management.md](docs/identity-management.md). |
| *(YAML only)* | `api_keys_store_path` | `/data/identity/api-keys.json` | Durable JSON file backing the **static** identity map (`sha256(key) -> contributor`). The in-process map is seeded from `api_keys` on first boot, then this file is the source of truth for runtime `/admin/keys` edits. |
| *(YAML only)* | `entra_identities_store_path` | `/data/identity/entra-identities.json` | Durable JSON file backing the **entra** identity map (`oid -> contributor`). Seeded from `entra_identities` on first boot, then this file is the source of truth for runtime `/admin/identities` edits. |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_ALLOW_UNAUTHENTICATED` | `allow_unauthenticated` | `false` | Opt-out of the fail-closed startup gate so the server can boot with no auth configured (every request passes through). **TEST/DEV ONLY — never set in production.** |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_WEB_UI_ENABLED` | `web_ui_enabled` | `true` | When `false`, locks down to API-only: no OpenAPI schema / Swagger UI, and the index, dashboard, static assets, and `/logs/stream` routes are unregistered and removed from the auth-exempt set (`/logs/stream` becomes auth-gated). |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J_URL` | `neo4j_url` | `neo4j://neo4j:7687` | Neo4j bolt/driver URL used for all graph operations. **Displayed verbatim in the web UI.** May point to a remote host — `bolt://db.internal:7687` is valid. Use `bolt://` scheme for Community Edition single-node installs. |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J_BROWSER_URL` | `neo4j_browser_url` | `http://localhost:7474` | Neo4j Browser HTTP UI URL. **Displayed verbatim as a clickable link in the web UI.** Set to the address reachable from your browser — not necessarily `localhost` if Neo4j is on a remote machine. |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J_USER` | `neo4j_user` | `neo4j` | Neo4j username (legacy single-credential form; used for both internal clients when the structured `neo4j:` block is omitted). |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J_PASSWORD` | `neo4j_password` | `password` | Neo4j password (legacy single-credential form). |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J__ADMIN__URL` | `neo4j.admin.url` | *(required if block set)* | **Two-client split (opt-in).** Bolt URL for the **admin / WRITE** client (ingest + schema). Note the `__` nested delimiter. |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J__ADMIN__USERNAME` | `neo4j.admin.username` | `neo4j` | Username for the admin/WRITE client. |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J__ADMIN__PASSWORD` | `neo4j.admin.password` | *(empty = no-auth, dev only)* | Password for the admin/WRITE client. |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J__ADMIN__ACCESS_MODE` | `neo4j.admin.access_mode` | `WRITE` | **MUST be `WRITE`** — startup fails otherwise. |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J__CYPHER_QUERY__URL` | `neo4j.cypher_query.url` | *(required if block set)* | Bolt URL for the **cypher_query / READ** client (`POST /cypher` + dashboard reads). May differ from admin (e.g. a read replica). |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J__CYPHER_QUERY__USERNAME` | `neo4j.cypher_query.username` | `neo4j` | Username for the read client (use a separate, ideally read-only, credential). |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J__CYPHER_QUERY__PASSWORD` | `neo4j.cypher_query.password` | *(empty = no-auth, dev only)* | Password for the read client. |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J__CYPHER_QUERY__ACCESS_MODE` | `neo4j.cypher_query.access_mode` | `WRITE` | **MUST be set to `READ`** — the default is `WRITE`, so omitting it is a hard startup error. |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J_REQUIRE_EXPLICIT_CLIENTS` | `neo4j_require_explicit_clients` | `false` | When `true`, the startup guard **refuses** the legacy flat fallback and requires the explicit `neo4j.admin` + `neo4j.cypher_query` block (even if both point at the same instance). See [docs/auth-troubleshooting-and-upgrades.md](docs/auth-troubleshooting-and-upgrades.md) § "Neo4j two-client split". |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_BLOB_PATH` | `blob_path` | `/data/blobs` | Blob storage root directory |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_QUEUES_PATH` | `queues_path` | `/data/queues` | Directory for the durable per-session append-logs (persist-then-202 ingest); mirrors `blob_path`. |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_WRITE_CONCURRENCY` | `write_concurrency` | `8` | Max concurrent Neo4j write flushes across all session drainers (starvation guard). |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_MAX_DELIVERY_ATTEMPTS` | `max_delivery_attempts` | `5` | Flush retries for one batch before its offending line is dead-lettered. |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_LOG_PATH` | `log_path` | `/data/logs/server.jsonl` | Structured log file path |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_LOG_LEVEL` | `log_level` | `INFO` | Log level (`DEBUG`/`INFO`/`WARNING`/`ERROR`) |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_SERVER_HOST` | `server_host` | `0.0.0.0` | Bind host |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_SERVER_PORT` | `server_port` | `8000` | Bind port |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_DASHBOARD_INACTIVE_TIMEOUT` | `dashboard_inactive_timeout` | `1800.0` | Seconds before a session is hidden from the dashboard (30 min) |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_STALE_SESSION_TIMEOUT` | `stale_session_timeout` | `432000.0` | Seconds before a session worker is reaped (5 days) |

> **Note:** `CONFIG_FILE` is resolved before any other setting and cannot itself be set from the YAML file — only from the environment.

---

## Data Persistence

When running locally, the server's data lives under the data directory that
`scripts/prime-local-config.py` creates — `./.context-intelligence-data/` by
default (override with `--data-dir`, or set the paths yourself in
`server-config.yaml`). The Neo4j graph is persisted separately by your Neo4j
install.

| Data | Local path (default) | Description |
|------|----------------------| ------------|
| Blobs | `./.context-intelligence-data/blobs` | Event blob JSON files |
| Queues | `./.context-intelligence-data/queues` | Durable per-session append-logs (`.log`, `.offset`, `.dead.jsonl`) |
| Logs | `./.context-intelligence-data/logs` | Rotating JSONL log files |
| Identity | `./.context-intelligence-data/identity` | API-key / identity-map store files |
| Neo4j graph | *(your Neo4j install's data dir)* | Property graph database |

Blob data and the durable per-session queues survive server restarts. On startup
the server replays any unprocessed queue lines and re-seeds its conservation
counters (accepted/written/in-queue/dead) from disk, so in-flight events are
recovered rather than lost across a restart. The durable per-session logs — not
just Neo4j — are the record for events that have been accepted but not yet written
to the graph.

For persistence on Azure Container Apps (persistent storage + Neo4j on AuraDB),
see [docs/azure-deployment.md](docs/azure-deployment.md).

---

## Neo4j Graph Model

All nodes carry a `workspace` property for multi-workspace isolation.

### Node types

| Label | Created by | Key properties |
|-------|-----------|----------------|
| `Session` + `RootSession`/`SubSession`/`ForkedSession` | `session:start`, `session:fork` | `node_id`, `status`, `started_at` |
| `Session` + `IncompleteSession` | `session:end` with no prior start/fork | `node_id`, `has_terminal: false`; health signal — spike indicates upstream event loss; WARNING logged at ingest; not a terminal type |
| `ToolCall` | `tool:pre` | `node_id` (session__tool_call__tool_call_id), `tool_name`, `tool_call_id` |
| `Event` + derived label | unclaimed events | `node_id`, `event_type` |

### Edge types

`SUBSESSION_OF` | `HAS_FORK` (session:fork parent→child) | `HAS_EVENT` | `HAS_TOOL_CALL` (Session→ToolCall, has started_at/ended_at)

### Example queries

```cypher
-- All sessions in a workspace
MATCH (s:Session {workspace: "my-project"})
RETURN s ORDER BY s.started_at DESC

-- Full session graph
MATCH path = (s:Session)-[*1..4]->(n)
WHERE s.node_id CONTAINS "my-session-id"
RETURN path

-- Find all events for a session
MATCH (s:Session {node_id: "your-session-id"})-[:HAS_EVENT]->(e:Event)
RETURN labels(e), e.occurred_at
ORDER BY e.occurred_at
```

---

## Development

### Prerequisites

- Python 3.11+
- [uv](https://github.com/astral-sh/uv)
- A local Neo4j 5.x install to run the app — see
  [docs/local-development.md](docs/local-development.md). The `-m neo4j` test tier
  additionally spins up an ephemeral Neo4j container via the `docker` dev
  dependency, but the app itself runs locally without Docker.

### Setup

```bash
git clone https://github.com/microsoft/amplifier-context-intelligence.git
cd amplifier-context-intelligence
uv sync
```

### Run tests

```bash
uv run pytest tests/ -q
```

### Project structure

```
amplifier-context-intelligence/
├── context_intelligence_server/         # Ingestion server (FastAPI)
│   ├── main.py                          # App factory, lifespan, static files
│   ├── config.py                        # Pydantic Settings + YAML source
│   ├── queue_manager.py                 # Durable per-session append-log (persist-then-202)
│   ├── registry.py                      # Per-session drainers (drain_worker, write semaphore, retry/dead-letter)
│   ├── services.py                      # Service wiring / lifecycle
│   ├── pipeline.py                      # Per-event dispatch spine (invoked by the drainer)
│   ├── neo4j_store.py                   # Neo4jGraphStore (managed-tx writes)
│   ├── graph_store.py                   # Graph store protocol / abstraction
│   ├── blob_store.py                    # AsyncDiskBlobStore
│   ├── idempotency.py                   # Idempotent MERGE / dedupe helpers
│   ├── auth.py                          # Bearer-token API authentication
│   ├── dashboard.py                     # Dashboard SSE stream
│   ├── routers/                         # API routers: queues.py, version.py
│   ├── handlers/                        # Event handlers: data_layer_1/2/3/ + field_lifters/
│   └── web/                             # Dashboard HTML + static assets
├── server-config.example.yaml           # Configuration file template
├── scripts/prime-local-config.py        # Local key/config bootstrap (non-Docker)
└── Dockerfile                           # Ingestion server image (Azure/shipping)
```

---

## Related

- [amplifier-bundle-context-intelligence](https://github.com/microsoft/amplifier-bundle-context-intelligence) — Amplifier bundle that forwards session events to this server
- [amplifier](https://github.com/microsoft/amplifier) — The Amplifier framework


## Contributing

> [!NOTE]
> This project is not currently accepting external contributions, but we're actively working toward opening this up. We value community input and look forward to collaborating in the future. For now, feel free to fork and experiment!

Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit [Contributor License Agreements](https://cla.opensource.microsoft.com).

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft
trademarks or logos is subject to and must follow
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
