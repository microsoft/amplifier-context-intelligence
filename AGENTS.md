# Context Intelligence Server

Event-driven telemetry platform for [Amplifier](https://github.com/microsoft/amplifier) sessions.
Captures session events as structured data and builds a property graph in Neo4j.

## Quick Overview

```
Amplifier CLI → hook (POST /events) → Ingestion Server (:8000) → Neo4j graph + blob storage
```

See [README.md](README.md) for full setup instructions.

---

## Project Structure

```
context_intelligence_server/      # FastAPI ingestion server
├── main.py                       # App factory, routes, lifespan
├── config.py                     # Settings (YAML + env vars via Pydantic)
├── queue_manager.py              # Durable per-session append-log (persist-then-202)
├── registry.py                   # Per-session drainers (drain_worker, write semaphore, retry/dead-letter)
├── pipeline.py                   # Per-event dispatch spine (invoked by the drainer)
├── neo4j_store.py                # Managed-transaction Neo4j writes
├── blob_store.py                 # Async disk blob storage
├── handlers/                     # Event handlers (data_layer_1/2/3)
│   ├── data_layer_1/             # Session/tool-call handlers
│   ├── data_layer_2/             # Graph enrichment handlers
│   └── data_layer_3/             # High-level insight handlers
├── routers/                      # API routers (queues.py = dead-letter inspect/replay/purge; admin.py = /admin/* identity-map CRUD)
├── auth.py                       # Bearer-token auth middleware (StaticKeyResolver / EntraResolver via PrincipalResolver; BearerTokenMiddleware; admin-key recognition)
├── identity_store.py             # Durable JSON identity map (write-file-then-swap, fail-closed load, live flat_dict)
├── dashboard.py                  # Dashboard SSE stream
├── models.py                     # Pydantic request/response models
└── web/                          # Dashboard HTML + static assets

docs/                             # ⚠️ PRODUCT DOCUMENTATION ONLY
├── architecture/                 # DOT diagrams: pipeline, handlers, graph model
└── service-setup.md              # Running as a system service

tests/
├── handlers/                     # Handler unit tests
├── integration/                  # Pipeline integration tests
└── neo4j/                        # Tests requiring a live Neo4j instance
```

**`docs/` is product documentation only.** Architecture diagrams and operational guides
that ship with the server. Plans, fix designs, and Superpowers-generated documents go in
the **workspace root `docs/`** (one level up from this repo), never here.

---

## Running Tests

Requires Python 3.11+ and [uv](https://github.com/astral-sh/uv).

```bash
uv sync
uv run pytest tests/ -q                    # All tests (no Neo4j required)
uv run pytest tests/neo4j/ -q              # Neo4j tests (requires running instance)
```

Most tests run against in-memory fakes. The `tests/neo4j/` suite requires a live Neo4j 5.x instance — see `tests/neo4j/conftest.py` for connection details.

---

## Running the Server Locally

```bash
# 1. Start Neo4j (Docker, easiest) — NEO4J_PLUGINS enables APOC (see "Neo4j / APOC setup" below)
docker run -d --name neo4j-ci \
  -p 7474:7474 -p 7687:7687 \
  -e NEO4J_AUTH=none \
  -e 'NEO4J_PLUGINS=["apoc"]' \
  neo4j:5.26.22-community

# 2. Configure and start the server
cp server-config.example.yaml server-config.yaml
# Edit server-config.yaml with your Neo4j connection details

# 3. Start
uvicorn context_intelligence_server.main:app --reload
```

Or use Docker Compose to run everything together:

```bash
./start.sh
```

---

## Key Conventions

### Neo4j / APOC setup

Neo4j runs with the **APOC** plugin enabled. Whenever you provision or document a
Neo4j instance for this server — Docker Compose, a standalone `docker run`, a
systemd-managed container, or a test fixture — APOC must be turned on the same way:

```yaml
# docker-compose.yml — neo4j service
environment:
  NEO4J_PLUGINS: '["apoc"]'
```

```bash
# standalone docker run
-e 'NEO4J_PLUGINS=["apoc"]'
```

Neo4j 5.x auto-installs the bundled `apoc-core` jar from `/var/lib/neo4j/labs`
into `/var/lib/neo4j/plugins` at every startup and applies APOC's default config
(including `dbms.security.procedures.unrestricted=apoc.*`). **No volume mount and
no manual jar download are required** — the jar lives on the image layer and
re-installs on each container start. `neo4j:5.26.22-community` bundles APOC Core;
APOC Extended is neither included nor needed. Hosted **AuraDB** already has APOC
Core preinstalled.

Verify: `cypher-shell -u neo4j -p <pw> "RETURN apoc.version();"` → matches the
Neo4j version (e.g. `5.26.22`). Canonical setup docs: `README.md`
("Neo4j Plugins (APOC)") and `docs/service-setup.md` (Step 2).

**Air-gapped / offline provisioning.** When provisioning Neo4j in an environment
with no internet egress, do NOT rely on a download. Two facts:

1. `NEO4J_PLUGINS=["apoc"]` is already offline-safe for APOC **Core** — Neo4j
   5.x copies the jar from the in-image `/var/lib/neo4j/labs/` dir, never the
   network. (Confirmed: the install happens even with networking disabled.)
2. For an air-tight guarantee that skips the installer entirely, use the baked
   image: **`neo4j.Dockerfile`** (copies the bundled APOC Core jar into
   `/var/lib/neo4j/plugins/` at build time + sets `unrestricted=apoc.*`) via the
   **`docker-compose.airgap.yml`** override:
   `docker compose -f docker-compose.yml -f docker-compose.airgap.yml up -d --build`.

On a fully disconnected host, also pre-load the **base image** itself
(`docker save neo4j:5.26.22-community` → `docker load`, or an internal registry
mirror) — you cannot `docker pull` it either. Do NOT set
`dbms.security.procedures.allowlist=apoc.*`; that blocks built-in `db.*`/`dbms.*`
procedures. Only `unrestricted=apoc.*` is needed. This path was validated in an
isolated environment with the Neo4j container cut off from the internet.

### Temporal properties are `ZONED DATETIME`

All temporal properties are stored as native Neo4j ZONED DATETIME, not ISO strings.
The single source of truth is the `TEMPORAL_PROPS` frozenset in `neo4j_store.py`.
Adding a temporal property to any handler requires adding its name to `TEMPORAL_PROPS` — forget
and the value lands as a plain string silently; no error, no warning; temporal predicates
`WHERE`/`duration.between` then behave inconsistently.

`last_updated` (on Session) is the only temporal field not ending in `_at` and is listed
deliberately. Do not replace the registry with a `*_at` suffix heuristic — it silently misses
`last_updated`. Edge `occurred_at` on `HAS_EVENT`, `HAS_SUBSESSION`, `FORKED` is covered by the
same registry and conversion path; no special-casing needed.

### `neo4j_store.py` is the type boundary

`neo4j.time` driver types (`DateTime`, etc.) must never leave `neo4j_store.py`.
`_convert_temporal_props` converts Python `datetime` → ZONED DATETIME on write.
`_normalize_temporal` converts `neo4j.time.DateTime` → Python `datetime` on read.
`services.py`, `pipeline.py`, and handlers deal in Python stdlib types only and never import
or reference `neo4j.time`.

### Setting up / deploying auth (per-user API keys)

If you are setting up or deploying this server, do NOT look for an `init`
subcommand — **it has been removed.** Use one of:

- **Docker bootstrap (default):** `./start.sh` (or the container entrypoint)
  generates credentials on first run, writes an `api_keys` keystore, and **prints
  the raw API token ONCE** behind a "SAVE THIS TOKEN" banner. Capture it then — the
  file stores only the SHA-256 digest, so you cannot grep the token back.
- **Manual:** follow [`docs/managing-api-keys.md`](docs/managing-api-keys.md).

Auth model facts:
- `api_keys` is the per-contributor keystore: `sha256_hex(token) -> {id: <contributor>}`.
  The server stores digests; the peer sends the raw token; the server hashes it to
  look up the contributor. The matched `id` surfaces as `created_by` on graph nodes.
- Legacy single `api_key` still works (folds to id `owner`) — back-compat.
- **`api_keys: {}` (empty map) is a HARD startup error** (fail-closed). To disable
  auth, omit `api_keys`/use `null` and set no `api_key` (dev only).

Canonical guide: `docs/managing-api-keys.md`. Design record:
`docs/designs/per-user-api-keys.md`.

### Entra (Azure AD) JWT auth (`auth_mode=entra`)

The server also supports **Microsoft Entra** authentication as an alternative to
static keys. `auth_mode` (in `config.py`) selects the active resolver — `static`
(default, the api_keys keystore above) or `entra` (JWT validation via Entra JWKS).
Exactly one mode is active; switching is a config change, no code change.

Auth model facts (the canonical statement of "which tokens are accepted" lives in
`docs/entra-auth-setup.md` — this is a pointer, not a restatement):
- Entra mode is **dual-path** (M2): after shared RS256 validation (audience
  `[<client_id>, api://<client_id>]`, issuer `…/<tenant_id>/v2.0`, explicit `tid`),
  a `scp`/`idtyp` discriminator selects the path:
  - **User (delegated)** — `scp` present: must contain `access_as_user`, then
    `oid` → contributor via `entra_identities`. *Unchanged.*
  - **Service (app / managed-identity)** — `scp` absent: authorized by an Entra
    **App Role** alone — `Contributor` (write+read) or `Reader` (read-only:
    `POST /cypher`, `GET /blobs/*`). `created_by` = `service_identities[oid]` if
    mapped, else the stable `appid`. App-only / MI tokens (which carry `roles`,
    not `scp`) **are now accepted** on this path.
  - `scp` + `idtyp=="app"` → 401 (ambiguous, fail-closed).
- The matched `created_by` surfaces on graph nodes — same provenance path as static
  mode.
- Config fields: required for boot — `azure_client_id`, `azure_tenant_id`,
  `entra_identities`. Optional service-path — `service_identities` (friendly
  `created_by` map, **not** an auth gate, **no runtime CRUD** — config + redeploy),
  `service_data_role` (default `Contributor`), `reader_role` (default `Reader`).
  Env prefix `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_`.
- **Fail-closed:** misconfig (missing field / empty or malformed `entra_identities`)
  is a HARD startup error. `allow_unauthenticated` defaults to `false`; a server
  with no auth configured refuses to start.
- **401** = bad/expired/wrong-audience/missing/ambiguous token; **403** = valid
  token lacking authorization — a user whose `oid` is unmapped, or a service token
  with no qualifying App Role (the 403 body names the principal + required roles).
  **Behavior change (M2):** a token with no `scp` and no qualifying role now returns
  **403** (was **401**).

> 🔒 **Secret hygiene — NO real identifiers in this product repo.** An `oid` is a
> persistent personal identifier (PII). Never commit real oids, client IDs, or
> tenant IDs here — **placeholders only** (e.g. `aaaaaaaa-0000-0000-0000-…`). Inject
> the real `entra_identities` map via env/secret or a git-ignored config file.

Canonical guide: `docs/entra-auth-setup.md` (operator + developer + ops runbook).

### Runtime identity-map admin API (`/admin/*`)

Both auth modes can add/remove identities **at runtime, no restart**, via the
`/admin/*` router (`routers/admin.py`). Facts:

- Each map is an **in-process live dict** backed by a durable JSON file on `/data`
  (`api_keys_store_path` / `entra_identities_store_path`, in `config.py`). The
  resolver holds the dict **by reference**, so a `/admin` `PUT`/`DELETE` is visible
  on the next request. **No cache, no TTL** — safe because the pilot runs a
  **single replica** (the in-process map is the source of truth).
- `IdentityStore` (`identity_store.py`) commits **write-file-then-swap-memory**
  (atomic file replace first, then memory) and **fails closed** on a corrupt file
  (empty map + loud log, never a crash-loop).
- **Admin authority** is separate from data auth: static mode uses a dedicated
  `admin_api_key` (recognized by the middleware, not in the data keystore); entra
  mode uses the App Role named by `entra_admin_role` (default `IdentityAdmin`) in
  the token's `roles` claim (**never `groups`**). Data credentials get `403` on
  `/admin/*`; an unconfigured admin credential → `503`. The admin key cannot be
  deleted/shadowed via the API (`409`).

Canonical guide: `docs/identity-management.md`.

### Deploying to Azure / Amplifier Online — post-deploy identity seed

The deployment manifest is `amplifier-online.yaml` (stack `web-app-aca`). The
`entra_identities` oid→handle map is **PII and is intentionally NOT in the manifest
or this repo** — see the secret-hygiene note above.

**One-command deploy** — use the repo-root wrapper, which runs `amplifier-online up`
and then onboards the identity map in a single shot:
```bash
export SERVER_URL="https://<server-fqdn-or-apim-gateway>"   # for the seed step
export AUTH_RESOURCE="api://<client_id>"                    # token audience
export SEED_FILE="scripts/entra-identities.local.json"     # uncommitted map
./deploy.sh
```
`deploy.sh` = `amplifier-online up` **+** `scripts/seed-entra-identities.sh`. The
seed is **idempotent and no-ops once `/data` is populated**, so it is safe on every
deploy: a fresh/empty `/data` gets onboarded, an existing one is left untouched.
Use `./deploy.sh --no-seed` for a deploy-only run.

**Why a wrapper and not the manifest:** `amplifier-online up` ships
`amplifier-online.yaml` verbatim (no `${VAR}`/shell/secret expansion), so the
`entra_identities` map — **PII, intentionally NOT in the manifest or this repo** —
cannot be supplied through `up` itself. The map lives in an **uncommitted,
git-ignored** local file (`scripts/entra-identities.local.json`, from
`scripts/entra-identities.example.json`) and is applied over the admin API
(`PUT /admin/identities/{oid}`) right after `up`. The server fails closed on an
empty `entra_identities` map, so this onboarding is required before it serves on a
fresh volume.

Under the hood the seeder can also be run alone (`./scripts/seed-entra-identities.sh
--check` for a dry run) — see `docs/azure-deployment.md`.

Full runbook: `docs/azure-deployment.md` → "Seeding the identity map on a FRESH
`/data`". Tooling: `scripts/seed-entra-identities.sh` (idempotent, fail-loud, no
real oids committed).

### Local static run (web UI + admin) — the common single-box setup

For a local server with static keys, the browser dashboard, and runtime
onboarding (no restart), set these in `server-config.yaml`:

- `auth_mode: static` (default) **+** an `api_keys` entry
  (`sha256(token) -> {id: <contributor>}`) — the data credential.
- `admin_api_key: "<raw token>"` — **enables `/admin/*` in static mode** (unset →
  `503`; a data key → `403`). Use a token **DISTINCT** from any data key: the
  admin key is recognized *before* the data resolver, so requests bearing it are
  attributed `created_by="admin"` — reusing the capture hook's token stamps your
  captured sessions `admin` instead of the real contributor.
- `web_ui_enabled: true` (default) — dashboard + `/docs` at `:8000`; `false` =
  API-only (those routes 404).
- `api_keys_store_path` — point at a **writable** dir; the `/data/...` default is
  not writable on a normal box. Seeded from `api_keys` on first boot, then it is
  the source of truth for `/admin/keys` edits.

Copy-paste quickstart: `docs/service-setup.md` ("Local quickstart — static mode
with web UI + admin"). Runtime runbook: `docs/identity-management.md`.

---

## Key Concepts

- **Event pipeline** — `POST /events` persists the raw event to a durable per-session append-log (`queue_manager.py`) and returns `202` immediately (persist-then-202). A single drainer per session (`registry.py`) processes batches and flushes them to Neo4j under a global write semaphore, with transient/deadlock retry, dead-letter isolation of poison events, and crash recovery (replay + counter re-seed) on startup. Each handler invoked by the per-event dispatch spine is a Python class in `handlers/data_layer_*/`.
- **Graph model** — session sub-labels: `RootSession`, `SubSession`, `ForkedSession`, `IncompleteSession` (health marker; not a terminal). Full schema with all node and edge types: see `docs/architecture/03-graph-model.dot` and `docs/architecture/README.md`.
- **Blob storage** — Large event payloads are written to disk and referenced by URI to avoid graph bloat.
- **Configuration** — Pydantic Settings reads from `server-config.yaml` first, then environment variables. See `config.py`.

---

## Making Changes

- **New handler**: Add a class to the appropriate `handlers/data_layer_*/` directory, register it in `handlers/__init__.py`.
- **New API endpoint**: Add a route to `main.py` or a new router under `routers/`.
- **Configuration**: Add fields to `ServerConfig` in `config.py`. Keep defaults conservative.
- **Tests**: Every handler should have a unit test in `tests/handlers/`. Integration tests live in `tests/integration/`.

Run `uv run pytest tests/ -q` to verify before committing.
