# Running as a System Service

How to run `context-intelligence-server` and `Neo4j` as persistent services on
**Linux (systemd)** or **macOS (launchd)**, with full authentication, and
integrated with the Amplifier CLI so sessions are automatically captured.

---

## Local quickstart — static mode with admin

The fastest path to a **local** server (server v6.0.0): static API-key auth and
the runtime `/admin/*` identity-map API **enabled**. The server is **headless**
(API-only) — there is no browser dashboard; the OpenAPI docs at `/docs` are always
on. Every value below is a **placeholder** — substitute your own. The numbered
sections after this one explain each step in full.

**1. Install uv and the server** (one binary lands in `~/.local/bin`):

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"
uv tool install git+https://github.com/microsoft/amplifier-context-intelligence
# Pre-merge / a specific branch instead (note --force to overwrite an existing install):
#   uv tool install --force "git+https://github.com/microsoft/amplifier-context-intelligence@feat/runtime-identity-map"
# A local checkout instead:
#   uv tool install --force /path/to/amplifier-context-intelligence
```

**2. Start Neo4j (Docker)** — APOC + GDS (Graph Data Science, Community edition), both required:

```bash
NEO4J_BOLT_PORT=37687              # bolt driver (standard would be 7687)
NEO4J_HTTP_PORT=37474              # browser UI  (standard would be 7474)
NEO4J_PASSWORD="<your-strong-password>"
DATA_DIR="$HOME/amplifier-context-intelligence-server-data-store"
mkdir -p "${DATA_DIR}/neo4j"

docker run -d \
  --name amplifier-context-intelligence-neo4j \
  --restart unless-stopped \
  -p ${NEO4J_HTTP_PORT}:7474 -p ${NEO4J_BOLT_PORT}:7687 \
  -e NEO4J_AUTH=neo4j/${NEO4J_PASSWORD} \
  -e 'NEO4J_PLUGINS=["apoc","graph-data-science"]' \
  -e 'NEO4J_dbms_security_procedures_unrestricted=apoc.*,gds.*' \
  -v "${DATA_DIR}/neo4j:/data" \
  neo4j:5.26.22-community
```

> **GDS (Graph Data Science):** the startup plugin-installer resolves the GDS
> Community build matching this Neo4j release from the official compatibility
> matrix (2.13.x for 5.26.x) —
> https://neo4j.com/docs/graph-data-science/current/installation/supported-neo4j-versions/
> This requires network egress at container start. Set only
> `NEO4J_dbms_security_procedures_unrestricted=apoc.*,gds.*` (no `allowlist`, which
> would block built-in `db.*`/`dbms.*` procedures). Verify after start with
> `docker exec amplifier-context-intelligence-neo4j cypher-shell -u neo4j -p "$NEO4J_PASSWORD" "RETURN gds.version();"`.
> See [local-development.md](local-development.md) §1 for the local-dev variant.

**3. Make two DISTINCT tokens** — one for data capture, one for admin:

```bash
# Data token: clients/the hook send this RAW; the config stores only its digest.
DATA_TOKEN=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))")
DATA_DIGEST=$(python3 -c "import hashlib,sys; print(hashlib.sha256(sys.argv[1].encode()).hexdigest())" "$DATA_TOKEN")

# Admin token: the RAW value goes in admin_api_key and is the bearer for /admin/*.
# Keep it DISTINCT from the data token (see the created_by note in step 4).
ADMIN_TOKEN=$(openssl rand -hex 32)

echo "DATA  token (use in the hook / clients): $DATA_TOKEN"
echo "DATA  digest (goes into api_keys):       $DATA_DIGEST"
echo "ADMIN token (bearer for /admin/* calls): $ADMIN_TOKEN"
```

**4. Write `server-config.yaml`** — static mode, admin on, **local
writable** store paths. Create the dirs, then drop in the file:

```bash
mkdir -p "${DATA_DIR}/identity" "${DATA_DIR}/blobs" "${DATA_DIR}/logs" "${DATA_DIR}/queues"
mkdir -p ~/.config/context-intelligence
```

```yaml
# ~/.config/context-intelligence/server-config.yaml  —  LOCAL STATIC MODE

# --- Auth: static keystore (static is the default; shown for clarity) ---
auth_mode: static
api_keys:
  "<DATA_DIGEST>":          # sha256(DATA_TOKEN) from step 3 — the DIGEST, not the raw token
    id: owner               # the contributor id; surfaces as created_by on graph nodes

# --- Admin API (runtime identity-map management) ---
# A request bearing this RAW token may call /admin/*. Without it, /admin/* → 503.
# IMPORTANT: keep this DISTINCT from any data token in api_keys (see note below).
admin_api_key: "<ADMIN_TOKEN>"

# The server is headless (API-only). OpenAPI docs at http://localhost:8000/docs
# are always on and auth-exempt.

# --- Identity-map store files (LOCAL writable dir — default /data/... is not writable on a normal box) ---
api_keys_store_path: "/home/you/amplifier-context-intelligence-server-data-store/identity/api-keys.json"
entra_identities_store_path: "/home/you/amplifier-context-intelligence-server-data-store/identity/entra-identities.json"

# --- Neo4j (use bolt:// for Community Edition) ---
neo4j_url: "bolt://localhost:37687"          # NEO4J_BOLT_PORT from step 2
neo4j_browser_url: "http://localhost:37474"  # NEO4J_HTTP_PORT from step 2 (clickable link in the UI)
neo4j_user: neo4j
neo4j_password: "<your-strong-password>"

# --- Server bind + storage ---
server_host: 0.0.0.0
server_port: 8000
blob_path:   "/home/you/amplifier-context-intelligence-server-data-store/blobs"
log_path:    "/home/you/amplifier-context-intelligence-server-data-store/logs/server.jsonl"
queues_path: "/home/you/amplifier-context-intelligence-server-data-store/queues"
```

> **`created_by="admin"` caveat — why two tokens.** The admin key is recognised
> by the middleware **before** the data-identity resolver, so any request bearing
> the admin token is attributed `created_by="admin"`. If you reuse one token for
> both capture and `/admin/*`, your captured sessions are stamped `admin` instead
> of your real contributor id. **Use a distinct `admin_api_key` vs your data
> `api_keys`** to preserve per-user attribution.

**5. Run it** — directly for a quick check (or install it as a service via §5/§6):

```bash
AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE=$HOME/.config/context-intelligence/server-config.yaml \
  context-intelligence-server
```

**6. Verify**:

```bash
curl -sS http://localhost:8000/status | jq '.auth'
# → {"mode":"static","admin_api_enabled":true}
curl -sS http://localhost:8000/version
# → {"version":"6.0.0"}
curl -s -o /dev/null -w "%{http_code}\n" -X POST http://localhost:8000/events \
  -H "Content-Type: application/json" -d '{}'
# → 401   (auth is enforced)
```

Explore the API interactively at `http://localhost:8000/docs` (Swagger UI, always
on and unauthenticated); data endpoints still require the **DATA token** as
`Authorization: Bearer <token>`.

**7. Use the admin API** (static — the bearer is the **ADMIN token**):

```bash
# List current key entries (hash + contributor id; never raw keys):
curl -sS http://localhost:8000/admin/keys -H "Authorization: Bearer $ADMIN_TOKEN"

# Onboard a peer at runtime (register the HASH of their key, never the raw key):
PEER_HASH=$(printf '%s' "<peer-raw-key>" | sha256sum | cut -d' ' -f1)
curl -sS -X PUT "http://localhost:8000/admin/keys/$PEER_HASH" \
  -H "Authorization: Bearer $ADMIN_TOKEN" -H "Content-Type: application/json" \
  -d '{"id": "<contributor>"}'
```

Full onboarding/offboarding runbook and status-code matrix:
[identity-management.md](identity-management.md).

---

## 1. Prerequisites

### Install uv

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Ensure `~/.local/bin` is in your `PATH`:

```bash
export PATH="$HOME/.local/bin:$PATH"   # add to ~/.bashrc or ~/.zshrc
```

### Neo4j

The server needs **Neo4j 5.x** reachable over **Bolt**, with the **APOC**
procedures available (and **GDS** for graph-analytics features). Install and run
it locally per [local-development.md](local-development.md) §1. This service guide
does not require Docker for Neo4j.

---

## 2. Start Neo4j

Install and start **Neo4j 5.x** locally with **APOC** (and **GDS** if you use
graph-analytics features), following [local-development.md](local-development.md)
§1 (Neo4j Desktop, or a package/tarball install with the plugin JARs).
Authentication is **always required** — the server refuses to connect to an
unauthenticated Neo4j instance, so set an initial password:

```bash
# Adjust these three values before running
NEO4J_HTTP_PORT=37474      # browser UI  (standard would be 7474)
NEO4J_BOLT_PORT=37687      # bolt driver (standard would be 7687)
NEO4J_PASSWORD="<your-strong-password>"

DATA_DIR="$HOME/amplifier-context-intelligence-server-data-store"
mkdir -p "${DATA_DIR}/neo4j"

docker run -d \
  --name amplifier-context-intelligence-neo4j \
  --restart unless-stopped \
  -p ${NEO4J_HTTP_PORT}:7474 \
  -p ${NEO4J_BOLT_PORT}:7687 \
  -e NEO4J_AUTH=neo4j/${NEO4J_PASSWORD} \
  -e 'NEO4J_PLUGINS=["apoc","graph-data-science"]' \
  -e 'NEO4J_dbms_security_procedures_unrestricted=apoc.*,gds.*' \
  -v "${DATA_DIR}/neo4j:/data" \
  neo4j:5.26.22-community
```

> **APOC + GDS plugins:** `-e 'NEO4J_PLUGINS=["apoc","graph-data-science"]'`
> enables both. Neo4j 5.x auto-installs the bundled `apoc-core` jar at startup
> (from `/var/lib/neo4j/labs` into `/var/lib/neo4j/plugins`) with no network
> fetch required — it re-installs on every container start. GDS is NOT bundled in
> the image, so the installer resolves and downloads the Community build matching
> this Neo4j release from the official compatibility matrix (2.13.x for 5.26.x) —
> this DOES require network egress at container start. Set only
> `NEO4J_dbms_security_procedures_unrestricted=apoc.*,gds.*` (do NOT set an
> `allowlist` of `apoc.*,gds.*` — it would block the built-in `db.*`/`dbms.*`
> procedures). Verify with
> `docker exec amplifier-context-intelligence-neo4j cypher-shell -u neo4j -p "${NEO4J_PASSWORD}" "RETURN apoc.version();"`
> and `"RETURN gds.version();"`. See
> [local-development.md](local-development.md) §1 for the local-dev variant.
> Hosted AuraDB has APOC Core preinstalled.
>
> **Air-gapped hosts:** `NEO4J_PLUGINS=["apoc","graph-data-science"]` needs
> internet egress at startup for the GDS half (APOC alone works offline — it's
> bundled in the image at `/var/lib/neo4j/labs/`). For a host with no egress,
> build your own Neo4j image with the matching GDS jar baked into
> `/var/lib/neo4j/plugins/` at build time (the build step needs network; the
> resulting container runs egress-free), or serve the jar from an internal
> mirror. On a fully disconnected host, pre-load the base image too:
> `docker save neo4j:5.26.22-community -o neo4j.tar` on a connected machine, then
> `docker load -i neo4j.tar` on the air-gapped host.

**Wait for Neo4j to be ready** (usually 15–30 seconds), then verify APOC:

```bash
cypher-shell -u neo4j -p '<your-strong-password>' "RETURN apoc.version();"
```

> **Important:** use `bolt://` (not `neo4j://`) for the server connection URL.
> The routing protocol (`neo4j://`) fails on Community Edition single-node installs.
> Set `neo4j_url` to `bolt://localhost:<NEO4J_BOLT_PORT>` in your config.
>
> Neo4j exposes **two ports**: the bolt driver port used for all data operations,
> and the HTTP browser UI port used only for the Neo4j Browser web interface. Both
> must be configured separately — `neo4j_url` for the driver connection,
> `neo4j_browser_url` for the Neo4j Browser link surfaced in the `/status`
> response. Both are displayed verbatim from the config, so if Neo4j is on a
> remote machine, use that machine's hostname in both values.

---

## 3. Install the Server

```bash
uv tool install git+https://github.com/microsoft/amplifier-context-intelligence
```

One binary is placed at `~/.local/bin`:

| Binary | Purpose |
|--------|---------|
| `context-intelligence-server` | Runs the FastAPI server |

**Upgrade later:**

```bash
uv tool upgrade context-intelligence-server
```

---

## 4. Configuration

There is **no `init` subcommand.** Write `server-config.yaml` by hand from the
annotated template, then add authentication.

```bash
mkdir -p ~/.config/context-intelligence
curl -o ~/.config/context-intelligence/server-config.yaml \
  https://raw.githubusercontent.com/microsoft/amplifier-context-intelligence/main/server-config.example.yaml
```

Edit the file and set values for your machine — at minimum `neo4j_url` (use the
`bolt://` scheme and your `NEO4J_BOLT_PORT` from Step 2), `neo4j_browser_url`
(your `NEO4J_HTTP_PORT`, surfaced in the `/status` response), and
`neo4j_password`. Configuration keys are grouped into three categories below.

### Authentication

Set up an API token so the server requires `Authorization: Bearer <token>`.
Generate a token, derive its SHA-256 digest, and add an `api_keys` entry (the
file stores the **digest**; clients send the **raw token**):

```bash
TOKEN=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))")
DIGEST=$(python3 -c "import hashlib,sys; print(hashlib.sha256(sys.argv[1].encode()).hexdigest())" "$TOKEN")
printf 'api_keys:\n  "%s":\n    id: owner\n' "$DIGEST" >> ~/.config/context-intelligence/server-config.yaml
echo "API token (use in Step 7): $TOKEN"
```

**Copy the API token** — you need it in Step 7 (Amplifier settings), and it is not
recoverable from the config file afterward. The legacy single-key mode
(`api_key: "<secret>"`) also still works. Full guide — adding/revoking/rotating
peers, the empty-`{}` hard-error rule, and the raw-token-vs-digest guardrail — in
[managing-api-keys.md](managing-api-keys.md).

> Prefer a one-shot bootstrap? `python scripts/prime-local-config.py` generates the
> `api_keys` keystore and prints the token once, then writes a ready-to-use
> `server-config.yaml` — see [local-development.md](local-development.md) §2.

> **Microsoft Entra JWT auth.** Instead of pre-shared keys, the server also supports
> `auth_mode=entra`, where clients authenticate with Azure AD bearer tokens and the
> server maps each token's `oid` to a contributor. Set `auth_mode`, `azure_client_id`,
> `azure_tenant_id`, and `entra_identities` (see the table below). Full guide:
> [entra-auth-setup.md](entra-auth-setup.md).

> **Runtime identity-map management (no restart).** Adding or removing a key/identity
> by editing config requires a restart. To onboard/offboard at runtime, enable the
> admin API — set `admin_api_key` (static mode) or the `IdentityAdmin` App Role
> (entra mode) — and use the `/admin/*` endpoints. Full runbook:
> [identity-management.md](identity-management.md).

---

### Admin API and identity-map stores (the local-static essentials)

These cover the three things a local static-mode run usually needs turned on or
pointed somewhere writable. All are plain `server-config.yaml` keys (or the
matching `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_*` env var; env wins).

| Key | Example | Purpose |
|-----|---------|---------|
| `admin_api_key` | *(a raw admin token)* | **Enables the `/admin/*` runtime identity-map API in static mode.** A request whose bearer token equals this value is recognised as admin — the middleware checks it **before** the data keystore. Unset/empty → every `/admin/*` call returns **503**; a valid *data* key gets **403**. The admin key cannot be deleted or shadowed via the API (**409**). **Use a token DISTINCT from your data keys** (see caveat). Env: `…_ADMIN_API_KEY`. |
| `api_keys_store_path` | *(local writable path)* | Durable JSON file backing the static `sha256(key) → contributor` map that `/admin/keys` edits. **The default `/data/identity/api-keys.json` is not writable on a normal box — point it at your data store** (e.g. `…/identity/api-keys.json`). It is **seeded from `api_keys` on first boot**, then this file is the source of truth for runtime edits. |
| `entra_identities_store_path` | *(local writable path)* | The entra equivalent (`oid → contributor`), used only in `auth_mode=entra`. Same default-not-writable caveat; set it to a writable path even in static mode if you want a clean layout. |

> **`created_by="admin"` caveat — use two distinct tokens.** The admin key is
> recognised by the middleware **before** the data-identity resolver, so any
> request bearing the admin token is attributed `created_by="admin"`. The
> session-capture hook uses a *data* token; if you reuse that same token as the
> admin key, your captured sessions are stamped `admin` instead of your real
> contributor id. **Keep `admin_api_key` distinct from your data `api_keys`** to
> preserve per-user attribution.

Runtime onboarding/offboarding via `/admin/keys` (static) or `/admin/identities`
(entra), the full status-code matrix (401/403/409/422/503), and how the live map
stays fresh without a restart: [identity-management.md](identity-management.md).

---

### Server settings

| Key | Example | Purpose |
|-----|---------|---------|
| `server_host` | `0.0.0.0` | Bind address. `0.0.0.0` = all interfaces; `127.0.0.1` = localhost only |
| `server_port` | `8000` | Listen port |
| `log_level` | `INFO` | Verbosity (`DEBUG` / `INFO` / `WARNING` / `ERROR`) |
| `api_key` | *(your secret)* | Legacy single bearer token (folds to contributor id `owner`). All endpoints except `/status` and static routes require `Authorization: Bearer <value>`. The server verifies it as `sha256(token)`. |
| `api_keys` | *(map)* | Per-contributor keystore: `sha256_hex(token) -> {id: <contributor>}`. The file holds digests; clients send raw tokens. `api_keys: {}` is a hard startup error (omit/`null` to disable auth). See [managing-api-keys.md](managing-api-keys.md). |

### Neo4j settings

| Key | Example | Purpose |
|-----|---------|---------|
| `neo4j_url` | `bolt://localhost:37687` | Bolt/driver URL for all graph operations. Use `bolt://` scheme. Port must match `NEO4J_BOLT_PORT` from Step 2. **Surfaced verbatim in the `/status` response** — use the address reachable by the server process, which may differ from what your browser can reach. |
| `neo4j_browser_url` | `http://localhost:37474` | Neo4j Browser HTTP UI URL. Port must match `NEO4J_HTTP_PORT` from Step 2. **Surfaced verbatim in the `/status` response** (`neo4j_browser_url`). Use the address reachable from your browser — if Neo4j is on a remote machine this will be that machine's hostname or IP, not `localhost`. Never used for driver connections. |
| `neo4j_user` | `neo4j` | Auth username (legacy single-credential form). |
| `neo4j_password` | *(your password)* | Auth password. Always required — the server refuses an unauthenticated Neo4j. Must match the password you set on Neo4j (e.g. via `neo4j-admin dbms set-initial-password`). |

> **Optional — separate read/write credentials or URLs (two-client split).** The
> four flat keys above are the legacy single-credential form and keep working
> unchanged. The `/cypher` endpoint reads run through an internal
> **read** client; ingest and schema changes run through an **admin/write**
> client. To give them separate credentials (and optionally separate URLs, e.g.
> a read replica), replace the flat keys with a structured `neo4j:` block:
>
> ```yaml
> neo4j:
>   admin:
>     url: bolt://localhost:7687
>     username: neo4j
>     password: "<write-password>"
>     access_mode: WRITE        # MUST be WRITE
>   cypher_query:
>     url: bolt://localhost:7687   # or a read replica URL
>     username: reader
>     password: "<read-password>"
>     access_mode: READ         # MUST be READ — default is WRITE, so this is required
> ```
>
> When the `neo4j:` block is present both sub-clients are required, and
> `access_mode` is validated at startup (`admin` MUST be `WRITE`,
> `cypher_query` MUST be `READ`) — a wrong or missing `access_mode` is a hard
> boot failure. Full migration guide and the Community-vs-Enterprise enforcement
> caveat: [auth-troubleshooting-and-upgrades.md](auth-troubleshooting-and-upgrades.md) § "Neo4j two-client split".

### Storage settings

| Key | Example | Purpose |
|-----|---------|---------|
| `blob_path` | `~/amplifier-context-intelligence-server-data-store/blobs` | Event payload storage (binary blobs from tool outputs) |
| `log_path` | `~/amplifier-context-intelligence-server-data-store/logs/server.jsonl` | Structured JSONL server log |
| `queues_path` | `~/amplifier-context-intelligence-server-data-store/queues` | Durable per-session ingest queues (`.log`/`.offset`/`.dead.jsonl`) |

### Create storage directories

```bash
DATA_DIR="$HOME/amplifier-context-intelligence-server-data-store"
mkdir -p "${DATA_DIR}/blobs" "${DATA_DIR}/logs" "${DATA_DIR}/queues"
```

---

## 5. Linux — systemd User Service

> ⚠️ **Single-instance invariant — run the server ONLY via this service.**
> Exactly **one** `context-intelligence-server` process may serve a given
> `(server_port, neo4j_url)` at a time. Once it is installed as a service, start,
> stop, and restart it **only** through `systemctl` (or launchd on macOS). Do
> **not** also launch it by hand — `uv run uvicorn …`, `python -m uvicorn …`, or a
> binary from a stray venv (`/opt/…`, a leftover `.venv`). The `--reload` dev
> command in the README is for throwaway local boxes only.
>
> **Why it matters:** extra copies silently bind-race the same port and share the
> same Neo4j + queue store. The kernel gives the socket to one; the rest keep
> running as **orphans on possibly-older code**, still draining events into the
> same graph. Real incidents this caused: an old orphan re-stamping
> `created_by="admin"` *after* an auth fix was deployed (corrupting attribution),
> and the event hook getting spurious **HTTP 401** floods from an orphan with a
> stale keystore. When behavior contradicts the code you just deployed, the
> question is **"which running copy answered this?"** — `ss` (who holds the
> socket) beats `ps` (who's merely alive).
>
> **Restart safely and verify a single listener:**
> ```bash
> sudo systemctl restart context-intelligence-server.service   # or: systemctl --user restart …
> ss -ltnp | grep ':8000'          # expect ONE pid (+ its worker), nothing else
> ps -eo pid,args | grep 'context_intelligence_server.main:asgi_app' | grep -v grep   # expect NONE outside the service
> # kill -TERM <pid> any orphan found, then re-check; SIGKILL only if it lingers
> curl -s -o /dev/null -w '%{http_code}\n' -X POST http://localhost:8000/events -d '{}'   # → 401 = up + auth enforced
> ```
> If orphans exist, fix whatever launched them (a manual `uv run`, an old deploy
> script, a duplicate unit) so this service is the **only** entry point.

### Create the unit file

```bash
mkdir -p ~/.config/systemd/user
cat > ~/.config/systemd/user/context-intelligence-server.service << 'EOF'
[Unit]
Description=Context Intelligence Server
After=network.target

[Service]
Type=simple
ExecStart=%h/.local/bin/context-intelligence-server
Environment=AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE=%h/.config/context-intelligence/server-config.yaml
Restart=on-failure
RestartSec=5s
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=default.target
EOF
```

`%h` is systemd's specifier for the user home directory — no hardcoded paths.

### Enable and start

```bash
systemctl --user daemon-reload
systemctl --user enable context-intelligence-server
systemctl --user start context-intelligence-server
```

### Auto-start on boot

```bash
loginctl enable-linger $USER
```

### Check status and logs

```bash
systemctl --user status context-intelligence-server
journalctl --user -u context-intelligence-server -f
```

---

## 6. macOS — launchd User Agent

The repository ships a plist template at
`service/macos/com.context-intelligence.server.plist.template`. Expand it
with `sed` (launchd does not expand `~` in paths):

```bash
mkdir -p ~/Library/LaunchAgents
sed "s|HOME_DIR|$HOME|g" \
  /path/to/repo/service/macos/com.context-intelligence.server.plist.template \
  > ~/Library/LaunchAgents/com.context-intelligence.server.plist

launchctl load ~/Library/LaunchAgents/com.context-intelligence.server.plist
```

```bash
# Check status
launchctl list | grep context-intelligence

# Stop
launchctl unload ~/Library/LaunchAgents/com.context-intelligence.server.plist

# Logs
tail -f ~/.local/share/context-intelligence/logs/server.stdout.log
tail -f ~/.local/share/context-intelligence/logs/server.stderr.log
```

---

## 7. Install the Amplifier Bundle and Configure settings.yaml

The `amplifier-bundle-context-intelligence` bundle hooks into the Amplifier
CLI and forwards all session events to the server in real time.

### Add the bundle

Add it to the `app` list in `~/.amplifier/settings.yaml`:

```yaml
bundle:
  app:
    - git+https://github.com/microsoft/amplifier-bundle-context-intelligence@main
```

Or use the CLI:

```bash
amplifier bundle add git+https://github.com/microsoft/amplifier-bundle-context-intelligence@main
```

### Configure the hook

Add the server URL and the API token (the raw token you saved in Step 4) to
`~/.amplifier/settings.yaml`:

```yaml
overrides:
  hook-context-intelligence:
    config:
      context_intelligence_server_url: "http://localhost:8000"
      context_intelligence_api_key: "<api-key-from-step-4>"
```

### What the config keys do

| Key | What it does |
|-----|-------------|
| `context_intelligence_server_url` | URL of the running server. The hook POSTs all session events here. |
| `context_intelligence_api_key` | Raw bearer token. The server verifies it as `sha256(token)` against its keystore (a legacy `api_key`, or an `api_keys` entry). If this token is missing or wrong, the hook logs a warning once and disables HTTP dispatch for the session. |

### Complete `~/.amplifier/settings.yaml` example

```yaml
bundle:
  active: <your-active-bundle>
  app:
    - git+https://github.com/microsoft/amplifier-bundle-context-intelligence@main
    # ... other bundles ...

overrides:
  hook-context-intelligence:
    config:
      context_intelligence_server_url: "http://localhost:8000"
      context_intelligence_api_key: "<api-key-from-step-4>"
```

---

## 8. Verification

```bash
# Health check (always unauthenticated)
curl http://localhost:8000/status
# → {"status":"ok","neo4j_connected":true,"neo4j_query_connected":true,"neo4j_url":"bolt://localhost:37687","neo4j_browser_url":"http://localhost:37474",...}
#
# Both neo4j_url and neo4j_browser_url are read verbatim from server-config.yaml.
# If Neo4j is on a remote host the response will show those remote addresses.

# Confirm auth is enforced — must return 401
curl -s -o /dev/null -w "%{http_code}" \
  -X POST http://localhost:8000/events \
  -H "Content-Type: application/json" -d '{}'
# → 401
```

If `neo4j_connected` is `false`, check:
1. Neo4j is running (e.g. `neo4j status`, or check Neo4j Desktop)
2. Bolt port in `server-config.yaml` matches the port Neo4j listens on (and the URL uses the `bolt://` scheme)
3. Neo4j password in config matches the password you set on Neo4j

**API docs:** the server is headless (no browser dashboard). Explore the API
interactively at `http://localhost:8000/docs` (Swagger UI) — always on and
unauthenticated. The raw spec is at `http://localhost:8000/openapi.json`. Data
endpoints still require the API token from Step 4 as `Authorization: Bearer`.

---

## 9. Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `command not found: context-intelligence-server` | `~/.local/bin` not in `PATH` | Add `export PATH="$HOME/.local/bin:$PATH"` to your shell profile |
| `neo4j_connected: false` | Bolt port mismatch or wrong scheme | Set `neo4j_url` to `bolt://localhost:<NEO4J_BOLT_PORT>` in config — must use `bolt://` not `neo4j://` |
| `neo4j_connected: false` (connection refused) | Neo4j not running | Check with `neo4j status` (or Neo4j Desktop); start it with `neo4j start` |
| `neo4j_query_connected: false` | The read/cypher_query driver is not connected (ingest via the admin driver may still work) | Check the cypher_query URL/credentials and, if using a cluster/read-replica, its reachability |
| `neo4j_browser_url` in `/status` points at the wrong host/port | `neo4j_browser_url` misconfigured | Update `neo4j_browser_url` in `server-config.yaml` to the address reachable from your browser — if Neo4j is remote, use the remote hostname, not `localhost` |
| Service starts then immediately stops | Config file missing or bad path | `journalctl --user -u context-intelligence-server` to see the error |
| `Permission denied` on blob/log path | Directories don't exist | `mkdir -p` the paths listed in your config |
| Port 8000 already in use | Conflict with another process | Change `server_port` in config and update the systemd unit |
| Linux: service doesn't start on boot | Lingering not enabled | `loginctl enable-linger $USER` |
| macOS: plist loaded but service not running | launchd silently failed | Check `server.stderr.log` for startup errors |
| Events stop dispatching, circuit breaker tripped | `context_intelligence_api_key` missing from `~/.amplifier/settings.yaml` | Add `context_intelligence_api_key: "<key>"` under `overrides.hook-context-intelligence.config` |
| Data attributed to the wrong `created_by`, client gets **HTTP 401** floods, or behavior contradicts the deployed code | **Multiple server instances** running at once (an orphan from a manual `uv run`/old deploy racing the service on the same port + Neo4j — see the single-instance invariant at the top of §5) | `ss -ltnp \| grep ':8000'` and `ps -eo pid,args \| grep asgi_app \| grep -v grep`; `kill -TERM` every process **not** owned by the service manager, then `sudo systemctl restart context-intelligence-server.service` and re-verify a single listener. Fix whatever launched the extra copies so the service is the only entry point. |

---

## 10. HTTPS / TLS

The server speaks plain HTTP; terminate TLS in front of it. In production, the
[Azure Container Apps](azure-deployment.md) platform edge handles HTTPS
automatically. For a self-hosted run that needs TLS, put your own reverse proxy
(nginx, Caddy, etc.) in front of the server and point it at
`http://localhost:8000`, then update `settings.yaml` with the HTTPS URL — same
pattern as the [Azure deployment guide](azure-deployment.md).
