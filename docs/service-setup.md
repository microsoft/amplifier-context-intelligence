# Running as a System Service

How to run `context-intelligence-server` and `Neo4j` as persistent services on
**Linux (systemd)** or **macOS (launchd)**, with full authentication, and
integrated with the Amplifier CLI so sessions are automatically captured.

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

### Install Docker

Neo4j runs as a Docker container. Install [Docker Desktop](https://docs.docker.com/desktop/)
or Docker Engine before continuing.

---

## 2. Start Neo4j

Run Neo4j as a standalone Docker container. Use **non-standard ports** to
avoid conflicts with any existing Neo4j installation. Authentication is
**always required** — the server refuses to connect to an unauthenticated
Neo4j instance.

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
  -e 'NEO4J_PLUGINS=["apoc"]' \
  -v "${DATA_DIR}/neo4j:/data" \
  neo4j:5.26.22-community
```

> **APOC plugin:** `-e 'NEO4J_PLUGINS=["apoc"]'` enables the APOC plugin, matching
> the Docker Compose stack. Neo4j 5.x auto-installs the bundled `apoc-core` jar at
> startup (from `/var/lib/neo4j/labs` into `/var/lib/neo4j/plugins`) and applies
> APOC's default config — no volume mount or manual jar download is required, and
> it re-installs on every container start. Verify with
> `docker exec amplifier-context-intelligence-neo4j cypher-shell -u neo4j -p "${NEO4J_PASSWORD}" "RETURN apoc.version();"`.
> See the README's "Neo4j Plugins (APOC)" section for details. Hosted AuraDB has
> APOC Core preinstalled.
>
> **Air-gapped hosts:** `NEO4J_PLUGINS=["apoc"]` installs APOC Core from a jar
> bundled *inside* the image (`/var/lib/neo4j/labs/`) — no internet download — so
> the line above already works offline. For an air-tight guarantee that skips the
> installer entirely, build a Neo4j image with the jar baked in using the repo's
> `neo4j.Dockerfile` + `docker-compose.airgap.yml`
> (`docker compose -f docker-compose.yml -f docker-compose.airgap.yml up -d --build`).
> On a fully disconnected host, also pre-load the base image first:
> `docker save neo4j:5.26.22-community -o neo4j.tar` on a connected machine, then
> `docker load -i neo4j.tar` on the air-gapped host (or use an internal registry
> mirror).

**Wait for Neo4j to be ready** (usually 15–30 seconds):

```bash
until curl -s -o /dev/null -w "%{http_code}" \
    -u "neo4j:${NEO4J_PASSWORD}" \
    http://localhost:${NEO4J_HTTP_PORT}/db/neo4j/tx \
    -H "Content-Type: application/json" \
    -d '{"statements":[{"statement":"RETURN 1"}]}' | grep -q 201; do
  echo "Waiting for Neo4j..."; sleep 3
done
echo "Neo4j ready."
```

> **Important:** use `bolt://` (not `neo4j://`) for the server connection URL.
> The routing protocol (`neo4j://`) fails on Community Edition single-node installs.
> Set `neo4j_url` to `bolt://localhost:${NEO4J_BOLT_PORT}` in your config.
>
> Neo4j exposes **two ports**: the bolt driver port (`NEO4J_BOLT_PORT`) used for
> all data operations, and the HTTP browser UI port (`NEO4J_HTTP_PORT`) used only
> for the Neo4j Browser web interface. Both must be configured separately —
> `neo4j_url` for the driver connection, `neo4j_browser_url` for the browser link
> shown in the web UI. Both are displayed verbatim from the config, so if Neo4j
> is on a remote machine, use that machine's hostname in both values.

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
(your `NEO4J_HTTP_PORT`, displayed as a clickable link in the web UI), and
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

> If you run under Docker instead, `./start.sh` (or the container entrypoint)
> bootstraps the `api_keys` keystore and prints the token once — no manual steps.

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
| `neo4j_url` | `bolt://localhost:37687` | Bolt/driver URL for all graph operations. Use `bolt://` scheme. Port must match `NEO4J_BOLT_PORT` from Step 2. **Displayed verbatim in the web UI** — use the address reachable by the server process, which may differ from what your browser can reach. |
| `neo4j_browser_url` | `http://localhost:37474` | Neo4j Browser HTTP UI URL. Port must match `NEO4J_HTTP_PORT` from Step 2. **Displayed verbatim as a clickable link in the web UI.** Use the address reachable from your browser — if Neo4j is on a remote machine this will be that machine's hostname or IP, not `localhost`. Never used for driver connections. |
| `neo4j_user` | `neo4j` | Auth username |
| `neo4j_password` | *(your password)* | Auth password. Always required for Docker deployments. Must match the password passed to `NEO4J_AUTH` when the container was created. |

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
# → {"status":"ok","neo4j_connected":true,"neo4j_url":"bolt://localhost:37687","neo4j_browser_url":"http://localhost:37474",...}
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
1. Container is running: `docker ps | grep amplifier-context-intelligence-neo4j`
2. Bolt port in `server-config.yaml` matches the port exposed by Docker (`NEO4J_BOLT_PORT`)
3. Neo4j password in config matches what was passed to `NEO4J_AUTH` when creating the container

**Dashboard:** open `http://localhost:8000` — enter the API key from Step 4
when prompted. If the prompt does not appear, hard-refresh (Ctrl+Shift+R) to
bypass the browser cache.

---

## 9. Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `command not found: context-intelligence-server` | `~/.local/bin` not in `PATH` | Add `export PATH="$HOME/.local/bin:$PATH"` to your shell profile |
| `neo4j_connected: false` | Bolt port mismatch or wrong scheme | Set `neo4j_url` to `bolt://localhost:<NEO4J_BOLT_PORT>` in config — must use `bolt://` not `neo4j://` |
| `neo4j_connected: false` (connection refused) | Neo4j container not running | `docker ps` to check; `docker start amplifier-context-intelligence-neo4j` to restart |
| Dashboard "Neo4j Browser" link doesn't work | `neo4j_browser_url` has wrong host/port | Update `neo4j_browser_url` in `server-config.yaml` to the address reachable from your browser — if Neo4j is remote, use the remote hostname, not `localhost` |
| Service starts then immediately stops | Config file missing or bad path | `journalctl --user -u context-intelligence-server` to see the error |
| `Permission denied` on blob/log path | Directories don't exist | `mkdir -p` the paths listed in your config |
| Port 8000 already in use | Conflict with another process | Change `server_port` in config and update the systemd unit |
| Linux: service doesn't start on boot | Lingering not enabled | `loginctl enable-linger $USER` |
| macOS: plist loaded but service not running | launchd silently failed | Check `server.stderr.log` for startup errors |
| Events stop dispatching, circuit breaker tripped | `context_intelligence_api_key` missing from `~/.amplifier/settings.yaml` | Add `context_intelligence_api_key: "<key>"` under `overrides.hook-context-intelligence.config` |
| Dashboard shows "Enter your API key" and won't load | API key prompt is active | Paste the raw API token you saved at setup into the prompt. It is not in `server-config.yaml` (which holds only the digest under `api_keys`); if lost, rotate it per [managing-api-keys.md](managing-api-keys.md) |

---

## 10. Self-Hosted HTTPS with Caddy (Local / Dev Only)

> **Scope:** This section covers local runs and development cycles only — for users who need HTTPS locally or on a self-hosted VM outside Azure. Production deployments use [Azure Container Apps](azure-deployment.md) which handles TLS automatically. The docker-compose setup exists to support local runs and dev cycles, not production hosting.

### Why Caddy and Not nginx

Caddy issues and renews Let's Encrypt certificates automatically — no certbot sidecar, no cron job, no renewal hook. Compare:

- **nginx**: requires 2 containers (nginx + certbot), a shared volume, a renewal cron job, and an nginx reload hook — 40+ lines of configuration
- **Caddy**: 3-line Caddyfile, done

Additional Caddy advantages:

- HTTP→HTTPS redirect on by default
- TLS 1.2+ and modern cipher suites out of the box
- One addition to docker-compose, zero cert management overhead

nginx remains a valid choice for teams with existing nginx expertise, but carries the certbot-sidecar overhead described above.

### Implementation

Drop a `docker-compose.override.yml` alongside the existing `docker-compose.yml` — no changes to the main compose file are required.

**`docker-compose.override.yml`**

```yaml
services:
  caddy:
    image: caddy:2-alpine
    restart: unless-stopped
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./Caddyfile:/etc/caddy/Caddyfile:ro
      - caddy_data:/data
      - caddy_config:/config
    depends_on:
      - context-intelligence-server
    networks:
      - context-intelligence

volumes:
  caddy_data:
  caddy_config:
```

**`Caddyfile`** (place alongside `docker-compose.yml`):

```
your-domain.example.com {
    reverse_proxy context-intelligence-server:8000
}
```

Start the stack:

```bash
docker compose -f docker-compose.yml -f docker-compose.override.yml up -d
```

Caddy fetches and renews the Let's Encrypt certificate automatically.

---

**Local HTTPS without a domain (testing only)**

For local testing where no public domain is available, use Caddy's internal CA:

**`Caddyfile`** (local testing):

```
localhost {
    tls internal
    reverse_proxy context-intelligence-server:8000
}
```

Caddy generates a local CA stored in `caddy_data`. To trust it on the host:

```bash
docker compose -f docker-compose.yml -f docker-compose.override.yml exec caddy caddy trust
```

> **Note:** This is local development only — not suitable for production.

---

Finally, update `settings.yaml` with the HTTPS URL — same pattern as the [Azure deployment guide](azure-deployment.md).
