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
  -v "${DATA_DIR}/neo4j:/data" \
  neo4j:5.26.22-community
```

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
| `context-intelligence-server` | Runs the FastAPI server; also accepts `init` subcommand for first-run configuration |

**Upgrade later:**

```bash
uv tool upgrade context-intelligence-server
```

---

## 4. Configuration

### Option A — Generate config with `context-intelligence-server init` (recommended)

Run `context-intelligence-server init` **once** to generate the server config
with all required settings. Do not run it again after the server is in use
— it regenerates `api_key`, which must then be updated everywhere.

```bash
DATA_DIR="$HOME/amplifier-context-intelligence-server-data-store"
mkdir -p "${DATA_DIR}/blobs" "${DATA_DIR}/logs" "${DATA_DIR}/cursors"

context-intelligence-server init \
  --config-path       ~/.config/context-intelligence/server-config.yaml \
  --neo4j-url         bolt://localhost:37687 \
  --neo4j-browser-url http://localhost:37474 \
  --neo4j-user        neo4j \
  --neo4j-password    "<your-neo4j-password>" \
  --blob-path         "${DATA_DIR}/blobs" \
  --log-path          "${DATA_DIR}/logs/server.jsonl" \
  --cursor-path       "${DATA_DIR}/cursors" \
  --server-host       0.0.0.0 \
  --server-port       8000
```

Replace `37687` with your `NEO4J_BOLT_PORT` and `37474` with your `NEO4J_HTTP_PORT` from Step 2.

The command prints:

```
Config written to: /home/<you>/.config/context-intelligence/server-config.yaml
API key: <generated-token>
```

**Copy the API key** — you need it in Step 7 (Amplifier settings).

`--neo4j-url` must use the `bolt://` scheme and the bolt port chosen in Step 2.
`--neo4j-browser-url` is the HTTP browser UI address — it is displayed verbatim
as a clickable link in the web UI and never used for driver connections.
If Neo4j runs on a remote machine, provide its hostname in both values.
All parameters and their defaults are described in the settings tables below.

### Option B — Manual config (advanced)

```bash
mkdir -p ~/.config/context-intelligence
curl -o ~/.config/context-intelligence/server-config.yaml \
  https://raw.githubusercontent.com/microsoft/amplifier-context-intelligence/main/server-config.example.yaml
```

Edit the file and set values for your machine. Configuration keys are
grouped into three categories below.

---

### Server settings

| Key | Example | Purpose |
|-----|---------|---------|
| `server_host` | `0.0.0.0` | Bind address. `0.0.0.0` = all interfaces; `127.0.0.1` = localhost only |
| `server_port` | `8000` | Listen port |
| `log_level` | `INFO` | Verbosity (`DEBUG` / `INFO` / `WARNING` / `ERROR`) |
| `api_key` | *(generated)* | Bearer token for API auth. All endpoints except `/status` and static routes require `Authorization: Bearer <value>`. Generate with `context-intelligence-server init`. |

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
| `cursor_path` | `~/amplifier-context-intelligence-server-data-store/cursors` | Session cursor state (enables resumption of event replay) |

### Create storage directories

```bash
DATA_DIR="$HOME/amplifier-context-intelligence-server-data-store"
mkdir -p "${DATA_DIR}/blobs" "${DATA_DIR}/logs" "${DATA_DIR}/cursors"
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

Add the server URL and the API key (printed by `context-intelligence-server init`
in Step 4) to `~/.amplifier/settings.yaml`:

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
| `context_intelligence_api_key` | Bearer token. Must match `api_key` in `server-config.yaml`. If this key is missing or wrong, the hook logs a warning once and disables HTTP dispatch for the session. |

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
| Dashboard shows "Enter your API key" and won't load | API key prompt is active | Open `server-config.yaml`, find `api_key:`, paste it into the dashboard prompt |
