# Running as a System Service

How to run the `context-intelligence-server` as a persistent background
service on **Linux (systemd)** or **macOS (launchd)** — auto-starting on
boot and restarting on crash.

> **Scope:** This guide covers the Python server only.
> Neo4j is assumed to be already running separately
> (Docker, Homebrew service, or native install).

---

## 1. Prerequisites

### Install uv

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### Install the server

```bash
uv tool install git+https://github.com/colombod/amplifier-context-intelligence
```

This places a `context-intelligence-server` binary at `~/.local/bin/context-intelligence-server`.
Ensure `~/.local/bin` is in your `PATH` (e.g. add `export PATH="$HOME/.local/bin:$PATH"` to your shell profile).

### Upgrade later

```bash
uv tool upgrade context-intelligence-server
```

---

## 2. Configuration

### Option A — Generate config with `context-intelligence-server-init` (recommended)

```bash
context-intelligence-server-init \
  --neo4j-url neo4j://localhost:7687 \
  --neo4j-user neo4j
```

You will be prompted for the Neo4j password. The command writes `server-config.yaml` to `~/.config/context-intelligence/` with all required fields including a generated `api_key`.

Copy the printed API key — you will need it in your bundle config as `context_intelligence_api_key`.

### Option B — Manual config (advanced)

```bash
mkdir -p ~/.config/context-intelligence
curl -o ~/.config/context-intelligence/server-config.yaml \
  https://raw.githubusercontent.com/colombod/amplifier-context-intelligence/main/server-config.example.yaml
```

Edit `~/.config/context-intelligence/server-config.yaml` and set values
appropriate for your machine. The configuration keys are grouped into
three categories:

### Server settings

| Key | Example | Purpose |
|-----|---------|---------|
| `server_host` | `127.0.0.1` | Bind address (`0.0.0.0` to expose on network) |
| `server_port` | `8000` | Listen port |
| `log_level` | `INFO` | Verbosity (`DEBUG` / `INFO` / `WARNING` / `ERROR`) |
| `api_key` | *(generated)* | Bearer token for API auth. All endpoints except `/status` and static routes require `Authorization: Bearer <value>`. Generate with `context-intelligence-server-init`. |

### Neo4j settings

| Key | Example | Purpose |
|-----|---------|---------|
| `neo4j_url` | `neo4j://localhost:7687` | Bolt connection URL |
| `neo4j_user` | `neo4j` | Auth username |
| `neo4j_password` | `password` | Auth password. Always required for Docker deployments. For local dev-only Neo4j with `NEO4J_AUTH=none`, may be left empty. |

### Storage settings

| Key | Example | Purpose |
|-----|---------|---------|
| `blob_path` | `~/.local/share/context-intelligence/blobs` | Event payload storage |
| `log_path` | `~/.local/share/context-intelligence/logs/server.jsonl` | Structured log file |
| `cursor_path` | `~/.local/share/context-intelligence/cursors` | Session cursor state |

> **Note:** The standalone service setup persists cursors by default via
> `cursor_path`. This is an improvement over the Docker Compose setup
> where cursors are ephemeral.

### Create storage directories

```bash
mkdir -p ~/.local/share/context-intelligence/{blobs,logs,cursors}
```

---

## 3. Linux — systemd User Service

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

`%h` is systemd's specifier for the user's home directory — no hardcoded
paths needed.

### Enable and start

```bash
systemctl --user daemon-reload
systemctl --user enable context-intelligence-server
systemctl --user start context-intelligence-server
```

### Check status and logs

```bash
systemctl --user status context-intelligence-server
journalctl --user -u context-intelligence-server -f
```

### Auto-start on boot

By default, systemd user services only run when a login session is active.
To start the service on system boot (even without logging in):

```bash
loginctl enable-linger $USER
```

---

## 4. macOS — launchd User Agent

### Install from template

The repository ships a plist template at
`service/macos/com.context-intelligence.server.plist.template` with
`HOME_DIR` as a placeholder token. Expand it at install time with `sed`:

```bash
mkdir -p ~/Library/LaunchAgents
sed "s|HOME_DIR|$HOME|g" \
  /path/to/repo/service/macos/com.context-intelligence.server.plist.template \
  > ~/Library/LaunchAgents/com.context-intelligence.server.plist
```

Replace `/path/to/repo` with the actual path to your clone of the
`amplifier-context-intelligence` repository.

> **Why not `~`?** launchd plist files do **not** expand `~` or shell
> variables — every path must be absolute. The `sed` substitution handles
> this.

### Load and start

```bash
launchctl load ~/Library/LaunchAgents/com.context-intelligence.server.plist
```

### Check status

```bash
launchctl list | grep context-intelligence
```

### Stop and unload

```bash
launchctl unload ~/Library/LaunchAgents/com.context-intelligence.server.plist
```

### View logs

```bash
tail -f ~/.local/share/context-intelligence/logs/server.stdout.log
tail -f ~/.local/share/context-intelligence/logs/server.stderr.log
```

---

## 5. Verification & Troubleshooting

### Health check (same on both platforms)

```bash
curl http://localhost:8000/status
# → {"status": "ok", ...}

# Open the dashboard
open http://localhost:8000
```

### Log locations

| Platform | How to view logs |
|----------|-----------------|
| Linux | `journalctl --user -u context-intelligence-server -f` |
| macOS | `tail -f ~/.local/share/context-intelligence/logs/server.stdout.log` |

### Common issues

| Symptom | Cause | Fix |
|---------|-------|-----|
| `command not found: context-intelligence-server` | `~/.local/bin` not in `PATH` | Add `export PATH="$HOME/.local/bin:$PATH"` to your shell profile |
| Service starts then immediately stops | Can't reach Neo4j | Check `neo4j_url` in config; verify Neo4j is running |
| `Permission denied` on blob/log path | Directories don't exist | `mkdir -p ~/.local/share/context-intelligence/{blobs,logs,cursors}` |
| Port 8000 already in use | Conflict with another process | Change `server_port` in config, update unit/plist accordingly |
| Linux: service doesn't start on boot | Lingering not enabled | `loginctl enable-linger $USER` |
| macOS: plist loaded but service not running | launchd silently failed | Check `server.stderr.log` for startup errors |
| Events stop dispatching, circuit breaker tripped | `api_key` set on server but `context_intelligence_api_key` missing from bundle config | Add `context_intelligence_api_key: "<your-key>"` to bundle config |
| Dashboard shows "Enter your API key" and won't load | API key prompt is active | Open `server-config.yaml`, find `api_key:`, paste it into the dashboard prompt |

