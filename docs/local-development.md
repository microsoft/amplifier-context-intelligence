# Running Locally

This is the canonical guide for running the Context Intelligence Server on your
own machine. It covers running **Neo4j** (with the **APOC** and **GDS** plugins),
**priming API keys**, and starting the server.

Neo4j runs in a **Docker container** (§1) — the easiest and recommended path for a
local / private / dev server. The repo does not *ship* a Docker Compose file or a
Neo4j Dockerfile; you run the container from a documented `docker run` command, so
there is no maintained container artifact to drift. (The only Dockerfile in the
repo is the server's S360-compliant shipping image — see
[azure-deployment.md](azure-deployment.md).)

Two ways to drive it:

- **A. Ask Amplifier to set it up** — paste the prompts in
  [§4](#4-ask-amplifier-to-set-it-up) and let Amplifier drive the steps below.
- **B. Do it by hand** — follow §1–§3.

Prerequisites: **Python 3.11+**, [uv](https://github.com/astral-sh/uv), and
**Docker** (Neo4j runs in a container — see §1).

---

## 1. Install and run Neo4j (with APOC + GDS)

The server needs **Neo4j 5.x** reachable over **Bolt** at `bolt://localhost:7687`,
with the **APOC** procedures available (and **GDS** if you use graph-analytics
features). Neo4j **Community Edition** is fine for local dev — it has a single
account (no multi-user / RBAC), which is exactly what the server's local config
expects.

Run Neo4j in a **Docker container** — this is the supported local path. The
official image **auto-installs and configures APOC and GDS** at startup (versions
matched to the image, so there's nothing to version-juggle):

```bash
docker run -d --name context-intelligence-neo4j \
  --restart unless-stopped \
  -p 7474:7474 -p 7687:7687 \
  -e NEO4J_AUTH=neo4j/<neo4j-password> \
  -e NEO4J_PLUGINS='["apoc","graph-data-science"]' \
  -e NEO4J_dbms_security_procedures_unrestricted='apoc.*,gds.*' \
  -v "$HOME/.context-intelligence-neo4j/data:/data" \
  neo4j:5.26.22-community
```

- `NEO4J_PLUGINS` makes the image install the plugins at startup, and
  `NEO4J_dbms_security_procedures_unrestricted='apoc.*,gds.*'` allows their
  procedures to load. **Set `unrestricted` only** — do NOT set an `allowlist` of
  `apoc.*,gds.*`, which would block the built-in `db.*`/`dbms.*` procedures the
  server needs. APOC is bundled in the image; **GDS is fetched on first start**
  (needs internet once) — the installer resolves the GDS Community build matching
  the Neo4j version per the
  [compatibility matrix](https://neo4j.com/docs/graph-data-science/current/installation/supported-neo4j-versions/)
  (GDS 2.13.x for Neo4j 5.26.x).
- The `-v …:/data` volume persists the graph across container restarts.
- Reachable at `bolt://localhost:7687`; browser UI at `http://localhost:7474`.
- Manage it with `docker stop/start context-intelligence-neo4j`.
- **Auto-restart on boot:** `--restart unless-stopped` restarts the container
  automatically after a crash or host reboot (unless you explicitly `docker stop`
  it). This only works if the **Docker daemon itself starts on boot** — enable it
  with `sudo systemctl enable --now docker` (Linux) or turn on "Start Docker
  Desktop when you log in" (Docker Desktop). To change the policy on an
  already-running container: `docker update --restart unless-stopped context-intelligence-neo4j`.

> This is a **documented `docker run` command, not a shipped Docker file** — there
> is no Compose file or Neo4j Dockerfile to maintain in the repo (the only
> Dockerfile is the server's shipping image; see
> [azure-deployment.md](azure-deployment.md)).

### Verify Neo4j
Open the browser UI at `http://localhost:7474`, or with `cypher-shell`:
```cypher
RETURN apoc.version();   -- APOC loaded
RETURN gds.version();    -- GDS loaded
```

> **Use `bolt://`, not `neo4j://`.** The `neo4j://` routing scheme expects a
> cluster and **fails against a Community single-node** install. The server's
> local config uses `bolt://localhost:7687`.

---

## 2. Prime API keys and local config

The server has no `init` subcommand — a helper script generates an API token and
writes a ready-to-use `server-config.yaml` (with a local writable data
directory). This replaces the credential bootstrap the old Docker entrypoint did.

```bash
# From the repo root. Use the Neo4j password you set in step 1.
python scripts/prime-local-config.py --neo4j-password '<neo4j-password>'
```

It prints your **API token once** (only its SHA-256 digest is stored — the server
authenticates by digest) and creates:

- `server-config.yaml` — Neo4j connection (`bolt://localhost:7687`), the API-key
  digest, and local storage paths.
- `./.context-intelligence-data/` — `blobs/`, `queues/`, `logs/`, `identity/`.

Useful flags: `--data-dir <path>` (where blobs/queues/logs live),
`--config-path <path>`, `--server-host` / `--server-port`, `--force` (overwrite an
existing config). Pass the **same password** you set in the container's
`NEO4J_AUTH` (§1). If you omit `--neo4j-password`, the script generates one and
prints it — then use that value as `NEO4J_AUTH=neo4j/<password>` when you start
the Neo4j container.

Save the API token — the Amplifier hook uses it as
`context_intelligence_api_key` to send events to the server.

---

## 3. Run the server

Point the server at the generated config and start it with uv:

```bash
export AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE="$(pwd)/server-config.yaml"
uv sync
uv run uvicorn context_intelligence_server.main:app --host 127.0.0.1 --port 8000
```

> `uvicorn --reload` is for **local dev only**. For a persistent/shared run, use
> the installed `context-intelligence-server` entry point (gunicorn + a single
> UvicornWorker) under a service manager — see
> [service-setup.md](service-setup.md).

Verify it's up:
```bash
curl -s http://127.0.0.1:8000/status
```
Then open the dashboard at `http://127.0.0.1:8000/` (if `web_ui_enabled`).

**Config resolution** is `env > server-config.yaml > built-in defaults`. Note the
built-in defaults assume a container (`neo4j://neo4j:7687`, `/data/...` paths);
the generated `server-config.yaml` overrides them for local use — don't rely on
the bare defaults.

---

## 4. Ask Amplifier to set it up

Prefer to let Amplifier drive it? Paste these prompts one at a time.

**Run Neo4j with APOC + GDS (Docker):**
```
Start Neo4j 5.x Community in a Docker container reachable at bolt://localhost:7687,
with the APOC and GDS plugins enabled via NEO4J_PLUGINS='["apoc","graph-data-science"]'
(the image installs and configures them). Set NEO4J_AUTH=neo4j/<choose-one>, publish
ports 7474 and 7687, mount a persistent volume at /data, and use
--restart unless-stopped so it comes back on reboot. Then verify with
`RETURN apoc.version();` and `RETURN gds.version();` via cypher-shell.
```

**Prime keys + config:**
```
From the repo root, run `python scripts/prime-local-config.py --neo4j-password
<the-password-you-set>` to generate my API token and a local server-config.yaml.
Show me the API token once and remind me to save it.
```

**Run the server:**
```
Set AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE to the server-config.yaml in
the repo root, run `uv sync`, then start the server with
`uv run uvicorn context_intelligence_server.main:app --host 127.0.0.1 --port 8000`.
Confirm it's healthy by curling http://127.0.0.1:8000/status.
```

---

## Two Neo4j clients — note for local dev

The server has an admin (write) client and a cypher_query (read) client. On Neo4j
**Community** there is a single account, so both clients share the **same**
credentials — the generated `server-config.yaml` uses the flat
`neo4j_url`/`neo4j_user`/`neo4j_password`, which the server fans out to both. You
do **not** need the structured two-client config locally. Separate read/write
accounts are a **Neo4j Enterprise** capability (multi-user + RBAC) and only matter
in a deployment that uses Enterprise — see [azure-deployment.md](azure-deployment.md).
