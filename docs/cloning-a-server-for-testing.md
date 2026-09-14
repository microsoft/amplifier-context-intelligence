# Cloning a Live Server for Isolated Testing

How to stand up a **throwaway Context Intelligence server loaded with a copy of a
real server's data**, without stopping, mutating, or risking the live instance.

Use this when you need to exercise destructive or high-blast-radius behaviour —
session deletion, `doctor --fix`, a migration script, a schema change — against
data that has the shape, scale, and messiness of production, rather than against
fixtures that quietly avoid the hard cases.

> **The live server is never stopped and never written to.** Every step below
> reads from it and writes only into the clone's own directories.

---

## Why not just re-ingest?

Two other routes exist, and both are worse for this purpose:

| Route | Why it falls short |
|---|---|
| Replay `~/.amplifier/projects/**/events.jsonl` through `POST /events` (the `context-intelligence-upload` tool) | Correct, but slow, and it reconstructs the graph rather than reproducing it — you get *a* graph, not *the* graph. Local event files also drift from what the server actually holds. |
| `neo4j-admin database dump` | **Refuses outright** on a running server: *"It is not possible to dump a database that is mounted in a running Neo4j server."* Online backup is Enterprise-only. |

A file-level copy of the store is the only route that reproduces the live graph
exactly while the live server keeps running.

---

## Constraints that will bite you

**1. The Neo4j image version must match the source store.**
Store formats move forward only. A store written by `neo4j:2026.05.0-community`
cannot be opened by `neo4j:5.26.22-community` — the clone will refuse to start.
Read the source's image tag and reuse it verbatim:

```bash
docker inspect context-intelligence-neo4j --format '{{.Config.Image}}'
# neo4j:2026.05.0-community
```

**2. The copy is crash-consistent, not transactionally consistent.**
The source is being written to as you read it. `databases/`, `transactions/`,
and `dbms/` must all travel together so Neo4j can replay the transaction logs on
first start — that recovery is what makes the copy usable. Copy the whole store
root, never just `databases/`.

**3. The copy captures a moving target.**
Sessions ingested during the copy window may be partially present. This is
expected and harmless for testing; just don't treat clone counts as an exact
census of the source (see [Verify](#verify)).

**4. Credentials travel with the store.**
`dbms/auth.ini` and the `system` database are copied too, so **the clone's Neo4j
password is the live one**. `NEO4J_AUTH` is ignored when an initialised store is
already present. Point the clone's `server-config.yaml` at the live password
rather than trying to set a new one.

**5. Never hardlink the blob directory.**
`cp -al` is tempting — same filesystem, instant, no extra space — and it is a
trap. Hardlinks share one inode, so anything that writes a blob path **in place**
in the clone corrupts the production file. Deleting is safe (it drops one name of
two), but the failure mode of getting it wrong is silent production data loss.
Copy the bytes, or copy a subset (below).

---

## Procedure

Throughout: `SRC` is the live server's data root, `DST` is the clone's.

```bash
SRC=/mnt/linuxdata/context-intelligence-server-data
DST=/mnt/linuxdata/ci-smoke-test-data/a
mkdir -p "$DST"/{neo4j,data/blobs}
```

### 1. Copy the Neo4j store

Store files are owned by uid `7474`; `sudo` preserves that ownership so the clone
container can open them. `nice`/`ionice` keep the copy from starving the live
server's I/O.

```bash
sudo nice -n 10 ionice -c2 -n7 rsync -a --info=progress2 \
     "$SRC/neo4j-store/" "$DST/neo4j/"
```

*Observed: 10.17 GB in 5m06s at ~32 MB/s.*

### 2. Start the clone's Neo4j on a free port

Same image tag as the source. Do **not** pass `NEO4J_AUTH` — the copied store
already has credentials.

```bash
docker run -d --name ci-smoke-neo4j-a --restart unless-stopped \
  -p 127.0.0.1:48687:7687 -p 127.0.0.1:48487:7474 \
  -e NEO4J_PLUGINS='["apoc","graph-data-science"]' \
  -e NEO4J_dbms_security_procedures_unrestricted='apoc.*,gds.*' \
  -e NEO4J_server_memory_heap_max__size=4G \
  -v "$DST/neo4j:/data" neo4j:2026.05.0-community

docker logs ci-smoke-neo4j-a 2>&1 | tail -5   # wait for "Started."
```

A `creationDate` in the startup banner matching the *original* store's creation
date confirms it opened the copy rather than bootstrapping a fresh database.

### 3. Copy blobs

Blob files are immutable once written, so copying them from a live server is
safe. The full set is large (96 GB here) and consists of many small files, which
copies slowly.

**Full copy** — when you need every session's blobs:

```bash
nice -n 10 ionice -c2 -n7 rsync -a --info=progress2 "$SRC/blob/" "$DST/data/blobs/"
```

**Selective copy** — usually the better trade. The graph is cheap (10 GB) and
carries every session; blobs are the bulk. Copy only the sessions you will
actually exercise:

```bash
# one session id per line; skip ids with no blob dir, or rsync exits 23
: > /tmp/sids.txt
for sid in <session-id> <session-id> ...; do
  [ -d "$SRC/blob/$sid" ] && echo "$sid" >> /tmp/sids.txt
done

rsync -a -r --files-from=/tmp/sids.txt "$SRC/blob/" "$DST/data/blobs/"
```

> **`--files-from` silently disables recursion — even under `-a`.** Without an
> explicit `-r` you get the session directories created and **zero blob files
> inside them**, with rsync exiting `0`. Always assert the file count, not the
> exit code:
>
> ```bash
> find "$DST/data/blobs" -type f | wc -l    # must be non-zero
> ```

Blobs live at `<blob_path>/<session_id>/blobs/`. A session whose blobs were not
copied still appears in the graph — its blob operations simply find nothing on
disk, which is fine unless the behaviour under test reconciles blob counts.

### 4. Generate the clone's server config

```bash
python3 scripts/prime-local-config.py \
  --config-path "$DST/server-config.yaml" \
  --data-dir    "$DST/data" \
  --neo4j-url   bolt://127.0.0.1:48687 \
  --neo4j-password "<the live Neo4j password>" \
  --server-host 127.0.0.1 --server-port 48000 --force
```

It prints an API token **once** — save it; only its SHA-256 digest is persisted.

Then set `blob_path` to the copied blob directory, and — if you want the clone's
provenance to match the source — set the API key's identity to the same
contributor name. `created_by` is stamped server-side from the authenticated
identity (`main.py`, `body_obj["created_by"] = contributor_id`), so the
identity on the key is what new writes will be attributed to:

```yaml
blob_path: /mnt/linuxdata/ci-smoke-test-data/a/data/blobs
api_keys:
  <sha256-digest>:
    id: colombod
```

### 5. Start the clone server

```bash
AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE="$DST/server-config.yaml" \
  uv run uvicorn context_intelligence_server.main:asgi_app --host 127.0.0.1 --port 48000
```

> **`asgi_app`, not `app`.** `main:app` is the bare FastAPI object with no
> `BearerTokenMiddleware`, so the clone would run completely unauthenticated —
> anonymous `POST /cypher` and `DELETE /sessions/{id}` — and stamp new events
> `created_by: null`. It fails silently; `/status` looks healthy either way.
> Confirm with `curl -o /dev/null -w '%{http_code}' .../whoami` → must be `401`.

One worker per instance — the durable drainer requires it. Independent instances
coexist freely as long as port, Neo4j URL, `blob_path`, `queues_path`,
`log_path`, and `cursor_path` are all distinct.

---

## Verify

Confirm the clone opened the copied store and the server reaches it:

```bash
curl -s -H "Authorization: Bearer $TOKEN" http://127.0.0.1:48000/status
# "neo4j_connected": true, "server_version": "6.9.1"
```

Compare graph counts against the source. They should be close but **not
necessarily identical** — the source keeps ingesting during the copy:

| Metric | Source (at copy start) | Clone |
|---|---|---|
| Sessions | 7,011 | 7,011 |
| Root sessions | 1,198 | 1,200 |
| Nodes | — | 2,706,506 |
| Relationships | — | 4,248,010 |

The two extra roots are sessions that began mid-copy. A clone that is *wildly*
short, or that starts with a fresh `creationDate`, means the store did not open —
check the image tag first.

---

## Teardown

The clone owns a container, a data tree, and a server process. All three are
yours to remove; none is shared with the live server.

```bash
kill "$(cat "$DST/server.pid")"
docker rm -f ci-smoke-neo4j-a
sudo rm -rf "$DST"
```

Store files are owned by uid `7474`, hence the `sudo` on the final removal.

---

## Safety checklist

- [ ] Clone Neo4j image tag **equals** the source's tag
- [ ] Copied `databases/` **and** `transactions/` **and** `dbms/` together
- [ ] Clone binds **different** ports from the live server
- [ ] Clone's `blob_path`, `queues_path`, `log_path`, `cursor_path` all point inside the clone's own data dir
- [ ] Blob directory was **copied, not hardlinked**
- [ ] Live server was never stopped, and nothing was written to `$SRC`
