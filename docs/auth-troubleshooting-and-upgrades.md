# Auth Setup, Troubleshooting & Upgrades

**Audience:** anyone standing up a Context Intelligence server with authentication,
configuring the Amplifier hook to publish events to it, or upgrading an existing
static-key deployment that suddenly started returning `401`.

This is the **map** that ties the two sides of the wire together. For the deep
per-topic detail it points at the existing focused docs:

| Topic | Authoritative doc |
|-------|-------------------|
| Static API keys (create / add peer / rotate / revoke) | [`managing-api-keys.md`](./managing-api-keys.md) |
| Entra (Azure AD) auth | [`entra-auth-setup.md`](./entra-auth-setup.md) |
| Runtime identity map (entra) | [`identity-management.md`](./identity-management.md) |
| Joining someone else's server | [`peer-onboarding.md`](./peer-onboarding.md) |
| Service / app-token callers | [`service-setup.md`](./service-setup.md) |

---

## 1. The mental model in 60 seconds

There are **two independent config surfaces**, one on each end of the wire. Almost
every "it won't authenticate" problem is a mismatch between them.

```
   AMPLIFIER SESSION (the client)                 CONTEXT INTELLIGENCE SERVER
   hook: hook-context-intelligence                FastAPI app
   ─────────────────────────────                  ──────────────────────────
   settings.yaml → destinations:                  server-config.yaml → api_keys:
     <name>:                                        "<sha256_hex_of_token>":
       url: http://host:8000                          id: <contributor>
       api_key: "${SOME_ENV_VAR}"   ── sends ──▶   validates:
       include: ["**"]               Authorization:   sha256(received_token)
                                       Bearer <raw>    must be a key in api_keys
```

Two facts that surprise people, and cause most tickets:

1. **The client sends the RAW token; the server stores only its SHA‑256 digest.**
   The value in the server's `api_keys:` map is **not** the token — it is
   `sha256hex(token)`. (`context_intelligence_server/auth.py`, `_resolve_token`;
   `config.py`, the `api_keys` model.)
2. **The transport header is always `Authorization: Bearer <token>`.** There is
   **no** `X-API-Key` support on the server. Both static and Entra modes use the
   same header; only the token source differs. (server `auth.py`
   `_extract_bearer_token`; hook `context_intelligence/auth.py` `ApiKeyAuth.headers`.)

---

## 2. Auth modes

The server picks a mode with `auth_mode` in `server-config.yaml`; the hook picks a
mode **per destination** with `auth_mode` in its destination block. **Default is
`static` on both sides.** Static and Entra are mutually exclusive on the server.

| | **static** (default) | **entra** |
|---|---|---|
| Client credential | a raw bearer token you generate | an Azure AD (Entra) JWT from `AzureCliCredential` (`az login`) |
| Server validates | `sha256(token)` is present in `api_keys:` | RS256 JWT: signature, `exp`, `aud`, `iss`, tenant; `oid` mapped to a contributor |
| Client config needed | `url`, `api_key` | `url`, `auth_mode: entra`, `auth_resource` |
| Good for | local / single-owner / home server | shared team server behind Entra |

If you installed your server "with static keys," you are in the **static** column —
Entra is not involved, and `az login` is irrelevant to your 401s.

---

## 3. Static-key setup, end to end

This is the minimal correct setup to publish from an Amplifier session to a local
static-key server. Both halves must agree on the token.

### 3.1 Server side (`~/.amplifier/context-intelligence/server-config.yaml`)

Generate a token and its digest (also in [`managing-api-keys.md` §2](./managing-api-keys.md)):

```bash
# 1) the raw token — this is the secret the CLIENT holds:
python3 -c "import secrets; print(secrets.token_urlsafe(32))"

# 2) its sha256 digest — this is what the SERVER stores:
python3 -c "import hashlib,sys; print(hashlib.sha256(sys.argv[1].encode()).hexdigest())" "<paste-token-from-step-1>"
```

Put the **digest** (not the token) in `api_keys:`:

```yaml
api_keys:
  "3f9a…64-hex-chars…":   # sha256hex(token) — 64 lowercase hex chars
    id: colombod           # who this key belongs to; shows up as created_by in the graph
```

Rules the server enforces at startup (fail-closed — see
[`managing-api-keys.md` §8](./managing-api-keys.md)):

- Each `api_keys` key **must** be 64 lowercase hex chars (a sha256 digest). A raw
  token here is rejected.
- `api_keys: {}` (present but empty) is a **hard startup error**, not "auth off."
- The server refuses to start with no auth configured at all unless
  `allow_unauthenticated` is explicitly set (dev only — never in production).

### 3.2 Client side (`~/.amplifier/settings.yaml`)

```yaml
overrides:
  hook-context-intelligence:
    config:
      destinations:
        local:
          url: "http://localhost:8000"     # events POST to <url>/events
          api_key: "${LOCAL_CI_KEY}"       # the RAW token from step 1
          include: ["**"]                  # REQUIRED — see the gotcha below
```

And the secret itself in `~/.amplifier/keys.env`:

```
LOCAL_CI_KEY=<the-raw-token-from-step-1>
```

> **Gotcha — `include` defaults to matching NOTHING.** If you omit `include` (or
> leave it empty) the destination is parsed and validated but **never fires** —
> events only land in the local `events.jsonl`, and you will scratch your head
> wondering why nothing reaches the server. Set `include: ["**"]` (or a path
> prefix) to activate it. (hook `config_resolver.py`, `Destination.include`.)

> **Gotcha — the hook does NOT read env vars or `settings.yaml` itself.** The
> `${LOCAL_CI_KEY}` placeholder is expanded by the Amplifier **app-cli** layer
> *before* the hook is mounted; the hook is a pure config-dict consumer. If the
> placeholder reaches the hook unexpanded, the client sends the literal string
> `Bearer ${LOCAL_CI_KEY}` and the server 401s on a digest miss. (hook
> `config_resolver.py` "D1 contract fix"; bundle `README.md`.)

---

## 4. The `401` decision tree

The server returns `401` on the **ingest** path (`POST /events`) for exactly these
reasons (server `auth.py`, `BearerTokenMiddleware`). Work top to bottom:

1. **`Missing or invalid bearer token`** in the response body
   → No `Authorization: Bearer …` header arrived, or it was malformed.
   - Client-side: the destination isn't actually authenticating — check the
     `api_key` expanded to a real value (not empty, not a literal `${VAR}`), and
     that `include` matches so the destination even fires.

2. **`401` with a valid-looking header** → the token's `sha256` is **not** a key in
   the server's `api_keys:` map (digest miss). Causes, most common first:
   - The value in server `api_keys:` is the **raw token**, not its **digest**.
     Re-run the two one-liners and store the digest.
   - The client's token and the server's stored digest are simply **different**
     (typo, wrong key, stale `keys.env`). Confirm `sha256(client_token)` equals a
     key in `api_keys:`.
   - You are publishing with the **admin key** — see §5, this is the classic
     post-upgrade break.

3. **Entra mode only** — `401` from bad signature / `exp` / `aud` / `iss` / wrong
   tenant / missing `oid`. Out of scope for static setups; see
   [`entra-auth-setup.md` §3.5](./entra-auth-setup.md).

Note the difference between **401 and 403**: `401` = "I don't know who you are"
(bad/missing token). `403` = "I know who you are, but you may not do this" (e.g. a
non-admin key hitting `/admin/*`, or an Entra identity not in the map). A publish
`403` is an authorization/role problem, not a key problem.

---

## 5. Upgrades — "it worked before, now everything 401s"

The authentication feature changed a few behaviors that can turn a
previously-working static deployment into a wall of `401`s **after you upgrade the
server binary** — even though you didn't touch your config. The two config files
are unchanged; the **binary's rules** changed. In order of likelihood:

### 5.1 You were publishing events with the *admin* key

**This is the most common upgrade break.** Previously a bare admin key
authenticated on **every** route, so a deployment could `POST /events` with it.
The fix that scoped the admin-key fast-path to `/admin/*` only means the admin key
now **falls through** on data routes and, unless it is *also* registered as a real
data key, is rejected `401`. (server `auth.py`, the admin-route guard.)

**Fix:** stop using the admin key to publish. Generate a proper **data** key
(§3.1), add its digest to `api_keys:`, and put the raw token in the client's
`keys.env`. Keep the admin key only for `/admin/*` operations.

### 5.2 A raw token is sitting in `api_keys:` where a digest belongs

If your `api_keys:` map contains raw tokens (because an older build tolerated it,
or by hand-editing), the digest lookup can never match and **every** request 401s
— or the server refuses to boot on the 64-hex validation. **Fix:** replace each
entry key with `sha256hex(token)` (§3.1).

### 5.3 `api_keys: {}` now refuses to boot

An empty keystore is fail-closed: the server won't start. If your server isn't
even up, the hook's 401/connection errors are downstream of that. Check the server
**startup logs** first (see §6).

### 5.4 You were (accidentally) running the fail-open app

Dev/DTU profiles now serve the auth-enforcing ASGI app (`main:asgi_app`), not the
un-middlewared `main:app`. If a deployment had been hitting the fail-open app, auth
begins enforcing after the switch and unauthenticated publishes start returning
`401`. **Fix:** this is correct behavior — configure a real key (§3).

> **First move on any upgrade 401:** read the **server startup logs**. The server
> emits a warning if your admin key is a raw token (`admin_api_key` vs the
> recommended `admin_api_key_sha256`), and fail-closed **errors** if the keystore
> is empty or malformed. Those two lines usually name the problem outright.

---

## 6. Verification probes (no config changes, read-only)

**Is the server up and what auth is it enforcing?** Check its logs on startup:

```bash
journalctl -u context-intelligence-server.service -n 40 --no-pager
```

Look for the listen line (`Listening at: http://0.0.0.0:8000`), any
`admin_api_key … DEPRECATED` warning, and any fail-closed keystore error.

**Does my key authenticate?** Send a deliberately-empty event with your bearer
token. Auth is checked before body validation, so the status code tells you which
layer you're stuck at:

```bash
curl -sS -o /dev/null -w "%{http_code}\n" \
  -X POST "http://localhost:8000/events" \
  -H "Authorization: Bearer $LOCAL_CI_KEY" \
  -H "Content-Type: application/json" \
  -d '{}'
```

| Result | Meaning |
|--------|---------|
| `400` / `422` | **Auth PASSED** — you got past the gate to body validation. Your key is good. |
| `401` | Auth failed — walk §4. Token digest isn't in `api_keys:`, or header missing. |
| `403` | Authenticated but not authorized — role/identity problem (§4), not a key problem. |
| connection refused | Server isn't listening — check §6 startup logs. |

**Confirm the digest match by hand:**

```bash
# this must equal one of the keys under api_keys: in server-config.yaml
python3 -c "import hashlib,os; print(hashlib.sha256(os.environ['LOCAL_CI_KEY'].encode()).hexdigest())"
```

---

## 7. Quick reference — common misconfigurations

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| Hook 401, header looks present | raw token stored in server `api_keys:` instead of its digest | store `sha256hex(token)` (§3.1) |
| Hook 401 after a server upgrade | publishing with the admin key (fast-path now scoped to `/admin/*`) | use a real data key (§5.1) |
| Events only in local `events.jsonl`, nothing sent | `include` omitted/empty → destination never fires | `include: ["**"]` (§3.2) |
| Client sends `Bearer ${VAR}` literally / empty-key mount error | `${VAR}` not expanded by app-cli; secret missing from `keys.env` | define the var in `~/.amplifier/keys.env` (§3.2) |
| Server won't start | `api_keys: {}` empty, or a non-64-hex entry | add a valid digest entry, or configure auth properly (§5.3) |
| Publish returns 403, not 401 | authenticated but wrong role/identity (e.g. admin key on data route, or Entra oid unmapped) | §4; for entra see [`identity-management.md`](./identity-management.md) |
| `admin_api_key … DEPRECATED` warning at startup | admin key stored as a raw token | move it to `admin_api_key_sha256` ([`managing-api-keys.md` §10](./managing-api-keys.md)) |

---

## 8. Where each rule lives in the code

So future readers can verify rather than trust:

- Server bearer extraction & digest resolution: `context_intelligence_server/auth.py`
  (`_extract_bearer_token`, `_resolve_token`, `BearerTokenMiddleware`).
- Server config model & fail-closed validation: `context_intelligence_server/config.py`
  (`api_keys`, `admin_api_key` / `admin_api_key_sha256`, `build_keystore`).
- Ingest route & capability check: `context_intelligence_server/main.py` (`POST /events`),
  `context_intelligence_server/authz.py` (`require_write`).
- Hook destination schema & validation: the hook's `config_resolver.py`
  (`Destination`, `validate_destinations`).
- Hook auth header construction: the hook's `context_intelligence/auth.py`
  (`ApiKeyAuth`, `EntraTokenAuth`, `build_auth_strategy`).

---

## 9. Neo4j two-client split (admin / cypher_query)

The server talks to Neo4j through **two internal clients**:

- **admin** — read/write. Used for ingest (`POST /events` drains) and schema.
- **cypher_query** — read-intent. Used for `POST /cypher` and dashboard reads.

This lets you give the read path a **separate credential** (and optionally a
**separate URL**, e.g. a read replica), so you can tighten data access later
**without a server code change** — you only edit config.

### 9.1 It is opt-in and backward compatible

You do **not** have to change anything. When the structured `neo4j:` block is
absent, both clients are synthesized from the legacy flat keys
(`neo4j_url` / `neo4j_user` / `neo4j_password`), differing only by an
`access_mode` hint. Existing deployments keep booting unchanged.

### 9.2 Opting into separate credentials / URLs

Replace the flat keys with a structured block:

```yaml
neo4j:
  admin:
    url: bolt://localhost:7687
    username: neo4j
    password: "<write-password>"
    access_mode: WRITE          # MUST be WRITE
  cypher_query:
    url: bolt://localhost:7687   # or a read-replica URL for separate-URL setups
    username: reader
    password: "<read-password>"
    access_mode: READ           # MUST be READ (default is WRITE — omitting it fails startup)
```

Environment-variable form uses the `__` nested delimiter, e.g.
`AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J__CYPHER_QUERY__URL` and
`...NEO4J__CYPHER_QUERY__ACCESS_MODE=READ`.

**Rules enforced at startup (fail-loud):** when the `neo4j:` block is present,
**both** `admin` and `cypher_query` are required, and the model validator
rejects boot unless `admin.access_mode == WRITE` and
`cypher_query.access_mode == READ`. The error names the offending client:
`Neo4j client config invariant violated: neo4j.cypher_query.access_mode must be 'READ', got 'WRITE'`.

To require the explicit block in a deployed profile (and refuse the legacy
fallback even when both clients would point at the same instance), set:

```yaml
neo4j_require_explicit_clients: true   # default false
```

### 9.3 Enforcement caveat — Community vs Enterprise

`access_mode: READ` makes the server open `/cypher` sessions in **read mode**.
On **Neo4j Community 5.26.x**, a write attempted inside a READ-mode session is
rejected at the session level (`Neo.ClientError.Statement.AccessMode:
Writing in read access mode not allowed`) — so the read *path* is protected.

But this is **session-mode** enforcement, **not per-credential RBAC**. Community
has no way to stop the `cypher_query` **credential** from simply opening a WRITE
session. To make the read credential *itself* incapable of writing (defense
against a compromised credential or a code path that opens a write session), you
need **Neo4j Enterprise RBAC** (assign the read credential a read-only role) or a
DB-level restricted user. Splitting the credentials in config now is exactly what
makes that later hardening a **config-only** change — no code, no redeploy of the
application image.

> **Do not read "READ client" as a security boundary on Community.** It is an
> operational separation and a routing hint that becomes a hard security boundary
> once backed by Enterprise RBAC or a restricted DB user.

### 9.4 Upgrade note

Upgrading to a build that contains the split requires **no action** — the legacy
flat keys continue to work. Adopt the `neo4j:` block only when you want separate
read/write credentials or URLs. The most common first-time error is omitting
`access_mode: READ` on `cypher_query` (it defaults to `WRITE`), which is a hard
startup failure with the invariant message shown in § 9.2.

### 9.5 Reading the query-driver status

`/status` now exposes a second boolean, **`neo4j_query_connected`**, alongside the
existing `neo4j_connected`. It reflects whether the **cypher_query** (read) driver
is connected:

- `true` — the read driver reached its Neo4j endpoint; `/cypher` and dashboard
  reads can serve.
- `false` — the read driver is not connected; `/cypher` reads will fail even if
  ingest is healthy.

`neo4j_query_connected` is **independent** from `neo4j_connected` (the **admin**
write driver). The two drivers connect separately — one can be up while the other
is down — so check both fields when diagnosing. On legacy flat config both drivers
point at the same instance and share credentials, so they will usually agree; a
disagreement then points at connectivity/routing to one endpoint, not a config
mismatch.

### 9.6 Query driver down but admin up

**Symptom:** ingest and writes succeed (`neo4j_connected: true`), but `/cypher`
reads fail and `/status` shows `neo4j_query_connected: false`.

**Diagnosis:** the **cypher_query** client's URL or credentials are wrong, or — on
a cluster — the read replica it targets is unreachable. On legacy flat config both
drivers share the same URL and credentials, so a split like this points at
**connectivity/routing** to the read endpoint (e.g. an unreachable read replica)
rather than a credential mismatch. On a structured `neo4j:` block with separate
`cypher_query` settings, re-check that block's URL/username/password and the
reachability of the read-replica host.

### 9.7 Write via `/cypher` rejected with `Neo.ClientError.Statement.AccessMode`

**Symptom:** a write statement (`CREATE`, `MERGE`, `SET`, `DELETE`, `REMOVE`)
issued through `POST /cypher` is rejected at runtime with
`Neo.ClientError.Statement.AccessMode`.

**This is expected, by design.** `/cypher` runs in a **READ-access session**, so
write statements are refused at the session level. Route writes through the
**admin/ingest** path (`POST /events`), not `/cypher`. See the enforcement caveat
in § 9.3: on Community this is session-mode enforcement (an operational separation
and routing hint), not per-credential RBAC — the read *path* is protected, but
making the read *credential* itself incapable of writing requires Neo4j Enterprise
RBAC or a restricted DB user.
