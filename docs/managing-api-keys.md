# Managing API Keys

The canonical guide to authentication for `context-intelligence-server` — for a
human operator or an agent setting one up. Covers the two key modes, the local
first-run bootstrap, adding/revoking/rotating per-peer keys by hand, and the
guardrails that keep you from locking yourself (or your peers) out with a 401.

> There is **no `init` subcommand.** Setup is either the local first-run
> bootstrap (`scripts/prime-local-config.py`, below) or the manual steps in this
> guide.

> To add/remove users at runtime without redeploying, see [docs/identity-management.md](identity-management.md).

> Using **Microsoft Entra JWT** auth (`auth_mode=entra`) instead of pre-shared keys?
> This guide covers the static-key modes; for the Entra setup see
> [entra-auth-setup.md](entra-auth-setup.md).

---

## 1. Two modes of authentication

The server reads two config keys. They coexist; both feed one keystore.

| Mode | Config key | Shape | Identity |
|------|-----------|-------|----------|
| **Legacy single key** | `api_key` | a single string secret | folds to contributor id `owner` |
| **Per-contributor keystore** | `api_keys` | map of `sha256_hex(token) -> { id: <contributor> }` | each entry names its own `id` |

How a request is verified (both modes):

1. The peer sends `Authorization: Bearer <raw_token>`.
2. The server computes `sha256_hex(raw_token)`.
3. It looks that digest up in the keystore. Hit → request proceeds, stamped with
   the matched contributor `id`. Miss → **401**.

Key facts that follow from this:

- The server stores only **digests**, never raw tokens. For `api_keys` you write
  the digest directly. For the legacy `api_key` you write the raw secret and the
  server hashes it at startup.
- **Auth is disabled** (every request passes) only when *no* keys are configured —
  `api_keys` omitted or `null` **and** no `api_key`. That is dev-only.
- **`api_keys: {}` (an explicit empty map) is a hard startup error** — fail-closed.
  An empty keystore is treated as a misconfiguration, not "auth off". To disable
  auth, omit `api_keys` or set it to `null`.
- Legacy `api_key` still works unchanged — existing single-key deployments are
  unaffected and need no migration.

> **Two ways to edit the keystore.** This guide covers the **config-file** path
> (edit `api_keys`, restart) — the right path for the day-zero bootstrap and for
> deployments that don't run the admin API. To add/revoke keys **at runtime with no
> restart**, use the `/admin/keys` API (gated by a separate `admin_api_key`); see
> [identity-management.md](identity-management.md). Both edit the same map — the
> in-process keystore that is the source of truth at runtime.

> **Admin key: store the digest, not the raw token.** The admin credential that
> gates `/admin/*` follows the same at-rest rule as `api_keys` — store its
> **SHA-256 digest**, never the raw token. Set `admin_api_key_sha256:` to the
> 64-hex digest (derived with the one-liner in §2). A config-file leak then
> yields only a one-way digest, not a usable admin credential. The legacy raw
> `admin_api_key:` field still works (it is hashed at load) but is **deprecated**
> — the server logs a warning at startup and, if both are set,
> `admin_api_key_sha256` wins and the raw field is ignored. Env var:
> `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_ADMIN_API_KEY_SHA256`.
>
> ```yaml
> # Recommended — digest at rest:
> admin_api_key_sha256: "<64-hex sha256 of the admin token>"
> # Deprecated — raw token at rest (still works, warns):
> # admin_api_key: "<raw admin token>"
> ```

---

## 2. The two one-liners

These derive a token and its digest exactly the way the server does
(`hashlib.sha256(token.encode()).hexdigest()`).

```bash
# 1. Generate a fresh raw token (this is the secret the peer will hold):
python3 -c "import secrets; print(secrets.token_urlsafe(32))"

# 2. Derive the sha256 digest of that token (this is what goes in api_keys):
python3 -c "import hashlib,sys; print(hashlib.sha256(sys.argv[1].encode()).hexdigest())" "<token>"
```

> **Shell-history leak — read before you run step 2.** Passing the raw token as a
> command argument writes it into your shell history (`~/.bash_history`,
> `~/.zsh_history`) and it may be visible in the process list while it runs. After
> deriving the digest, scrub it: `history -d $(history 1)` (or clear the relevant
> lines), or avoid the exposure entirely by deriving the digest in one step
> without ever putting the token on a command line:
>
> ```bash
> # Generate token + digest together; the token is printed once, never an argv:
> python3 - <<'PY'
> import secrets, hashlib
> t = secrets.token_urlsafe(32)
> print("token: ", t)
> print("digest:", hashlib.sha256(t.encode()).hexdigest())
> PY
> ```
>
> Treat any token that has touched shell history as compromised — rotate it
> (§6) if it protects anything that matters.

---

## 3. Local first-run bootstrap

For a local (non-Docker) run, `scripts/prime-local-config.py` does the credential
bootstrap in one shot. From the repo root:

```bash
python scripts/prime-local-config.py --neo4j-password '<neo4j-password>'
```

It:

1. generates a random raw API token,
2. writes `server-config.yaml` containing the Neo4j connection
   (`bolt://localhost:7687`) and an `api_keys` block holding **only the digest**:

   ```yaml
   api_keys:
     "<64-hex sha256 of the token>":
       id: owner
   ```

   plus a local `./.context-intelligence-data/` tree (`blobs/`, `queues/`,
   `logs/`, `identity/`), and

3. **prints the raw token once**, behind this banner:

   ```
   ========================================================
     SAVE THIS TOKEN — it will NOT be shown again.
     The file stores only its SHA-256 digest, not the token.

     API token: <raw-token>
   ========================================================
   ```

**Capture that token immediately.** It is the bearer token your hook/clients use.
It is **not** recoverable from `server-config.yaml` — that file holds only the
digest. If you lose it, you cannot grep it back; rotate (§6) to issue a new one.

The script refuses to overwrite an existing `server-config.yaml` unless you pass
`--force`. See [local-development.md](local-development.md) §2 for the full flag
list (`--data-dir`, `--config-path`, `--server-host`/`--server-port`). Prefer to
do it by hand? Use the two one-liners in §2 and add the `api_keys` entry yourself.

---

## 4. Add a peer (manual, per-peer key)

Each peer gets their **own** unique token so you can revoke them individually.
Never share one token across people.

1. **Generate a token and digest** for the peer (§2). Pick a stable, lowercase
   contributor id (e.g. `alice`, `peer-laptop`).

2. **Add the digest to the keystore.** In your server config
   (`server-config.yaml`), under `api_keys`, add one entry — the digest is the key,
   quoted; the value carries `id`:

   ```yaml
   api_keys:
     "f1d2...<existing owner digest>...":
       id: owner
     "a3c9...<the new peer's digest>...":
       id: alice
   ```

3. **Restart the server** so it reloads the keystore.

4. **Send the peer the RAW token** (the output of step 1, *not* the digest) over a
   secure channel — a password manager share, an encrypted message, etc. They put
   it in their client as the bearer token (see
   [peer-onboarding.md](peer-onboarding.md)).

> **RAW-TOKEN vs DIGEST — the #1 way to lock yourself out.**
> The **config file holds the DIGEST** (64 hex chars). The **peer holds the RAW
> token** (the `secrets.token_urlsafe` string). They are not interchangeable:
> - Put the **raw token** in `api_keys` → its digest won't match anything → every
>   request from that peer is **401**.
> - Send the **digest** to the peer → the server hashes *the digest* and gets a
>   different value → **401**.
> If you see an unexpected 401, check this first: file = digest, peer = raw token.

---

## 5. Revoke a peer

1. Delete that contributor's entry (the digest line and its nested `id`) from
   `api_keys`.
2. Restart the server.

The peer's token now hashes to a digest that is no longer in the keystore, so
their requests return 401. Other peers are unaffected — this is the whole point of
per-peer keys. Their already-uploaded data remains in the graph.

---

## 6. Rotate a key

Rotation = issue a new token for a contributor and retire the old one.

1. Generate a fresh token + digest (§2).
2. Replace that contributor's digest in `api_keys` with the new one (keep the same
   `id`).
3. Restart the server.
4. Send the new **raw token** to the holder; they swap it into their client.

The old token stops working the moment the old digest leaves the keystore. To
rotate without downtime for the holder, you can temporarily keep both digests
(same `id`) in the keystore, let them switch, then remove the old one.

---

## 7. Migrate legacy `api_key` → `api_keys`

You do not have to — legacy `api_key` keeps working. Migrate when you want
per-peer identity/revocation. The legacy key folds to id `owner`; preserve that
token by carrying its digest forward.

1. Derive the digest of your **existing** `api_key` value:

   ```bash
   python3 -c "import hashlib,sys; print(hashlib.sha256(sys.argv[1].encode()).hexdigest())" "<current-api_key>"
   ```

2. Add an `api_keys` block with that digest under id `owner`, then remove the
   `api_key` line:

   ```yaml
   # before:
   # api_key: "s3cr3t-existing-token"

   # after:
   api_keys:
     "<digest of s3cr3t-existing-token>":
       id: owner
   ```

3. Restart. The same raw token your clients already hold continues to work,
   because its digest is unchanged — you have only changed how the server stores
   it. Now you can add more peers (§4).

> You can also run both keys at once during a transition: keep `api_key` *and* an
> `api_keys` block — the server merges them into one keystore. (If the same digest
> appears in both, the `api_keys` entry's `id` wins.)

---

## 8. Validation rules (what the server rejects at startup)

Fail-closed: a bad keystore stops the server rather than silently disabling auth.

| Rule | Behavior |
|------|----------|
| `api_keys: {}` (empty map) | **Hard error.** Omit or use `null` to disable auth. |
| Digest key not exactly 64 hex chars | **Hard error.** (Uppercase is accepted and normalized to lowercase.) |
| A value that is not a map / missing `id` | **Hard error.** |
| `id` empty, whitespace-only, or non-string | **Hard error.** |
| `api_keys` omitted or `null`, no `api_key` | Auth disabled (dev only). |

Quote the digest key in YAML. A bare 64-hex string usually parses fine, but
quoting is unambiguous and avoids surprises with digests that start with a digit
or look numeric.

---

## 9. How `id` shows up in the graph (`created_by`)

The matched contributor `id` is recorded on graph data as `created_by`, so you can
see who contributed what. Two different guarantees:

- **Nodes — per-contributor, write-once.** Each session/tool-call node is stamped
  with the `created_by` of the contributor who first created it. This is sound
  because **each `session_id` is owned by exactly one contributor.** The stamp is
  write-once: once set it is never overwritten (anti-spoof). Nodes created before
  this feature shipped keep `created_by = null`.

- **Edges (relationships) — first-asserter, weaker.** Relationships also get a
  write-once `created_by`, but edges are inherently cross-session, so it means
  "whoever first asserted this edge" — **race-determined for cross-session edges,
  not ownership.** It is intentionally **not indexed in v1**: querying edges by
  contributor is unsupported for now. Bare endpoint placeholder nodes that an edge
  creates are left `null` (not stamped) to avoid mis-attributing them across
  sessions.

Don't build access control on `created_by`. It is provenance/attribution, not an
authorization boundary.

---

## 10. The admin key (`admin_api_key_sha256`)

### What it is

A **single, separate credential** that gates the server's `/admin/*` endpoints.
It is **not** part of the data-auth keystore (`api_key` / `api_keys`) and is
never used to send events — it authorizes *administration* of the keystore, not
contribution to the graph. It applies to **static mode only**; in `auth_mode=entra`
the admin capability is granted by an Entra app-role claim (`entra_admin_role`)
instead, and the admin key is ignored.

### What it's needed for / what it does

It unlocks **runtime management of the per-contributor keystore with no restart** —
the `/admin/keys` API described in [identity-management.md](identity-management.md):

- **Add** a peer key while the server is running (`PUT /admin/keys`).
- **Revoke** a peer key (`DELETE /admin/keys`).
- **Rotate** keys without editing YAML and restarting.

Without an admin key configured, `/admin/*` is **disabled** (callers get 403/503)
and the only way to change keys is the config-file path (edit `api_keys`, restart).
So you need the admin key **only if** you want live keystore management; a static,
config-file-only deployment can leave it unset.

How verification works (same one-way shape as data keys): a caller sends
`Authorization: Bearer <raw_admin_token>`; the server computes `sha256(token)` and
compares it to the configured admin digest. Match → the request is treated as
admin (`is_admin=True`) and **bypasses the data keystore**; miss → falls through
to normal auth. The admin check runs **before** the keystore resolver.

### How to set it up

1. **Generate a token and its digest** (token never touches shell history):

   ```bash
   python3 - <<'PY'
   import secrets, hashlib
   t = secrets.token_urlsafe(32)
   print("admin token: ", t)                                    # keep secret; use as Bearer
   print("admin digest:", hashlib.sha256(t.encode()).hexdigest())  # goes in config
   PY
   ```

2. **Store the digest at rest** in `server-config.yaml` (or via env var
   `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_ADMIN_API_KEY_SHA256`):

   ```yaml
   admin_api_key_sha256: "<64-hex admin digest>"
   ```

3. **Restart** the server. Confirm it came up admin-enabled — the startup log
   reads `create_asgi_app: auth_mode=static admin_api=enabled`, and unauthenticated
   `/status` reports `auth.admin_api_key_configured: true`.

4. **Use the raw token** to call the admin API:

   ```bash
   curl -H "Authorization: Bearer <raw_admin_token>" https://<host>/admin/keys
   ```

**Capture the raw token when you mint it.** The server keeps only the digest and
cannot give the token back; lose it and you mint a new one and update the config.

### Storage at rest — digest, not raw token

`admin_api_key_sha256` stores the **one-way SHA-256 digest**, so a leak of the
config file yields no usable admin credential — the same guarantee `api_keys`
provides. The legacy raw field `admin_api_key: "<token>"` still works (it is
hashed at load) but is **deprecated**: it puts the secret in plaintext at rest,
and the server logs a startup warning when it is used. If both are set,
`admin_api_key_sha256` wins and the raw field is ignored.

To migrate off the raw field: compute the digest of your existing admin token
(step 1's one-liner), move it to `admin_api_key_sha256`, delete the raw
`admin_api_key`, and restart.

### Security notes

- Treat the admin token like **root** for this server: it can add/revoke any
  contributor key. Keep it separate from ordinary per-peer ingestion tokens.
- Don't reuse the admin token as a data-ingestion key. If you do, the admin
  fast-path authenticates it as `admin` **before** the keystore resolver, so your
  events are stamped `created_by=admin` instead of your real contributor `id`.
- Rotate it independently of data keys; because client and server hold the secret
  in different forms (raw vs digest), rotate **both sides together**.

### Validation (fail-closed)

| Rule | Behavior |
|------|----------|
| `admin_api_key_sha256` not exactly 64 hex chars | **Hard startup error.** (Uppercase is accepted and normalized to lowercase.) |
| `admin_api_key_sha256: ""` (empty string) | Normalized to unset (admin disabled), mirrors `admin_api_key`. |
| Both `admin_api_key` and `admin_api_key_sha256` set | Digest wins; raw ignored; startup warning. |
| Raw `admin_api_key` set (no digest) | Works, but logs a deprecation warning. |

---

## See also

- [peer-onboarding.md](peer-onboarding.md) — the guide to hand a peer who receives a token.
- [remote-access-sharing.md](remote-access-sharing.md) — exposing only `/events` to external peers.
- [service-setup.md](service-setup.md) — running the server as a system service.
- `server-config.example.yaml` — annotated `api_key` / `api_keys` reference.
- `docs/designs/per-user-api-keys.md` — the design record behind this model.
