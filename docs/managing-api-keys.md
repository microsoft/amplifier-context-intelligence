# Managing API Keys

The canonical guide to authentication for `context-intelligence-server` — for a
human operator or an agent setting one up. Covers the two key modes, the first-run
Docker bootstrap, adding/revoking/rotating per-peer keys by hand, and the
guardrails that keep you from locking yourself (or your peers) out with a 401.

> There is **no `init` subcommand.** Setup is either the Docker first-run
> bootstrap (below) or the manual steps in this guide.

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

## 3. First-run Docker bootstrap

`./start.sh` (Docker Compose) and `docker-entrypoint.sh` (single container) both
self-bootstrap on first run when no `credentials.yaml` exists. They:

1. generate a random Neo4j password and a random raw API token,
2. write `credentials.yaml` containing the Neo4j credentials and an `api_keys`
   block holding **only the digest**:

   ```yaml
   api_keys:
     "<64-hex sha256 of the token>":
       id: owner
   ```

3. **print the raw token once**, behind this banner:

   ```
   ========================================================
     SAVE THIS TOKEN — it will NOT be shown again.
     The file stores only its SHA-256 digest, not the token.

     API token: <raw-token>
   ========================================================
   ```

**Capture that token immediately.** It is the bearer token your hook/clients use.
It is **not** recoverable from `credentials.yaml` — that file holds only the
digest. If you lose it, you cannot grep it back; rotate (§6) to issue a new one.

- Docker Compose: the banner is in the `./start.sh` output.
- Single container: it is in the container's first-run logs —
  `docker logs <container>` right after the first start.

On every subsequent run the scripts find the existing `credentials.yaml`, print
"Existing credentials found", and do **not** regenerate or reprint anything.

---

## 4. Add a peer (manual, per-peer key)

Each peer gets their **own** unique token so you can revoke them individually.
Never share one token across people.

1. **Generate a token and digest** for the peer (§2). Pick a stable, lowercase
   contributor id (e.g. `alice`, `peer-laptop`).

2. **Add the digest to the keystore.** In your server config (`server-config.yaml`,
   or the Docker `credentials.yaml`), under `api_keys`, add one entry — the digest
   is the key, quoted; the value carries `id`:

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

## See also

- [peer-onboarding.md](peer-onboarding.md) — the guide to hand a peer who receives a token.
- [remote-access-sharing.md](remote-access-sharing.md) — exposing only `/events` to external peers.
- [service-setup.md](service-setup.md) — running the server as a system service.
- `server-config.example.yaml` — annotated `api_key` / `api_keys` reference.
- `docs/designs/per-user-api-keys.md` — the design record behind this model.
