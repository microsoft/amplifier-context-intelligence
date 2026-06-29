# Runtime Identity-Map Management

> **Audience:** operators who need to add or remove a user's access to the
> Context Intelligence Server **at runtime, without a redeploy**.
>
> **Secret hygiene — read first.** Every identifier in this document is a
> **placeholder**. Never paste a real Azure Object ID (`oid`), client ID,
> tenant ID, API key, or key hash into a shared or public repo. Inject secrets
> via environment variables / Key Vault or a **git-ignored** config file.

---

## 1. What this is

The server maps an authenticated **principal → contributor id** and stamps that
contributor onto the graph as the write-once `created_by` provenance field. This
guide covers the **`/admin/*` API** that edits that mapping **live**:

- **Entra mode** (`auth_mode=entra`): the **OID → contributor** map.
- **Static mode** (`auth_mode=static`): the **SHA-256(key) → contributor** map.

Two properties make this safe and immediate:

- **Durable.** Each map is a JSON file on the `/data` Azure Files volume:
  - `entra_identities_store_path` — default `/data/identity/entra-identities.json`
  - `api_keys_store_path` — default `/data/identity/api-keys.json`

  Mutations are committed **write-file-then-swap-memory** (the file is updated
  atomically *before* the in-process map), so the file is never behind memory.

- **Effective immediately.** The pilot runs a **single replica**, so the
  in-process map is the single source of truth. A `PUT`/`DELETE` is visible to
  the resolver on the **very next request — no restart, no redeploy**.

---

## 2. Day-zero setup (per mode)

You enable the admin API once, per auth mode. Confirm it with `/status` (§7).

### Entra mode — create and assign the `IdentityAdmin` App Role

Admin authority rides on an **App Role** in the token's `roles` claim. The
server checks the role named by `entra_admin_role` (**default `IdentityAdmin`**).

1. In the **App Registration**, define an App Role (e.g. via `approles-patch.json`):
   - **Value:** `IdentityAdmin`
   - **Allowed member types:** Users/Groups (and/or Applications)
2. **Assign** the role to the admin user(s) under *Enterprise Applications →
   your app → Users and groups*.
3. The admin signs in and acquires a token; the role appears in the token's
   **`roles`** claim. (The server reads **only `roles`** — never `groups`.)

> To rename the role, set `entra_admin_role` in YAML or
> `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_ENTRA_ADMIN_ROLE`. Setting it empty
> (`null`) **disables** the admin API → callers get `503`.

### Static mode — set `admin_api_key`

The admin key is a credential **separate from the data `api_keys`**, and it is
the only thing that can call `/admin/*`. It follows the **same config pattern**
as the rest of the settings — a YAML field and/or an env override (env wins):

```yaml
# server-config.yaml  (the same CONFIG_FILE that carries api_keys)
admin_api_key: "<admin-key-placeholder>"
```

or, preferred for production (sourced from Key Vault):

```bash
export AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_ADMIN_API_KEY="<admin-key-placeholder>"
```

Generate a strong value: `openssl rand -hex 32`. An empty string is treated as
unset → the admin API is **disabled** (`503`). Regular data keys authenticate to
the data API but receive **403** on `/admin/*`.

---

## 3. The admin endpoints

All routes are under the `/admin` prefix and require admin authority (§2):

- **Entra mode:** a bearer token whose `roles` claim contains the `IdentityAdmin`
  role.
- **Static mode:** the **`admin_api_key`** sent as the bearer token.

In static mode, the entra endpoints return `503`, and vice-versa (the store for
the inactive mode is not loaded).

### Entra identities — `auth_mode=entra`

| Method & path | Body | Success |
|---|---|---|
| `PUT /admin/identities/{oid}` | `{"id": "<contributor>", "display_name": "<optional>"}` | `200 {"oid","id"[,"display_name"]}` |
| `DELETE /admin/identities/{oid}` | — | `200 {"oid","deleted":true}` (`404` if absent) |
| `GET /admin/identities` | — | `200 {"identities":[{"oid","id"[,"display_name"]}]}` |

- `{oid}` must be a **lowercase-hex GUID** (`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`),
  not the all-zeros sentinel → otherwise `422`.

### Static API keys — `auth_mode=static`

| Method & path | Body | Success |
|---|---|---|
| `PUT /admin/keys/{sha256hash}` | `{"id": "<contributor>"}` | `200 {"hash","id"}` |
| `DELETE /admin/keys/{sha256hash}` | — | `200 {"hash","deleted":true}` (`404` if absent) |
| `GET /admin/keys` | — | `200 {"keys":[{"hash","id"}]}` |

- `{sha256hash}` is the **SHA-256 hex digest** (exactly 64 lowercase hex chars)
  of an externally-generated key → otherwise `422`. The server never sees or
  stores the raw key; **raw keys are never returned** by `GET /admin/keys`.

The contributor `id` must be a **non-empty, non-whitespace** string, **≤ 256
chars**, with no null bytes → otherwise `422`.

---

## 4. Onboarding runbook

### Entra (`auth_mode=entra`)

1. The new user calls the API. Because their `oid` is not yet mapped, they get a
   **403 that names their `oid`**. They send that `oid` to you.
   (They can also self-fetch it: `az ad signed-in-user show --query id -o tsv`.)
2. You register it (replace placeholders; `$SERVER` is the server base URL,
   `$ADMIN_TOKEN` is your `IdentityAdmin` bearer token):

   ```bash
   OID="aaaaaaaa-0000-0000-0000-000000000001"   # the user's real oid
   curl -sS -X PUT "$SERVER/admin/identities/$OID" \
     -H "Authorization: Bearer $ADMIN_TOKEN" \
     -H "Content-Type: application/json" \
     -d '{"id": "<contributor>", "display_name": "<optional>"}'
   ```
3. The user authenticates **immediately** on their next request — no redeploy.

### Static (`auth_mode=static`)

1. Generate a key, derive its hash, and register the **hash** (never the key):

   ```bash
   RAW_KEY="$(openssl rand -hex 32)"
   HASH="$(printf '%s' "$RAW_KEY" | sha256sum | cut -d' ' -f1)"

   curl -sS -X PUT "$SERVER/admin/keys/$HASH" \
     -H "Authorization: Bearer $ADMIN_API_KEY" \
     -H "Content-Type: application/json" \
     -d '{"id": "<contributor>"}'
   ```
2. Hand the **raw key** (`$RAW_KEY`) to the user over a **secure channel**. They
   use it as their bearer token. The server only ever stores the hash.

---

## 5. Offboarding

Delete the entry — effective on the next request, immediately:

```bash
# Entra
curl -sS -X DELETE "$SERVER/admin/identities/$OID" \
  -H "Authorization: Bearer $ADMIN_TOKEN"

# Static
curl -sS -X DELETE "$SERVER/admin/keys/$HASH" \
  -H "Authorization: Bearer $ADMIN_API_KEY"
```

Confirm the removal with a `GET`:

```bash
curl -sS "$SERVER/admin/identities" -H "Authorization: Bearer $ADMIN_TOKEN"   # entra
curl -sS "$SERVER/admin/keys"       -H "Authorization: Bearer $ADMIN_API_KEY" # static
```

---

## 6. Status codes the operator will see

| Code | Meaning | Fix |
|---|---|---|
| **401** | No / invalid bearer token. Authentication runs (in the middleware) on `/admin/*` before authorization. | Send a valid token: the `IdentityAdmin` token (entra) or the `admin_api_key` (static). |
| **403** | Authenticated, but **not an admin**. Entra: token lacks the required role — the message names it (`IdentityAdmin`). Static: a *data* key was used — the message says to use the **admin key**. | Entra: assign the `IdentityAdmin` App Role and re-acquire the token (ensure it lands in `roles`, not `groups`). Static: use the configured `admin_api_key`. |
| **503** | Admin API **not configured for this mode**. Static: `admin_api_key` is unset. Entra: `entra_admin_role` is empty. (Also returned if you hit the *other* mode's endpoints — the store for an inactive mode is not loaded.) | Configure the credential per §2, or call the endpoints for the **active** `auth_mode`. |
| **409** | You tried to **delete or shadow the admin key** via `/admin/keys/{hash}`. The admin key is a config-level credential and is protected as the bootstrap floor. | Don't manage the admin key through the data store. To rotate it, update `admin_api_key` in config and restart. |
| **422** | **Bad path param or body.** Invalid OID (not a lowercase-hex GUID, or all-zeros), invalid hash (not exactly 64 lowercase hex chars), or invalid contributor `id` (empty/whitespace, > 256 chars, or null bytes). | Correct the value. Use the real oid (`az ad signed-in-user show --query id -o tsv`) or a real SHA-256 digest, and a sane contributor id. |
| **404** | `DELETE` of an entry that isn't present. | No action needed — the mapping already doesn't exist. |

---

## 7. Checking state

`GET /status` is unauthenticated and surfaces config-level booleans only (no
credentials, no hashes). Use it to confirm the admin API is live:

```bash
curl -sS "$SERVER/status" | jq '.auth'
```

```jsonc
// static mode
{ "mode": "static", "admin_api_enabled": true }

// entra mode
{ "mode": "entra", "admin_api_enabled": true, "entra_admin_role": "IdentityAdmin" }
```

- `auth.mode` — the active `auth_mode`.
- `auth.admin_api_enabled` — `true` once the admin credential for that mode is
  configured (`admin_api_key` set, or `entra_admin_role` non-empty). If this is
  `false`, every `/admin/*` call returns `503`.

---

## 8. How resolution stays fresh (no cache) & durability

**There is no cache and no TTL.** Each map is an **in-process live dictionary**
that the active resolver (`StaticKeyResolver` / `EntraResolver`) holds **by
reference**. A `/admin/*` `PUT`/`DELETE` mutates that same dict, so the change is
visible to the resolver on the **very next request** — there is nothing to expire
or invalidate.

- **Commit order (write-file-then-swap-memory).** A mutation first writes the JSON
  file atomically (tempfile → `os.replace`), and **only on success** updates the
  in-process dict. The file is therefore never behind memory; a mid-write crash
  cannot leave memory ahead of disk.
- **Single replica is why this is safe and simple.** The pilot runs `maxReplicas=1`
  (the drainer is a single writer), so there is no second process whose copy of the
  map could go stale — the one in-process map *is* the source of truth. If a future
  read-tier (M3) adds replicas, it would introduce a short TTL / poll re-read of the
  store file; **that does not exist today** and is not needed at one replica.
- Both maps live on the **`/data` Azure Files volume** and survive restarts and
  redeploys. Loads are **fail-closed**: a corrupt store starts empty and loud
  rather than crash-looping.
- The **admin authority itself cannot be removed via the API**:
  - **Static:** the configured `admin_api_key` is a config credential, not a
    data-store entry — it cannot be deleted or shadow-bound through
    `/admin/keys/{hash}` (`409`). Rotate it by editing config and restarting.
  - **Entra:** the `IdentityAdmin` App Role lives in the **App Registration**
    (Azure-side); it is not editable through this API.

---

## Related

- [entra-auth-setup.md](entra-auth-setup.md) — standing up `auth_mode=entra`.
- [managing-api-keys.md](managing-api-keys.md) — static-key bootstrap and rotation.
