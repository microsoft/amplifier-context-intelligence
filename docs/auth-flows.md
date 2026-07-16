# Auth flows — reference

> A concise, mode-by-mode reference for how the Context Intelligence Server
> authenticates and authorizes requests, what the two identity maps bind, how
> admin is authorized, what gates data vs. admin routes, and the **empty-map
> bootstrap** sequence. For the full entra operator guide see
> [entra-auth-setup.md](entra-auth-setup.md); for static keys see
> [managing-api-keys.md](managing-api-keys.md).

The server runs in exactly one `auth_mode` at a time: **`static`** or **`entra`**.

---

## 1. The two identity maps

| Mode | Map | Key (what's stored) | Value | Source of truth at runtime |
|---|---|---|---|---|
| `static` | **keystore** | `sha256(bearer_token)` — a 64-hex digest, never the raw token | contributor `id` | `api-keys.json` in `/data`, editable via `/admin/keys` |
| `entra` | **identity map** | Entra **oid** (Azure Object ID, lowercased GUID) | contributor `id` | `entra-identities.json` in `/data`, editable via `/admin/identities` |

Each map binds a **credential → contributor id**. On a successful data write the
contributor id is stamped as the write-once `created_by` provenance field.

- **static keystore** binds a *pre-shared token's digest* to a contributor.
- **entra identity map** binds a *person's Entra oid* to a contributor. The oid is
  extracted from a cryptographically validated JWT — the map does **not** perform
  any authenticity check, only the oid→id lookup.

---

## 2. How ADMIN is authorized (per mode)

`/admin/*` endpoints (`/admin/identities`, `/admin/keys`) are the **population
path** for the maps above. Admin authority is **by ROLE**, independent of whether
the caller's own credential is in the map:

| Mode | Admin credential | How it's checked |
|---|---|---|
| `static` | **admin api-key** (`admin_api_key_sha256`, or legacy `admin_api_key`) | The middleware compares `sha256(bearer)` to the admin digest **before** the data keystore lookup. A match ⇒ `is_admin=True`. The admin key is a **config credential**, not a data keystore entry. |
| `entra` | **`IdentityAdmin` App Role** | The JWT's `roles` claim must contain the configured `entra_admin_role` (default `IdentityAdmin`). Checked by the `require_admin` dependency. Only `roles` — never `groups` — grants admin. |

If the admin capability is unconfigured, `/admin/*` returns **503** (disabled), not
403. A wrong/absent admin credential returns **401/403**.

---

## 3. What gates DATA routes vs. ADMIN routes

| Route class | Gated by map membership? | Authorized by |
|---|---|---|
| **Data / non-admin** (`POST /events`, blobs, `/cypher`, …) | **YES** — hard-gated | A valid credential whose digest/oid **is in the map**. An unmapped-but-valid entra token → **403**; an unregistered static token → **401**. |
| **`/admin/*`** | **NO** — exempt from map membership | **Role** only (§2). This is the bootstrap exemption: an admin can populate an empty map even when their own credential isn't in it yet. |

**Empty map ⇒ fail-CLOSED.** With an empty keystore/identity map, every **data**
request 401/403s until the map is populated. The server still **boots** (it does not
refuse to start) and logs a loud startup WARNING.

**Security scope of the admin exemption (entra):** the `/admin`-path exemption
relaxes **only** the oid→id map-membership lookup. Every JWT authenticity check
(signature, issuer, audience, expiry, tenant, `access_as_user` scope, oid presence)
is still enforced. On any non-admin path the exemption does not apply, so an
unmapped oid is still a hard 403.

**The ONLY fail-open path** is the explicit `allow_unauthenticated=true` opt-out
(test/dev only) combined with no credentials configured — which passes **every**
request through unauthenticated and logs a loud "WIDE OPEN" warning at startup. In
`auth_mode=entra` the opt-out has no effect (entra is always auth-enabled).

---

## 4. Empty-map bootstrap sequence

### 4.1 Entra mode

```
1. Boot on a fresh /data volume with entra_identities empty/omitted.
   → server is UP, logs "entra identity map is EMPTY at startup (0 bound oids)".
   → azure_client_id / azure_tenant_id are still REQUIRED (absence = startup error).
2. A holder of the IdentityAdmin App Role calls:
       PUT /admin/identities/{oid}   Authorization: Bearer <IdentityAdmin JWT>
   → admitted to /admin even though the caller's own oid is NOT yet mapped
     (admin-path exemption); require_admin authorizes on the `roles` claim.
   → the first oid→contributor binding is written to the live map.
3. That user's next DATA request (POST /events) now resolves oid → contributor.
4. Onboard everyone else the same way (PUT /admin/identities/{oid}).
   Meanwhile any unmapped delegated token still 403s on data routes.
```

### 4.2 Static mode

```
1. Boot on a fresh /data volume with api_keys empty/omitted, AND an admin
   credential configured (admin_api_key_sha256 recommended).
   → server is UP, logs "static keystore is EMPTY at startup (0 bound keys)".
   → every data request 401s (fail-closed).
2. The admin-key holder calls:
       PUT /admin/keys/{sha256hash}  Authorization: Bearer <admin token>
   → admin-key fast-path sets is_admin=True (admin key is not in the keystore);
     require_admin passes; the first digest→contributor binding is written.
3. A data request bearing the matching raw token now authenticates.
4. Add more peers the same way (PUT /admin/keys/{sha256hash}).

If NO admin credential is configured, the empty keystore cannot be bootstrapped
at runtime (the /admin API is itself unreachable — every token 401s at the
middleware before require_admin runs). Then the only path is config-and-restart:
add api_keys in config and restart.
```

---

## 5. One-line summary

- **Two maps:** static `sha256(token)→id` keystore; entra `oid→id` identity map.
- **Admin auth:** static admin api-key (fast-path) vs. entra `IdentityAdmin` App Role.
- **Gating:** data routes require map membership; `/admin/*` is role-authorized and
  exempt from map membership (the bootstrap path).
- **Empty map:** boots **fail-closed** + warns; populated at runtime via `/admin`.
- **Only fail-open:** explicit `allow_unauthenticated=true` opt-out (test/dev), warned.
