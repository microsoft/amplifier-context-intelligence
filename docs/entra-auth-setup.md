# Microsoft Entra (Azure AD) authentication

> **Audience:** operators standing up the server with `auth_mode=entra`, and developers
> calling the API with an Entra bearer token.
>
> **Secret hygiene — read first.** This document uses **placeholder** identifiers
> only (e.g. `aaaaaaaa-0000-0000-0000-000000000001`). The `entra_identities` map
> contains Azure **Object IDs (oid)** tied to real people. **Never commit real oids,
> client IDs, or tenant IDs to a shared or public repo.** Inject them via environment
> variables / a secret store, or a **git-ignored** config file. See §2 (PII warning).

---

## 1. Overview

The Context Intelligence Server authenticates every ingest request (`POST /events`)
and stamps the writer's identity onto the graph as a **write-once `created_by`**
provenance field. Two authentication modes exist, selected by the single
`auth_mode` setting:

| `auth_mode` | Credential | What the server checks |
|---|---|---|
| `static` (default) | Pre-shared bearer tokens | `sha256(token)` looked up in the `api_keys` keystore → contributor id |
| `entra` | Microsoft Entra JWT | RS256 signature (via Entra JWKS) + audience/issuer/tenant/scope, then `oid` → contributor id |

In **entra mode** the chain is:

```
az access token  →  server validates the JWT  →  extracts the oid claim
   →  oid → contributor (your entra_identities map)  →  created_by = <contributor>
```

So a real person's Azure identity is attributed to a stable contributor name on
every node they write. The two modes are mutually exclusive — exactly one resolver
is active at a time. Switching is a one-line config change (`auth_mode: static` →
`auth_mode: entra`) plus the supporting fields below.

All config is read by Pydantic Settings (`config.py`): **environment variables**
(prefix `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_`) take precedence over the **YAML
config file**, which takes precedence over defaults.

---

## Authentication model & Entra App Registration

> Read this before the operator steps below — it explains the model the rest of the
> document configures. The operator and developer guides (§2, §3) are the concrete
> steps; this section is the *why* they take the shape they do.

### The model — delegated (user) tokens only

As configured today, the server authenticates **delegated user tokens** — tokens
that Entra issues **in the context of a signed-in person**. The decisive check is
in `EntraResolver.resolve()` (`auth.py`): the token's **`scp`** claim must contain
**`access_as_user`**, and `scp` is a claim that **only ever appears on a delegated
(user-context) token**.

App-only / daemon tokens (client-credentials flow) are therefore **not accepted**
as configured: they carry a **`roles`** claim instead of `scp`, so they fail the
`access_as_user` check and are rejected with **401**. This is by design for the
pilot — see the limitation note at the end of this section.

### The App Registration setup (what is configured)

A **single Entra App Registration** backs the whole flow. It plays two roles at
once: it is **the protected API** (the resource the server validates tokens for)
*and* the registration whose **Application ID URI** the caller requests a token
against.

| Registration property | Value (placeholder) | Maps to server setting |
|---|---|---|
| Application (client) ID | `<AZURE_CLIENT_ID>` | `azure_client_id` |
| Directory (tenant) ID | `<AZURE_TENANT_ID>` | `azure_tenant_id` |
| Application ID URI | `api://<AZURE_CLIENT_ID>` | (the `--resource` callers request) |

**Expose an API** (Entra portal → *Expose an API*):

- **Application ID URI:** `api://<AZURE_CLIENT_ID>`.
- One **delegated** scope named **`access_as_user`**, with **"Who can consent:
  Admins and users."**
- The scope's **GUID is auto-generated and internal to the registration**. Callers
  **never reference the scope GUID directly** — they request the scope by its
  *value* (`access_as_user`) or, via the SDK, the resource's **`/.default`** form
  (see below). The GUID is plumbing; it never appears in client code or in this doc.

**What the server checks** (every claim below is enforced in `EntraResolver`,
`auth.py`):

| Check | Value (placeholder) | Code |
|---|---|---|
| Signature algorithm | **RS256** only (`alg=none`/HS256 rejected) | `algorithms=["RS256"]` |
| Signing key | tenant **JWKS**, eager-fetched at startup | `PyJWKClient(.../<AZURE_TENANT_ID>/discovery/v2.0/keys)` |
| Audience (`aud`) | one of `<AZURE_CLIENT_ID>` **or** `api://<AZURE_CLIENT_ID>` | `audience=[client_id, "api://"+client_id]` |
| Issuer (`iss`) | `https://login.microsoftonline.com/<AZURE_TENANT_ID>/v2.0` | `issuer=...` |
| Tenant (`tid`) | must equal `<AZURE_TENANT_ID>` (explicit, defense-in-depth) | `claims["tid"] == tenant_id` |
| Scope (`scp`) | must contain **`access_as_user`** | `"access_as_user" in scp.split()` |
| Object ID (`oid`) | looked up in `entra_identities` → `created_by` | `identity_map[oid.lower()]` |
| Required claims | `exp`, `iss`, `aud` must be present | `options={"require": ["exp","iss","aud"]}` |

A valid token whose `oid` is **not** in the map is a **403** (identity unbound);
any other failure is a **401**.

### How a token is obtained today — the Azure CLI

The relied-on path is the **Azure CLI as a signed-in user**:

```bash
az login                                                                 # sign in as a user
az account get-access-token --resource api://<AZURE_CLIENT_ID> \
  --query accessToken -o tsv
```

`--resource api://<AZURE_CLIENT_ID>` makes Entra mint an **access token for this
API**: a **delegated** token whose **`aud`** is the bare `<AZURE_CLIENT_ID>` and
whose **`scp`** contains `access_as_user`. Send it as a normal bearer header:

```
Authorization: Bearer <token>
```

**Via the Azure SDK (`DefaultAzureCredential`)** — request the **`/.default`**
scope, **not** the bare scope:

```python
from azure.identity import DefaultAzureCredential

cred = DefaultAzureCredential()
token = cred.get_token("api://<AZURE_CLIENT_ID>/.default").token   # NOT the bare scope
```

`/.default` is required because the SDK credential chain does not request bare
delegated scopes uniformly. **Important caveat:** this yields a server-compatible
(delegated, `scp=access_as_user`) token **only when the credential resolves to a
user-context credential** — `AzureCliCredential` (the same `az login`), the VS Code
sign-in, or an interactive browser login. If the chain instead resolves to an
**app-only** credential (Managed Identity, or `EnvironmentCredential` with a client
secret/cert), the resulting `/.default` token has the right `aud` but **no `scp`**
(it carries `roles`) and the server rejects it with 401 — see the limitation below.

### Limitation — delegated-only, by design today

> **App-only credentials are not supported as configured.** A **Managed Identity**,
> or a **service principal with a client secret / certificate** (the
> client-credentials flow), produces a token carrying **`roles`, not `scp`**. The
> server's `scp` must contain `access_as_user` check therefore **rejects it (401)**.
> Headless / daemon / service-to-service callers are **not** supported in this
> configuration; the pilot deliberately relies on the **user-context (Azure CLI)
> delegated** flow only.
>
> Supporting service identities later would require **adding an App Role** to the
> registration and **teaching the server to accept a `roles` claim** (in addition to
> `scp`). That is **explicitly not done today** — `EntraResolver` checks `scp` only
> (`auth.py`, "V1 scope: delegated USER tokens from the `az` CLI client").

---

## 2. Operator guide — configure `auth_mode=entra`

**Goal:** zero to a protected server in under 30 minutes.

### 2.1 Required settings

When `auth_mode=entra`, **all** of these are required (the server refuses to start
otherwise — see §2.5):

| Field | YAML key | Env var (`AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_` + …) | Meaning |
|---|---|---|---|
| Auth mode | `auth_mode` | `AUTH_MODE` | Set to `entra` |
| Client ID | `azure_client_id` | `AZURE_CLIENT_ID` | App Registration (client) GUID |
| Tenant ID | `azure_tenant_id` | `AZURE_TENANT_ID` | Azure AD tenant GUID |
| Identity map | `entra_identities` | `ENTRA_IDENTITIES` (JSON) | `oid → {id: <contributor>}` |

### 2.2 YAML config

```yaml
# server-config.yaml
auth_mode: entra
azure_client_id: "<AZURE_CLIENT_ID>"     # App Registration client GUID
azure_tenant_id: "<AZURE_TENANT_ID>"     # Azure AD tenant GUID

# oid → { id: <contributor> }.  The value carries ONLY `id` — no email, no name.
# Keys are Azure Object IDs (GUIDs); the `id` becomes `created_by` on the graph.
entra_identities:
  "aaaaaaaa-0000-0000-0000-000000000001":
    id: alice
  "aaaaaaaa-0000-0000-0000-000000000002":
    id: bob
  # Many oids → one contributor is fine: give each oid the same `id`
  # (e.g. a person with two AD identities).
  "aaaaaaaa-0000-0000-0000-000000000003":
    id: alice
```

### 2.3 Env-var equivalent (the production path)

The map is supplied as a **JSON string** in the env var. This is the path to prefer
when you do not want oids written to a config file on disk:

```bash
export AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_AUTH_MODE=entra
export AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_AZURE_CLIENT_ID='<AZURE_CLIENT_ID>'
export AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_AZURE_TENANT_ID='<AZURE_TENANT_ID>'
export AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_ENTRA_IDENTITIES='{
  "aaaaaaaa-0000-0000-0000-000000000001": {"id": "alice"},
  "aaaaaaaa-0000-0000-0000-000000000002": {"id": "bob"}
}'
```

> Env vars override YAML, so you can keep a placeholder `entra_identities` out of
> config entirely and supply the real map only at runtime via the env var or a
> secret store.

### 2.4 Getting a user's oid

Ask each developer to run `az ad signed-in-user show --query id -o tsv` (§3.2) and
send you the value. To look it up yourself from their UPN:

```bash
az ad user show --id <user@tenant> --query id -o tsv
```

That GUID is the map **key**; you choose the contributor **value** (`id`).

> ⚠️ **PII / secret-hygiene warning.** An `oid` is a **persistent personal
> identifier** for a real person. The `entra_identities` map is therefore
> **sensitive** — treat it like a secret:
> - **Never** commit real oids to a shared/public repo. Git history has no erasure path.
> - Prefer **env-var / secret-store injection** (§2.3) or a **`.gitignore`-d** config file.
> - This product repo's docs and samples use **placeholder GUIDs only** (`aaaaaaaa-…`).

### 2.5 What the startup validator does (fail-closed)

The config validators (`config.py`) enforce the map shape **at startup** and the
server **refuses to boot** on misconfiguration — there is no silent fail-open.

**On success:** the server starts normally and entra auth is active. (No special
log line — a clean boot means the validators passed.)

**On misconfiguration**, the server raises and exits with one of these (the message
names both the env var and the YAML key):

- Missing required field(s) — a single combined message:

  ```
  Entra auth misconfiguration (startup refused): azure_client_id is required when
  auth_mode='entra'; set AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_AZURE_CLIENT_ID or
  azure_client_id in the config file; azure_tenant_id is required when
  auth_mode='entra'; …; entra_identities must be a non-empty map when
  auth_mode='entra'; provide at least one oid → {id: contributor} entry
  ```

- An **empty** identity map (`entra_identities: {}`) — rejected, not "auth off":

  ```
  entra_identities must contain at least one entry if specified; omit it or use
  null to disable Entra authentication
  ```

- A malformed oid key:

  ```
  entra_identities key '<value>' must be a valid GUID
  (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)
  ```

- The all-zeros placeholder GUID (a stray template value must never authorize anyone):

  ```
  entra_identities key '00000000-0000-0000-0000-000000000000' must not be the
  all-zeros GUID; use the real oid from 'az ad signed-in-user show --query id -o tsv'
  ```

- A bad value (`id` missing/empty/whitespace):

  ```
  entra_identities['<oid>']['id'] must be a non-empty, non-whitespace string, got <value>
  ```

> **Fail-closed by default.** `allow_unauthenticated` defaults to **`false`**. A
> server with **no** auth configured **refuses to start** (loud `RuntimeError`). The
> `allow_unauthenticated=true` opt-out exists only for the test harness / local dev —
> **never set it in production.**

### 2.6 The recovery loop — binding a new developer (a `403`)

When a developer with a **valid** token whose oid is **not** in your map calls the
API, they get a **403** whose body **names the unbound oid**:

```json
{
  "detail": "Identity not authorized: oid 'aaaaaaaa-0000-0000-0000-000000000009' is not in the identity map; contact the server administrator to add this identity (tenant '<AZURE_TENANT_ID>')"
}
```

To bind them:

1. Copy the oid out of the 403 body (or have them run `az ad signed-in-user show
   --query id -o tsv`).
2. **Verify** the oid maps to the right person (`az ad user show --id <oid> --query
   userPrincipalName -o tsv`) **before** adding it — see §4.1 (write-once is permanent).
3. Add `"<oid>": {id: <contributor>}` to `entra_identities`.
4. Restart the server. The next call from that user → `created_by = <contributor>`.

---

## 3. Developer guide — call the API with an Entra bearer

**Goal:** from zero to a first authenticated call in under 15 minutes.

### 3.1 The scope

The server accepts **delegated user** tokens carrying the scope
**`access_as_user`** on the resource **`api://<AZURE_CLIENT_ID>`** — see
[Authentication model & Entra App Registration](#authentication-model--entra-app-registration)
for the full model, including the delegated-only limitation and the
`DefaultAzureCredential` `/.default` caveat. The `az` commands below are the
relied-on, out-of-the-box path.

### 3.2 Give the operator your oid

```bash
az ad signed-in-user show --query id -o tsv
```

Send that GUID to the operator so they can bind it (§2.6). Until they do, you'll
get a **403**.

### 3.3 Get a token

```bash
az account get-access-token --resource api://<AZURE_CLIENT_ID> --query accessToken -o tsv
```

The token is a short-lived JWT (typically ~60–90 min). Re-acquire when it expires.

### 3.4 Call the API

The token goes in the `Authorization: Bearer` header. The event body needs a
`data.timestamp` (ISO-8601) for the graph write:

```bash
TOKEN=$(az account get-access-token --resource api://<AZURE_CLIENT_ID> --query accessToken -o tsv)

curl -sS -X POST https://<server>/events \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "event": "session_start",
    "workspace": "my-workspace",
    "data": {
      "timestamp": "2026-06-27T12:00:00Z",
      "session_id": "demo-session-1"
    }
  }'
# → 202 { "status": "queued", ... }
```

On success the event is queued and the resulting graph nodes are stamped
`created_by = <your contributor id>` (the `id` the operator mapped your oid to).

> Health/monitoring paths are exempt from auth and need no token: `/status`,
> `/version`, `/`, `/dashboard`, `/docs`, `/openapi.json`, plus `/static/*` and
> `/skills/*`. This is the full-web exempt set (`web_ui_enabled=true`, the default).
>
> **API-only mode (`web_ui_enabled=false`):** the exempt set shrinks to just
> `{/status, /version}`. The web-UI routes (`/`, `/dashboard`, `/docs`,
> `/openapi.json`) are not registered (→ 404), and `/logs/stream` becomes
> **auth-gated** — it is removed from the exempt set so it can no longer be reached
> as an unauthenticated log drain. See the `web_ui_enabled` field in `config.py`.

### 3.5 What 401 vs 403 mean **to you**

| Status | Meaning | What to do |
|---|---|---|
| **401** | **Token problem.** Missing / expired / wrong audience / wrong tenant / missing `access_as_user` scope / missing `oid`. | Re-acquire the token (§3.3). Confirm you used `--resource api://<AZURE_CLIENT_ID>`. |
| **403** | **Identity not bound.** Your token is valid, but your `oid` isn't in the operator's map. The body names your oid. | Send your oid (§3.2) to the operator to be added (§2.6). |

---

## 4. Operations runbook

### 4.1 Wrong-oid mapping is **permanent**

`created_by` is **write-once at write time** in Neo4j — once a node is written with
a contributor, it is **not** rewritten. A wrong `oid → contributor` mapping
therefore **mislabels attribution permanently** and is **not** self-healing in the
graph.

- **Always verify the oid before adding it:** `az ad user show --id <oid> --query
  userPrincipalName -o tsv` and confirm it's the right person.
- **Recovery** if a wrong mapping already wrote data: fix the map + restart **and**
  manually correct the already-written `created_by` values in the graph (a data
  operation — there is no automatic backfill).

### 4.2 JWKS / signing-key rotation

The server validates token signatures against Entra's published signing keys
(JWKS), fetched once **eagerly at startup** (a server with an unreachable JWKS
endpoint **refuses to start** — fail-closed) and cached thereafter (`PyJWKClient`,
~5-minute lifespan). Entra rotates signing keys roughly **every 6 weeks**.

- If a wave of **401s** appears immediately after a key rotation, the cache is
  briefly stale. **Wait one cache cycle (~5 min) or restart once, cleanly.**
- **Do not** restart repeatedly in a tight loop — that hammers the JWKS endpoint and
  doesn't speed recovery. One clean restart refreshes the keys.

### 4.3 Reading auth logs

Two distinct, greppable tags separate a normal token rejection from an unexpected
internal error. The raw bearer token is **never** logged (credential hygiene).

| Log tag | Level | Meaning | Grep |
|---|---|---|---|
| `auth_event=auth_denied` | **INFO** | A normal token rejection (401 or 403) — bad/expired/unbound token. Expected, not a server fault. The line includes the reason and status code. | `grep 'auth_event=auth_denied'` |
| `auth_event=resolver_unexpected_exception` | **ERROR** | An *unexpected* error inside `resolver.resolve()` (e.g. a transient library bug). The request is still denied fail-closed (401, never a 500) and a full stack trace is logged for investigation. | `grep 'auth_event=resolver_unexpected_exception'` |

A steady trickle of `auth_denied` is normal. Any `resolver_unexpected_exception` is
worth investigating — it means the resolver hit something it did not expect.

### 4.4 Events must include `data.timestamp` — rejected with `400` at ingest

Every ingested event must carry a `data.timestamp` (a non-empty ISO-8601 string).
This is **ingest payload validation, not auth**: `post_events` calls
`_validate_data_timestamp(request.data)` **before queuing**, so a missing, empty,
non-string, or non-ISO-8601 `data.timestamp` is rejected immediately with **HTTP 400**
— the event is never accepted (no 202), never queued, and never dead-lettered. The
response body names the field:

```json
{ "detail": "data.timestamp is required and must be a non-empty ISO-8601 string" }
```
```json
{ "detail": "data.timestamp must be a valid ISO-8601 string; got '<value>'" }
```

Real Amplifier clients **always** send `data.timestamp` (verified: 224,530 events on
disk, 0 missing), so this 400 only ever catches hand-rolled / `curl` test payloads —
it cannot reject legitimate traffic. As defense-in-depth, the graph drainer's
`make_node_id` additionally re-raises a **named** error if an unparseable timestamp
ever reaches it, so anything that somehow bypasses the ingest check dead-letters
legibly instead of as a bare `Invalid isoformat string: ''`.

**Operator action:** when smoke-testing with `curl`, include a valid `data.timestamp`
(see §3.4). A `400` here means a malformed payload, not an auth problem.

---

## Reference — accurate to the code

- Config fields & validators: `context_intelligence_server/config.py`
  (`auth_mode`, `azure_client_id`, `azure_tenant_id`, `entra_identities`,
  `allow_unauthenticated`, `build_identity_map()`).
- JWT validation: `context_intelligence_server/auth.py` (`EntraResolver`) — RS256
  pinned, audience `[<client_id>, api://<client_id>]`, issuer
  `https://login.microsoftonline.com/<tenant_id>/v2.0`, explicit `tid`, `scp` must
  contain `access_as_user`, `oid` → contributor; **401** = invalid/expired/missing/
  wrong-audience token, **403** = valid token whose `oid` is unbound.
- Static-key mode: `docs/managing-api-keys.md`.
