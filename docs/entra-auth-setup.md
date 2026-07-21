# Microsoft Entra (Azure AD) authentication

> **Audience:** operators standing up the server with `auth_mode=entra`, and developers
> calling the API with an Entra bearer token.
>
> **Secret hygiene — read first.** This document uses **placeholder** identifiers
> only (e.g. `aaaaaaaa-0000-0000-0000-000000000001`). The `entra_identities` map
> contains Azure **Object IDs (oid)** tied to real people. **Never commit real oids,
> client IDs, or tenant IDs to a shared or public repo.** Inject them via environment
> variables / a secret store, or a **git-ignored** config file. See §2 (PII warning).

> To add/remove users at runtime without redeploying, see [docs/identity-management.md](identity-management.md).
>
> For a concise cross-mode reference — the two identity maps, how admin is
> authorized in each mode, what gates data vs. `/admin/*` routes, and the
> **empty-map bootstrap sequence** — see [docs/auth-flows.md](auth-flows.md).

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
every node they write. Entra mode authenticates **two kinds of token** along
separate paths — delegated **user** tokens (the chain above) and **service**
(app / managed-identity) tokens authorized by an Entra **App Role** — see
[Authentication model](#the-model--two-authentication-paths-user--service) below
for the full dual-path model. The two *auth modes* (`static`/`entra`) remain
mutually exclusive — exactly one resolver is active at a time. Switching is a
one-line config change (`auth_mode: static` → `auth_mode: entra`) plus the
supporting fields below.

All config is read by Pydantic Settings (`config.py`): **environment variables**
(prefix `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_`) take precedence over the **YAML
config file**, which takes precedence over defaults.

---

## Authentication model & Entra App Registration

> Read this before the operator steps below — it explains the model the rest of the
> document configures. The operator and developer guides (§2, §3) are the concrete
> steps; this section is the *why* they take the shape they do.

### The model — two authentication paths (user + service)

> **Canonical statement.** This is the single source of truth for "which Entra
> tokens the server accepts." Other docs (README, AGENTS.md) point here rather
> than restating it.

In entra mode the server authenticates **two kinds of token** along separate
paths inside `EntraResolver.resolve()` (`auth.py`). After the shared JWT
validation (signature / audience / issuer / `tid`, below), a single
**discriminator** picks the path from the token's `scp` and `idtyp` claims:

| Token shape | Path | How it is authorized | `created_by` |
|---|---|---|---|
| **`scp` present** (and `idtyp != "app"`) | **User (delegated)** — *unchanged* | `scp` must contain `access_as_user`; then `oid` → `entra_identities` map | the mapped contributor `id` |
| **`scp` absent** | **Service (app / daemon / managed-identity)** — *new* | an **App Role** alone: `roles` must contain `Contributor`, `Reader`, or `IdentityAdmin` | `service_identities[oid]` if mapped, else the stable `appid` |
| **`scp` present *and* `idtyp == "app"`** | — | anomalous (no legitimate Entra token is both) → **401**, fail-closed | — |

- **User path — delegated, byte-for-byte unchanged.** A token Entra issues **in
  the context of a signed-in person** carries `scp` (always present on a
  delegated token). It must contain `access_as_user`; the `oid` is then looked
  up in `entra_identities`. A valid user token whose `oid` is **not** mapped is a
  **403**. This path behaves exactly as it did before service tokens existed.

- **Service path — authorized by an App Role alone.** An **app-only token**
  (client-credentials flow, a managed identity, or a federated-OIDC workload)
  carries **`roles`, not `scp`**. The server admits it **iff** its `roles` claim
  contains a qualifying App Role:
  - **`Contributor`** (default `service_data_role`) — **write + read**.
  - **`Reader`** (default `reader_role`) — **read only**: `POST /cypher` and
    `GET /blobs/*` (not `POST /events`).
  - **`IdentityAdmin`** (default `entra_admin_role`) — the `/admin/*` map API.

  A service token with **no** qualifying role is a **403** whose body names the
  rejected principal (its `appid`/`oid`) and the required roles. The role
  assignment **is** the authorization decision — there is no server-side
  allow-list of service principals and no pre-registration step.

> **`service_identities` is *not* an auth gate.** It is an **optional, static**
> `oid → {id: <contributor>}` map (env/YAML, same shape as `entra_identities`)
> that only supplies a **friendly `created_by` name**. An unmapped but
> role-bearing service is fully authorized; its `created_by` simply falls back to
> the stable `appid`. There is **no** runtime `/admin/services` endpoint — service
> identities change only by editing config and redeploying.

> **Tenant policy note.** In a locked-down tenant that blocks client secrets,
> service callers must obtain tokens via **Managed Identity** or **federated
> OIDC**, **not** a client secret/certificate. See
> [docs/azure-deployment.md](azure-deployment.md#deploying-with-entra-auth).

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
| Required claims | `exp`, `iss`, `aud` must be present | `options={"require": ["exp","iss","aud"]}` |

The next checks are **path-specific** — selected by the `scp`/`idtyp`
discriminator (see the model table above):

| Path | Check | Value | Code |
|---|---|---|---|
| Discriminator | Token type | `scp` present → user; `scp` absent → service; `scp` + `idtyp=="app"` → 401 | `has_scp` / `idtyp` |
| User | Scope (`scp`) | must contain **`access_as_user`** | `"access_as_user" in scp.split()` |
| User | Object ID (`oid`) | looked up in `entra_identities` → `created_by` | `identity_map[oid.lower()]` |
| Service | App Role (`roles`) | must contain `Contributor`, `Reader`, **or** `IdentityAdmin` | `service_data_role`/`reader_role`/`entra_admin_role in roles` |
| Service | Identity (`created_by`) | `service_identities[oid]` if mapped, else `appid` (never `app_displayname`) | truthiness chain |

A valid **user** token whose `oid` is **not** in the map is a **403** (identity
unbound). A valid **service** token with **no** qualifying App Role is a **403**
(named principal + required roles). Any other failure is a **401**.

> **`created_by` legend — a GUID means a machine.** When `created_by` is a **GUID**
> it is an **`appid`** (a service principal's application ID) — i.e. a **machine**
> identity, resolvable in Entra by that app id. A **friendly** `created_by` name
> appears only when the service's `oid` is present in the optional
> `service_identities` map. (Delegated **users** always resolve to the friendly
> contributor `id` from `entra_identities`.)

### Admin authority and service roles — the `roles` claim

The token's **`roles`** claim (Entra App Role assignments) now drives **two**
things:

- **Service data authorization (new).** On the service path, a qualifying role —
  `Contributor` (`service_data_role`, write+read) or `Reader` (`reader_role`,
  read-only) — is the **sole** authorization gate. This has **no effect on the
  user path**: a delegated user token authorizes via its mapped `oid` regardless
  of `roles`.
- **Admin authority (unchanged).** A token whose `roles` contains the App Role
  named by `entra_admin_role` (**default `IdentityAdmin`**) may call `/admin/*`;
  any other valid token gets **403** there. This holds for **both** paths.

The server reads **only `roles`** — never `groups` (group membership can never
grant access). Full runtime onboarding/offboarding API and runbook:
[identity-management.md](identity-management.md).

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
delegated scopes uniformly. **Which path you get depends on how the credential
resolves:**

- **User-context credential** — `AzureCliCredential` (the same `az login`), the
  VS Code sign-in, or an interactive browser login — yields a **delegated**
  token (`scp=access_as_user`) that takes the **user path**.
- **App-only credential** — a **Managed Identity**, or `EnvironmentCredential`
  with a client secret/cert — yields a token with the right `aud` but **no
  `scp`** (it carries **`roles`**). This now takes the **service path**: with a
  qualifying App Role (`Contributor`/`Reader`/`IdentityAdmin`) it authenticates
  as a service; **without** any qualifying role it is a **403**.

### Service callers — app / daemon / managed-identity tokens

> **App-only credentials are now supported via the service path.** A **Managed
> Identity**, a **federated-OIDC workload**, or (where the tenant allows) a
> **service principal with a secret/cert** produces a token carrying **`roles`,
> not `scp`**. The server admits it **iff** its `roles` claim holds a qualifying
> App Role — `Contributor` (write+read), `Reader` (read-only), or `IdentityAdmin`
> (admin). The App Role assignment *is* the authorization; there is no
> server-side allow-list and no pre-registration of the service principal.
>
> **Behavior change (was 401, now 403).** Before M2, any token with `roles` and
> no `scp` failed the `access_as_user` check and was rejected with **401**. Now
> such a token reaches the service path: with a qualifying role it is **admitted**;
> with **no** qualifying role it is a **403** that names the principal and the
> required roles. See [What 401 vs 403 mean](#35-what-401-vs-403-mean-to-you).
>
> **Onboarding a service caller:** see
> [identity-management.md → service callers](identity-management.md#service-callers-entra-app-tokens).

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
| Identity map | `entra_identities` | `ENTRA_IDENTITIES` (JSON) | `oid → {id: <contributor>}` (the **user** path) |

These four boot the **user** path. The **service** path needs **no required
settings** — it works out of the box once an App Role is assigned in Entra. Its
settings are all **optional** and have working defaults:

| Field | YAML key | Env var (`AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_` + …) | Default | Meaning |
|---|---|---|---|---|
| Service data role | `service_data_role` | `SERVICE_DATA_ROLE` | `Contributor` | App Role granting service **write + read**. `""`/`null` disables it. |
| Reader role | `reader_role` | `READER_ROLE` | `Reader` | App Role granting service **read-only** (`POST /cypher`, `GET /blobs/*`). `""`/`null` disables it. |
| Service identities | `service_identities` | `SERVICE_IDENTITIES` (JSON) | *(unset)* | **Optional** `oid → {id: <contributor>}` map — a friendly `created_by` override only, **not** an auth gate. Unmapped services still authorize (via App Role); their `created_by` falls back to `appid`. No runtime CRUD — edit config and redeploy. |

> `service_identities` is validated with the **same** GUID-key / non-empty-`id`
> rules as `entra_identities`, but it is **never** required for boot and never
> participates in the entra startup validator. Omit it entirely if you don't need
> friendly machine names.

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

The config validators (`config.py`) enforce the map shape **at startup**. Required
fields are still hard-required, and a *malformed* map is still fatal — there is no
silent fail-open. **But an empty or omitted identity map is NOT an error:** it is a
supported **bootstrap** state (see §2.5.1).

**`azure_client_id` and `azure_tenant_id` are still REQUIRED** in `auth_mode=entra`.
Their absence is a hard startup error.

**On success:** the server starts normally and entra auth is active. (No special
log line — a clean boot means the validators passed. If the effective identity map
is empty, the server logs a loud bootstrap WARNING; see §2.5.1.)

**On misconfiguration**, the server raises and exits with one of these (the message
names both the env var and the YAML key):

- Missing required field(s) — a single combined message (note: only
  `azure_client_id` / `azure_tenant_id` appear here now; `entra_identities` is no
  longer required):

  ```
  Entra auth misconfiguration (startup refused): azure_client_id is required when
  auth_mode='entra'; set AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_AZURE_CLIENT_ID or
  azure_client_id in the config file; azure_tenant_id is required when
  auth_mode='entra'; set AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_AZURE_TENANT_ID or
  azure_tenant_id in the config file
  ```

The following map-*shape* violations are still fatal (a present-but-malformed entry
is a mistake, never a bootstrap state):

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
> server with **no** identity map configured **still boots** — but **fail-CLOSED**:
> it logs a loud startup WARNING and every **delegated (human)** token receives a
> **403** until identities are onboarded (see §2.5.1). The **only** way to make the
> server pass every request through **unauthenticated** is the explicit
> `allow_unauthenticated=true` opt-out (which itself logs a loud "WIDE OPEN"
> warning) — it exists only for the test harness / local dev, **never set it in
> production.** Note: in `auth_mode=entra` the opt-out has **no effect** — entra is
> always authentication-enabled, so an empty map fail-closes regardless.

### 2.5.1 Boot with an EMPTY map — the supported bootstrap state

An **empty or omitted** `entra_identities` map is a first-class **bootstrap** state,
not an error. On a fresh `/data` volume the server **boots and serves**, and logs:

```
entra identity map is EMPTY at startup (0 bound oids) — server is UP and serving,
but every delegated (human) token will receive 403 until identities are onboarded.
Bind the first user with an IdentityAdmin-role token via
PUT /admin/identities/{oid} (store=/data/identity/entra-identities.json).
This is expected on a fresh /data volume.
```

While the map is empty:

- **Data/non-admin routes** (e.g. `POST /events`) are **hard-gated by map
  membership** — a valid delegated token whose oid is unmapped gets **403**.
- **`/admin/*` routes are NOT gated by map membership.** They are authorized by
  **role** (the `IdentityAdmin` App Role in the token's `roles` claim). This is the
  **admin-path bootstrap exemption**: an `IdentityAdmin` role-holder can call
  `PUT /admin/identities/{oid}` to bind the **first** identity **even when their own
  oid is not yet in the map**. No token-authenticity check is relaxed — only the
  oid→id map-membership lookup, and only on `/admin/*`.

So the day-zero sequence is: **boot empty → an `IdentityAdmin` token calls
`PUT /admin/identities/{oid}` → the first identity is bound → that user (and any
others onboarded the same way) can now use the data API.** A config-file seed is
**optional** (see §2.6, step 3).

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
3. Bind it. Two paths:
   - **Runtime, no restart (preferred — and the primary bootstrap path):**
     `PUT /admin/identities/{oid}` with an `IdentityAdmin` token — effective on the
     user's **next request**. This works even on a **completely empty** map: an
     `IdentityAdmin` role-holder can onboard the **first** identity **even when
     their own oid is not yet mapped** (the `/admin`-path bootstrap exemption).
     Full runbook: [identity-management.md](identity-management.md).
   - **Config + restart (OPTIONAL seed):** add `"<oid>": {id: <contributor>}` to
     `entra_identities` and restart. A config seed is **no longer required** — it
     only pre-populates an initially-empty store. The primary path is boot-empty +
     the `/admin` API above.

The next call from that user → `created_by = <contributor>`.

---

## 3. Developer guide — call the API with an Entra bearer

**Goal:** from zero to a first authenticated call in under 15 minutes.

### 3.1 The scope

**As a human developer** you use the **user path**: a **delegated** token
carrying the scope **`access_as_user`** on the resource
**`api://<AZURE_CLIENT_ID>`**. The `az` commands below are the relied-on,
out-of-the-box path. (Headless **service** callers use a different path — an
App-Role-bearing app token with no scope; see
[Authentication model](#the-model--two-authentication-paths-user--service) and
[service callers](identity-management.md#service-callers-entra-app-tokens).) See
[Authentication model & Entra App Registration](#authentication-model--entra-app-registration)
for the full dual-path model and the `DefaultAzureCredential` `/.default` caveat.

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

> Health/monitoring and API-docs paths are exempt from auth and need no token.
> The server is **headless** (API-only), so there is a single fixed exempt set —
> `{/status, /version, /docs, /openapi.json}` — with no `web_ui_enabled` branch and
> no exempt prefixes (the `/static` mount was removed). `/docs` (Swagger UI) and
> `/openapi.json` are **always on**. See the `_EXEMPT_PATHS` frozenset in `auth.py`.

### 3.5 What 401 vs 403 mean **to you**

`401` is always a **token problem**; `403` is always an **authorization problem**
(the token is valid, but you lack the binding/role). Note that "missing
`access_as_user`" is **no longer a universal 401 cause** — it only applies on the
**user path** (a token that *has* `scp`). A token with **no** `scp` is routed to
the service path instead, where the question is App Roles, not scope.

| Status | Path | Meaning | What to do |
|---|---|---|---|
| **401** | both | **Token problem.** Missing / expired / wrong audience / wrong tenant / missing `oid`; or a **user** token whose `scp` lacks `access_as_user`; or an **ambiguous** token (`scp` *and* `idtyp=="app"`). | Re-acquire the token (§3.3). Confirm `--resource api://<AZURE_CLIENT_ID>`. |
| **403** | user | **Identity not bound.** Your delegated token is valid, but your `oid` isn't in the operator's map. The body names your oid. | Send your oid (§3.2) to the operator to be added (§2.6). |
| **403** | service | **No qualifying App Role.** Your app/service token is valid, but its `roles` has none of `Contributor`/`Reader`/`IdentityAdmin`. The body names your `appid`/`oid` and the required roles. | Have an admin assign the App Role in Entra ([service callers](identity-management.md#service-callers-entra-app-tokens)). |

> **Behavior change (M2): 401 → 403 for role-less app tokens.** Before M2, an
> app-only token (with `roles`, no `scp`) failed the `access_as_user` check and
> got **401**. It now reaches the service path: with a qualifying role it is
> admitted; with **no** qualifying role it is a **403** (named principal +
> required roles) instead of a 401. If you previously saw 401 for a daemon/MI
> caller, expect **403** now — and the fix is an **App Role assignment**, not a
> token re-acquire.

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
  `service_identities`, `service_data_role`, `reader_role`, `entra_admin_role`,
  `allow_unauthenticated`, `build_identity_map()`, `build_service_identity_map()`).
- JWT validation: `context_intelligence_server/auth.py` (`EntraResolver`) — RS256
  pinned, audience `[<client_id>, api://<client_id>]`, issuer
  `https://login.microsoftonline.com/<tenant_id>/v2.0`, explicit `tid`; then the
  **dual-path discriminator** on `scp`/`idtyp`:
  - **User** (`scp` present): `scp` must contain `access_as_user`, `oid` →
    contributor; **403** when the `oid` is unbound.
  - **Service** (`scp` absent): `roles` must contain `Contributor`/`Reader`/
    `IdentityAdmin`; `created_by` = `service_identities[oid]` → `appid` → `azp` →
    `oid` (never `app_displayname`); **403** when no qualifying role.
  - `scp` + `idtyp=="app"` → **401** (ambiguous, fail-closed).
  - **401** otherwise = invalid/expired/missing/wrong-audience/wrong-tenant token.
- Per-route capability gates: `context_intelligence_server/authz.py`
  (`require_write` on `POST /events`; `require_read` on `POST /cypher`,
  `GET /blobs/*`).
- Static-key mode: `docs/managing-api-keys.md`.
