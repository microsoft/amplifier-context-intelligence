# M2 auth — live-token acceptance gate

> **Current status: EXECUTED — green on 2026-06-30.** The live-token
> acceptance gate ran successfully against a **real** Microsoft Entra app-only
> token from the corp tenant (`72f988bf-…`):
> [run 28479002620](https://github.com/microsoft/amplifier-context-intelligence/actions/runs/28479002620)
> — STEP A.1 reported `ALL CLAIM-SHAPE ASSERTIONS PASSED` and STEP A.2 reported
> `Resolver ACCEPTED the live token … is_service=True`. M2 is now **verified
> against reality**: a real app-only token has the shape `EntraResolver`
> assumes. Acquisition path: federated-OIDC self-call, no secret. One STEP A.1
> confidence assertion was deliberately broadened to match this tenant's actual
> token (no `idtyp`/`appidacr` emitted) — see §6. The offline synthetic-token
> unit tests remain the fast regression layer.

> **Secret hygiene.** This document uses **placeholder** identifiers only.
> Never commit real client IDs, tenant IDs, or object IDs to this repo — see
> the PII warning in [`entra-auth-setup.md`](entra-auth-setup.md).

---

## 1. Why this gate exists

`EntraResolver.resolve()` makes several assumptions about what a real
app-only Entra token looks like, derived from documentation and from the
Team Pulse precedent (`auth.py` docstring), **not** from having seen one:

- `scp` is **absent** on an app-only token (this is the primary
  discriminator that routes the token to the service branch instead of the
  user branch).
- `roles` carries the App Role assignment as a list of strings (never
  `groups`).
- `appid` / `azp` are present and stable, usable as a `created_by` fallback
  when no `service_identities` mapping exists.
- `idtyp == "app"` is *sometimes* present (only actually checked by the
  resolver in the `[B1]` ambiguous-token case — when `scp` is unexpectedly
  also present).

All of this is correct **per Microsoft's documentation**, but the only way
to be sure the design holds is to acquire one real token and check it. That
is the entire purpose of this gate — nothing more.

## 2. Why GitHub Actions OIDC (and not a client secret)

**The target tenant blocks client-secret and certificate credentials for
this purpose.** The only credential path available is **workload identity
federation**: GitHub Actions presents its own short-lived OIDC token to
Entra, and Entra exchanges it for an access token **based on a pre-configured
trust relationship** (a *federated identity credential*) — no secret is
issued, stored, or rotated anywhere. See
[`entra-auth-setup.md` → "Tenant policy note"](entra-auth-setup.md#the-model--two-authentication-paths-user--service)
and
[`azure-deployment.md`](azure-deployment.md#deploying-with-entra-auth).

This is also why the workflow is **`workflow_dispatch`-only**: it is a
manual, human-initiated acceptance check against a real tenant (and
optionally a real deployed server), not something that should run on every
push/PR.

## 3. Prerequisites

You need **tenant access** (or someone who has it) to complete these before
the workflow can do anything beyond skip with a warning.

### 3.1 A caller app registration with a GitHub-OIDC federated credential

Decide which Entra App Registration's service principal will act as the
**caller** — the identity GitHub Actions authenticates as. The simplest
setup (and the one this workflow defaults to) is **self-call**: use the
*same* App Registration that is the protected API (`AZURE_CLIENT_ID` /
`azure_client_id` in server config) as the caller too. If your tenant
requires a separate caller app, see §3.4.

> **Self-call is a test-gate convenience, not a production identity pattern.**
> Reusing the resource app as its own caller keeps this acceptance check to a
> single registration, which is fine for a one-shot gate. In production,
> callers should be **distinct** identities (separate app registrations or
> managed identities) per service, each assigned its own App Role — never the
> API impersonating itself. See §3.4 for the separate-caller setup.

On that app registration → **Certificates & secrets → Federated credentials
→ Add credential → GitHub Actions deploying Azure resources** (or the
generic OIDC federated-credential blade), set the **subject** to match this
repository's workflow.

> **Use the ID-based subject form.** GitHub OIDC subjects come in two
> spellings, and Entra matches the string **exactly** — a mismatch fails the
> `Azure login` step with `AADSTS7002381` (see [§4.1](#41-the-aadsts7002381-trap)).
> The **name-based** form (`repo:<org>/<repo>:…`) breaks the moment the org or
> repo is renamed and is the more common source of that error. Prefer the
> **ID-based** form, which uses immutable numeric identifiers:

| Scenario | Subject (ID-based, preferred) |
|---|---|
| Dispatched against a specific branch (e.g. `main`) | `repository_owner_id:<ORG_ID>:repository_id:<REPO_ID>:ref:refs/heads/main` |
| Scoped to a GitHub Environment named `<env>` | `repository_owner_id:<ORG_ID>:repository_id:<REPO_ID>:environment:<env>` |

Look up the two numeric IDs once:

```bash
gh api repos/microsoft/amplifier-context-intelligence \
  --jq '{owner_id: .owner.id, repo_id: .id}'
```

> **Branch vs. environment scope.** `workflow_dispatch` runs use whichever ref
> the operator picks at dispatch time, so a `:ref:` subject only matches if you
> always dispatch from that exact branch. An `:environment:` subject is
> independent of the ref. If you use it, add `environment: <env>` to the
> `acceptance-gate` job in the workflow file before running (a one-line edit —
> intentionally not parameterized, to keep the trigger surface minimal and
> auditable).

Audience: leave at the GitHub Actions default
(`api://AzureADTokenExchange`) — this is the OIDC *token-exchange* audience,
unrelated to the server's resource audience (`api://<AZURE_CLIENT_ID>`)
checked later by `EntraResolver`.

### 3.2 An Application App Role assignment (not User/Group)

The caller service principal must hold at least one of the server's
configured App Roles, assigned as an **Application**-type assignment (App
Role assignments come in `User`/`Group`/`Application` member types — only
`Application` is honored for app-only tokens; this is an Entra concept, not
something the server enforces, since it never sees the assignment, only the
resulting `roles` claim):

- **`Contributor`** (default `service_data_role`) — needed for STEP A and
  the optional STEP B (`POST /events` write probe).
- **`Reader`** (default `reader_role`) — only needed if you also want to run
  the optional negative test (§3.3 / STEP C).

If your server's `service_data_role` / `reader_role` are not the defaults,
set the matching optional repo Variables (§4) so the workflow checks for
the *actual* configured role names.

> **Also set "Assignment required?" = Yes on the enterprise application.**
> On the resource app's **Enterprise application → Properties**, set
> **Assignment required?** to **Yes** (the `appRoleAssignmentRequired: true`
> service-principal property). The server authorizes on the `roles` claim
> alone — there is no `azp`/`appid` allow-list — which is the *Entra-Enforced
> Assignment* posture: it is only sound when Entra itself refuses to issue a
> token to an **unassigned** caller. Without this toggle, any app in the
> tenant can request a token for this API; the server would still deny it
> (no qualifying role ⇒ 403), but the assignment gate belongs at the identity
> provider, not only in application code. Setting it is what makes
> "roles-only authorization" defensible rather than merely defense-in-depth.

### 3.3 (Optional) a second caller for the negative test

STEP C proves a **Reader-only** token is correctly **rejected with 403** on
a write route (`POST /events`). This requires a **second**, separate
caller service principal (or a second App Registration) that:

- has its **own** GitHub-OIDC federated credential (same subject rules as
  §3.1),
- is assigned **only** the `Reader` App Role (must **not** also hold
  `Contributor` — otherwise the negative test cannot fail correctly).

If you don't need this proof, skip it — set no `AZURE_READER_CLIENT_ID`
Variable and STEP C is skipped with a note in the job summary.

### 3.4 (Optional) a genuinely separate caller app

If your tenant's policy requires the caller to be a distinct registration
from the resource (rather than the self-call pattern in §3.1), register a
second app, give **it** the federated credential and the App Role
assignment *on the resource app*, and set `AZURE_CALLER_CLIENT_ID` to its
client ID (§4). The workflow uses
`AZURE_CALLER_CLIENT_ID || AZURE_CLIENT_ID` for the `azure/login` step, so
this is additive — omit it entirely for the self-call default.

### 3.5 Repo Variables (Settings → Secrets and variables → Actions → Variables)

| Variable | Required | Meaning |
|---|---|---|
| `AZURE_CLIENT_ID` | **Yes** | The API app registration's client ID (the resource). Server's `azure_client_id`. |
| `AZURE_TENANT_ID` | **Yes** | The Azure AD tenant ID. Server's `azure_tenant_id`. |
| `AZURE_CALLER_CLIENT_ID` | No (default: same as `AZURE_CLIENT_ID`) | Client ID of a separate caller app registration — only needed for the §3.4 setup. |
| `ACCEPTANCE_SERVER_URL` | No | Base URL of a **deployed** server to drive STEP B/C end-to-end (e.g. `https://ci-server.example.com`). Omit to run STEP A only (the core gate). |
| `AZURE_READER_CLIENT_ID` | No | Client ID of the §3.3 Reader-only caller. Enables STEP C. |
| `ACCEPTANCE_SERVICE_DATA_ROLE` | No (default `Contributor`) | Set only if the live server's `service_data_role` differs from the default. |
| `ACCEPTANCE_READER_ROLE` | No (default `Reader`) | Set only if the live server's `reader_role` differs from the default. |
| `ACCEPTANCE_ADMIN_ROLE` | No (default `IdentityAdmin`) | Set only if the live server's `entra_admin_role` differs from the default. |

No secret values are configured anywhere for this workflow — by design,
since federated OIDC is the whole point.

## 4. Running it

GitHub UI → **Actions** → **M2 Auth — Live-Token Acceptance Gate** → **Run
workflow**. No inputs to fill in; all configuration comes from the repo
Variables above. If `AZURE_CLIENT_ID` / `AZURE_TENANT_ID` are unset, the
`preflight` job prints a warning and the rest of the workflow is skipped
(not failed) — see the job summary for exactly which Variable is missing.

> **Must run from the canonical org repo.** Dispatch from
> `microsoft/amplifier-context-intelligence` on GitHub-hosted runners — that is
> what carries the enterprise claim Entra's federated credential requires. A
> fork or personal-account repo fails at login with `AADSTS7002381` (§4.1).
> CLI equivalent:
>
> ```bash
> gh workflow run m2-auth-acceptance.yml \
>   --repo microsoft/amplifier-context-intelligence \
>   --ref <pr-branch>
> ```

### 4.1 The `AADSTS7002381` trap

If the **Azure login** or **token acquisition** step fails with
`AADSTS7002381: The token issued by the identity provider does not match the
expected configuration`, it is almost always one of two things — and the error
**does not distinguish them**. It fails **loudly at the login step, before any
server code runs**, so look here first rather than hunting downstream:

1. **Name-based subject form was used.** Switch the federated credential to the
   **ID-based** subject (§3.1).
2. **The run is not from a Microsoft-enterprise org repo.** A fork or personal
   repo's OIDC token lacks the enterprise claim Entra requires — dispatch from
   `microsoft/amplifier-context-intelligence` itself.

**Escape hatch if CI federation is blocked entirely:** run the same assertions
locally against a real **Managed Identity** caller — from an Azure VM or
container whose MI is assigned the App Role:

```bash
az account get-access-token --scope "api://<AZURE_CLIENT_ID>/.default"
```

then feed that token through the same STEP A checks. Same token shape, a
different acquisition path — it still proves the resolver accepts a real Entra
token (note this in the PR evidence per §5).

## 5. What it proves (pass criteria)

### STEP A — the gate (job fails red if either sub-step fails)

**A.1 — claim-shape assertions** (hand-decodes the real JWT payload,
base64url + JSON, no signature check at this point — see §6 for why that's
fine):

1. `scp` is **absent** (the discriminator must route this token to the
   service branch).
2. At least one app-only signal: `idtyp == "app"` **or** `appidacr` is
   present.
3. `roles` contains at least one of the server's configured App Roles
   (`Contributor` / `Reader` / `IdentityAdmin`, or your overrides from §3.5).
4. `appid` or `azp` is present (the resolver's `created_by` fallback chain
   needs one of these before it would fall back to bare `oid`).

A redacted claim summary is always printed (`aud`, `iss`, `tid`, `scp`
presence, `idtyp`, `appidacr`, `roles`, `appid`, `azp`, and a truncated
`oid`). **The raw token is never printed**, only ever referenced through a
GitHub Actions masked environment variable.

**A.2 — the real resolver, not a re-implementation.** Constructs the actual
`context_intelligence_server.auth.EntraResolver` (which performs its own
live JWKS fetch against `https://login.microsoftonline.com/<tenant>/...` —
i.e. the **real signature verification** the hand-decode in A.1 skips) and
calls `.resolve(token)` — the exact code path production uses. Asserts:

- it does not raise `AuthError` (i.e. the real tenant's signing key
  validates the token, the audience/issuer/tenant checks pass, and a
  qualifying App Role was found), and
- it returns `is_service=True` (classified as a service/app token, not a
  delegated user token).

If A.2 fails with a 403, the caller's App Role assignment (§3.2) is missing
or wrong. If it fails with a 401, re-check the federated credential subject
(§3.1), the resource URI (`api://<AZURE_CLIENT_ID>`), or the tenant ID.

### STEP B — optional, best-effort, never fails the job

Only runs when `ACCEPTANCE_SERVER_URL` is set:

- `POST /events` with the real token → asserts **HTTP 202**.
- Polls `POST /cypher` (best-effort, ~30s) for a node with
  `created_by == appid` under the probe's `session_id`. **This match
  pattern is a best guess at the live graph schema** — the event pipeline
  is persist-then-202 (async drain to Neo4j), so this is inherently a
  polling check, not an immediate read. A failure here is a warning, not a
  job failure — verify manually if it doesn't confirm.

### STEP C — optional negative test, best-effort, never fails the job

Only runs when **both** `ACCEPTANCE_SERVER_URL` and `AZURE_READER_CLIENT_ID`
are set: logs in as the Reader-only caller (§3.3), requests a token, and
asserts `POST /events` returns **403** (write requires `Contributor`; a
Reader-only token must be rejected on a write route per
`authz.require_write`).

### Pass criterion, in one sentence

Preflight **proceeded** (not skipped), and **both** STEP A.1
(`ALL CLAIM-SHAPE ASSERTIONS PASSED`) and STEP A.2 (`Resolver ACCEPTED … is_service=True`)
are green on a real federated-OIDC token from the corp tenant — with the run
link and decoded-claims output attached to the PR. STEP B/C are best-effort and
never decide the verdict.

### Evidence to capture for the PR

A green run is only useful if it is auditable. Attach to the PR:

1. **Permalink to the green run**, explicitly noting it ran in
   `microsoft/amplifier-context-intelligence` (not a fork — see §4).
2. **STEP A.1** (step id `assert_shape`): the `ALL CLAIM-SHAPE ASSERTIONS PASSED`
   line plus the redacted claim summary (`aud`/`iss`/`tid`/`roles`/`appid`/`azp`,
   `scp` absent). **Never paste a raw token.**
3. **STEP A.2** (step id `assert_resolver`): the `Resolver ACCEPTED the live
   token: created_by=… is_service=True` line — the load-bearing proof that
   production code (with its live JWKS fetch) accepted a real token.
   `is_service` MUST be `True`.
4. If run: **STEP B** `202` and **STEP C** `403` lines.
5. One line naming the **acquisition path** (federated-OIDC self-call, or the
   Managed Identity escape hatch in §4.1) so reviewers can confirm no secret was
   involved.
6. Flip the **status banner** at the top of this file and the PR's
   outstanding-gate note: **DEFERRED → Executed `<date>`, run `<link>`, green.**

> **Match on step IDs, not messages.** The pass lines above are emitted by the
> steps `assert_shape` (STEP A.1) and `assert_resolver` (STEP A.2). If a step's
> *message* text is reworded later, key your evidence off those stable step IDs.

## 6. Honest gaps — what we could not determine without a real token

> **RESOLVED 2026-06-30 — [run 28479002620](https://github.com/microsoft/amplifier-context-intelligence/actions/runs/28479002620).**
> The gate has now seen a real app-only token from this tenant. Confirmed
> claim shape (redacted):
>
> ```
> scp      = None            absent  → routes to the SERVICE branch ✓
> roles    = ['Contributor'] App Role assignment honored            ✓
> azp      = <present>       created_by source                      ✓
> appid    = None
> idtyp    = None            ← NOT emitted by this tenant/flow
> appidacr = None            ← NOT emitted by this tenant/flow
> aud/iss/tid = api://<client> / login.microsoftonline.com/<tid>/v2.0 / 72f988bf-…  ✓
> ```
>
> **Finding:** this tenant emits **neither `idtyp` nor `appidacr`** on a
> federated app-only token. The token is nonetheless genuinely app-only, and
> the real `EntraResolver` accepts it (`is_service=True`) — its service branch
> (`auth.py`) keys on `scp`-absence plus a qualifying role and never reads
> `idtyp`/`appidacr`. STEP A.1's Assertion 2 (an extra confidence check,
> stricter than the resolver) was therefore **deliberately broadened** to
> accept `scp`-absent + (`appid` | `azp`) as a valid app-only signal. The
> pre-run uncertainties below are retained for history.

We have never seen a real Microsoft Entra app-only access token from this
tenant, only Microsoft's general documentation and the Team Pulse mirror
referenced in `auth.py`'s docstring. Specifically uncertain, and exactly
what this gate exists to settle:

- **Whether `appidacr` is actually present** on a v2.0 app-only access
  token issued by this tenant for this audience. It is a well-documented
  optional claim, but optional claims configuration on the app registration
  can suppress it. The A.1 check accepts `idtyp == "app"` **or**
  `appidacr` present — if neither shows up, that is a real finding, not a
  workflow bug; check what the printed claim summary actually contains and
  loosen/adjust the assertion deliberately (with eyes open) rather than
  assuming the workflow is broken.
- **Whether `idtyp` is present at all** on a v2.0 token from a workload
  federated identity vs. a Managed Identity vs. a client-credentials flow —
  these can differ. This gate's first real run is the first time we'll know
  for this tenant.
- **The exact graph schema for the STEP B read-back.** The Cypher query in
  STEP B assumes some node in the graph carries a `session_id` property
  matching the probe event. This is plausible (events are queued under
  `session_id`) but not verified against the live schema docs
  (`docs/architecture/03-graph-model.dot`) as part of this work — hence
  STEP B is explicitly best-effort/non-blocking.

If A.1 or A.2 fail on the **first real run**, that is exactly the signal
this gate was built to surface — do not treat it as a workflow bug without
first reading the printed claim summary and the resolver's actual error.

## 7. Reference — accurate to the code

- Resolver under test: `context_intelligence_server/auth.py` —
  `EntraResolver.resolve()`, the `scp`/`idtyp` discriminator, the
  `created_by` fallback chain (`service_identities[oid]` → `appid` →
  `azp` → `oid`).
- Config fields referenced: `context_intelligence_server/config.py` —
  `azure_client_id`, `azure_tenant_id`, `service_data_role`, `reader_role`,
  `entra_admin_role`.
- Capability gates exercised by STEP B/C: `context_intelligence_server/authz.py`
  — `require_write` (`POST /events`), `require_read` (`POST /cypher`).
- Canonical statement of the auth model: [`entra-auth-setup.md`](entra-auth-setup.md).
- Workflow: [`.github/workflows/m2-auth-acceptance.yml`](../.github/workflows/m2-auth-acceptance.yml).
