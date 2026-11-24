# Deploying to Azure (compliant, via `amplifier-online`)

This guide covers the **Amplifier Online (AO)** API project declared in
[`amplifier-online.yaml`](../amplifier-online.yaml). Use `amplifier-online up` to
reconcile infrastructure and manifest changes, and `amplifier-online deploy --ref`
to build and roll out a reviewed code revision. The earlier `web-app-aca` API
manifest and its push-to-deploy workflow are historical. Migrating a legacy
project or rolling back to the legacy deployment is a separately reviewed
operation: verify the actual storage, identity and writer handoff first.

Use placeholders for the API deployment examples; resolve real resource and
identity values privately. Never add secrets, object IDs, private addresses, or
deployment resource names to this repo.

> **Two moving parts.** The **server** runs as an Azure Container App (deployed by
> `amplifier-online`). **Neo4j** runs separately on a **VM inside a private
> virtual network** (you provision this once). The server reaches Neo4j over
> private Bolt; Neo4j is never publicly exposed. The API manifest names logical
> secrets in the **AO-managed project vault**, never their values.

---

## Container base image policy (S360 / SCA compliance) — READ BEFORE TOUCHING THE Dockerfile

**Rule: the server image MUST be built on an approved Microsoft Container
Registry (MCR) Azure Linux base. Do not base any container we ship on Docker Hub
or any other non-approved registry.**

Current approved base (in `Dockerfile`):

```
mcr.microsoft.com/azurelinux/base/python:<tag>
```

### Why this is a hard requirement

This service is scanned by **S360** (Microsoft's security-compliance program)
using **Qualys SCA** (Software Composition Analysis). SCA flags, as security
findings that block compliance:

1. **Non-approved base images.** MCR Azure Linux images are continuously patched
   by Microsoft via a **fix-only feed**, so a build-time `tdnf update` clears
   reported CRITICAL/HIGH CVEs **without suppression/exception lists**. Docker
   Hub bases (e.g. a `python:*-slim`) have no patch SLA and cannot make that
   guarantee.
2. **The base should match the runtime OS.** This service targets **Azure Linux**
   nodes; keeping the base aligned to Azure Linux (and to the `amplifier-online`
   provisioner pattern) avoids drift.
3. **EOL packaging tools inherited from the base.** `pip` / `setuptools` /
   `wheel` shipped in a base as OS packages carry CVEs and get flagged.

### The remediation pattern established by PR #50 — preserve it

This policy and the current `Dockerfile` were set by
**PR #50 — "fix(docker): adopt Azure Linux base + remove EOL packaging tools to
clear S360 SCA findings"** (@payneio, merged):
https://github.com/microsoft/amplifier-context-intelligence/pull/50

It cleared four Qualys findings: `CVE-2026-6357` and `CVE-2025-8869` (pip),
`CVE-2026-24049` (wheel), `CVE-2026-23949` (jaraco.context, via setuptools).

Preserve these load-bearing steps when editing the `Dockerfile`:

- **Remove the EOL `pip`/`setuptools`/`wheel` OS rpms outright** (this app builds
  with hatchling and imports none of them at runtime), then reinstall **only a
  patched `setuptools`** (some transitive deps still import `pkg_resources`).
  An in-place `pip install --upgrade` does **not** clear the findings: the
  rpm-installed `dist-info` has no `RECORD`, so the old metadata is left orphaned
  next to the new, and the scanner keys on that `dist-info/METADATA`.
- **Run `tdnf update` at build time** so the image captures the latest fix-only
  patches before it is pushed.

The image build/run is exercised by `tests/integration/test_docker_image.py`.

### The image is built on the approved base, then imported and deployed by `amplifier-online`

The manifest uses `services.api.build: .` to build from this repository's
`Dockerfile`. Amplifier Online owns image delivery and revision creation. Do not
create an app by hand or push to the shared platform registry. A source push or
passing GitHub CI is not a deployment or proof that the running revision meets
the release and compliance gates. Existing stateful deployments require the
[non-overlapping upgrade handoff](#611-to-613-upgrade-and-rollback-boundary).

### Do NOT create new containers on non-approved bases

This is why **Neo4j is not shipped as a container image** in this repo — a
`neo4j:*-community` Docker Hub base would fail S360. Neo4j runs on a VM built from
an **approved Azure VM image** (see the Neo4j section). Any future container we
build must use an approved MCR Azure Linux base and follow the remediation
pattern above.

---

## Prerequisites

Have these in place before you deploy:

**Tooling (local):**

- **Azure CLI** (`az`), logged in to the target tenant/subscription:
  ```bash
  az login
  az account set --subscription "<your-subscription>"
  ```
- **`amplifier-online` CLI**, authenticated to the intended project and Azure
  subscription. Confirm it supports `up --dry-run` and project-level
  `deploy --ref` before proceeding.
- `jq` and `curl` (used by the identity-seed step).

**Azure resources / access:**

- A **subscription** and **resource group** you can deploy into.
- **No project ACR to create.** Use Amplifier Online's image delivery path.
- **No hand-rolled VNet/subnet/NSG.** The API service runs in the shared
  platform VNet; the Neo4j VM is a separate **`vm`-stack** Amplifier Online project
  that owns its own private subnet placement and NSG (see Step 2).
- The **AO-managed project vault** and authorized access to maintain the project
  secrets. The platform wires the API's managed identity and secret references.
- **Entra (Azure AD) app registration** for the server's JWT auth (if deploying
  in `entra` mode) — a client (app) ID, the tenant ID, and any App Roles you
  gate on.
- Rights to register providers the first time:
  ```bash
  az provider register --namespace Microsoft.App
  az provider register --namespace Microsoft.OperationalInsights
  ```

**Compliance gate:** the server image must be built on the **approved MCR Azure
Linux base** (see the policy section above) before it is pushed and deployed.

---

## Step 1 — Review the source and build requirements

Select a reviewed source ref, record the commit it resolves to, and confirm its
repository checks and approved Azure Linux base. Separately verify the applicable
image scan, SBOM and provenance evidence for the artifact that will be deployed.
Do not assume the legacy generated workflow runs those checks for an Amplifier
Online build.

---

## Step 2 — Provision Neo4j as a second `vm`-stack Amplifier Online project

Neo4j is **not** a hand-rolled VM and **not** a container. Amplifier Online has a
first-class **`vm` stack** whose canonical example is exactly this: a private Neo4j
VM. Run Neo4j as its **own Amplifier Online project** — a second
`amplifier-online.yaml` (in its own directory/repo) with `stack: vm`, deployed with
its own `amplifier-online up`. This keeps the whole system "compliant, via
`amplifier-online`": the stack inherits the approved VM base image and private
networking instead of re-deriving them.

> **Do NOT hand-roll VNet / subnet / NSG / public-IP / data-disk Bicep.** The `vm`
> stack owns all of it (see the table below). Your job is the manifest + a
> cloud-init file.

### 2a. What the `vm` stack owns (stop re-deriving it)

| You used to hand-roll | The `vm` stack gives you |
|---|---|
| VNet + subnet placement | VM placed in the shared platform VNet's `vms` subnet automatically |
| Approved OS base image | Defaults to **Ubuntu 24.04 LTS** (approved); override via `vm.image` |
| NSG + inbound rules | **Default-deny** NSG; you declare only `ports` + `source` |
| Public-IP hardening | **No public IP** at all — private-only by construction |
| Persistent data disk | `data_disk_gib` disk, preserved across idempotent `up` re-runs |
| First-boot provisioning | `cloud_init` inlined + executed on first boot |
| SSH exposure | Key-only auth, no public SSH — operate via `az vm run-command` |

### 2b. Author the `vm` manifest

Scaffold it with `amplifier-online init --stack vm` (writes the `vm:` block), then
fill it in. Shape for this Neo4j:

```yaml
name: context-intelligence-neo4j
stack: vm

vm:
  size: Standard_E8s_v5            # memory-optimized (E-series) — sized for the graph
  admin_username: azureuser
  ssh_public_key: "ssh-ed25519 AAAA..."   # PUBLIC half only; key auth, no passwords
  cloud_init: ./cloud-init.yaml            # contents inlined at `up`; runs on first boot
  data_disk_gib: 256                       # persistent graph store; survives `up` re-runs
  static_private_ip: 10.100.4.4            # fixes the Bolt endpoint the web app hard-codes
  ports:
    - port: 7687                           # Neo4j Bolt — the ONLY inbound rule
      protocol: Tcp
      source: cae-infra                    # the CAE app-egress subnet (where the ACA app calls from)
```

> **As-built (production, verified 2026-08-26).** The live Neo4j VM
> `context-intelligence-neo4j` (RG `AO-NEO4J-RG`, westus2) runs
> **`Standard_E8s_v5`** (8 vCPU / 64 GB), **Ubuntu 22.04 LTS gen2**, private IP
> `10.100.4.4`. The graph is on an **attached 256 GiB Premium_LRS data disk**
> (LUN 0); the OS disk is a separate 30 GiB StandardSSD_LRS. Both disks are
> `deleteOption: Detach`, so they survive VM deletion. (The `vm` stack's default
> base image is Ubuntu 24.04 LTS; this VM predates that default and is pinned to
> 22.04 — override via `vm.image` only on a deliberate rebuild.)

- **`static_private_ip`** must be a free address in the `vms` subnet
  (`10.100.4.0/24`); set it precisely because the web app's `NEO4J_URL` hard-codes
  `bolt://10.100.4.4:7687`. Neo4j Browser (7474) is **not** exposed — do not add it.
- Bring this VM up **before** the web app (producer before consumer):
  `amplifier-online up` from the manifest's directory.

### 2c. Install Neo4j 5.26 LTS + APOC + GDS 2.13.11 via cloud-init

Software is installed on first boot by the `cloud_init` file referenced above — not
by hand. This project targets **Neo4j Community 5.26 LTS** with **GDS Community
2.13.11** (the latest `2.13.x` patch — the `2.13` series pairs with the `5.26` line
per the official
[GDS ↔ Neo4j compatibility matrix](https://neo4j.com/docs/graph-data-science/current/installation/supported-neo4j-versions/);
pin the newest patch so GDS loads on recent `5.26.x` releases). `./cloud-init.yaml`:

```yaml
#cloud-config
package_update: true
packages:
  - openjdk-21-jre-headless        # Neo4j 5.26 requires Java 21
  - wget
  - gnupg

runcmd:
  # 1. Format + mount the persistent data disk at Neo4j's data dir
  - |
    set -euxo pipefail
    DISK=$(lsblk -rno NAME,TYPE,MOUNTPOINT | awk '$2=="disk" && $3=="" {print "/dev/"$1}' | head -n1)
    if ! blkid "$DISK"; then mkfs.ext4 -F "$DISK"; fi
    mkdir -p /var/lib/neo4j
    echo "$DISK /var/lib/neo4j ext4 defaults,nofail 0 2" >> /etc/fstab
    mount -a
  # 2. Install Neo4j Community 5.26 LTS (pin the 5.26 line; do NOT jump to CalVer 2026.xx)
  - |
    set -euxo pipefail
    wget -qO- https://debian.neo4j.com/neotechnology.gpg.key | gpg --dearmor -o /usr/share/keyrings/neo4j.gpg
    echo 'deb [signed-by=/usr/share/keyrings/neo4j.gpg] https://debian.neo4j.com stable 5' > /etc/apt/sources.list.d/neo4j.list
    apt-get update
    apt-get install -y neo4j=1:5.26.0
  # 3. Plugins: APOC Core (bundled in labs/) + GDS 2.13.11 (downloaded)
  - |
    set -euxo pipefail
    PLUGINS=/var/lib/neo4j/plugins; mkdir -p "$PLUGINS"
    cp /var/lib/neo4j/labs/apoc-*-core.jar "$PLUGINS"/ || true
    wget -qO "$PLUGINS/neo4j-graph-data-science-2.13.11.jar" \
      https://graphdatascience.ninja/neo4j-graph-data-science-2.13.11.jar
    chown -R neo4j:neo4j /var/lib/neo4j
  # 4. Bind Bolt on the private interface + allow the plugin procedures.
  #    Set ONLY `unrestricted` — an `allowlist` of apoc.*,gds.* would block the
  #    built-in db.*/dbms.* procedures the server relies on.
  - |
    set -euxo pipefail
    CONF=/etc/neo4j/neo4j.conf
    sed -i 's/^#\?server.default_listen_address=.*/server.default_listen_address=0.0.0.0/' "$CONF"
    sed -i 's/^#\?server.bolt.listen_address=.*/server.bolt.listen_address=0.0.0.0:7687/' "$CONF"
    echo 'dbms.security.procedures.unrestricted=apoc.*,gds.*' >> "$CONF"
  # 5. Historical guest example: set an initial password, then start.
  #    For current API credential wiring, follow Step 3 separately.
  - neo4j-admin dbms set-initial-password "$(openssl rand -base64 24)"
  - systemctl enable neo4j
  - systemctl restart neo4j
```

After first boot, confirm the plugins loaded (`az vm run-command` — there is no
public SSH):

```cypher
RETURN apoc.version();
RETURN gds.version();
```

### 2d. How the web app reaches it

The API project reaches the separate Neo4j VM over **private Bolt** on
the shared platform VNet. Preserve the existing private endpoint and the VM NSG
allow rule for TCP 7687 from the app's egress subnet; do not expose Bolt publicly.
The guest provisioning example above is not an API deployment or credential
rotation procedure. For current API credential wiring, use Step 3.

---

## Step 3 — Wire and rotate Neo4j credentials

The manifest declares the API's credential references under top-level
`secrets`, using `keyVaultSecret` and `service: api`. Both Neo4j clients use
`neo4j-user` and `neo4j-password` in the **AO-managed project vault**. Keep the
Bolt URL and access modes in the API's `env` mapping; never put credential values
there. Do not hand-patch Container Apps env vars or secrets after `up` or deploy.

For **initial setup**, have an authorized operator populate both logical secrets
with the matching database credentials in the intended AO project. For
**password rotation**, plan a maintenance window, producer buffering and a
recovery path before updating either side. There can be a credential mismatch and
service interruption between changing the database and refreshing the API.

1. Rotate the shared account's password on Neo4j through the approved database
   procedure. Set the same value in the AO project vault with the prompted
   command below. Do not pass the value on the command line or record it in logs.
   Update `neo4j-user` using the corresponding prompted command only if the
   username changes.

   ```bash
   amplifier-online secret set neo4j-password
   ```

2. Confirm privately that the active API's references still point to the
   AO-managed vault and are versionless. Azure Container Apps can refresh a
   versionless Key Vault secret automatically and may restart the app containers
   that consume it. Do not assume immediate uptake or restart the app ad hoc;
   use the approved platform refresh/restart path and protect single-writer
   ownership if intervention is needed.
3. Verify the active revision picked up the change: check `/status` for both
   Neo4j connections, make an authenticated query and confirm a controlled test
   event actually drains into Neo4j. `/health`, an accepted secret write, or a
   `202` from ingress alone doesn't prove the new credential works.

The legacy vault is **not the API's credential source**. It may still serve
other consumers, be maintained as an upstream copy with an explicitly approved
manual sync, or be needed for rollback. Updating it does not update the AO vault.
Do not remove an old **Key Vault Secrets User** assignment until the owner has
checked all consumers and rollback needs and authorized its removal.

### The two Neo4j clients (admin/write and cypher_query/read)

The API manifest requires structured `neo4j.admin` and `neo4j.cypher_query`
configuration (env delimiter `__`) for the write/admin path and `/cypher` read
path. Both use the same Community account and private Bolt URL. Do not switch
to flat settings as part of a rollout. Separate credentials and a genuinely
read-only account require Neo4j Enterprise RBAC.

> **Hard rules (fail-loud at startup):** `admin.access_mode` MUST be `WRITE` and
> `cypher_query.access_mode` MUST be `READ` — the field defaults to `WRITE`, so
> the read client's `READ` **must be set explicitly** or the server refuses to
> boot. Do NOT mix flat `NEO4J_*` keys with structured `NEO4J__*` keys.

**Least-privilege read account — Neo4j Enterprise only.** Genuinely separating the
read client onto a read-only account needs **Enterprise** (multi-user + RBAC).
There, create a reader account and give the `cypher_query` client its **own**
credential (a second Key Vault secret + secretRef):

```cypher
-- Enterprise only
CREATE USER readonly SET PASSWORD '<read-only-account-password>' CHANGE NOT REQUIRED;
GRANT ROLE reader TO readonly;
```

> **`ACCESS_MODE=READ` is not a security boundary.** It marks the driver session
> read-intent (a routing hint); it does **not** prevent writes. On Community —
> where both clients share the one full-privilege account — `READ` provides **no**
> write protection at all. Real read-only isolation comes only from an Enterprise
> `reader`-role account.

Keep the manifest's guard against silently falling back to flat settings:

```
AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J_REQUIRE_EXPLICIT_CLIENTS=true
```

With that flag on, the server refuses to start unless both `admin` and
`cypher_query` are explicitly configured — a good guardrail for production so a
missing structured block can't silently degrade to one shared account.

---

## Step 4 — Review the Amplifier Online manifest

Use the checked-in [`amplifier-online.yaml`](../amplifier-online.yaml) as the
source of truth. The excerpt below shows the service, volume and secret-ref
structure only; it is not a complete replacement manifest. Keep the existing API
registration, caller allowlist, internal Bolt endpoint, and durable paths.

```yaml
name: <ao-project>
services:
  api:
    build: .
    port: 8000
    auth: token
    app_id: "<entra-app-client-id>"
    min_replicas: 1
    volume:
      mount_path: /data
      size_gib: 2048
      tier: premium
secrets:
  - name: AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J__ADMIN__PASSWORD
    keyVaultSecret: neo4j-password
    service: api
```

The complete manifest supplies both username/password references for both
clients, the Entra and storage settings, and the separate retention service.
Do not add credential values or an `entra_identities` object ID → handle map to
the manifest. See [identity seeding](#seeding-the-identity-map-on-a-fresh-data-dr--first-boot)
for fresh storage only.

---

## Step 5 — Reconcile infrastructure, then roll out the reviewed code

For first-time provisioning or an intended manifest/infrastructure change, run
the following from the API project directory. Review the plan before running
`up`. Stop if it proposes an unexpected app, identity, network or storage
replacement. Treat any revision or scheduling change as an operational change,
not proof that writer handoff or retention is safe.

```bash
amplifier-online up --dry-run
# Only after reviewing the plan and securing the applicable approvals:
amplifier-online up
```

For a **code-only rollout** on existing Amplifier Online infrastructure, deploy
the reviewed ref at **project scope**; don't run `up` as a substitute or assume
that pushing the ref deploys it. Complete the
[non-overlapping handoff](#611-to-613-upgrade-and-rollback-boundary) before
allowing a successor to write. Review impact on the scheduled job as well as
the API when rolling out the project.

```bash
amplifier-online deploy --ref '<reviewed-ref>'
```

Compare `/health`'s `commit` on the active API with the commit selected for
deployment. A healthy listener doesn't test authentication, Neo4j or `/data`.
On fresh Entra storage, wait for identity-store readiness and follow the
[seed procedure](#seeding-the-identity-map-on-a-fresh-data-dr--first-boot) before
data-path acceptance. Then verify both authorized and rejected requests, an
authenticated Neo4j read, and a controlled test event observed in Neo4j after
ingestion, not just a `202` receipt. Confirm the actual attached storage and
identity state before resuming normal traffic.

---

## Deploying with Entra auth

To run the server in **Microsoft Entra** mode (`auth_mode=entra`) instead of
static keys, supply the entra settings via manifest env / Key Vault. The full
model is in [entra-auth-setup.md](entra-auth-setup.md); the deployment-relevant
variables are:

| Env var (`AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_` + …) | Required? | Purpose |
|---|---|---|
| `AUTH_MODE=entra` | yes | Select the Entra resolver |
| `AZURE_CLIENT_ID` | yes | App Registration (client) GUID |
| `AZURE_TENANT_ID` | yes | Azure AD tenant GUID |
| `ENTRA_IDENTITIES` (JSON) | no | Optional initial `oid → {id}` map (**PII**). Seed fresh `/data` via the admin API instead of committing a map. Unmapped data callers are denied. |
| `SERVICE_IDENTITIES` (JSON) | no | First-boot seed for **service** principals into the same shared identity store `entra_identities` uses — not itself an auth gate, but an unmapped role-bearing service now gets **403**; may be empty, and identities can be onboarded/removed later via `/admin/identities`, no redeploy |
| `SERVICE_DATA_ROLE` | no | App Role granting service write+read (default `Contributor`) |
| `READER_ROLE` | no | App Role granting service read-only (default `Reader`) |

> **Note: `SERVICE_IDENTITIES` seeds only on first boot.** The `service_identities`
> config seeds the shared identity store on **first boot only**. Once the store file
> (`/data/identity/entra-identities.json`) exists, config changes to `service_identities`
> have no effect. The server logs a **WARNING** at startup naming any `service_identities`
> oids being ignored for this reason, and points the operator at `PUT /admin/identities/{oid}`
> instead for runtime onboarding (no redeploy needed).

> **Operator note — service callers must use Managed Identity or federated OIDC,
> not client secrets.** In a locked-down tenant, the service path (app-only tokens
> authorized by an Entra App Role) should be driven by a **Managed Identity** or a
> **federated-OIDC** workload credential. Avoid client secrets / certificates:
> they are long-lived bearer credentials that tenant policy typically forbids and
> that are easy to leak from a container. Assign the `Contributor` (or `Reader`)
> App Role to the caller's service principal in Entra; that assignment is the
> authorization — see
> [identity-management.md → service callers](identity-management.md#service-callers-entra-app-tokens).

### Seeding the identity map on a FRESH `/data` (DR / first boot)

The `ENTRA_IDENTITIES` map (oid → `{id}`) is **PII and is never committed** to this
repo or the deployment manifest. The map on durable `/data`
(`entra_identities_store_path`, e.g. `/data/identity/entra-identities.json`) is
authoritative once initialized. Verify that a routine rollout finds that same
store; do not re-seed a populated store.

Fresh storage starts with no data identities unless seeded, not with auth
disabled. Wait for `/status` to report `identity_ready: true` after startup.
Protected operations return `503` until the identity store is ready; affected
data callers must still be onboarded. Use an authorized IdentityAdmin through
the approved API route.

```bash
# 1. One-time: create the uncommitted seed file from the example, then edit it to
#    hold the real  "<oid>": {"id":"<github-handle>"}  entries. It is git-ignored.
cp scripts/entra-identities.example.json scripts/entra-identities.local.json

# 2. az login as an IdentityAdmin, then use the API URL and app registration audience:
az login                                                    # IdentityAdmin identity
export SERVER_URL="https://<approved-api-endpoint>"
export AUTH_RESOURCE="api://<entra-app-client-id>"
export SEED_FILE="scripts/entra-identities.local.json"
./scripts/seed-entra-identities.sh --check   # dry run — shows what it WOULD PUT
./scripts/seed-entra-identities.sh           # apply + verify all oids present
```

The seeder fails loud if the seed file is missing/empty (it will not "seed
nothing"), is idempotent (re-adding an existing mapping is a `200` no-op), and
verifies every oid is present in the live map afterward. Keep seed output
private: `--check` also prints identifiers. See
[identity-management.md](identity-management.md) for the admin API details.

The existing `deploy.sh` wrapper runs `up` and then the seed (`--no-seed` runs
only `up`); it does not select or roll out a reviewed code ref. Use the seeder
directly for approved fresh-volume onboarding after the readiness check, not
the wrapper for a routine rollout.

---

## TLS and connecting the Amplifier bundle

The Amplifier Online edge terminates HTTPS for the API, whose container listens on
port 8000. Use the project-approved HTTPS route and satisfy both platform
edge and Context Intelligence authorization.

Point the Amplifier hook at the server's HTTPS endpoint:

```yaml
overrides:
  hook-context-intelligence:
    config:
      context_intelligence_server_url: "https://<approved-api-endpoint>"
      # In Entra mode, the hook presents an Entra bearer token (no static key).
```

---

## Azure scheduled blob retention — explicit opt-in

The manifest declares a separate **Azure Container Apps scheduled Job** in
the API's project. The runner calls the existing online retention API and
defaults to a non-deleting preview. Reconciling the schedule can enable recurring
previews; it is not approval to apply retention. Review the plan and obtain
approval before starting the job against production. This runbook does not
qualify Azure Files or establish effective production authorization.

### 1. Qualify storage before any production apply

Online pruning requires a single server worker on **64-bit Linux with local
filesystem semantics**. Azure Files persistence across revisions does not prove
those semantics. First qualify a **disposable equivalent Azure Files share**,
using the same SMB protocol, mount options, container runtime, and server storage
layout as production. Record evidence for:

- Directory-cookie continuation after closing/reopening scan directories, including
  concurrent entry changes.
- Stable stat identity and timestamps (device/inode/size/mtime/ctime), as used by
  preview/apply fingerprint checks.
- POSIX `flock` exclusion, atomic rename/unlink, and directory `fsync`.
- SQLite locking, crash recovery, and durability, including WAL where used by
  server stores. The expiry ledger itself uses rollback journaling, not WAL.

**Local pytest results cannot qualify a network share. Production apply on this
Azure Files setup is unsupported until qualification succeeds.** If these
guarantees cannot be established, leave scheduling disabled and arrange a
separately reviewed supported storage path; do not infer safety from a successful
mount or preview.

Review the [service guide's coordination boundary](service-setup.md#online-keep-ingestion-available)
as well: process-local online leases do not protect against separate/old writers.
Inventory and control those writers through their existing service managers
before apply. Do not blindly stop production ingestion or add stop/restart hooks
to the scheduled job; online pruning keeps the supported server running.

### 2. Verify the scheduled job and both authorization gates

This is the job service in the project manifest (not a second API replica):

```yaml
services:
  retention:
    build: .
    schedule: "0 4 * * *"
    calls: [api]
    command: [python3, -m, context_intelligence_server.retention_job]
```

| Setting | Required choice |
|---|---|
| Schedule | `0 4 * * *`: daily at **04:00 UTC**, independent of daylight saving |
| Policy | Fixed **60d** storage modification age for this runner. Manual `blobs prune` retains its separate `30d` default. |
| Image and command | Record the observed `sha256` digest of the reviewed job image. The effective command must run the Python module directly, not the API entrypoint or listener. A tag alone does not pin content. |
| Per-execution limits | Verify effective parallelism **1**, replica completion count **1** and retry limit **0** before production use. |
| Timeout | The runner fixes `--max-runtime 1200` and `--cancel-timeout 120`. Verify the effective platform timeout exceeds their sum with headroom for token acquisition, startup and shutdown. |
| Access | The job needs registry pull and access to the platform-supplied HTTPS API route. It does not get a server `/data` mount, Neo4j credentials or storage-delete RBAC. |

Parallelism 1 limits replicas **within one execution**; it is not a global mutex
between cron executions. Executions can overlap. The server's one-current-job
guard makes another scheduled invocation return `skipped_busy` without taking
over or cancelling that job. Do not add automatic retries to defeat this guard.

The runner uses platform-provided `API_URL`, `API_AUDIENCE` and
`AZURE_CALLER_CLIENT_ID`. On each start it requests an app-only Entra token for
that API audience through the Container Apps identity endpoint. It checks that
the token can cover both per-run budgets plus margin and passes it to the
existing `blobs prune-scheduled` client via a private ephemeral file, not a
command-line value or committed token. Do not substitute a storage/ARM token,
client secret, shared key or pre-minted token.

`calls: [api]` declares the platform service-to-service edge. Verify the actual
job identity can reach the retention API with its app-only token; a working
delegated-user call does not prove that route. Separately, assign the job
principal the API's **`IdentityAdmin`** App Role (or configured
`entra_admin_role`) for **Application** members. An authorized CI administrator
must map the principal's **`oid`** through `PUT /admin/identities/{oid}`.
Neither platform edge access nor the API's data roles nor Azure RBAC grants CI
admin access. Keep all real identity and routing details private.

### 3. Preview production, review, then explicitly approve deletion

Only after storage qualification and authorization checks, run the approved
job in **preview mode** via the platform. Its default command takes no
arguments:

```bash
python3 -m context_intelligence_server.retention_job
```

The module passes `--dry-run` to the underlying client. Dry-run creates a
preview and cancels **only its owned preview UUID**, observing
cancellation before reporting `dry_run`. A pre-existing active job produces
`skipped_busy`; that is not a completed preview. Inspect the sanitized JSON
receipt's cutoff, eligible file count, logical/allocated bytes, errors/changes,
and cleanup state. Allocated bytes are an estimate, not promised space savings.
Retain the receipt for operator review without credential/bootstrap logs.

The **first apply includes the existing backlog** whose recognized payload
`mtime_ns` is strictly older than server preview-start time minus 60 days. This
is storage modification age, **not session age or last-access age**; equality,
future-dated files, and changed candidates are retained. Preview estimates the
backlog; apply runs a new preview and may produce different counts. Review the
actual preview with the data owner and obtain explicit production approval
from the data and deployment owners before enabling deletion. Rehearse an
approved backup and restore on equivalent storage, keeping payloads, expiry
state and other coupled state consistent. Retention is **deletion, not an
archive or a recovery system**.

Only after these gates, make a separate reviewed manifest change to add
**`--apply`** to the job module's `command`. Review `up --dry-run` and reconcile
it through the approved Amplifier Online path; verify the effective job command
and first outcome. Unlike the Azure module, the lower-level `blobs prune-scheduled`
applies without a prompt when called without `--dry-run`; it does **not** take
`--apply`. Neither path deletes graph/session metadata, queues or unknown
files. Preserve the retention lock and expiry ledger: tombstones prevent
replay from rehydrating expired payloads. Authorized reads of missing expired
blobs return **410 `blob_expired`**.

### 4. Monitor, disable, and update deliberately

Collect the sanitized receipt and process exit code for every execution.
Exit zero can mean `completed`, `dry_run`, **or `skipped_busy`**, so monitor the
outcome, not just the exit code. Alert on `failed`, `cancel_unconfirmed`, nonzero
exits, and repeated busy skips. Keep the owned UUID for investigation; HTTP 202
or `cancelling` does not establish successful completion or cancellation. See
[nightly client behavior](service-setup.md#nightly-online-pruning--explicit-unattended-opt-in)
for receipt and deadline semantics.

Disable future starts through the approved platform-managed job path when
pausing retention. **Disabling the schedule does not cancel an active server
UUID job.** Allow the client its SIGTERM cleanup window where possible; after
forced termination, network loss, or `cancel_unconfirmed`, inspect/cancel the
specific UUID using the existing
[status/cancel commands](service-setup.md#online-keep-ingestion-available), with
the same HTTPS URL and a fresh authorized admin credential. Observe a terminal
state; never cancel an unrelated "current" job or bulk-delete storage as cleanup.

Project-level code rollout can affect the job as well as the API. Review the
effective job image digest, command, schedule, identity and limits after a
reconcile or rollout; repeat preview review before re-enabling apply. Do not
create a separate production project, mutate the server volume, or grant the
job direct access to the data store.

---

## Updating the server (build & deploy a new version — Neo4j-safe)

For a routine code rollout, keep the API volume, identities, private Bolt
settings and Neo4j project unchanged. Review the ref and image compliance
evidence (Step 1), then use the project-level command and acceptance checks in
[Step 5](#step-5--reconcile-infrastructure-then-roll-out-the-reviewed-code).
Do not combine a capacity change, password rotation, retention activation or
legacy project migration with a routine code rollout.

### 6.11 to 6.13 upgrade and rollback boundary

This older transition illustrates the stateful handoff; it is not a recipe for
migrating legacy infrastructure or downgrading the server. Use the actual source
and target versions and verify compatibility for the exact stored state before
switching binaries.

**Use a planned stop/handoff, not overlapping rolling revisions on shared
`/data`.** 6.13 takes a nonblocking exclusive lock on the canonical state root
before starting workers and holds it for the whole lifespan. An upgraded
predecessor prevents an upgraded successor from starting until shutdown releases
ownership. A 6.11 writer does **not** honor this lock or expiry tombstones:
absence of lock contention does not make mixed-version operation safe.

The actual Azure handoff is **not qualified here**. First rehearse the approved
platform-managed stop/start and restore procedure in an isolated equivalent
deployment. Azure Files remains **unqualified**, including state-root/identity
locking, directory fsync, identity mutation/restart and SQLite durability—not
just retention scanning. Qualify those contracts on disposable equivalent storage
before approving this upgrade on that backing; disabling pruning is insufficient.
See [storage qualification](#1-qualify-storage-before-any-production-apply).

**Operator checklist — before allowing the successor to write:**

1. **Gate replacement.** Arrange the maintenance window, producer buffering/retry
   plan and approved platform controls before requesting a rollout. Check for
   any remaining automatic deploy triggers on older projects; neither automated
   delivery nor selecting a code ref makes a safe writer handoff. Keep retention
   schedules disabled during the transition, and inspect/cancel any active owned job using
   the [existing UUID procedure](#4-monitor-disable-and-update-deliberately).
2. **Inventory the actual configuration privately.** Resolve canonical paths from
   effective settings, not just the illustrative `/data` layout:
   - `queues_path` and its entire live spool, offsets, dead letters and retry
     evidence; `recovery.queues_path`, `recovery.receipt_store_path`,
     `recovery.claims_store_path`, and `recovery.guard_lease_path` where used.
     Omitted recovery paths derive beside the live queue. The live queue, claim
     ledger, and enabled recovery queue/receipt ledger must be **direct children**
     of the live queue's canonical parent (the exclusive state root).
   - `blob_path`, payloads, `.ci-blob-retention.lock`, and the complete private
     `.ci-blob-retention/` state if present: expiry database, journals and
     current-job snapshot. Do not omit tombstones because payloads are absent.
   - The selected `api_keys_store_path` or `entra_identities_store_path` and its
     `<store-name>.lock` sidecar. Identity storage is independently locked and may
     be outside the state root; keep its configured location and writable trusted
     parent. Include the state-root `.context-intelligence-server.lock` if present.
3. **Quiesce every writer, then back up consistently.** Stop the old revision and
   external/standalone writers through their approved platform/service managers.
   Wait for all actual I/O, drainers and identity cleanup to settle and ownership
   to be released normally; a cancelled await or a traffic switch is not proof.
   Never unlink a root lock, identity sidecar or retention lock to force startup.
   Hung I/O has no promised finite graceful shutdown; stop the procedure and
   investigate rather than force-unlock. With writers quiescent, take and verify
   a consistent backup of **all coupled state above**, preserving ownership,
   modes and regular-file semantics. Use a reviewed SQLite-consistent backup
   method; do not blindly copy live databases/WAL files or discard journals.
   A share snapshot alone does not cover stores outside the share or establish
   application consistency. Record matching configuration/secret references
   privately; do not dump credential contents. Plan reconciliation with graph
   state and producer-held events without changing the Neo4j VM in this rollout.
4. **Match the actual artifact and start once.** Approve the selected ref and
   resulting image and record the deployed image's observed registry **`sha256`
   digest**. A commit-SHA tag alone does not pin content. Do not invent a
   release tag or infer installed version from source metadata. Only after the
   predecessor has fully stopped, start the
   successor as **one replica, one worker** against the same canonical roots,
   using the approved non-overlapping platform path. If that path cannot enforce
   the ordering, do not deploy. Keep scoped claims, grants, identity bindings,
   external credential references and storage layout intact.
5. **Verify before restoring traffic/producers.** Confirm the actual image digest,
   `/health` commit, and intended installed version (6.13.0 for this transition);
   `/version.version` and `/status.server_version` must match. Platform health alone
   is insufficient: check top-level `/status` fields `identity_ready: true`, `identity_reason: null`,
   `neo4j_connected: true`, `neo4j_query_connected: true`, `recovery_complete`
   and `recovery_error`. Inspect queue backlog and authenticated queue APIs
   (and recovery APIs when enabled),
   check expected blob reads and external/offline consumers against the new API,
   and confirm identity/auth enforcement, query and controlled ingestion as in
   Step 5, without printing credentials. Backlog
   can remain while draining; do not disable scoped access or discard denied
   claims/queues to accelerate readiness. No destructive prune is needed for
   upgrade verification. Resume producers only after these checks are accepted.

**Rollback gate — an image revert is not a state rollback.** Do not run 6.11
against migrated or post-retention 6.13 state unless compatibility for that exact
state and binary is evidenced and explicitly approved. Otherwise require a
reviewed, consistent restore of the **entire pre-upgrade coupled state**, matching
old image/configuration and a non-overlapping handoff. Account for events accepted
after the backup, producer replay, queue offsets/receipts, claim/identity bindings
and graph effects. The operator must approve reconciliation or any loss of
post-backup accepted data; restoring an old snapshot is not silently lossless.

Legacy receipt initialization replaces the database schema; it does not retain
an automatic old-schema backup. `recovery.permit_required: false` is client
protocol compatibility, **not schema downgrade** (see
[native recovery](native-recovery.md#upgrade-and-rollback-state-compatibility)).
Old readers do not understand new mapped blob URIs, and old writers can rehydrate
expired ordinary blobs even if tombstones remain. Disabling cron, cancelling a
job, or changing the image cannot restore deleted payloads. Never clear expiry
metadata to "roll back". Physical-first reads can read separately restored bytes,
but payload-only restoration does not clear expiry or supply a supported
unexpire/rollback procedure; keep tombstones and require a reviewed restore plan.
No automatic downgrade/restore tool or tested full rollback is promised here.

### Neo4j safety — guarantees & do-NOT-touch list

The Neo4j VM and its disk belong to a **separate project**, not to the API
manifest. API releases still depend on the private network path, existing
database credentials and compatibility with the graph. Leave these unchanged
during a routine API rollout:
- The Neo4j VM, its data disk, or snapshots/backups.
- **VNet / subnet / NSG rules** — especially the Bolt `7687` allow-rule from the
  app subnet. Don't bundle network edits into a deploy window.
- The `NEO4J__*__URL` values (private Bolt endpoint) — keep pointing at the same
  `<neo4j-private-address>:7687`. A wrong URL = server up, graph unreachable.
- The AO-managed `neo4j-user` and `neo4j-password` **values** and their manifest
  references. Database/secret rotation is a separate operation (Step 3).
- The single-account model — do **not** invent a second read-only account on
  Community; there is only one pair.

---

## Persistence & durability — what survives a version bump

Keeping the volume declaration unchanged is the expected path for retaining
the same backing storage; verify the actual mount and state after a rollout.
That does not guarantee schema compatibility, safe overlapping writers or
image-only rollback: follow the
[6.11 to 6.13 handoff and rollback gate](#611-to-613-upgrade-and-rollback-boundary).

- **Server `/data`** — blobs (`/data/blobs`), the **oid identity map**
  (`/data/identity/entra-identities.json`), durable ingest queues (`/data/queues`),
  and logs — lives on an **Azure Files share**, a separate durable resource that
  exists *independently* of the container image/revision.
- **The oid identity map** on the existing share remains authoritative. Verify
  it is still present and usable; do not run the seeder to fix a missing mount.
- **Key Vault references** in the API manifest select existing values. Rollout
  does not replace the separately coordinated rotation of those values.
- **Neo4j data** lives on the VM's **persistent managed data disk**, entirely
  outside the manifest (see Neo4j safety, above). Untouched by any server redeploy.

### Infrastructure actions that can lose `/data` — avoid during a version bump

| Destructive action | Why it loses data |
|--------------------|-------------------|
| Remove/rename the `volume:` block | New revision has no `/data` mount → writes to ephemeral FS |
| Change `mount_path` | App reads an empty path; share persists but is "gone" from the server's view |
| Resize without proving it is in place, or change `tier` | A replacement share is not a data migration; stop if the plan proposes replacement |
| `amplifier-online destroy` | Tears down per-project resources including the share |
| Delete/re-provision the Azure Files share or storage account out-of-band | Removes the backing store |

### Capacity planning and in-place resize

The manifest configures a **2048 GiB** premium volume at `/data`. Monitor
used capacity against quota and alert with time to act on the growth trend.
Include queues, logs and retained blob payloads; measure actual share usage
rather than assuming preview's estimated allocated bytes will be reclaimed.
Capacity growth is separate from enabling deletion.

If more headroom is needed, plan a separate `size_gib` increase on the
**same existing share**, up to **5120 GiB** under the Amplifier Online volume
limit.
Confirm applicable platform limits, take and verify a recoverable backup, and
review `amplifier-online up --dry-run` with the platform owner. Proceed with
approved `up` only when in-place growth is established: preserve the mount
path, tier and backing share; **stop** on any proposed recreation, replacement
or uncertain storage effect. Afterwards verify quota, the unchanged backing
share, and that the API can read existing state and process new ingestion.

### Protect-the-durable-data checklist (before a version bump)

```
[ ] Reviewed code ref recorded; no change to API volume, storage paths, or
    credential references. Infra changes require a separate reviewed plan.
[ ] Complete the quiescent, consistent coupled-state backup and rollback review
    above; a live share snapshot alone is not sufficient.
[ ] Do NOT run `amplifier-online destroy`; do NOT touch the share/storage account.
[ ] Do not rotate or manually rewrite credentials during the code rollout.
[ ] After deploy, verify the /health commit, same backing share and existing
    identities, authorized Neo4j reads, and accepted test ingestion actually drained.
```
