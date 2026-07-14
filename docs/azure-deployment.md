# Deploying to Azure (compliant, via `amplifier-online`)

This is a guide for deploying the Context Intelligence Server to Azure **the
compliant way**. It describes how to deploy using the **`amplifier-online`
bundle tooling** — you author a project manifest (`amplifier-online.yaml`) and run
`amplifier-online up`, which provisions the container app, its ingress, and the
supporting Azure resources for you.

It is a *how-to* guide, not a description of any existing deployment. All names,
IDs, addresses, and endpoints below are **placeholders** — substitute your own.
Never commit real secrets, object IDs, private addresses, or resource names to
this repo.

> **Two moving parts.** The **server** runs as an Azure Container App (deployed by
> `amplifier-online`). **Neo4j** runs separately on a **VM inside a private
> virtual network** (you provision this once). The server reaches Neo4j over
> private Bolt; Neo4j is never publicly exposed. Secrets flow through **Key
> Vault**, never through the manifest.

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

You do **not** push to a project ACR. Amplifier Online uses **one shared platform
registry** (`amplifieronlinecr`, admin disabled) that **no account can push to** —
a manual `docker push` / `az acr build` against it returns `UNAUTHORIZED`. Instead,
image delivery is **push-to-deploy**:

1. `amplifier-online cicd create` enrolls this repo and writes a GitHub Actions
   workflow.
2. On `git push`, CI builds the image from this repo's `Dockerfile` (approved base,
   above) and pushes it to **your GitHub repo's `ghcr.io`**.
3. The Amplifier Online **provisioner imports `ghcr.io → amplifieronlinecr`** and
   rolls a new Container Apps revision.

So the `services.<name>.image` value in `amplifier-online.yaml`
(`amplifieronlinecr.azurecr.io/...`) is a **destination the provisioner writes to,
not a registry you push to**. There is no hand-rolled `az containerapp create`;
`amplifier-online` owns provisioning, and the CI pipeline owns image build + import.

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
- **`amplifier-online` CLI** (the deployment tooling this guide uses). Verify it
  is installed and authenticated to the same subscription.
- **GitHub CLI (`gh`)**, authenticated to the repo — `amplifier-online cicd create`
  uses it to enrol the repo and set the deploy workflow's variables. Image build +
  push happens **in CI** (to `ghcr.io`); you do **not** build/push locally.
- `jq` and `curl` (used by the identity-seed step).

**Azure resources / access:**

- A **subscription** and **resource group** you can deploy into.
- **No project ACR to create.** Images live in the **shared platform registry**
  (`amplifieronlinecr`), which the provisioner writes to via a `ghcr.io → ACR`
  import — you never push to it (see the image-policy section).
- **No hand-rolled VNet/subnet/NSG.** The `web-app-aca` app runs in the shared
  platform VNet; the Neo4j VM is a separate **`vm`-stack** Amplifier Online project
  that owns its own private subnet placement and NSG (see Step 2).
- A **Key Vault** for secrets (Neo4j credentials, any static keys), and a
  **managed identity** the container app can use to read those secrets.
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

## Step 1 — Enrol the repo for push-to-deploy (CI builds the approved-base image)

You do **not** build or push the image by hand. Amplifier Online delivers images
**push-to-deploy**: enrol the repo once, then every `git push` builds the image on
the approved base in CI and hands it to the provisioner to import into the shared
platform registry.

```bash
# One-time: generate the GitHub Actions workflow + enrol the repo.
# Preview first (writes nothing), then create for real:
amplifier-online cicd create --dry-run
amplifier-online cicd create
```

`cicd create` writes `.github/workflows/api-build-deploy.yaml` and sets the repo
variables the deploy job needs (`AO_PROVISIONER_URL`, `AO_PROVISIONER_AUDIENCE`,
`AO_PROJECT_NAME`, and the Entra IDs). Commit the workflow and push:

```bash
git add .github/workflows/ && git commit -m "ci: add amplifier-online deploy workflow"
git push        # CI builds from this repo's Dockerfile (approved base) → ghcr.io,
                # then the provisioner imports ghcr.io → amplifieronlinecr and rolls a revision
```

What happens on each push:

1. **Build job** builds the image from this repo's `Dockerfile` (approved MCR Azure
   Linux base) and pushes it to **`ghcr.io/<owner>/<repo>-api:<sha>`**.
2. Compliance is enforced **in CI, before the image is imported**: a Trivy scan
   (fails on CRITICAL/HIGH), an SBOM, and a build-provenance attestation — layered
   on top of the approved `FROM` base in the Dockerfile.
3. **Deploy job** authenticates to the provisioner with a GitHub **OIDC** token (no
   stored Azure secret); the provisioner **imports `ghcr.io → amplifieronlinecr`**
   and rolls a new Container Apps revision.

> **There is no manual push path.** The shared platform registry
> (`amplifieronlinecr`) has ACR admin disabled and grants **no** push access — a
> `docker push` / `az acr build` against it returns `UNAUTHORIZED`. The
> `amplifieronlinecr.azurecr.io/...` value in the manifest is the **destination the
> provisioner writes to**, never a registry you push to. The only registry you push
> to (via CI) is your repo's `ghcr.io`.

Check status after a push:

```bash
amplifier-online status
```

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
  size: Standard_D2as_v5           # memory-optimized SKUs (E-series) for larger graphs
  admin_username: azureuser
  ssh_public_key: "ssh-ed25519 AAAA..."   # PUBLIC half only; key auth, no passwords
  cloud_init: ./cloud-init.yaml            # contents inlined at `up`; runs on first boot
  data_disk_gib: 64                        # persistent graph store; survives `up` re-runs
  static_private_ip: 10.100.4.4            # fixes the Bolt endpoint the web app hard-codes
  ports:
    - port: 7687                           # Neo4j Bolt — the ONLY inbound rule
      protocol: Tcp
      source: cae-infra                    # the CAE app-egress subnet (where the ACA app calls from)
```

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
  # 5. Set an initial password, then start. Capture the password into Key Vault
  #    (kv-context-intel) out of band so the web app's NEO4J_PASSWORD secretRef matches.
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

The `web-app-aca` project (Steps 1, 4) reaches Neo4j over **private IP on the shared
platform VNet**: the ACA container's egress comes from the `cae-infra` subnet, the
VM's NSG allows `7687` from `cae-infra`, so the app connects to
`bolt://10.100.4.4:7687` directly — no DNS, no public exposure. The private IP is a
**coordinate, not a secret** (reachability is already gated by the NSG + no-public-IP
posture), which is why it sits in plaintext in the web-app manifest while the Neo4j
**credentials** are Key Vault secretRefs (Step 3). Do not move the IP into Key Vault.

---

## Step 3 — Secrets: Key Vault + the declarative-deploy rule

> **Read this first — it is the load-bearing rule.** `amplifier-online up` is
> **declarative**: it rewrites the container's **entire env list** from the
> manifest (+ auto-injected vars) on **every** deploy. Anything you set on the app
> out of band with `az ... --set-env-vars` is **wiped on the next `up`**. Because
> the server's Neo4j access-mode validators **fail loud**, a wiped env then makes
> the server **refuse to boot**. So split responsibilities precisely:
>
> - **Non-secret env leaves** (URLs, usernames, access modes) → **in the manifest
>   `env:` block** (Step 4). They survive every `up`.
> - **Secret values** → **Key Vault**, exposed as **Container Apps secrets**
>   (these persist on the app resource), and referenced by env via `secretref:`.
> - **The env→secretref mapping** is the *only* out-of-band piece, and it **must be
>   re-applied after every `amplifier-online up`** (fold it into `deploy.sh`).
> - **Never** put a `secretref:`/`keyvaultref:` string in the manifest — it ships
>   **verbatim** (no expansion) and would land as a literal string, not a resolved
>   reference.

**3a — Store secrets in Key Vault.**

```bash
az keyvault secret set --vault-name <your-key-vault> \
  --name context-intelligence-neo4j-password --value "<neo4j-password>"
```

**3b — Grant the app's managed identity read access BEFORE referencing** (or the
new revision can't pull the secret and won't go healthy):

```bash
# RBAC-authorization vault (recommended): assign the built-in role
az role assignment create \
  --assignee <app-managed-identity-principal-id> \
  --role "Key Vault Secrets User" \
  --scope <key-vault-resource-id>

# Access-policy vault (legacy model): grant get + list instead
# az keyvault set-policy --name <your-key-vault> \
#   --object-id <app-managed-identity-principal-id> --secret-permissions get list
```

**3c — Create the Container Apps secret as a Key Vault reference** bound to the
identity. These secrets persist on the app across `up`:

```bash
az containerapp secret set -n <container-app-name> -g <resource-group> \
  --secrets \
    neo4j-password=keyvaultref:https://<your-key-vault>.vault.azure.net/secrets/context-intelligence-neo4j-password,identityref:<app-managed-identity-resource-id>
```

- **User-assigned identity:** `identityref:<managed-identity-resource-id>` (as above).
- **System-assigned identity:** use the literal `identityref:system`.
- **Omit the secret version** (`.../secrets/<name>`, no trailing version) so
  Container Apps re-resolves it periodically and **rotated passwords are picked
  up automatically** (~30 min). Pinning a version freezes the value.

**3d — Map env → secret (RE-APPLY AFTER EVERY `up`).** `az containerapp update
--set-env-vars` automatically rolls a **new revision** (no manual restart); in
single-revision mode it supersedes the old one:

```bash
az containerapp update -n <container-app-name> -g <resource-group> \
  --set-env-vars \
    AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J_PASSWORD=secretref:neo4j-password
```

Non-secret leaves (`NEO4J_URL`, `NEO4J_USER`) live in the **manifest** (Step 4),
not here. Never place secret values as plaintext env vars, and never commit them.

### The two Neo4j clients (admin/write and cypher_query/read)

The server has two internal Neo4j clients — a write/admin client and a read client
for the `/cypher` endpoint. There are two ways to configure them:

- **Flat config (Step 3, above):** one URL / user / password that the server fans
  out to *both* clients. Simplest, and it is what the **current setup uses**.
- **Structured config:** the `neo4j` field with `admin` and `cypher_query`
  sub-clients, each `{url, username, password, access_mode}`, read via the nested
  delimiter `__`. Use this when you want the two clients to have **different
  credentials** — which requires Neo4j **Enterprise** (see below).

> **Current setup: Neo4j Community → ONE account, shared by both clients.**
> Neo4j **Community Edition does not support multiple users or role-based access
> control** — there is a single account (`neo4j`). So admin and read use the
> **same username and password**, and **Key Vault holds exactly one pair**
> (`context-intelligence-neo4j-user` + `context-intelligence-neo4j-password`).
> There is no separate read-only account to store or wire. On Community the
> flat config (Step 3) is the correct choice — nothing further to do here.

**If you configure the structured block anyway on Community**, both clients point
at the **same** account (same username, same password secret) and differ only by
`access_mode`:

```yaml
# manifest env: — non-secret leaves (survive every `up`). Same user, same URL.
- { name: AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J__ADMIN__URL,             value: "bolt://<neo4j-private-address>:7687" }
- { name: AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J__ADMIN__USERNAME,        value: "neo4j" }
- { name: AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J__ADMIN__ACCESS_MODE,     value: "WRITE" }
- { name: AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J__CYPHER_QUERY__URL,      value: "bolt://<neo4j-private-address>:7687" }
- { name: AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J__CYPHER_QUERY__USERNAME, value: "neo4j" }
- { name: AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J__CYPHER_QUERY__ACCESS_MODE, value: "READ" }
```

```bash
# Both password leaves reference the SAME Key Vault secret (one account).
# RE-APPLY after every `up` (fold into deploy.sh):
az containerapp update -n <container-app-name> -g <resource-group> \
  --set-env-vars \
    AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J__ADMIN__PASSWORD=secretref:neo4j-password \
    AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J__CYPHER_QUERY__PASSWORD=secretref:neo4j-password
```

> **Hard rules (fail-loud at startup):** `admin.access_mode` MUST be `WRITE` and
> `cypher_query.access_mode` MUST be `READ` — the field defaults to `WRITE`, so
> the read client's `READ` **must be set explicitly** or the server refuses to
> boot. Do NOT mix flat `NEO4J_*` keys with structured `NEO4J__*` keys — pick one
> shape. On Community, the structured block buys you **nothing** over the flat
> config (same single account), so prefer the flat config there.

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

**To require the structured block** (reject a silent fall-back to the flat single
account in a deployed profile), also set:

```
AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J_REQUIRE_EXPLICIT_CLIENTS=true
```

With that flag on, the server refuses to start unless both `admin` and
`cypher_query` are explicitly configured — a good guardrail for production so a
missing structured block can't silently degrade to one shared account.

---

## Step 4 — Author `amplifier-online.yaml`

Describe the deployment in the project manifest. Keep only **non-secret** values
here; the Neo4j URL is a **private address**, and credentials come from Key Vault
(Step 3). Sketch (placeholders):

```yaml
name: context-intelligence
stack: web-app-aca            # public ingress + APIM front when auth.expose: true

auth:
  api_app_id: "<entra-app-client-id>"
  expose: true                # provision the public edge (ingress + APIM gateway)

services:
  api:
    image: amplifieronlinecr.azurecr.io/context-intelligence-server:<version>  # shared platform ACR — the provisioner's import destination, NOT a push target
    port: 8000                # matches EXPOSE 8000 in the Dockerfile

    volume:                   # persistent /data (identity store, queues, blobs, logs)
      mount_path: /data
      size_gib: 16

    env:
      # Force Entra auth (server DEFAULTS to static — must override)
      - name: AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_AUTH_MODE
        value: entra
      - name: AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_AZURE_CLIENT_ID
        value: "<entra-app-client-id>"
      - name: AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_AZURE_TENANT_ID
        value: "<entra-tenant-id>"
      - name: AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_ENTRA_ADMIN_ROLE
        value: IdentityAdmin

      # Durable /data paths (must live on the mounted volume)
      - name: AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_QUEUES_PATH
        value: /data/queues
      - name: AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_BLOB_PATH
        value: /data/blobs
      - name: AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_LOG_PATH
        value: /data/logs/server.jsonl
      - name: AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_ENTRA_IDENTITIES_STORE_PATH
        value: /data/identity/entra-identities.json

      # Neo4j over private Bolt (credentials are wired from Key Vault, Step 3)
      - name: AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J_URL
        value: bolt://<neo4j-private-address>:7687
```

> The manifest is committed; **it must contain no secrets and no PII**. The
> `entra_identities` map (Entra object IDs → handles) is PII — it is seeded over
> the admin API after `up` (see "Seeding the identity map"), never committed.

---

## Step 5 — Deploy with `amplifier-online up`

Provision/update everything from the manifest:

```bash
amplifier-online up
```

Then wire the Key Vault secret references (Step 3) and — on a fresh `/data` in
Entra mode — seed the identity map (below). The repo-root `deploy.sh` runs
`amplifier-online up` and the identity seed together:

```bash
./deploy.sh                 # = amplifier-online up  +  seed (idempotent)
./deploy.sh --no-seed       # deploy only, skip the seed step
```

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
| `ENTRA_IDENTITIES` (JSON) | yes | `oid → {id}` map for the **user (delegated)** path (**PII** — seed via admin API, do not commit) |
| `SERVICE_IDENTITIES` (JSON) | no | Optional friendly-`created_by` map for **service** principals — not an auth gate, no runtime CRUD |
| `SERVICE_DATA_ROLE` | no | App Role granting service write+read (default `Contributor`) |
| `READER_ROLE` | no | App Role granting service read-only (default `Reader`) |

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
repo or the deployment manifest. In steady state you don't re-seed it: after first
boot the map is authoritative in the durable `/data` volume
(`entra_identities_store_path`, e.g. `/data/identity/entra-identities.json`), and a
routine redeploy (including `amplifier-online up`) leaves that volume untouched.

Because `amplifier-online up` ships the manifest **verbatim** (no `${VAR}`, shell,
file, or secret expansion), the map cannot be supplied through `up` itself. It is
applied over the runtime admin API right after `up`. An empty `entra_identities`
map is a fail-closed startup error, so this onboarding is required before the
server will serve on a fresh volume. Once `/data` is populated it is authoritative,
so re-running the seed is a harmless idempotent no-op.

```bash
# 1. One-time: create the uncommitted seed file from the example, then edit it to
#    hold the real  "<oid>": {"id":"<github-handle>"}  entries. It is git-ignored.
cp scripts/entra-identities.example.json scripts/entra-identities.local.json

# 2. az login as an IdentityAdmin, point at the server + app-reg audience, deploy:
az login                                                    # IdentityAdmin identity
export SERVER_URL="https://<your-server-fqdn-or-apim-gateway>"
export AUTH_RESOURCE="api://<entra-app-client-id>"
export SEED_FILE="scripts/entra-identities.local.json"
./deploy.sh                 # = amplifier-online up  +  seed (idempotent)
```

**Seeder alone.** `deploy.sh` calls `scripts/seed-entra-identities.sh`, which you
can also run directly (e.g. to re-seed without redeploying). It reads the same
uncommitted local file and applies it over `PUT /admin/identities/{oid}`:

```bash
./scripts/seed-entra-identities.sh --check   # dry run — shows what it WOULD PUT
./scripts/seed-entra-identities.sh           # apply + verify all oids present
```

The seeder fails loud if the seed file is missing/empty (it will not "seed
nothing"), is idempotent (re-adding an existing mapping is a `200` no-op), and
verifies every oid is present in the live map afterward. See
[identity-management.md](identity-management.md) for the admin API details.

---

## TLS and connecting the Amplifier bundle

Azure Container Apps issues and renews TLS automatically — the container serves
plain HTTP on port 8000 and Azure terminates HTTPS at the platform edge. With
`auth.expose: true` the public edge is an APIM gateway in front of the app.

Point the Amplifier hook at the server's HTTPS endpoint:

```yaml
overrides:
  hook-context-intelligence:
    config:
      context_intelligence_server_url: "https://<your-server-fqdn-or-apim-gateway>"
      # In Entra mode, the hook presents an Entra bearer token (no static key).
```

---

## Updating the server (build & deploy a new version — Neo4j-safe)

Runbook for shipping a new version (e.g. `v6.7.0`) without disturbing Neo4j.
Placeholders in `<angle-brackets>`.

**Pre-flight**
- Repo `pyproject.toml` version == the version you're shipping.
- `Dockerfile` `FROM` is the **approved MCR Azure Linux base** (not Docker Hub /
  any non-approved base). If not, STOP — it fails S360.
- Build from a clean, committed tree; capture `<sha> = git rev-parse --short HEAD`.

**1. Build + deliver via CI push-to-deploy.** Bump `pyproject.toml`, commit on the
approved base, and push — CI builds `ghcr.io/<owner>/<repo>-api:<sha>` and the
provisioner imports it into `amplifieronlinecr`:
```bash
git commit -am "release: v6.7.0" && git push
```
There is **no manual `az acr build`** — the shared platform ACR rejects direct
pushes (`UNAUTHORIZED`), and compliance is enforced **in CI** (approved `FROM` base
→ Trivy CRITICAL/HIGH gate → SBOM + provenance attestation) before the image is
imported. Watch the run and confirm the new revision:
```bash
amplifier-online status
```

**2. Bump the manifest — the ONLY manifest change.** The image tag in
`amplifier-online.yaml` records which built tag the app runs; CI produces it and the
provisioner imports it. Change *only* the tag:
```yaml
services:
  api:
    image: amplifieronlinecr.azurecr.io/context-intelligence-server:v6.7.0   # was :v6.6.6 — platform ACR (import destination)
    port: 8000
```
Do **not** change `name`, `stack`, `port`, the `/data` volume, `resources`, or any
`NEO4J__*` env leaves. Preview first: `amplifier-online up --dry-run` — expect it
to report the image tag change and **nothing** about the volume, Neo4j, or infra.
If it wants to recreate the volume or change infra, STOP.

**4. Deploy:** `amplifier-online up` — rolls a new revision on the same app, same
`/data` volume, same VNet.

**5. Re-apply the password secretRef env (REQUIRED after every `up`).** `up`
rewrites the container's entire env list; the non-secret `NEO4J__*` leaves survive
(they're in the manifest) but the password secretRef mappings are wiped. On Neo4j
**Community**, both clients reference the **one** shared secret:
```bash
az containerapp update -n <container-app-name> -g <resource-group> \
  --set-env-vars \
    AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J__ADMIN__PASSWORD=secretref:neo4j-password \
    AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J__CYPHER_QUERY__PASSWORD=secretref:neo4j-password
```
> Bake this step into `deploy.sh` so a version bump can never boot without the
> Neo4j password (a missing password trips the fail-loud validators and *looks*
> like a Neo4j outage but is really a missing-env problem).

**6. Verify:** `amplifier-online status` + `amplifier-online logs --since 10` —
expect the new revision Running/Healthy, no `access_mode` validation error, and
Neo4j connected on **both** clients.

### Neo4j safety — guarantees & do-NOT-touch list

**Why `amplifier-online up` cannot disturb Neo4j:** the Neo4j VM is a **separate
Azure resource, not declared in `amplifier-online.yaml`**. `up` only reconciles the
container app + its declared resources (image, port, `/data` volume, env). It has
no handle on the VM, its managed disk, or its data — it cannot touch them. The
server's `/data` volume is unrelated to Neo4j storage; a tag-only image bump does
not alter it.

**Do NOT touch during a version bump** (these are the *only* ways to break Neo4j):
- The Neo4j VM, its data disk, or snapshots/backups.
- **VNet / subnet / NSG rules** — especially the Bolt `7687` allow-rule from the
  app subnet. Don't bundle network edits into a deploy window.
- The `NEO4J__*__URL` values (private Bolt endpoint) — keep pointing at the same
  `<neo4j-private-address>:7687`. A wrong URL = server up, graph unreachable.
- The Key Vault secret **values** (`context-intelligence-neo4j-user/-password`).
  Re-applying the secret**ref** mapping is fine and required; changing the secret
  **value** rotates the shared account and must be coordinated with the VM.
- The single-account model — do **not** invent a second read-only account on
  Community; there is only one pair.

**On Community, prefer the flat single-account config** (`NEO4J_URL` /
`NEO4J_USER` / `NEO4J_PASSWORD`) over the structured block: one account means the
structured split buys no isolation and adds the `ACCESS_MODE=READ` defaults trap.
The structured block (both passwords → the same secret) becomes worthwhile **only
if you move to Neo4j Enterprise** and create a genuine read-only account with its
own second secret.

---

## Persistence & durability — what survives a version bump

A tag-only image bump + `amplifier-online up` **preserves all durable data**. The
guarantee is structural, not "be careful":

- **Server `/data`** — blobs (`/data/blobs`), the **oid identity map**
  (`/data/identity/entra-identities.json`), durable ingest queues (`/data/queues`),
  and logs — lives on an **Azure Files share**, a separate durable resource that
  exists *independently* of the container image/revision. The image is stateless
  compute; the share is stateful backing storage. `up` swaps the compute and
  **re-mounts the same share** at `/data`. Nothing about a new revision recreates
  or empties it.
- **The oid identity map survives two ways:** (1) the file is on the durable share,
  so a redeploy finds the existing authoritative map; (2) the `ENTRA_IDENTITIES`
  seed is idempotent — it only populates an **empty** store, so it never overwrites
  runtime identity edits made via the admin API.
- **Key Vault is READ-ONLY in this flow.** `keyvaultref`/`secretref` only *read*
  secret values at resolve time; nothing in the deploy writes to the vault. Mapping
  a secretRef creates a reference *to* the secret — it does not mutate it. The
  managed identity needs only **Key Vault Secrets User** (read).
- **Neo4j data** lives on the VM's **persistent managed data disk**, entirely
  outside the manifest (see Neo4j safety, above). Untouched by any server redeploy.

### The ONLY things that can lose `/data` — avoid during a version bump

| Destructive action | Why it loses data |
|--------------------|-------------------|
| Remove/rename the `volume:` block | New revision has no `/data` mount → writes to ephemeral FS |
| Change `mount_path` | App reads an empty path; share persists but is "gone" from the server's view |
| Change `size_gib` **or** add/change `tier` | Resize/tier change can **re-provision a new, empty share — no data migration** |
| `amplifier-online destroy` | Tears down per-project resources including the share |
| Delete/re-provision the Azure Files share or storage account out-of-band | Removes the backing store |

`size_gib: 16` is safe to leave unchanged — leaving it is the safe path. **Treat
any `size_gib`/`tier` edit as potentially destructive.** A version bump changes
**only the image tag**.

### Protect-the-durable-data checklist (before a version bump)

```
[ ] Manifest diff shows ONLY the image tag change (v6.6.6 → v6.7.0).
    volume block byte-identical: mount_path: /data, size_gib: 16
    (no size_gib change, no tier added, no mount_path change).
[ ] `amplifier-online up --dry-run` reports ONLY the image/revision change —
    NO volume, share, or storage change. If it mentions volume/storage → STOP.
[ ] (Extra safety) Snapshot the Azure Files share first:
      az storage share snapshot --name <share> --account-name <acct>
[ ] Do NOT run `amplifier-online destroy`; do NOT touch the share/storage account.
[ ] Key Vault: READ + map only (secretref/keyvaultref). No `az keyvault secret set`.
[ ] After `up`: re-apply the password secretRef env (mandatory every up), then
    verify /data intact — blobs under /data/blobs, and
    /data/identity/entra-identities.json non-empty and unchanged.
```
