# Deploying to Azure (Container Apps)

## Overview

Azure Container Apps issues and renews TLS certificates automatically — no TLS
configuration needed in the app. The container serves plain HTTP on port 8000;
Azure terminates HTTPS at the platform edge.

The auto-issued HTTPS FQDN takes the form:

```
https://<app-name>.<unique-env-id>.<region>.azurecontainerapps.io
```

## Required Azure Components

| Resource | Role |
|---|---|
| Resource Group | Container for all resources |
| Azure Container Registry (ACR) | Stores the Docker image |
| Container Apps Environment | Managed runtime (networking, ingress, scaling) |
| Azure Files share | Persistent `/data` — blobs, logs, credentials.yaml across restarts |
| Neo4j (AuraDB recommended) | Graph database |

## Neo4j on Azure — Two Options

**Option A (recommended): Neo4j AuraDB** — managed cloud service, zero ops, free
tier available. Connection string:
`neo4j+s://xxxx.databases.neo4j.io:7687` (TLS built-in). No network configuration
needed.

**Option B: Neo4j as a Container App** in the same environment — internal service
discovery (`bolt://neo4j:7687`), no public port exposed.

## Preferred Auth Pattern Between Server and Neo4j

Store **all** sensitive values as Container Apps secrets — never plain env vars:

```bash
# Store ALL sensitive values as Container Apps secrets — never plain env vars
az containerapp secret set \
  --name context-intelligence-server \
  --resource-group myapp-rg \
  --secrets "neo4j-password=<value>" "api-key=<value>"

# Reference secrets in env vars when creating the container app (see step 6):
az containerapp update \
  --name context-intelligence-server \
  --resource-group myapp-rg \
  --set-env-vars \
    "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J_PASSWORD=secretref:neo4j-password" \
    "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_API_KEY=secretref:api-key"
```

## Step-by-Step Deployment

### Step 1 — Prerequisites

```bash
az extension add --name containerapp --upgrade
az provider register --namespace Microsoft.App
az provider register --namespace Microsoft.OperationalInsights
az login
```

### Step 2 — Create resource group

```bash
az group create --name myapp-rg --location eastus
```

### Step 3 — Create ACR and build the image

```bash
az acr create --resource-group myapp-rg --name myappregistry --sku Basic

# Build and push from local source
az acr build \
  --registry myappregistry \
  --image context-intelligence-server:latest \
  --file Dockerfile .
```

### Step 4 — Create Container Apps Environment

```bash
az containerapp env create \
  --name myapp-env \
  --resource-group myapp-rg \
  --location eastus
```

### Step 5 — Create Azure Files share for persistent `/data`

```bash
az storage account create \
  --name myappstorage \
  --resource-group myapp-rg \
  --location eastus

STORAGE_KEY=$(az storage account keys list \
  --account-name myappstorage \
  --resource-group myapp-rg \
  --query [0].value -o tsv)

az storage share create \
  --account-name myappstorage \
  --name app-data

az containerapp env storage set \
  --name myapp-env \
  --resource-group myapp-rg \
  --storage-name appdata \
  --storage-type AzureFile \
  --azure-file-account-name myappstorage \
  --azure-file-account-key "${STORAGE_KEY}" \
  --azure-file-share-name app-data \
  --access-mode ReadWrite
```

### Step 6 — Create the Container App

```bash
ACR_LOGIN=$(az acr show --resource-group myapp-rg --name myappregistry --query loginServer -o tsv)
ACR_USER=$(az acr credential show --resource-group myapp-rg --name myappregistry --query username -o tsv)
ACR_PASS=$(az acr credential show --resource-group myapp-rg --name myappregistry --query passwords[0].value -o tsv)

az containerapp create \
  --name context-intelligence-server \
  --resource-group myapp-rg \
  --environment myapp-env \
  --image "${ACR_LOGIN}/context-intelligence-server:latest" \
  --ingress external \
  --target-port 8000 \
  --cpu 0.5 \
  --memory 1.0Gi \
  --min-replicas 1 \
  --max-replicas 3 \
  --secrets "neo4j-password=<your-neo4j-password>" "api-key=<your-api-key>" \
  --env-vars \
    "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J_URL=<neo4j-url>" \
    "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J_PASSWORD=secretref:neo4j-password" \
    "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_API_KEY=secretref:api-key" \
    "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE=/data/credentials.yaml" \
    "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_BLOB_PATH=/data/blobs" \
    "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_LOG_PATH=/data/logs/server.jsonl" \
    "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_QUEUES_PATH=/data/queues" \
  --registry-server "${ACR_LOGIN}" \
  --registry-username "${ACR_USER}" \
  --registry-password "${ACR_PASS}" \
  --revisions-mode single
```

### Step 7 — Mount Azure Files to `/data`

Export the app configuration, add volume mounts, and apply:

```bash
az containerapp show \
  --name context-intelligence-server \
  --resource-group myapp-rg \
  -o yaml > app.yaml
```

Edit `app.yaml` to add under `template.containers[0]`:
```yaml
volumeMounts:
  - volumeName: azure-data
    mountPath: /data
```

And at the same level as `containers`:
```yaml
volumes:
  - name: azure-data
    storageType: AzureFile
    storageName: appdata
```

Apply:
```bash
az containerapp update \
  --name context-intelligence-server \
  --resource-group myapp-rg \
  --yaml app.yaml
```

### Step 8 — Configure health probes

Add to the container spec in `app.yaml` alongside `volumeMounts`:
```yaml
probes:
  - type: Readiness
    httpGet:
      path: /status
      port: 8000
    initialDelaySeconds: 5
    periodSeconds: 10
    failureThreshold: 3
  - type: Liveness
    httpGet:
      path: /status
      port: 8000
    initialDelaySeconds: 30
    periodSeconds: 10
    failureThreshold: 3
```

Apply again:
```bash
az containerapp update \
  --name context-intelligence-server \
  --resource-group myapp-rg \
  --yaml app.yaml
```

## First-Run Credentials Note

`docker-entrypoint.sh` writes `/data/credentials.yaml` on first run. With Azure
Files mounted to `/data/`, this persists across restarts. All sensitive values
supplied via Container Apps secrets override the yaml (env vars take precedence in
the config system), so the auto-generated values in the file are safely ignored.

> **Note:** `QUEUES_PATH` must point at the mounted `/data` Azure Files volume (durable
> storage) — it holds events that have been accepted (`202`) but not yet written to Neo4j;
> placing it on the container's ephemeral filesystem would lose in-flight events on restart.

## HTTPS and Connecting the Amplifier Bundle

```bash
# Get the auto-issued FQDN
az containerapp show \
  --name context-intelligence-server \
  --resource-group myapp-rg \
  --query properties.configuration.ingress.fqdn -o tsv
```

Update `settings.yaml`:

```yaml
overrides:
  hook-context-intelligence:
    config:
      context_intelligence_server_url: "https://<fqdn>"
      context_intelligence_api_key: "<api-key>"
```

## Updating the Server

```bash
az acr build --registry <acr-name> --image context-intelligence-server:latest .
az containerapp update --name context-intelligence-server --resource-group myapp-rg \
  --image <acr-name>.azurecr.io/context-intelligence-server:latest
```

## Cost Reference

`--min-replicas 1` prevents scale-to-zero. 0.5 vCPU / 1 GiB ≈ $33/month. Free
tier: 180k vCPU-seconds/month per subscription.
