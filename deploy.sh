#!/usr/bin/env bash
#
# deploy.sh — one-command deploy for the context-intelligence server.
#
# Runs the two steps a real deploy needs, in order:
#   1. `amplifier-online up`          — provision/update the Azure Container App
#                                       from amplifier-online.yaml (this repo).
#   2. scripts/seed-entra-identities.sh — onboard the oid->handle identity map.
#
# WHY TWO STEPS IN ONE SCRIPT: `amplifier-online up` ships the manifest verbatim
# and does NO variable expansion, so the identity map cannot be supplied through
# the manifest without committing real Entra object IDs (PII) to git. Instead the
# map lives in an UNCOMMITTED, git-ignored local file and is applied over the
# server's admin API right after `up`. The seed is idempotent and a no-op once
# the server's /data volume is already populated, so running this on every deploy
# is safe — a fresh/empty /data gets seeded, an existing one is left untouched.
#
# REQUIREMENTS: amplifier-online CLI, az CLI logged in as an IdentityAdmin, jq, curl.
#
# USAGE (from the repo root):
#   export SERVER_URL="https://<server-fqdn-or-apim-gateway>"   # for the seed step
#   export AUTH_RESOURCE="api://<client_id>"                    # token audience
#   export SEED_FILE="scripts/entra-identities.local.json"     # uncommitted map
#   ./deploy.sh
#
#   # Skip the seed (deploy only) if you just want `up`:
#   ./deploy.sh --no-seed
#
set -euo pipefail
cd "$(dirname "$0")"

SEED=1
[ "${1:-}" = "--no-seed" ] && SEED=0

echo "==> [1/2] amplifier-online up"
amplifier-online up

if [ "$SEED" = 0 ]; then
  echo "==> [2/2] seed skipped (--no-seed)"
  exit 0
fi

echo "==> [2/2] onboard identity map (idempotent; no-op if /data already seeded)"
if [ ! -x scripts/seed-entra-identities.sh ]; then
  echo "FATAL: scripts/seed-entra-identities.sh missing or not executable" >&2
  exit 1
fi
./scripts/seed-entra-identities.sh

echo "==> deploy complete."
