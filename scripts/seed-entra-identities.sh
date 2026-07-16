#!/usr/bin/env bash
#
# seed-entra-identities.sh — Disaster-recovery / first-boot re-seed of the
# oid -> contributor identity map, for the ENTRA auth mode.
#
# WHEN TO RUN THIS: ONLY when the server has been deployed onto a FRESH / EMPTY
# /data volume (a brand-new environment, or a DR rebuild where the persistent
# volume was lost). In steady state you do NOT need this: the oid->handle map is
# authoritative in the server's /data volume (entra_identities_store_path,
# e.g. /data/identity/entra-identities.json) after first boot, and a routine
# redeploy (including `amplifier-online up`) leaves that volume untouched.
#
# WHY IT EXISTS: the identity map contains real Microsoft Entra object IDs (PII)
# and is intentionally NOT committed to this repo or the deployment manifest.
# This script seeds the LIVE server from an UNCOMMITTED, gitignored local file
# via the server's IdentityAdmin-gated admin API (PUT /admin/identities/{oid}).
# There is no bulk endpoint, so it loops one idempotent PUT per oid.
#
# SAFETY:
#   - Fails loud if the seed file is missing/empty (refuses to "seed nothing").
#     Note: an empty entra_identities map is NOT a startup error server-side —
#     it is a supported bootstrap state (the server BOOTS fail-closed and warns,
#     then is populated at runtime via /admin/identities). This script's
#     empty-seed guard is a local convenience so you don't invoke it with
#     nothing to seed, not a server-side requirement.
#   - Idempotent: re-adding an existing mapping is a 200 no-op.
#   - Never deletes anything; only ensures each oid->handle mapping is present.
#   - --check does a dry run (prints what it WOULD PUT, makes no writes).
#
# REQUIREMENTS: az CLI logged in as an IdentityAdmin, jq, curl.
#
# USAGE (from the repo root):
#   export SERVER_URL="https://<your-server-fqdn-or-apim-gateway>"
#   export SEED_FILE="scripts/entra-identities.local.json"   # uncommitted, gitignored
#   # (optional) override the token audience if not the default below:
#   #   export AUTH_RESOURCE="api://<client_id>"
#   ./scripts/seed-entra-identities.sh --check     # dry run, no writes
#   ./scripts/seed-entra-identities.sh             # perform the seed + verify
#
# Seed-file format — object of { "<entra-oid>": {"id": "<github-handle>"} }:
#   see scripts/entra-identities.example.json (placeholder oids only).
#
set -euo pipefail

# Token audience for the server's app registration. Override via AUTH_RESOURCE.
RESOURCE="${AUTH_RESOURCE:-api://REPLACE-WITH-CLIENT-ID}"
SERVER_URL="${SERVER_URL:-}"
SEED_FILE="${SEED_FILE:-scripts/entra-identities.local.json}"
CHECK=0
[ "${1:-}" = "--check" ] && CHECK=1

die() { echo "FATAL: $*" >&2; exit 1; }

# --- preconditions (fail loud) ---
command -v az   >/dev/null || die "az CLI not found"
command -v jq   >/dev/null || die "jq not found"
command -v curl >/dev/null || die "curl not found"
[ -n "$SERVER_URL" ] || die "SERVER_URL is not set (server FQDN or APIM gateway)"
case "$RESOURCE" in
  *REPLACE-WITH-CLIENT-ID*) die "AUTH_RESOURCE not set (export AUTH_RESOURCE=api://<client_id>)";;
esac
[ -s "$SEED_FILE" ]  || die "seed file '$SEED_FILE' is missing or empty — refusing to seed nothing"

# Validate JSON shape: object of { "<oid>": {"id":"<handle>"} }
jq -e 'type=="object" and (to_entries|length>0) and all(.[]; has("id"))' "$SEED_FILE" >/dev/null \
  || die "seed file '$SEED_FILE' is not a non-empty object of {\"<oid>\":{\"id\":\"<handle>\"}}"

COUNT=$(jq 'length' "$SEED_FILE")
echo "Seed file : $SEED_FILE ($COUNT identities)"
echo "Server    : $SERVER_URL"
echo "Resource  : $RESOURCE"
echo "Mode      : $([ "$CHECK" = 1 ] && echo 'CHECK (dry run, no writes)' || echo 'APPLY')"
echo

TOKEN=$(az account get-access-token --resource "$RESOURCE" --query accessToken -o tsv) \
  || die "could not mint an Entra token (az login as an IdentityAdmin first)"
[ -n "$TOKEN" ] || die "empty token"

ok=0; fail=0
while IFS=$'\t' read -r oid id; do
  [ -n "$oid" ] || continue
  if [ "$CHECK" = 1 ]; then
    printf 'WOULD PUT  %s -> %s\n' "$oid" "$id"
    ok=$((ok+1)); continue
  fi
  code=$(curl -s -o /tmp/seed.out -w "%{http_code}" --max-time 30 \
    -X PUT "$SERVER_URL/admin/identities/$oid" \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -d "$(jq -nc --arg id "$id" '{id:$id}')")
  if [ "$code" = "200" ] || [ "$code" = "201" ]; then
    printf 'OK   %s  %s -> %s\n' "$code" "$oid" "$id"; ok=$((ok+1))
  else
    printf 'FAIL %s  %s -> %s | %s\n' "$code" "$oid" "$id" "$(head -c 200 /tmp/seed.out)"; fail=$((fail+1))
  fi
done < <(jq -r 'to_entries[] | "\(.key)\t\(.value.id)"' "$SEED_FILE")

echo
echo "SEED_SUMMARY ok=$ok fail=$fail total=$COUNT"
[ "$fail" -eq 0 ] || die "$fail identities failed to seed"

if [ "$CHECK" = 0 ]; then
  echo "Verifying live map contains all seeded oids..."
  live=$(curl -s --max-time 30 -H "Authorization: Bearer $TOKEN" "$SERVER_URL/admin/identities")
  missing=0
  while IFS= read -r oid; do
    echo "$live" | jq -e --arg o "$oid" 'any(.identities[]?; .oid==$o)' >/dev/null \
      || { echo "  MISSING after seed: $oid"; missing=$((missing+1)); }
  done < <(jq -r 'keys[]' "$SEED_FILE")
  [ "$missing" -eq 0 ] && echo "VERIFY OK — all $COUNT oids present in live map" \
    || die "$missing oids missing after seed"
fi
