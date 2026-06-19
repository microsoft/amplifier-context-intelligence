#!/usr/bin/env bash
#
# verify-airgap-apoc.sh — prove APOC loads in Neo4j with ZERO network download.
#
# Builds the air-tight image from ../../neo4j.Dockerfile (which bakes the APOC
# Core jar into /var/lib/neo4j/plugins at build time), runs it on a Docker
# *internal* network (no NAT / no internet egress), and verifies APOC is
# functional while the container is provably cut off from the internet.
#
# This is the portable, Docker-only regression check for the air-gapped APOC
# path. It needs only Docker — no Incus/DTU. Re-run it whenever the Neo4j base
# image version changes. For deeper, Incus-level isolation see dtu-profile.yaml
# in this directory.
#
# Usage:
#   test/airgap/verify-airgap-apoc.sh
#
# Env overrides:
#   NEO4J_VERSION   expected APOC/Neo4j version (default: 5.26.22)
#   IMAGE_TAG       built image tag (default: amplifier-ci-neo4j-apoc-airgap:test)
#   KEEP            set to 1 to leave the container/network/image for inspection
#
# Exit code 0 = PASS, non-zero = FAIL.
set -euo pipefail

NEO4J_VERSION="${NEO4J_VERSION:-5.26.22}"
IMAGE_TAG="${IMAGE_TAG:-amplifier-ci-neo4j-apoc-airgap:test}"
KEEP="${KEEP:-0}"

# Resolve repo root from this script's location (test/airgap/ -> repo root).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DOCKERFILE="${REPO_ROOT}/neo4j.Dockerfile"

CTR="ci-apoc-airgap-$$"
NET="ci-apoc-airgap-net-$$"
PW="airgaptest$$pw"
FAILED=0

log()  { printf '\n=== %s ===\n' "$*"; }
pass() { printf 'PASS: %s\n' "$*"; }
fail() { printf 'FAIL: %s\n' "$*"; FAILED=1; }

cleanup() {
  if [ "${KEEP}" = "1" ]; then
    printf '\n[KEEP=1] Leaving container=%s network=%s image=%s for inspection.\n' "${CTR}" "${NET}" "${IMAGE_TAG}"
    return
  fi
  docker rm -f "${CTR}" >/dev/null 2>&1 || true
  docker network rm "${NET}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

[ -f "${DOCKERFILE}" ] || { echo "FAIL: ${DOCKERFILE} not found"; exit 2; }
command -v docker >/dev/null 2>&1 || { echo "FAIL: docker not installed"; exit 2; }

log "Build air-tight image from neo4j.Dockerfile (bakes APOC jar, no runtime download)"
docker build -t "${IMAGE_TAG}" -f "${DOCKERFILE}" "${REPO_ROOT}"

log "Confirm APOC jar is BAKED into the image layer (present before any run)"
if docker run --rm --entrypoint sh "${IMAGE_TAG}" -c 'ls -1 /var/lib/neo4j/plugins/apoc.jar' >/dev/null 2>&1; then
  pass "apoc.jar present in image at /var/lib/neo4j/plugins/apoc.jar"
else
  fail "apoc.jar NOT baked into image"
fi

log "Create an INTERNAL Docker network (no NAT, no internet egress)"
docker network create --internal "${NET}" >/dev/null
if [ "$(docker network inspect -f '{{.Internal}}' "${NET}")" = "true" ]; then
  pass "network ${NET} is internal (Docker guarantees no egress)"
else
  fail "network ${NET} is NOT internal"
fi

log "Run Neo4j on the air-gapped network"
docker run -d --name "${CTR}" --network "${NET}" \
  -e NEO4J_AUTH="neo4j/${PW}" "${IMAGE_TAG}" >/dev/null

# Wait for Neo4j to report Started (up to ~60s).
started=0
for _ in $(seq 1 30); do
  if docker logs "${CTR}" 2>&1 | grep -q "Started."; then started=1; break; fi
  sleep 2
done
[ "${started}" = "1" ] && pass "Neo4j started on internal network" || fail "Neo4j did not start in time"

log "Probe egress from INSIDE the Neo4j container (expect blocked)"
if docker exec "${CTR}" bash -c 'timeout 5 bash -c "cat < /dev/null > /dev/tcp/8.8.8.8/53" 2>/dev/null'; then
  fail "container reached 8.8.8.8:53 — network is NOT air-gapped"
else
  pass "outbound to 8.8.8.8:53 blocked (no internet egress)"
fi

log "Verify APOC is functional (loaded from baked jar, never downloaded)"
APOC_VER="$(docker exec "${CTR}" cypher-shell -u neo4j -p "${PW}" --format plain \
  "RETURN apoc.version();" 2>/dev/null | tr -d '"' | tail -1 | tr -d '[:space:]')"
if [ "${APOC_VER}" = "${NEO4J_VERSION}" ]; then
  pass "apoc.version() = ${APOC_VER} (matches Neo4j ${NEO4J_VERSION})"
else
  fail "apoc.version() returned '${APOC_VER}', expected '${NEO4J_VERSION}'"
fi

if docker exec "${CTR}" cypher-shell -u neo4j -p "${PW}" --format plain \
    "CALL apoc.meta.stats() YIELD nodeCount RETURN nodeCount;" >/dev/null 2>&1; then
  pass "apoc.meta.stats() executes"
else
  fail "apoc.meta.stats() failed"
fi

# Built-in db.* must still work — proves we did NOT over-restrict with an allowlist.
if docker exec "${CTR}" cypher-shell -u neo4j -p "${PW}" --format plain \
    "CALL db.labels() YIELD label RETURN count(label);" >/dev/null 2>&1; then
  pass "built-in db.labels() works (security config not over-restricted)"
else
  fail "built-in db.labels() blocked — allowlist likely mis-set"
fi

log "Confirm NO plugin install/download happened in startup logs"
if docker logs "${CTR}" 2>&1 | grep -iE "Installing Plugin|downloading|fetch.*plugin|dist\.neo4j" >/dev/null; then
  fail "startup logs show plugin install/download activity"
else
  pass "no plugin install/download in startup logs (jar was already baked)"
fi

log "RESULT"
if [ "${FAILED}" = "0" ]; then
  echo "PASS — APOC functional with the Neo4j container cut off from the internet; zero download."
  exit 0
else
  echo "FAIL — see lines above."
  exit 1
fi
