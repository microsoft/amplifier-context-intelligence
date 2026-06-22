#!/bin/bash
set -euo pipefail

CREDENTIALS_FILE="/data/credentials.yaml"

if [ ! -f "$CREDENTIALS_FILE" ]; then
    echo "First run detected — generating credentials..."

    NEO4J_PASSWORD=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))")
    RAW_TOKEN=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))")
    DIGEST=$(python3 -c "import hashlib, sys; print(hashlib.sha256(sys.argv[1].encode()).hexdigest())" "${RAW_TOKEN}")

    mkdir -p /data

    cat > "${CREDENTIALS_FILE}.tmp" <<EOF
neo4j_url: bolt://neo4j:7687
neo4j_user: neo4j
neo4j_password: ${NEO4J_PASSWORD}
api_keys:
  "${DIGEST}":
    id: owner
EOF
    mv "${CREDENTIALS_FILE}.tmp" "$CREDENTIALS_FILE"
    chmod 600 "$CREDENTIALS_FILE"

    cat > /data/neo4j-auth.env.tmp <<EOF
NEO4J_AUTH=neo4j/${NEO4J_PASSWORD}
EOF
    mv /data/neo4j-auth.env.tmp /data/neo4j-auth.env
    chmod 600 /data/neo4j-auth.env

    echo ""
    echo "========================================================"
    echo "  SAVE THIS TOKEN — it will NOT be shown again."
    echo "  The file stores only its SHA-256 digest, not the token."
    echo ""
    echo "  API token: ${RAW_TOKEN}"
    echo "========================================================"
    echo ""
    echo "Credentials written to ${CREDENTIALS_FILE}"
else
    echo "Existing credentials found."
fi

if [ $# -gt 0 ]; then
    exec "$@"
fi
