#!/bin/bash
set -euo pipefail

DATA_DIR="${HOME}/amplifier-context-intelligence-server-data-store"
CREDENTIALS_FILE="${DATA_DIR}/credentials.yaml"
NEO4J_AUTH_FILE="${DATA_DIR}/neo4j-auth.env"

mkdir -p "${DATA_DIR}" "${DATA_DIR}/blobs" "${DATA_DIR}/logs" "${DATA_DIR}/neo4j"

if [ ! -f "${CREDENTIALS_FILE}" ] || [ ! -f "${NEO4J_AUTH_FILE}" ]; then
    echo "First run — generating credentials..."

    NEO4J_PASSWORD=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))")
    API_KEY=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))")

    cat > "${CREDENTIALS_FILE}.tmp" <<EOF
neo4j_url: bolt://neo4j:7687
neo4j_user: neo4j
neo4j_password: ${NEO4J_PASSWORD}
api_key: ${API_KEY}
EOF
    mv "${CREDENTIALS_FILE}.tmp" "${CREDENTIALS_FILE}"
    chmod 600 "${CREDENTIALS_FILE}"

    cat > "${NEO4J_AUTH_FILE}.tmp" <<EOF
NEO4J_AUTH=neo4j/${NEO4J_PASSWORD}
EOF
    mv "${NEO4J_AUTH_FILE}.tmp" "${NEO4J_AUTH_FILE}"
    chmod 600 "${NEO4J_AUTH_FILE}"

    echo "Credentials written to: ${CREDENTIALS_FILE}"
else
    echo "Existing credentials found — reusing."
fi

echo "Context Intelligence credentials: ${CREDENTIALS_FILE}"
docker compose up -d "$@"
