# Air-gapped (offline) Neo4j image with the APOC Core plugin baked in.
#
# WHY THIS EXISTS
# ---------------
# The default stack enables APOC via `NEO4J_PLUGINS=["apoc"]` (see
# docker-compose.yml). On Neo4j 5.x that mechanism installs APOC *Core* from a
# jar already bundled inside the official image at /var/lib/neo4j/labs — it does
# NOT reach the internet for APOC Core. That is already safe for most offline
# environments.
#
# This image goes one step further for *air-tight* deployments — environments
# where the Neo4j container has no internet egress at all and you want zero
# reliance on the startup plugin-installer step. It copies the bundled APOC Core
# jar into the live plugins directory at BUILD time and sets APOC's security
# config directly. The jar becomes an immutable image layer: there is nothing to
# download at run time, by construction.
#
# BUILD / RUN
# -----------
#   docker compose -f docker-compose.yml -f docker-compose.airgap.yml up -d --build
#
# In a truly disconnected host you must also pre-load the BASE image
# (neo4j:5.26.22-community) — e.g. `docker save` it on a connected machine and
# `docker load` it on the air-gapped host, or pull it from an internal registry
# mirror. The base image carries the bundled APOC Core jar this build copies.
FROM neo4j:5.26.22-community

# Bake the bundled APOC Core jar into the live plugins dir (no download).
RUN cp /var/lib/neo4j/labs/apoc-*-core.jar /var/lib/neo4j/plugins/apoc.jar

# APOC's own default config. ONLY `unrestricted` — do NOT set `allowlist` to
# `apoc.*`, that would block built-in db.*/dbms.* procedures.
ENV NEO4J_dbms_security_procedures_unrestricted="apoc.*"
