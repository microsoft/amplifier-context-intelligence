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

# Bake GDS (Graph Data Science) Community Edition into the live plugins dir.
# Unlike APOC, GDS is NOT bundled inside the base image, so this requires a
# one-time BUILD-time download. This does NOT break air-tightness: only
# runtime egress is disallowed for this image; the build step below runs on
# whatever connected machine builds the image (same assumption already made
# for `FROM neo4j:...` itself, which pulls over the network at build time).
#
# 2.13.x is the GDS series matching Neo4j 5.26.x per the official compatibility
# matrix: https://neo4j.com/docs/graph-data-science/current/installation/supported-neo4j-versions/
# 2.13.11 is the latest available 2.13.x patch; pin the newest patch so GDS loads
# on recent 5.26 patch releases (older GDS patches can reject a newer Neo4j patch).
# Bump this ARG in lockstep whenever the base image tag above changes.
ARG GDS_VERSION=2.13.11
RUN wget -q -O /var/lib/neo4j/plugins/graph-data-science.jar \
      "https://graphdatascience.ninja/neo4j-graph-data-science-${GDS_VERSION}.jar" \
    && [ -s /var/lib/neo4j/plugins/graph-data-science.jar ]

# APOC + GDS security config. ONLY `unrestricted` — do NOT set `allowlist` to
# `apoc.*,gds.*`, that would block built-in db.*/dbms.* procedures.
ENV NEO4J_dbms_security_procedures_unrestricted="apoc.*,gds.*"
