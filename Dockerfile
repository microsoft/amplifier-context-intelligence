# Runtime base: Microsoft Azure Linux 3.0 (CBL-Mariner). Continuously patched by
# MS with a fix-only security feed, so a build-time `tdnf update` clears reported
# CRITICAL/HIGH without suppression lists. Also matches the AzureLinux 3.0 AKS
# nodes this service runs on.
FROM mcr.microsoft.com/azurelinux/base/python:3.12.9-13-azl3.0.20260706

# Pull the latest security patches at build time so we don't have to wait for MS
# to republish the base image. Every azl3 CVE with a fix lands here.
RUN tdnf -y update && tdnf -y install ca-certificates curl && tdnf -y clean all

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

COPY pyproject.toml .
COPY context_intelligence_server/ context_intelligence_server/
COPY docker-entrypoint.sh .
RUN chmod +x docker-entrypoint.sh

RUN uv pip install --system --no-cache .

# security: the Azure Linux base ships EOL Python packaging tools (pip/setuptools/
# wheel) that Qualys SCA (S360) flags. In-place upgrades don't clear the findings
# because the rpm-installed dist-info has no RECORD, so pip/uv leave the old
# metadata orphaned next to the new (scanners key on the dist-info METADATA).
# This app builds with hatchling and imports none of these at runtime, so remove
# the vulnerable OS rpms outright and delete their leftover files, then reinstall
# only a patched setuptools (some transitive deps still import pkg_resources).
#   pip   — removed: QIDs 5011855 (CVE-2026-6357), 5005553 (CVE-2025-8869)
#   wheel — removed: QID  5007163 (CVE-2026-24049)
#   setuptools>=79 — patched pkg_resources + vendored jaraco.context: QID 5006986
#                    (CVE-2026-23949)
RUN tdnf -y remove python3-pip python3-setuptools python3-wheel && \
    rm -rf /usr/lib/python3.12/site-packages/pip \
           /usr/lib/python3.12/site-packages/pip-*.dist-info \
           /usr/lib/python3.12/site-packages/setuptools \
           /usr/lib/python3.12/site-packages/setuptools-*.dist-info \
           /usr/lib/python3.12/site-packages/pkg_resources \
           /usr/lib/python3.12/site-packages/_distutils_hack \
           /usr/lib/python3.12/site-packages/distutils-precedence.pth \
           /usr/lib/python3.12/site-packages/wheel \
           /usr/lib/python3.12/site-packages/wheel-*.dist-info && \
    uv pip install --system --no-cache "setuptools>=79" && \
    tdnf -y clean all

EXPOSE 8000

ENTRYPOINT ["./docker-entrypoint.sh"]
CMD ["context-intelligence-server"]
