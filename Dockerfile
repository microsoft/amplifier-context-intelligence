# Runtime base: Microsoft Azure Linux 3.0 (CBL-Mariner). Continuously patched by
# MS with a fix-only security feed, so a build-time `tdnf update` clears reported
# CRITICAL/HIGH without suppression lists. Also matches the AzureLinux 3.0 AKS
# nodes this service runs on.
#
# We intentionally track the floating `3.12` azl3 tag rather than pinning a dated
# snapshot. The Trivy gate scans against an always-current vuln DB (trivy-action
# @master), so pinning an old base snapshot guarantees eventual scan failure the
# moment a new CVE lands — a green build today goes red tomorrow with zero code
# change. Floating the base keeps the newest patched OS packages baked in and,
# combined with the `tdnf update` below, keeps the image consistently current
# with the fix-only feed. Reproducibility is deliberately traded for a stronger,
# self-healing security posture (SFI).
FROM mcr.microsoft.com/azurelinux/base/python:3.12

# Pull the latest security patches at build time so we don't have to wait for MS
# to republish the base image. Every azl3 CVE with a fix lands here.
#
# OS_PATCH_EPOCH exists ONLY to bust the build cache for the layer below. Without
# it this RUN is keyed on its instruction text, which never changes -- so a
# registry buildcache (`cache-from` in the build-deploy workflow) serves a stale
# layer and `tdnf update` NEVER EXECUTES. That silently freezes the image at
# whatever OS snapshot first populated the cache, while the Trivy gate keeps
# scanning against a fresh vuln DB: a green build goes red with zero code change,
# and no amount of rebuilding fixes it. Observed 2026-09-21 -- two consecutive
# main builds failed on 8 HIGH (pcre2 CVE-2026-86145, util-linux CVE-2026-78408/
# 78410) that `tdnf update` clears outright; `#11 CACHED` in the build log was the
# whole defect. The workflow passes the UTC date, so the cache is busted once per
# day rather than per build: the first build of the day absorbs the day's patches
# (~+2 min) and every later build that day reuses the refreshed layer.
ARG OS_PATCH_EPOCH
RUN tdnf -y update && tdnf -y install ca-certificates curl && tdnf -y clean all

WORKDIR /app

# Amplifier Online's build pool reaches MCR, packages.microsoft.com and the
# platform's package feed only (not ghcr.io or pypi.org): uv comes from the
# feed, and the dependencies install from requirements.txt, exported from
# uv.lock with hashes (re-export when uv.lock changes:
#   uv export --frozen --no-dev --no-emit-project --format requirements-txt > requirements.txt).
COPY requirements.txt pyproject.toml ./
COPY context_intelligence_server/ context_intelligence_server/
COPY docker-entrypoint.sh .
RUN chmod +x docker-entrypoint.sh

# Install, then drop the base image's pip/setuptools/wheel (CVE surface) and put
# back a patched setuptools — all with the feed as the only index.
RUN --mount=type=secret,id=pip_index_url \
    export PIP_INDEX_URL="$(cat /run/secrets/pip_index_url)" UV_INDEX_URL="$(cat /run/secrets/pip_index_url)" && \
    pip install --no-cache-dir uv && \
    uv pip install --system --no-cache --require-hashes -r requirements.txt && \
    uv pip install --system --no-cache --no-deps . && \
    tdnf -y remove python3-pip python3-setuptools python3-wheel && \
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

# The deployed commit, reported by GET /health.
ARG GIT_COMMIT=unknown
ENV GIT_COMMIT=${GIT_COMMIT}

EXPOSE 8000

ENTRYPOINT ["./docker-entrypoint.sh"]
CMD ["context-intelligence-server"]
