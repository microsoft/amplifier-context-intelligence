"""Tests for Dockerfile.intelligence — programmatic Amplifier service container.

TDD phase: These tests FAIL before the Dockerfile is updated.

Spec requirements:
- Base: python:3.13-slim
- Install uv from ghcr.io/astral-sh/uv:latest
- Install build tools: git, build-essential, pkg-config, libssl-dev (Rust bindings)
- Two-stage uv sync for layer caching
- Pre-bake server bundle to /app/bundles/context-intelligence-server/
- ENV: AMPLIFIER_HOME, BUNDLE_PATH
- EXPOSE 8100
- HEALTHCHECK with --start-period=180s, --retries=60, python urllib
- CMD: uv run uvicorn intelligence_service.app:app --host 0.0.0.0 --port 8100
- No CLI installation (no 'uv tool install amplifier')
- No entrypoint.sh script
"""

import pathlib

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
DOCKERFILE = PROJECT_ROOT / "Dockerfile.intelligence"


def _content() -> str:
    return DOCKERFILE.read_text()


# ---------------------------------------------------------------------------
# Existence
# ---------------------------------------------------------------------------


def test_dockerfile_intelligence_exists() -> None:
    assert DOCKERFILE.exists(), "Dockerfile.intelligence must exist at project root"


# ---------------------------------------------------------------------------
# Base image
# ---------------------------------------------------------------------------


def test_uses_python_313_slim() -> None:
    assert "FROM python:3.13-slim" in _content(), (
        "Dockerfile.intelligence must use python:3.13-slim as base image"
    )


# ---------------------------------------------------------------------------
# uv installation
# ---------------------------------------------------------------------------


def test_installs_uv_from_ghcr() -> None:
    content = _content()
    assert "ghcr.io/astral-sh/uv:latest" in content, (
        "Dockerfile.intelligence must install uv from ghcr.io/astral-sh/uv:latest"
    )
    assert "COPY --from=ghcr.io/astral-sh/uv:latest" in content, (
        "uv must be installed via COPY --from=ghcr.io/astral-sh/uv:latest"
    )


# ---------------------------------------------------------------------------
# Build tools (required for Rust bindings in amplifier-core)
# ---------------------------------------------------------------------------


def test_installs_build_tools() -> None:
    content = _content()
    assert "build-essential" in content, (
        "Dockerfile.intelligence must install build-essential (for Rust bindings)"
    )
    assert "pkg-config" in content, (
        "Dockerfile.intelligence must install pkg-config (for Rust bindings)"
    )
    assert "libssl-dev" in content, (
        "Dockerfile.intelligence must install libssl-dev (for Rust bindings)"
    )


def test_installs_git() -> None:
    content = _content()
    assert "    git " in content or (
        "apt-get install" in content and "git" in content
    ), "Dockerfile.intelligence must install git (for git+https dependencies)"


# ---------------------------------------------------------------------------
# Two-stage uv sync for layer caching
# ---------------------------------------------------------------------------


def test_uv_sync_deps_only_stage() -> None:
    """First stage: copy lock files and sync deps without installing project."""
    content = _content()
    assert "uv sync --frozen --no-dev --no-install-project" in content, (
        "Dockerfile.intelligence must run 'uv sync --frozen --no-dev --no-install-project' "
        "for dependency-only layer caching"
    )


def test_uv_sync_full_stage() -> None:
    """Second stage: copy source and install project."""
    content = _content()
    lines = content.splitlines()
    full_sync_lines = [
        line
        for line in lines
        if "uv sync --frozen --no-dev" in line and "--no-install-project" not in line
    ]
    assert full_sync_lines, (
        "Dockerfile.intelligence must run 'uv sync --frozen --no-dev' (without --no-install-project) "
        "to install the project in the second stage"
    )


def test_copies_pyproject_toml_for_layer_caching() -> None:
    content = _content()
    assert "pyproject.toml" in content, (
        "Dockerfile.intelligence must COPY pyproject.toml for layer caching"
    )


def test_copies_uv_lock_for_layer_caching() -> None:
    content = _content()
    assert "uv.lock" in content, (
        "Dockerfile.intelligence must COPY uv.lock for layer caching"
    )


# ---------------------------------------------------------------------------
# No 'uv tool install amplifier' (no CLI installation)
# ---------------------------------------------------------------------------


def test_no_uv_tool_install_amplifier() -> None:
    content = _content()
    assert "uv tool install amplifier" not in content, (
        "Dockerfile.intelligence must NOT install the amplifier CLI "
        "(programmatic mode uses the Python library directly)"
    )


# ---------------------------------------------------------------------------
# No entrypoint shell script
# ---------------------------------------------------------------------------


def test_no_entrypoint_script() -> None:
    content = _content()
    assert "entrypoint" not in content.lower(), (
        "Dockerfile.intelligence must NOT use an entrypoint.sh script "
        "(pure Python startup via CMD)"
    )


# ---------------------------------------------------------------------------
# Pre-baked server bundle
# ---------------------------------------------------------------------------


def test_pre_bakes_server_bundle() -> None:
    content = _content()
    assert "amplifier-bundle-context-intelligence-server" in content, (
        "Dockerfile.intelligence must COPY the amplifier-bundle-context-intelligence-server "
        "directory into the image"
    )
    assert "/app/bundles/context-intelligence-server" in content, (
        "Bundle must be copied to /app/bundles/context-intelligence-server/"
    )


# ---------------------------------------------------------------------------
# Environment variables
# ---------------------------------------------------------------------------


def test_env_amplifier_home() -> None:
    content = _content()
    assert "AMPLIFIER_HOME=/data/context-intelligence-service" in content, (
        "Dockerfile.intelligence must set AMPLIFIER_HOME=/data/context-intelligence-service"
    )


def test_env_bundle_path() -> None:
    content = _content()
    assert (
        "BUNDLE_PATH=/app/bundles/context-intelligence-server/bundle.md" in content
    ), (
        "Dockerfile.intelligence must set "
        "BUNDLE_PATH=/app/bundles/context-intelligence-server/bundle.md"
    )


# ---------------------------------------------------------------------------
# Port exposure
# ---------------------------------------------------------------------------


def test_expose_8100() -> None:
    content = _content()
    assert "EXPOSE 8100" in content, "Dockerfile.intelligence must EXPOSE 8100"


# ---------------------------------------------------------------------------
# Healthcheck
# ---------------------------------------------------------------------------


def test_healthcheck_present() -> None:
    content = _content()
    assert "HEALTHCHECK" in content, "Dockerfile.intelligence must define a HEALTHCHECK"


def test_healthcheck_start_period_180s() -> None:
    content = _content()
    assert "--start-period=180s" in content, (
        "HEALTHCHECK must have --start-period=180s (cold start takes up to 3 minutes "
        "while prepare downloads modules)"
    )


def test_healthcheck_retries_60() -> None:
    content = _content()
    assert "--retries=60" in content, "HEALTHCHECK must have --retries=60"


def test_healthcheck_uses_python_urllib() -> None:
    content = _content()
    # The healthcheck must use python -c with urllib (not curl or wget)
    assert "python" in content.lower(), "HEALTHCHECK must use python urllib (not curl)"
    assert "urllib" in content, (
        "HEALTHCHECK must use python urllib for the health check"
    )
    assert "curl" not in content.lower(), "HEALTHCHECK must not use curl"


def test_healthcheck_checks_port_8100() -> None:
    content = _content()
    # The healthcheck should reference port 8100
    # Find the HEALTHCHECK line and check it references 8100
    lines = content.splitlines()
    hc_lines = [
        line
        for line in lines
        if "HEALTHCHECK" in line or "urllib" in line or "8100" in line
    ]
    hc_content = "\n".join(hc_lines)
    assert "8100" in hc_content, "HEALTHCHECK must check port 8100"


# ---------------------------------------------------------------------------
# CMD — uvicorn startup
# ---------------------------------------------------------------------------


def test_cmd_uses_uv_run_uvicorn() -> None:
    content = _content()
    # Accepts both shell form ("uv run uvicorn") and exec form (["uv", "run", "uvicorn", ...])
    assert "uv run uvicorn" in content or (
        '"uv"' in content and '"run"' in content and '"uvicorn"' in content
    ), "CMD must use 'uv run uvicorn' to start the service"


def test_cmd_points_to_intelligence_service_app() -> None:
    content = _content()
    assert "intelligence_service.app:app" in content, (
        "CMD must point uvicorn to intelligence_service.app:app"
    )


def test_cmd_binds_to_all_interfaces() -> None:
    content = _content()
    # Accepts both shell form ("--host 0.0.0.0") and exec form ("--host", "0.0.0.0")
    assert "--host 0.0.0.0" in content or (
        '"--host"' in content and '"0.0.0.0"' in content
    ), "CMD must bind uvicorn to 0.0.0.0"


def test_cmd_uses_port_8100() -> None:
    content = _content()
    # Accepts both shell form ("--port 8100") and exec form ("--port", "8100")
    assert "--port 8100" in content or (
        '"--port"' in content and '"8100"' in content
    ), "CMD must use port 8100"
