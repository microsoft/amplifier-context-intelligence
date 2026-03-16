"""Tests for Dockerfile.intelligence — programmatic Amplifier service container.

TDD phase: These tests define the NEW required state of Dockerfile.intelligence.

Spec requirements:
- Base: python:3.13-slim
- Install uv from ghcr.io/astral-sh/uv:latest
- Install build tools: git, build-essential, pkg-config, libssl-dev (Rust bindings)
- Two-stage uv sync for layer caching
- NO pre-baked server bundle (no amplifier-bundle-context-intelligence-server or /app/bundles/)
- ENV: AMPLIFIER_CONTEXT_INTELLIGENCE_SERVICE_RUNTIME_STATE_PATH, AMPLIFIER_CONTEXT_INTELLIGENCE_SERVICE_ROUTING_MATRIX
- NO AMPLIFIER_HOME= or BUNDLE_PATH= env vars
- EXPOSE 8100
- HEALTHCHECK with --start-period=180s, --retries=60, python urllib
- ENTRYPOINT ["/app/entrypoint.sh"]
- CMD: uv run uvicorn intelligence_service.app:app --host 0.0.0.0 --port 8100
- No CLI installation (no 'uv tool install amplifier')
"""

import functools
import pathlib

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
DOCKERFILE = PROJECT_ROOT / "Dockerfile.intelligence"


@functools.lru_cache(maxsize=1)
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
    assert "    git \\" in content or "    git " in content, (
        "Dockerfile.intelligence must install git (for git+https dependencies)"
    )


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
# Entrypoint shell script
# ---------------------------------------------------------------------------


def test_has_entrypoint_script() -> None:
    content = _content()
    assert "entrypoint" in content.lower(), (
        "Dockerfile.intelligence must use an entrypoint.sh script "
        "(configures git auth for private repo access before starting uvicorn)"
    )
    assert 'ENTRYPOINT ["/app/entrypoint.sh"]' in content, (
        "Dockerfile.intelligence must set ENTRYPOINT to /app/entrypoint.sh"
    )


# ---------------------------------------------------------------------------
# No pre-baked server bundle
# ---------------------------------------------------------------------------


def test_no_pre_baked_server_bundle() -> None:
    content = _content()
    assert "amplifier-bundle-context-intelligence-server" not in content, (
        "Dockerfile.intelligence must NOT copy the pre-baked server bundle "
        "(bundle is no longer needed in programmatic runtime mode)"
    )
    assert "/app/bundles/" not in content, (
        "Dockerfile.intelligence must NOT reference /app/bundles/ "
        "(no pre-baked bundle directory)"
    )


# ---------------------------------------------------------------------------
# Environment variables
# ---------------------------------------------------------------------------


def test_env_runtime_state_path() -> None:
    content = _content()
    assert (
        "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVICE_RUNTIME_STATE_PATH=/data/intelligence-runtime"
        in content
    ), (
        "Dockerfile.intelligence must set "
        "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVICE_RUNTIME_STATE_PATH=/data/intelligence-runtime"
    )


def test_env_routing_matrix() -> None:
    content = _content()
    assert (
        "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVICE_ROUTING_MATRIX=balanced" in content
    ), (
        "Dockerfile.intelligence must set "
        "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVICE_ROUTING_MATRIX=balanced"
    )


def test_no_env_amplifier_home() -> None:
    content = _content()
    # Must not have a bare AMPLIFIER_HOME= assignment
    lines = content.splitlines()
    for line in lines:
        stripped = line.strip()
        assert not (
            stripped.startswith("AMPLIFIER_HOME=") or "AMPLIFIER_HOME=" in stripped
        ), (
            "Dockerfile.intelligence must NOT set AMPLIFIER_HOME= "
            "(removed in new runtime composition)"
        )


def test_no_env_bundle_path() -> None:
    content = _content()
    # Must not have any BUNDLE_PATH= assignment
    lines = content.splitlines()
    for line in lines:
        stripped = line.strip()
        assert "BUNDLE_PATH=" not in stripped, (
            "Dockerfile.intelligence must NOT set BUNDLE_PATH= "
            "(no pre-baked bundle in new runtime composition)"
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
        "while service initialises)"
    )


def test_healthcheck_retries_60() -> None:
    content = _content()
    assert "--retries=60" in content, "HEALTHCHECK must have --retries=60"


def test_healthcheck_uses_python_urllib() -> None:
    content = _content()
    lines = content.splitlines()
    # Find the CMD line of the HEALTHCHECK — it contains "urllib" directly
    hc_cmd_line = next((l for l in lines if "urllib" in l), "")
    assert hc_cmd_line, "HEALTHCHECK must use python urllib for the health check"
    assert "python" in hc_cmd_line, "HEALTHCHECK must use python urllib (not curl)"
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
