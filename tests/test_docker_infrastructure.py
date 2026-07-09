"""Tests for Docker infrastructure — Dockerfile and docker-compose.yml.

TDD phase: These tests FAIL before the files are created.
"""

import pathlib
import re
import shutil

import pytest
import yaml

# Paths relative to project root
PROJECT_ROOT = pathlib.Path(__file__).parent.parent
DOCKERFILE = PROJECT_ROOT / "Dockerfile"
COMPOSE_FILE = PROJECT_ROOT / "docker-compose.yml"


# ---------------------------------------------------------------------------
# Dockerfile tests
# ---------------------------------------------------------------------------


def test_dockerfile_exists() -> None:
    assert DOCKERFILE.exists(), "Dockerfile must exist at project root"


def test_dockerfile_base_image() -> None:
    """The base image must be a Python image at a version the project supports.

    Verifies a capability (a Python >= 3.11 base, per requires-python) rather than
    a hardcoded image string, so the base can change for security or platform
    reasons (e.g. Azure Linux) without breaking this test.
    """
    content = DOCKERFILE.read_text()

    from_lines = [
        line.strip()
        for line in content.splitlines()
        if line.strip().upper().startswith("FROM ")
    ]
    assert from_lines, "Dockerfile must declare a base image with FROM"

    base = from_lines[0].lower()
    assert "python" in base, f"Base image must be a Python image, got: {from_lines[0]}"

    match = re.search(r"python[:/].*?(\d+)\.(\d+)", base)
    assert match, f"Could not determine Python version from base image: {from_lines[0]}"

    major, minor = int(match.group(1)), int(match.group(2))
    assert (major, minor) >= (3, 11), (
        f"Base image Python must be >= 3.11 (requires-python), got {major}.{minor}"
    )


def test_dockerfile_workdir() -> None:
    content = DOCKERFILE.read_text()
    assert "WORKDIR /app" in content, "Dockerfile must set WORKDIR /app"


def test_dockerfile_installs_curl() -> None:
    content = DOCKERFILE.read_text()
    assert "curl" in content, "Dockerfile must install curl (for healthcheck)"


def test_dockerfile_copies_pyproject_toml() -> None:
    content = DOCKERFILE.read_text()
    assert "pyproject.toml" in content, "Dockerfile must COPY pyproject.toml"


def test_dockerfile_copies_server_package() -> None:
    content = DOCKERFILE.read_text()
    assert "context_intelligence_server" in content, (
        "Dockerfile must COPY context_intelligence_server/"
    )


def test_dockerfile_uv_install() -> None:
    content = DOCKERFILE.read_text()
    assert "uv pip install --system --no-cache ." in content, (
        "Dockerfile must run uv pip install --system --no-cache . (not pip)"
    )


def test_dockerfile_expose_8000() -> None:
    content = DOCKERFILE.read_text()
    assert "EXPOSE 8000" in content, "Dockerfile must EXPOSE 8000"


def test_dockerfile_cmd_uses_entry_point() -> None:
    content = DOCKERFILE.read_text()
    assert 'CMD ["context-intelligence-server"]' in content, (
        "Dockerfile CMD must use the named entry point 'context-intelligence-server' "
        "(not a hardcoded uvicorn command)"
    )
    assert "8000" in content, "Dockerfile must reference port 8000 (EXPOSE)"


def test_dockerfile_copies_entrypoint_script() -> None:
    content = DOCKERFILE.read_text()
    assert "COPY docker-entrypoint.sh ." in content, (
        "Dockerfile must COPY docker-entrypoint.sh into the image"
    )


def test_dockerfile_chmod_entrypoint_script() -> None:
    content = DOCKERFILE.read_text()
    assert "RUN chmod +x docker-entrypoint.sh" in content, (
        "Dockerfile must RUN chmod +x docker-entrypoint.sh"
    )


# ---------------------------------------------------------------------------
# docker-compose.yml tests
# ---------------------------------------------------------------------------


def test_compose_file_exists() -> None:
    assert COMPOSE_FILE.exists(), "docker-compose.yml must exist at project root"


@pytest.fixture
def compose() -> dict:
    """Parse docker-compose.yml and return as dict."""
    return yaml.safe_load(COMPOSE_FILE.read_text())


def test_compose_has_server_service(compose: dict) -> None:
    services = compose.get("services", {})
    assert "context-intelligence-server" in services, (
        "docker-compose.yml must define context-intelligence-server service"
    )


def test_compose_has_neo4j_service(compose: dict) -> None:
    services = compose.get("services", {})
    assert "neo4j" in services, "docker-compose.yml must define neo4j service"


def test_compose_server_port_mapping(compose: dict) -> None:
    server = compose["services"]["context-intelligence-server"]
    ports = server.get("ports", [])
    port_strings = [str(p) for p in ports]
    assert any("8000" in p for p in port_strings), (
        "context-intelligence-server must map port 8000:8000"
    )


def test_compose_server_neo4j_url_env(compose: dict) -> None:
    server = compose["services"]["context-intelligence-server"]
    env = server.get("environment", {})
    # environment can be dict or list
    if isinstance(env, list):
        env_str = "\n".join(env)
        assert "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J_URL" in env_str, (
            "context-intelligence-server must set AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J_URL"
        )
    else:
        assert "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J_URL" in env, (
            "context-intelligence-server must set AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J_URL"
        )


def test_compose_server_blob_path_env(compose: dict) -> None:
    server = compose["services"]["context-intelligence-server"]
    env = server.get("environment", {})
    if isinstance(env, list):
        env_str = "\n".join(env)
        assert "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_BLOB_PATH" in env_str, (
            "context-intelligence-server must set AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_BLOB_PATH"
        )
    else:
        assert "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_BLOB_PATH" in env, (
            "context-intelligence-server must set AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_BLOB_PATH"
        )


def test_compose_server_depends_on_neo4j(compose: dict) -> None:
    server = compose["services"]["context-intelligence-server"]
    depends = server.get("depends_on", [])
    assert "neo4j" in depends, "context-intelligence-server must depend on neo4j"


def test_compose_server_has_healthcheck(compose: dict) -> None:
    server = compose["services"]["context-intelligence-server"]
    assert "healthcheck" in server, (
        "context-intelligence-server must have a healthcheck"
    )
    hc = server["healthcheck"]
    test_cmd = hc.get("test", "")
    test_str = str(test_cmd)
    assert "curl" in test_str and "localhost:8000/status" in test_str, (
        "healthcheck must use curl to check http://localhost:8000/status"
    )


def test_compose_server_blob_volume(compose: dict) -> None:
    server = compose["services"]["context-intelligence-server"]
    volumes = server.get("volumes", [])
    vol_str = "\n".join(str(v) for v in volumes)
    assert "/data/blobs" in vol_str, (
        "context-intelligence-server must mount a volume at /data/blobs"
    )


def test_compose_neo4j_image(compose: dict) -> None:
    neo4j = compose["services"]["neo4j"]
    assert "neo4j:5" in str(neo4j.get("image", "")), (
        "neo4j service must use neo4j:5 image"
    )


def test_compose_neo4j_auth_env(compose: dict) -> None:
    """Neo4j auth is provided via env_file (generated by init), not inline env vars.

    NEO4J_AUTH must NOT appear in the inline environment block — it comes from
    the generated neo4j-auth.env file so credentials can be securely created at
    container start rather than hardcoded in the compose file.
    """
    neo4j = compose["services"]["neo4j"]
    env = neo4j.get("environment", {}) or {}
    if isinstance(env, list):
        env_str = "\n".join(env)
        assert "NEO4J_AUTH" not in env_str, (
            "NEO4J_AUTH must not be set inline — use env_file for auth credentials"
        )
    else:
        assert "NEO4J_AUTH" not in env, (
            "NEO4J_AUTH must not be set inline — use env_file for auth credentials"
        )
    # Auth is provided via env_file instead
    assert neo4j.get("env_file") is not None, (
        "neo4j must use env_file for auth credentials"
    )


def test_compose_neo4j_ports(compose: dict) -> None:
    neo4j = compose["services"]["neo4j"]
    ports = neo4j.get("ports", [])
    port_str = "\n".join(str(p) for p in ports)
    assert "7474" in port_str, "neo4j must expose port 7474"


def test_compose_top_level_volumes(compose: dict) -> None:
    """All storage uses bind mounts instead of named volumes.

    Verify that /data/blobs is mounted in context-intelligence-server
    and /data is mounted in neo4j.
    """
    server_vols = "\n".join(
        str(v)
        for v in compose["services"]["context-intelligence-server"].get("volumes", [])
    )
    assert "/data/blobs" in server_vols, (
        "context-intelligence-server must have a bind mount to /data/blobs"
    )
    neo4j_vols = "\n".join(
        str(v) for v in compose["services"]["neo4j"].get("volumes", [])
    )
    assert "/data" in neo4j_vols, "neo4j must have a bind mount to /data"


def test_compose_docker_command_available() -> None:
    """Validate docker compose is available for running 'docker compose config'."""
    assert shutil.which("docker") is not None, "docker must be installed"
