"""Tests for Docker infrastructure — Dockerfile and docker-compose.yml.

TDD phase: These tests FAIL before the files are created.
"""

import pathlib
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
    content = DOCKERFILE.read_text()
    assert "FROM python:3.11-slim" in content, "Dockerfile must use python:3.11-slim"


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


def test_dockerfile_pip_install() -> None:
    content = DOCKERFILE.read_text()
    assert "pip install --no-cache-dir ." in content, (
        "Dockerfile must run pip install --no-cache-dir ."
    )


def test_dockerfile_expose_8000() -> None:
    content = DOCKERFILE.read_text()
    assert "EXPOSE 8000" in content, "Dockerfile must EXPOSE 8000"


def test_dockerfile_uvicorn_cmd() -> None:
    content = DOCKERFILE.read_text()
    assert "uvicorn" in content, "Dockerfile CMD must use uvicorn"
    assert "context_intelligence_server.main:app" in content, (
        "Dockerfile CMD must point to context_intelligence_server.main:app"
    )
    assert "0.0.0.0" in content, "Dockerfile CMD must bind to 0.0.0.0"
    assert "8000" in content, "Dockerfile CMD must use port 8000"


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
    if isinstance(depends, dict):
        assert "neo4j" in depends, "context-intelligence-server must depend on neo4j"
    else:
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
    assert "blob_data" in vol_str, (
        "context-intelligence-server must mount blob_data volume"
    )


def test_compose_neo4j_image(compose: dict) -> None:
    neo4j = compose["services"]["neo4j"]
    assert "neo4j:5" in str(neo4j.get("image", "")), (
        "neo4j service must use neo4j:5 image"
    )


def test_compose_neo4j_auth_env(compose: dict) -> None:
    neo4j = compose["services"]["neo4j"]
    env = neo4j.get("environment", {})
    if isinstance(env, list):
        env_str = "\n".join(env)
        assert "NEO4J_AUTH" in env_str, "neo4j must set NEO4J_AUTH"
    else:
        assert "NEO4J_AUTH" in env, "neo4j must set NEO4J_AUTH"


def test_compose_neo4j_ports(compose: dict) -> None:
    neo4j = compose["services"]["neo4j"]
    ports = neo4j.get("ports", [])
    port_str = "\n".join(str(p) for p in ports)
    assert "7474" in port_str, "neo4j must expose port 7474"


def test_compose_top_level_volumes(compose: dict) -> None:
    volumes = compose.get("volumes", {})
    assert "blob_data" in volumes, "docker-compose.yml must declare blob_data volume"
    assert "neo4j_data" in volumes, "docker-compose.yml must declare neo4j_data volume"


def test_compose_docker_command_available() -> None:
    """Validate docker compose is available for running 'docker compose config'."""
    assert shutil.which("docker") is not None, "docker must be installed"
