"""Tests for docker-compose.yml intelligence-service configuration.

TDD phase: These tests define the EXPECTED state after the docker-compose.yml update.
Tests cover:
- Four services: context-intelligence-server, intelligence-service, frontend, neo4j
- intelligence-service: builds from Dockerfile.intelligence, port 8100
- intelligence-service: depends_on context-intelligence-server (healthy)
- intelligence-service: volumes intelligence_runtime_state:/data/intelligence-runtime + blob_data:ro
- intelligence-service: env_file config/secrets.env
- intelligence-service: environment with AMPLIFIER_CONTEXT_INTELLIGENCE_SERVICE_ prefix
  - AMPLIFIER_CONTEXT_INTELLIGENCE_SERVICE_RUNTIME_STATE_PATH=/data/intelligence-runtime
  - AMPLIFIER_CONTEXT_INTELLIGENCE_SERVICE_ROUTING_MATRIX=balanced
  - AMPLIFIER_CONTEXT_INTELLIGENCE_SERVICE_INGESTION_URL=http://context-intelligence-server:8000
  - NO bare AMPLIFIER_HOME, BUNDLE_PATH, ROUTING_MATRIX, or prefixed AMPLIFIER_HOME/BUNDLE_PATH
- intelligence-service: healthcheck python urllib on /health, 180s start_period, 60 retries
- Four named volumes: blob_data, neo4j_data, log_data, intelligence_runtime_state
  - NO context_intelligence_service_data volume
- Network: context-intelligence (bridge)
"""

import pathlib
import subprocess

import pytest
import yaml

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
COMPOSE_FILE = PROJECT_ROOT / "docker-compose.yml"


@pytest.fixture
def compose() -> dict:
    """Parse docker-compose.yml and return as dict."""
    return yaml.safe_load(COMPOSE_FILE.read_text())


# ---------------------------------------------------------------------------
# Service inventory
# ---------------------------------------------------------------------------


def test_compose_has_exactly_four_services(compose: dict) -> None:
    services = compose.get("services", {})
    assert len(services) == 4, (
        f"docker-compose.yml must have exactly 4 services, found: {list(services.keys())}"
    )


def test_compose_has_intelligence_service(compose: dict) -> None:
    services = compose.get("services", {})
    assert "intelligence-service" in services, (
        "docker-compose.yml must define intelligence-service"
    )


def test_compose_has_frontend_service(compose: dict) -> None:
    services = compose.get("services", {})
    assert "frontend" in services, "docker-compose.yml must define frontend service"


# ---------------------------------------------------------------------------
# intelligence-service: build
# ---------------------------------------------------------------------------


def test_intelligence_service_build_from_dockerfile_intelligence(compose: dict) -> None:
    svc = compose["services"]["intelligence-service"]
    build = svc.get("build", {})
    if isinstance(build, str):
        dockerfile = None
    else:
        dockerfile = build.get("dockerfile", "")
    assert dockerfile == "amplifier-context-intelligence/Dockerfile.intelligence", (
        "intelligence-service must build from amplifier-context-intelligence/Dockerfile.intelligence"
    )


# ---------------------------------------------------------------------------
# intelligence-service: port
# ---------------------------------------------------------------------------


def test_intelligence_service_port_8100(compose: dict) -> None:
    svc = compose["services"]["intelligence-service"]
    ports = svc.get("ports", [])
    port_strings = [str(p) for p in ports]
    assert any("8100" in p for p in port_strings), (
        "intelligence-service must map port 8100:8100"
    )


# ---------------------------------------------------------------------------
# intelligence-service: depends_on
# ---------------------------------------------------------------------------


def test_intelligence_service_depends_on_server(compose: dict) -> None:
    svc = compose["services"]["intelligence-service"]
    depends = svc.get("depends_on", {})
    assert "context-intelligence-server" in depends, (
        "intelligence-service must depend on context-intelligence-server"
    )


def test_intelligence_service_depends_on_server_healthy(compose: dict) -> None:
    svc = compose["services"]["intelligence-service"]
    depends = svc.get("depends_on", {})
    assert isinstance(depends, dict), (
        "intelligence-service depends_on must use dict form with condition"
    )
    condition = depends.get("context-intelligence-server", {}).get("condition", "")
    assert condition == "service_healthy", (
        "intelligence-service must depend on context-intelligence-server with condition: service_healthy"
    )


# ---------------------------------------------------------------------------
# intelligence-service: volumes (new naming)
# ---------------------------------------------------------------------------


def test_intelligence_service_has_runtime_state_volume(compose: dict) -> None:
    svc = compose["services"]["intelligence-service"]
    volumes = svc.get("volumes", [])
    vol_str = "\n".join(str(v) for v in volumes)
    assert "intelligence_runtime_state" in vol_str, (
        "intelligence-service must mount intelligence_runtime_state volume"
    )
    assert "/data/intelligence-runtime" in vol_str, (
        "intelligence_runtime_state must be mounted at /data/intelligence-runtime"
    )


def test_intelligence_service_has_blob_data_readonly(compose: dict) -> None:
    svc = compose["services"]["intelligence-service"]
    volumes = svc.get("volumes", [])
    vol_str = "\n".join(str(v) for v in volumes)
    assert "blob_data" in vol_str, "intelligence-service must mount blob_data volume"
    # Check for read-only mount
    assert ":ro" in vol_str or "read_only" in str(svc), (
        "intelligence-service blob_data volume must be read-only (:ro)"
    )


def test_intelligence_service_no_old_volume(compose: dict) -> None:
    svc = compose["services"]["intelligence-service"]
    volumes = svc.get("volumes", [])
    vol_str = "\n".join(str(v) for v in volumes)
    assert "context_intelligence_service_data" not in vol_str, (
        "intelligence-service must NOT mount old context_intelligence_service_data volume"
    )


# ---------------------------------------------------------------------------
# intelligence-service: env_file
# ---------------------------------------------------------------------------


def test_intelligence_service_env_file(compose: dict) -> None:
    svc = compose["services"]["intelligence-service"]
    env_file = svc.get("env_file", [])
    if isinstance(env_file, str):
        env_file_str = env_file
    elif isinstance(env_file, list):
        env_file_str = "\n".join(str(e) for e in env_file)
    else:
        env_file_str = ""
    assert "config/secrets.env" in env_file_str, (
        "intelligence-service must reference env_file: config/secrets.env"
    )


# ---------------------------------------------------------------------------
# intelligence-service: environment variables
# ---------------------------------------------------------------------------


def _get_env_dict(svc: dict) -> dict:
    """Normalize environment to dict for testing."""
    env = svc.get("environment", {})
    if isinstance(env, list):
        result = {}
        for item in env:
            if "=" in item:
                k, _, v = item.partition("=")
                result[k] = v
            else:
                result[item] = None
        return result
    return env or {}


def test_intelligence_service_env_runtime_state_path(compose: dict) -> None:
    svc = compose["services"]["intelligence-service"]
    env = _get_env_dict(svc)
    assert "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVICE_RUNTIME_STATE_PATH" in env, (
        "intelligence-service must set AMPLIFIER_CONTEXT_INTELLIGENCE_SERVICE_RUNTIME_STATE_PATH"
    )
    assert (
        env["AMPLIFIER_CONTEXT_INTELLIGENCE_SERVICE_RUNTIME_STATE_PATH"]
        == "/data/intelligence-runtime"
    ), (
        "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVICE_RUNTIME_STATE_PATH must be /data/intelligence-runtime"
    )


def test_intelligence_service_env_routing_matrix_balanced(compose: dict) -> None:
    svc = compose["services"]["intelligence-service"]
    env = _get_env_dict(svc)
    assert "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVICE_ROUTING_MATRIX" in env, (
        "intelligence-service must set AMPLIFIER_CONTEXT_INTELLIGENCE_SERVICE_ROUTING_MATRIX"
    )
    assert env["AMPLIFIER_CONTEXT_INTELLIGENCE_SERVICE_ROUTING_MATRIX"] == "balanced", (
        "intelligence-service AMPLIFIER_CONTEXT_INTELLIGENCE_SERVICE_ROUTING_MATRIX must be 'balanced'"
    )


def test_intelligence_service_env_ingestion_url(compose: dict) -> None:
    svc = compose["services"]["intelligence-service"]
    env = _get_env_dict(svc)
    assert "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVICE_INGESTION_URL" in env, (
        "intelligence-service must set AMPLIFIER_CONTEXT_INTELLIGENCE_SERVICE_INGESTION_URL environment variable"
    )
    assert "context-intelligence-server:8000" in str(
        env["AMPLIFIER_CONTEXT_INTELLIGENCE_SERVICE_INGESTION_URL"]
    ), (
        "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVICE_INGESTION_URL must point to http://context-intelligence-server:8000"
    )


def test_intelligence_service_env_ci_server_url(compose: dict) -> None:
    """intelligence-service must set AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_URL for the telemetry hook."""
    svc = compose["services"]["intelligence-service"]
    env = _get_env_dict(svc)
    assert "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_URL" in env, (
        "intelligence-service must set AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_URL "
        "so the context-intelligence hook can POST events to the ingestion server"
    )
    assert "context-intelligence-server:8000" in str(
        env["AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_URL"]
    ), (
        "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_URL must point to "
        "http://context-intelligence-server:8000"
    )


def test_intelligence_service_env_ci_workspace(compose: dict) -> None:
    """intelligence-service must set AMPLIFIER_CONTEXT_INTELLIGENCE_WORKSPACE for graph scoping."""
    svc = compose["services"]["intelligence-service"]
    env = _get_env_dict(svc)
    assert "AMPLIFIER_CONTEXT_INTELLIGENCE_WORKSPACE" in env, (
        "intelligence-service must set AMPLIFIER_CONTEXT_INTELLIGENCE_WORKSPACE "
        "to scope telemetry data in the graph"
    )
    assert env["AMPLIFIER_CONTEXT_INTELLIGENCE_WORKSPACE"] == "intelligence-runtime", (
        "AMPLIFIER_CONTEXT_INTELLIGENCE_WORKSPACE must be 'intelligence-runtime'"
    )


def test_intelligence_service_env_no_old_vars(compose: dict) -> None:
    """Verify old/disallowed env vars are NOT present."""
    svc = compose["services"]["intelligence-service"]
    env = _get_env_dict(svc)
    forbidden = [
        "AMPLIFIER_HOME",
        "BUNDLE_PATH",
        "ROUTING_MATRIX",
        "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVICE_AMPLIFIER_HOME",
        "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVICE_BUNDLE_PATH",
    ]
    found = [k for k in forbidden if k in env]
    assert not found, f"intelligence-service must NOT set these env vars: {found}"


# ---------------------------------------------------------------------------
# intelligence-service: healthcheck
# ---------------------------------------------------------------------------


def test_intelligence_service_has_healthcheck(compose: dict) -> None:
    svc = compose["services"]["intelligence-service"]
    assert "healthcheck" in svc, "intelligence-service must have a healthcheck"


def test_intelligence_service_healthcheck_uses_urllib(compose: dict) -> None:
    svc = compose["services"]["intelligence-service"]
    hc = svc.get("healthcheck", {})
    test_cmd = str(hc.get("test", ""))
    assert "urllib" in test_cmd or "urllib.request" in test_cmd, (
        "intelligence-service healthcheck must use python urllib"
    )


def test_intelligence_service_healthcheck_endpoint_health(compose: dict) -> None:
    svc = compose["services"]["intelligence-service"]
    hc = svc.get("healthcheck", {})
    test_cmd = str(hc.get("test", ""))
    assert "/health" in test_cmd, (
        "intelligence-service healthcheck must check /health endpoint"
    )


def test_intelligence_service_healthcheck_start_period_180s(compose: dict) -> None:
    svc = compose["services"]["intelligence-service"]
    hc = svc.get("healthcheck", {})
    start_period = str(hc.get("start_period", ""))
    assert "180s" in start_period, (
        "intelligence-service healthcheck start_period must be 180s"
    )


def test_intelligence_service_healthcheck_retries_60(compose: dict) -> None:
    svc = compose["services"]["intelligence-service"]
    hc = svc.get("healthcheck", {})
    retries = hc.get("retries", 0)
    assert retries == 60, (
        f"intelligence-service healthcheck retries must be 60, got {retries}"
    )


# ---------------------------------------------------------------------------
# Named volumes: 4 total
# ---------------------------------------------------------------------------


def test_compose_four_named_volumes(compose: dict) -> None:
    volumes = compose.get("volumes", {})
    assert len(volumes) == 4, (
        f"docker-compose.yml must declare exactly 4 named volumes, found: {list(volumes.keys())}"
    )


def test_compose_volume_intelligence_runtime_state(compose: dict) -> None:
    volumes = compose.get("volumes", {})
    assert "intelligence_runtime_state" in volumes, (
        "docker-compose.yml must declare intelligence_runtime_state volume"
    )


def test_compose_no_old_volume(compose: dict) -> None:
    volumes = compose.get("volumes", {})
    assert "context_intelligence_service_data" not in volumes, (
        "docker-compose.yml must NOT declare old context_intelligence_service_data volume"
    )


def test_compose_volume_log_data(compose: dict) -> None:
    volumes = compose.get("volumes", {})
    assert "log_data" in volumes, "docker-compose.yml must declare log_data volume"


# ---------------------------------------------------------------------------
# Network: context-intelligence (bridge)
# ---------------------------------------------------------------------------


def test_compose_network_context_intelligence(compose: dict) -> None:
    networks = compose.get("networks", {})
    assert "context-intelligence" in networks, (
        "docker-compose.yml must define context-intelligence network"
    )


def test_compose_network_is_bridge(compose: dict) -> None:
    networks = compose.get("networks", {})
    network = networks.get("context-intelligence", {})
    driver = (network or {}).get("driver", "bridge")  # default is bridge
    assert driver == "bridge", "context-intelligence network must use bridge driver"


# ---------------------------------------------------------------------------
# docker compose config --quiet validation
# ---------------------------------------------------------------------------


def test_docker_compose_config_valid() -> None:
    """Validate the compose file using docker compose config --quiet."""
    result = subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE_FILE), "config", "--quiet"],
        capture_output=True,
        text=True,
        cwd=str(PROJECT_ROOT),
    )
    assert result.returncode == 0, (
        f"docker compose config --quiet failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )
