"""Tests for the simplified 2-service docker-compose.yml.

TDD phase: These tests defined the EXPECTED state after task-5 removed
intelligence-service (port 8100) and frontend (port 3000). They failed RED
against the 4-service compose and passed GREEN after the removal.
"""

import pathlib

import yaml

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
COMPOSE_FILE = PROJECT_ROOT / "docker-compose.yml"


def _compose() -> dict:
    return yaml.safe_load(COMPOSE_FILE.read_text())


def test_valid_yaml() -> None:
    """docker-compose.yml must be valid YAML."""
    content = COMPOSE_FILE.read_text()
    parsed = yaml.safe_load(content)
    assert isinstance(parsed, dict), "docker-compose.yml must parse to a dict"
    assert "services" in parsed, "docker-compose.yml must have a 'services' key"


def test_exactly_two_services() -> None:
    """docker-compose.yml must contain exactly 2 services after simplification."""
    services = _compose().get("services", {})
    assert list(services.keys()) == ["context-intelligence-server", "neo4j"], (
        f"Expected exactly ['context-intelligence-server', 'neo4j'], got {list(services.keys())}"
    )


def test_no_intelligence_service_or_frontend() -> None:
    """intelligence-service and frontend must have been removed from the stack."""
    services = _compose().get("services", {})
    assert "intelligence-service" not in services, (
        "intelligence-service must not be present in docker-compose.yml"
    )
    assert "frontend" not in services, (
        "frontend must not be present in docker-compose.yml"
    )
