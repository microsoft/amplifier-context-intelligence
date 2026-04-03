"""Pytest fixtures for Tier 3 Neo4j integration tests.

Provides two fixtures:
- neo4j_container (session-scoped): spins up a Neo4j 5.26.22-community
  container with random ports, waits for readiness, yields connection info.
- neo4j_services (function-scoped): creates a Neo4jGraphStore connected to
  the container and wraps it in a HookStateService; cleans up data after
  each test.

These fixtures are only used by tests marked with @pytest.mark.neo4j and
require the ``docker`` Python package to be installed.
"""

from __future__ import annotations

import socket
import time
from typing import Any, Generator

import httpx
import pytest
from neo4j import GraphDatabase

from context_intelligence_server.neo4j_store import Neo4jGraphStore
from context_intelligence_server.services import HookStateService


def _get_free_port() -> int:
    """Return a free TCP port on localhost."""
    with socket.socket() as s:
        s.bind(("", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def neo4j_container() -> Generator[dict[str, Any], None, None]:
    """Spin up a test-only Neo4j container with random ports.

    Uses random ports to avoid collision with any production instance.
    Container is destroyed unconditionally after the test session ends.
    Skips the entire test session if the ``docker`` package is not installed.
    """
    try:
        import docker  # type: ignore[import-untyped]
    except ImportError:
        pytest.skip("docker package not installed — skip Neo4j tests")

    client = docker.from_env()
    http_port = _get_free_port()
    bolt_port = _get_free_port()

    container = client.containers.run(
        "neo4j:5.26.22-community",
        environment={"NEO4J_AUTH": "neo4j/testpassword"},
        ports={"7474/tcp": http_port, "7687/tcp": bolt_port},
        detach=True,
        remove=True,
    )

    # Wait for Neo4j readiness (up to 60 seconds)
    for _ in range(60):
        try:
            r = httpx.get(f"http://localhost:{http_port}", timeout=2.0)
            if r.status_code < 500:
                break
        except Exception:
            pass
        time.sleep(1)
    else:
        container.stop()
        pytest.fail("Neo4j container did not become ready within 60 seconds")

    yield {
        "bolt_url": f"bolt://localhost:{bolt_port}",
        "http_url": f"http://localhost:{http_port}",
        "user": "neo4j",
        "password": "testpassword",
        "http_port": http_port,
        "bolt_port": bolt_port,
    }

    container.stop()


@pytest.fixture
def neo4j_services(
    neo4j_container: dict[str, Any],
) -> Generator[Any, None, None]:
    """HookStateService backed by the real Neo4j test container.

    Creates a Neo4jGraphStore connected to the test container, wraps it
    in a HookStateService, and cleans up all data between tests.
    """
    graph_store = Neo4jGraphStore(
        uri=neo4j_container["bolt_url"],
        auth=(neo4j_container["user"], neo4j_container["password"]),
        workspace="test",
    )
    services = HookStateService(workspace="test", graph_store=graph_store)

    yield services

    # Clean up all data between tests — synchronous driver for teardown
    driver = GraphDatabase.driver(
        neo4j_container["bolt_url"],
        auth=(neo4j_container["user"], neo4j_container["password"]),
    )
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
    driver.close()
