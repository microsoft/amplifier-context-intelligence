"""Pytest fixtures for Tier 3 Neo4j integration tests.

Provides three fixtures:
- neo4j_container (session-scoped): spins up a Neo4j 5.26.22-community
  container with random ports, waits for readiness, yields connection info.
- neo4j_container_capped (module-scoped): spins up a Neo4j 5.26.22-community
  container with a 2 MiB per-transaction memory cap; fully independent of
  the shared session container (zero cap-leakage).
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
        from docker.errors import APIError  # type: ignore[import-untyped]
    except ImportError:
        pytest.skip("docker package not installed — skip Neo4j tests")

    client = docker.from_env()

    # Start the container, retrying on the transient docker port-allocation flake.
    #
    # On busy hosts (notably Docker Desktop / WSL2) the daemon intermittently
    # rejects the port forward with:
    #   APIError: 500 ... ports are not available: exposing port TCP
    #   0.0.0.0:<p> -> 127.0.0.1:0: /forwards/expose returned unexpected status: 500
    # This is pure infrastructure noise — the requested host port could not be
    # bound at that instant — NOT a test or product failure.  A 100-run
    # classification of the concurrent dual-label adversarial test measured
    # 97 pass / 0 dual / 3 of exactly this port-allocation error; without this
    # retry those 3 surface as spurious red and mask the (deterministic) real
    # result.  Re-pick fresh free ports and retry a few times before giving up.
    container = None
    http_port = bolt_port = 0
    last_exc: Exception | None = None
    for _attempt in range(5):
        http_port = _get_free_port()
        bolt_port = _get_free_port()
        try:
            container = client.containers.run(
                "neo4j:5.26.22-community",
                environment={"NEO4J_AUTH": "neo4j/testpassword"},
                ports={"7474/tcp": http_port, "7687/tcp": bolt_port},
                detach=True,
                remove=True,
            )
            break
        except APIError as exc:
            # Only swallow the port-allocation flake; re-raise anything else.
            if "ports are not available" not in str(exc):
                raise
            last_exc = exc
            time.sleep(1)
    if container is None:
        pytest.fail(
            f"Neo4j container could not acquire host ports after 5 attempts "
            f"(transient docker port-allocation flake): {last_exc}"
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


@pytest.fixture(scope="module")
def neo4j_container_capped() -> Generator[dict[str, Any], None, None]:
    """Spin up a memory-capped Neo4j container with random ports (MODULE scope).

    Identical bootstrap logic to ``neo4j_container`` (random ports, port-flake
    retry, 60s readiness poll, remove=True, stop on teardown) but:

    - **MODULE-scoped** — one container per test module, not the full session.
    - **2 MiB per-transaction cap** via ``NEO4J_db_memory_transaction_max=2m``.
      The runtime ``dbms.setConfigValue`` API does NOT exist on Community
      Edition (raises ProcedureNotFound), so the cap MUST be set at container
      startup via environment variable.
    - **Fully independent** of ``neo4j_container`` — no shared state, no cap
      leakage into the shared session container.

    Skips if the ``docker`` package is not importable.
    Yields a dict with bolt_url, http_url, user, password, http_port, bolt_port.
    """
    try:
        import docker  # type: ignore[import-untyped]
        from docker.errors import APIError  # type: ignore[import-untyped]
    except ImportError:
        pytest.skip("docker package not installed — skip Neo4j tests")

    client = docker.from_env()

    container = None
    http_port = bolt_port = 0
    last_exc: Exception | None = None
    for _attempt in range(5):
        http_port = _get_free_port()
        bolt_port = _get_free_port()
        try:
            container = client.containers.run(
                "neo4j:5.26.22-community",
                environment={
                    "NEO4J_AUTH": "neo4j/testpassword",
                    "NEO4J_db_memory_transaction_max": "2m",
                },
                ports={"7474/tcp": http_port, "7687/tcp": bolt_port},
                detach=True,
                remove=True,
            )
            break
        except APIError as exc:
            if "ports are not available" not in str(exc):
                raise
            last_exc = exc
            time.sleep(1)
    if container is None:
        pytest.fail(
            f"Capped Neo4j container could not acquire host ports after 5 attempts "
            f"(transient docker port-allocation flake): {last_exc}"
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
        pytest.fail("Capped Neo4j container did not become ready within 60 seconds")

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
