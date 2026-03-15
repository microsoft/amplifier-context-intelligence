"""
Tests for system architecture DOT files in docs/dot/.
TDD RED phase: These tests fail until the DOT files are created.
"""

import re
from pathlib import Path

DOCS_DOT_DIR = Path(__file__).parent.parent / "docs" / "dot"


def _load_dot(filename: str) -> str:
    """Load a DOT file and return its contents."""
    path = DOCS_DOT_DIR / filename
    assert path.exists(), f"DOT file not found: {path}"
    return path.read_text()


def _validate_dot_structure(content: str, rankdir: str) -> None:
    """Validate basic DOT digraph structure."""
    # Must be a digraph
    assert re.search(r"\bdigraph\b", content), "Must be a digraph"
    # Must have opening brace
    assert "{" in content, "Must have opening brace"
    # Must have closing brace
    assert "}" in content, "Must have closing brace"
    # Braces must be balanced
    assert content.count("{") == content.count("}"), "Braces must be balanced"
    # Must have the correct rankdir
    assert f"rankdir={rankdir}" in content, f"Must have rankdir={rankdir}"


# ---------------------------------------------------------------------------
# system-architecture.dot tests
# ---------------------------------------------------------------------------


class TestSystemArchitectureDot:
    def test_file_exists(self):
        assert (DOCS_DOT_DIR / "system-architecture.dot").exists()

    def test_valid_digraph_structure(self):
        content = _load_dot("system-architecture.dot")
        _validate_dot_structure(content, "TB")

    def test_contains_ingestion_server(self):
        content = _load_dot("system-architecture.dot")
        assert "Ingestion" in content or "ingestion" in content
        assert "8000" in content

    def test_contains_ingestion_endpoints(self):
        content = _load_dot("system-architecture.dot")
        assert "/dashboard" in content
        assert "/status" in content
        assert "/cypher" in content
        assert "/events" in content

    def test_contains_intelligence_service(self):
        content = _load_dot("system-architecture.dot")
        assert "Intelligence" in content or "intelligence" in content
        assert "8100" in content

    def test_contains_intelligence_endpoints(self):
        content = _load_dot("system-architecture.dot")
        assert "/ws" in content or "WebSocket" in content or "websocket" in content
        assert "/health" in content

    def test_contains_frontend(self):
        content = _load_dot("system-architecture.dot")
        assert (
            "frontend" in content.lower()
            or "nginx" in content.lower()
            or "Frontend" in content
        )
        assert "3000" in content

    def test_contains_neo4j(self):
        content = _load_dot("system-architecture.dot")
        assert "Neo4j" in content or "neo4j" in content
        assert "7474" in content
        assert "7687" in content

    def test_contains_volumes(self):
        content = _load_dot("system-architecture.dot")
        assert "blob_data" in content
        assert "neo4j_data" in content
        assert "projects" in content

    def test_contains_service_dependencies(self):
        content = _load_dot("system-architecture.dot")
        # Intelligence depends on ingestion, frontend depends on intelligence
        # These should be represented as edges in the graph
        assert "->" in content  # must have directed edges


# ---------------------------------------------------------------------------
# container-initialization.dot tests
# ---------------------------------------------------------------------------


class TestContainerInitializationDot:
    def test_file_exists(self):
        assert (DOCS_DOT_DIR / "container-initialization.dot").exists()

    def test_valid_digraph_structure(self):
        content = _load_dot("container-initialization.dot")
        _validate_dot_structure(content, "TB")

    def test_contains_container_start(self):
        content = _load_dot("container-initialization.dot")
        assert (
            "Container Start" in content
            or "container_start" in content
            or "start" in content.lower()
        )

    def test_contains_config_overlay(self):
        content = _load_dot("container-initialization.dot")
        assert "Config" in content or "config" in content
        assert "Overlay" in content or "overlay" in content or "cp" in content

    def test_contains_apply_settings(self):
        content = _load_dot("container-initialization.dot")
        assert "Settings" in content or "settings" in content
        assert "Apply" in content or "apply" in content or "amplifier" in content

    def test_contains_install_bundle(self):
        content = _load_dot("container-initialization.dot")
        assert "Bundle" in content or "bundle" in content
        assert "Install" in content or "install" in content or "add" in content

    def test_contains_bridge_start(self):
        content = _load_dot("container-initialization.dot")
        assert (
            "Bridge" in content
            or "bridge" in content
            or "uvicorn" in content
            or "8100" in content
        )

    def test_contains_health_ready(self):
        content = _load_dot("container-initialization.dot")
        assert "Health" in content or "health" in content
        assert "Ready" in content or "ready" in content or "200" in content

    def test_contains_error_path(self):
        content = _load_dot("container-initialization.dot")
        assert "Error" in content or "error" in content
        assert "restart" in content or "Restart" in content or "Docker" in content

    def test_contains_directed_edges(self):
        content = _load_dot("container-initialization.dot")
        assert "->" in content


# ---------------------------------------------------------------------------
# data-access.dot tests
# ---------------------------------------------------------------------------


class TestDataAccessDot:
    def test_file_exists(self):
        assert (DOCS_DOT_DIR / "data-access.dot").exists()

    def test_valid_digraph_structure(self):
        content = _load_dot("data-access.dot")
        _validate_dot_structure(content, "LR")

    def test_contains_graph_query_path(self):
        content = _load_dot("data-access.dot")
        assert (
            "graph_query" in content
            or "graph query" in content.lower()
            or "cypher" in content.lower()
        )

    def test_contains_cypher_endpoint(self):
        content = _load_dot("data-access.dot")
        assert "/cypher" in content or "cypher" in content.lower()

    def test_contains_neo4j_bolt(self):
        content = _load_dot("data-access.dot")
        assert "Neo4j" in content or "neo4j" in content
        assert "Bolt" in content or "bolt" in content or "7687" in content

    def test_contains_blob_reader_path(self):
        content = _load_dot("data-access.dot")
        assert "blob_reader" in content or "blob reader" in content.lower()
        assert "blob_data" in content

    def test_contains_session_tools_path(self):
        content = _load_dot("data-access.dot")
        assert "Session" in content or "session" in content
        assert "projects" in content
        assert "events.jsonl" in content or "events" in content

    def test_contains_return_paths(self):
        content = _load_dot("data-access.dot")
        # Return paths should be dashed - check for style=dashed
        assert "dashed" in content

    def test_contains_directed_edges(self):
        content = _load_dot("data-access.dot")
        assert "->" in content
