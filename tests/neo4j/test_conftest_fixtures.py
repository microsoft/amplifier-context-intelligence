"""Sanity tests for the Neo4j container fixtures in tests/neo4j/conftest.py.

These tests verify that:
1. The conftest module can be imported without errors.
2. The neo4j_container and neo4j_services fixture functions are defined.
3. The fixture scopes are correct (session vs function).
4. The helper _get_free_port returns a valid TCP port.

These tests do NOT require a running Neo4j container.
"""

from __future__ import annotations

import importlib
import sys
from collections.abc import Generator

import pytest


@pytest.fixture(autouse=True)
def _clear_conftest_module_cache() -> Generator[None, None, None]:
    """Clear the cached tests.neo4j.conftest module before each test."""
    sys.modules.pop("tests.neo4j.conftest", None)
    yield
    sys.modules.pop("tests.neo4j.conftest", None)


def test_conftest_module_importable() -> None:
    """tests.neo4j.conftest must be importable without errors."""
    mod = importlib.import_module("tests.neo4j.conftest")
    assert mod is not None


def test_neo4j_container_fixture_exists() -> None:
    """neo4j_container fixture must be defined in tests.neo4j.conftest."""
    mod = importlib.import_module("tests.neo4j.conftest")
    assert hasattr(mod, "neo4j_container"), (
        "neo4j_container fixture not found in tests.neo4j.conftest"
    )


def test_neo4j_services_fixture_exists() -> None:
    """neo4j_services fixture must be defined in tests.neo4j.conftest."""
    mod = importlib.import_module("tests.neo4j.conftest")
    assert hasattr(mod, "neo4j_services"), (
        "neo4j_services fixture not found in tests.neo4j.conftest"
    )


def test_get_free_port_returns_valid_port() -> None:
    """_get_free_port helper must return a valid TCP port (1-65535)."""
    mod = importlib.import_module("tests.neo4j.conftest")
    assert hasattr(mod, "_get_free_port"), (
        "_get_free_port helper not found in tests.neo4j.conftest"
    )
    port = mod._get_free_port()
    assert isinstance(port, int), f"Expected int, got {type(port)}"
    assert 1 <= port <= 65535, f"Port {port} out of valid range"


def test_neo4j_container_fixture_is_session_scoped() -> None:
    """neo4j_container must be session-scoped."""
    mod = importlib.import_module("tests.neo4j.conftest")
    fixture_fn = mod.neo4j_container
    # pytest wraps fixtures; check the _pytestfixturefunction marker
    fixtureinfo = getattr(fixture_fn, "_pytestfixturefunction", None)
    if fixtureinfo is not None:
        assert fixtureinfo.scope == "session", (
            f"neo4j_container scope must be 'session', got {fixtureinfo.scope!r}"
        )


def test_neo4j_services_fixture_is_function_scoped() -> None:
    """neo4j_services must be function-scoped (default)."""
    mod = importlib.import_module("tests.neo4j.conftest")
    fixture_fn = mod.neo4j_services
    fixtureinfo = getattr(fixture_fn, "_pytestfixturefunction", None)
    if fixtureinfo is not None:
        assert fixtureinfo.scope == "function", (
            f"neo4j_services scope must be 'function', got {fixtureinfo.scope!r}"
        )
