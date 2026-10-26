"""Pytest configuration for tests/integration/.

Integration tests exercise live asyncio drain-worker pipelines, real
SessionRegistry instances, and (optionally) a running Neo4j container.
They are deliberately heavier than unit tests and may be sensitive to
asyncio scheduling on small CI runners.

Tests in this directory are auto-marked with ``integration``, except the
credential-free delivery contracts in test_docker_image.py. That file explicitly
marks its real Docker test with both ``integration`` and ``image``.
The heavier tests can be excluded from the fast unit-test sweep:

    pytest -m "not neo4j and not integration"

The separate ``integration-tests`` CI job runs them in isolation with
verbose output so failures are immediately identifiable.
"""

from __future__ import annotations

import pytest


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Auto-mark integration tests, leaving delivery contracts in the unit lane."""
    for item in items:
        if item.path.name == "test_docker_image.py":
            continue
        path = str(item.fspath)
        if "/integration/" in path or "\\integration\\" in path:
            item.add_marker(pytest.mark.integration)
