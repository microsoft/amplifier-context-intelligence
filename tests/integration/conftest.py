"""Pytest configuration for tests/integration/.

Integration tests exercise live asyncio drain-worker pipelines, real
SessionRegistry instances, and (optionally) a running Neo4j container.
They are deliberately heavier than unit tests and may be sensitive to
asyncio scheduling on small CI runners.

Every test in this directory is auto-marked with ``integration`` so it
can be excluded from the fast unit-test sweep:

    pytest -m "not neo4j and not integration"

The separate ``integration-tests`` CI job runs them in isolation with
verbose output so failures are immediately identifiable.
"""

from __future__ import annotations

import pytest


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Auto-apply the *integration* marker to every test under tests/integration/."""
    for item in items:
        path = str(item.fspath)
        if "/integration/" in path or "\\integration\\" in path:
            item.add_marker(pytest.mark.integration)
