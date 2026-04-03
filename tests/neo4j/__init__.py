"""Tier 3 integration tests requiring a live Neo4j container.

Tests in this directory are gated behind the ``@pytest.mark.neo4j`` marker and
are excluded from the default test run.  To execute them you must have a running
Neo4j instance available and opt-in explicitly::

    uv run pytest -m neo4j

To exclude them (the default behaviour) run pytest normally or use::

    uv run pytest -m "not neo4j"
"""
