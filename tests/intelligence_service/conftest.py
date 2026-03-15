"""Pytest configuration and shared fixtures for the intelligence_service test suite."""

from collections.abc import AsyncGenerator

import httpx
import pytest

from intelligence_service.app import app


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Async HTTP client wired directly to the ASGI app (no real network).

    Runs the app's lifespan (startup/shutdown) so that app.state is fully
    populated before any request handler runs.
    """
    # Starlette internal — recommended pattern for ASGI lifespan in httpx tests
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as c:
            yield c
