"""Pytest configuration and shared fixtures for the intelligence_service test suite."""

from collections.abc import AsyncGenerator

import httpx
import pytest

from intelligence_service.app import app


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Async HTTP client wired directly to the ASGI app (no real network)."""
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as c:
        yield c
