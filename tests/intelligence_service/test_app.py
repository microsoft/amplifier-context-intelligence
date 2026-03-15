"""Tests for the Intelligence Service FastAPI application."""

import pytest
import httpx

from intelligence_service.app import app


@pytest.mark.asyncio
async def test_health_returns_200_with_status_ok() -> None:
    """GET /health returns 200 with {'status': 'ok'}."""
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.get("/health")

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"


@pytest.mark.asyncio
async def test_reload_bundle_returns_stub_response() -> None:
    """GET /admin/reload-bundle returns 200 with data['status']=='reload_not_implemented'."""
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.get("/admin/reload-bundle")

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "reload_not_implemented"
