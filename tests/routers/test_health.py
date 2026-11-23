"""Tests for the GET /health endpoint (hosting platform liveness)."""

from __future__ import annotations

import httpx
import pytest


@pytest.mark.anyio
async def test_health_is_200_with_the_commit(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("GIT_COMMIT", "abc123")
    response = await client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "commit": "abc123"}
