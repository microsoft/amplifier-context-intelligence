"""W4 (doc 16 §6) — Neo4j Browser (7474 HTTP) URL hide flag.

Fail-safe hidden: the browser URL is surfaced on /status ONLY in development or
via the explicit opt-in. In Azure it points into a private VNet and must not be
exposed. The Bolt URL (neo4j_url) and health stay visible regardless.

Isolation-only: predicate unit tests (no app) + /status composition tests routed
through the allow_unauthenticated dev ``client`` fixture (W3's auth gate is inert
there). ``get_status`` reads the module-level ``main._settings``, so the
composition tests monkeypatch that.
"""

from __future__ import annotations

import httpx
import pytest

import context_intelligence_server.main as main_module
from context_intelligence_server.config import Settings

_BROWSER_URL = "http://localhost:7474"


# ---------------------------------------------------------------------------
# Predicate unit tests (fast, no app)
# ---------------------------------------------------------------------------


def test_browser_url_hidden_by_default() -> None:
    """Default (production, no opt-in) → hidden."""
    assert Settings().neo4j_browser_url_visible() is False


def test_browser_url_visible_in_development() -> None:
    """is_development=True → visible."""
    assert Settings(is_development=True).neo4j_browser_url_visible() is True


def test_browser_url_visible_when_opt_in() -> None:
    """show_neo4j_browser_url=True → visible even outside development."""
    assert Settings(show_neo4j_browser_url=True).neo4j_browser_url_visible() is True


# ---------------------------------------------------------------------------
# /status composition tests (dev client — auth gate inert)
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_status_hides_browser_url_by_default(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Default settings → neo4j_browser_url key present but null; Bolt url stays."""
    monkeypatch.setattr(
        main_module,
        "_settings",
        main_module._settings.model_copy(
            update={
                "is_development": False,
                "show_neo4j_browser_url": False,
                "neo4j_browser_url": _BROWSER_URL,
            }
        ),
    )

    response = await client.get("/status")
    assert response.status_code == 200
    data = response.json()
    # Key ALWAYS present (stable /status shape) but null when hidden.
    assert "neo4j_browser_url" in data
    assert data["neo4j_browser_url"] is None
    # Bolt url (neo4j_url) stays visible and non-null.
    assert data.get("neo4j_url")


@pytest.mark.anyio
async def test_status_shows_browser_url_in_development(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """is_development=True → neo4j_browser_url equals the configured URL."""
    monkeypatch.setattr(
        main_module,
        "_settings",
        main_module._settings.model_copy(
            update={
                "is_development": True,
                "neo4j_browser_url": _BROWSER_URL,
            }
        ),
    )

    response = await client.get("/status")
    assert response.status_code == 200
    data = response.json()
    assert data["neo4j_browser_url"] == _BROWSER_URL
