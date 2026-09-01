"""Tests for the session-deletion routes: GET .../summary and DELETE .../{session_id}.

These are router tests, not full end-to-end tests: they replace the two
dependency functions that build a real DeletionService (``read_deletion_service``
and ``delete_route_service``) with a fake service, using
``app.dependency_overrides``. This proves the route-to-service wiring (which
session id reaches the service, how a 404/409 is produced) and the read/write
auth gating, without needing a real Neo4j connection. A real-server-plus-Neo4j
end-to-end test is a separate, later item.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, MutableMapping
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

import httpx
import pytest
from context_intelligence_server.authz import require_write
from context_intelligence_server.deletion import DeletionPreview, DeletionResult
from context_intelligence_server.graph_store import AmbiguousSessionError
from context_intelligence_server.main import app
from context_intelligence_server.routers import deletion as deletion_router
from fastapi import HTTPException


def _sample_preview(**overrides: Any) -> DeletionPreview:
    values: dict[str, Any] = {
        "root_id": "root-1",
        "session_ids": frozenset({"root-1", "sub-1"}),
        "node_count": 10,
        "edge_count": 5,
        "blob_count": 2,
        "created_by": "alice",
        "started_at": datetime(2024, 1, 1, 12, 0, 0),
        "last_change": datetime(2024, 1, 2, 8, 30, 0),
        "subsession_count": 1,
        "workspace": "ws1",
        "working_dir": "/home/alice/project",
        "deletable": True,
        "pending_sessions": [],
    }
    values.update(overrides)
    return DeletionPreview(**values)


def _sample_result(**overrides: Any) -> DeletionResult:
    values: dict[str, Any] = {
        "root_id": "root-1",
        "session_count": 2,
        "nodes_deleted": 10,
        "relationships_deleted": 5,
        "blobs_deleted": 2,
        "queue_sessions_cleaned": 2,
    }
    values.update(overrides)
    return DeletionResult(**values)


class _FakeDeletionService:
    """Stands in for a real DeletionService. Records what it was called with."""

    def __init__(
        self,
        preview: DeletionPreview | None = None,
        result: DeletionResult | None = None,
        apply_error: Exception | None = None,
        preview_error: Exception | None = None,
    ) -> None:
        self._preview = preview
        self._result = result
        self._apply_error = apply_error
        self._preview_error = preview_error
        self.preview_calls: list[str] = []
        self.apply_calls: list[tuple[str, str | None]] = []

    async def preview(self, session_id: str) -> DeletionPreview | None:
        self.preview_calls.append(session_id)
        if self._preview_error is not None:
            raise self._preview_error
        return self._preview

    async def apply(
        self, session_id: str, *, requested_by: str | None = None
    ) -> DeletionResult | None:
        self.apply_calls.append((session_id, requested_by))
        if self._apply_error is not None:
            raise self._apply_error
        return self._result


def _reject_write() -> None:
    """A require_write stand-in for a caller who is not write-capable."""
    raise HTTPException(status_code=403, detail="write access refused (test double)")


@pytest.fixture(autouse=True)
def _clear_overrides() -> Any:
    """Every test starts and ends with a clean dependency_overrides map."""
    yield
    app.dependency_overrides.pop(deletion_router.read_deletion_service, None)
    app.dependency_overrides.pop(deletion_router.delete_route_service, None)
    app.dependency_overrides.pop(require_write, None)


def _override_read_service(fake: _FakeDeletionService) -> None:
    async def _fake() -> _FakeDeletionService:
        return fake

    app.dependency_overrides[deletion_router.read_deletion_service] = _fake


def _override_delete_service(fake: _FakeDeletionService) -> None:
    async def _fake() -> _FakeDeletionService:
        return fake

    app.dependency_overrides[deletion_router.delete_route_service] = _fake


@asynccontextmanager
async def _client_with_scope_state(
    state: dict[str, Any],
) -> AsyncIterator[httpx.AsyncClient]:
    """A client whose requests carry the given scope state (e.g. contributor_id).

    ``app`` (used by the plain ``client`` fixture) has no auth middleware, so
    ``request.scope["state"]`` is otherwise empty. This wraps ``app`` with a
    minimal ASGI layer that injects the given state before the request
    reaches any route -- enough to prove the router reads
    ``contributor_id`` correctly, without building a real bearer token.
    """

    async def _wrapped(
        scope: MutableMapping[str, Any], receive: Any, send: Any
    ) -> None:
        if scope["type"] == "http":
            scope = {**scope, "state": dict(state)}
        await app(scope, receive, send)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_wrapped), base_url="http://test"
    ) as c:
        yield c


class TestGetSessionSummary:
    @pytest.mark.anyio
    async def test_known_session_returns_expected_fields(
        self, client: httpx.AsyncClient
    ) -> None:
        preview = _sample_preview()
        fake = _FakeDeletionService(preview=preview)
        _override_read_service(fake)

        response = await client.get("/sessions/root-1/summary")

        assert response.status_code == 200
        body = response.json()
        assert body == {
            "root_id": "root-1",
            "session_ids": ["root-1", "sub-1"],
            "node_count": 10,
            "edge_count": 5,
            "blob_count": 2,
            "created_by": "alice",
            "started_at": "2024-01-01T12:00:00",
            "last_change": "2024-01-02T08:30:00",
            "subsession_count": 1,
            "workspace": "ws1",
            "working_dir": "/home/alice/project",
            "deletable": True,
            "pending_sessions": [],
        }
        assert fake.preview_calls == ["root-1"]

    @pytest.mark.anyio
    async def test_no_workspace_query_param_needed(
        self, client: httpx.AsyncClient
    ) -> None:
        """The summary route takes no workspace query param at all -- the
        session id alone is enough to reach the service."""
        fake = _FakeDeletionService(preview=_sample_preview())
        _override_read_service(fake)

        response = await client.get("/sessions/root-1/summary")

        assert response.status_code == 200
        assert fake.preview_calls == ["root-1"]

    @pytest.mark.anyio
    async def test_unknown_session_returns_404(self, client: httpx.AsyncClient) -> None:
        fake = _FakeDeletionService(preview=None)
        _override_read_service(fake)

        response = await client.get("/sessions/does-not-exist/summary")

        assert response.status_code == 404

    @pytest.mark.anyio
    async def test_ambiguous_session_id_returns_409(
        self, client: httpx.AsyncClient
    ) -> None:
        """A session id found in more than one workspace is a 409, not a 500
        or a silent guess."""
        fake = _FakeDeletionService(
            preview_error=AmbiguousSessionError("root-1", ["ws1", "ws2"])
        )
        _override_read_service(fake)

        response = await client.get("/sessions/root-1/summary")

        assert response.status_code == 409
        assert "more than one workspace" in response.json()["detail"]

    @pytest.mark.anyio
    async def test_allows_a_caller_who_fails_require_write(
        self, client: httpx.AsyncClient
    ) -> None:
        """The summary route is gated by require_read only. A caller who is
        refused by require_write must still be able to read the summary."""
        fake = _FakeDeletionService(preview=_sample_preview())
        _override_read_service(fake)
        app.dependency_overrides[require_write] = _reject_write

        response = await client.get("/sessions/root-1/summary")

        assert response.status_code == 200


class TestDeleteSession:
    @pytest.mark.anyio
    async def test_delete_returns_result(self, client: httpx.AsyncClient) -> None:
        result = _sample_result()
        fake = _FakeDeletionService(result=result)
        _override_delete_service(fake)

        response = await client.delete("/sessions/root-1")

        assert response.status_code == 200
        assert response.json() == {
            "root_id": "root-1",
            "session_count": 2,
            "nodes_deleted": 10,
            "relationships_deleted": 5,
            "blobs_deleted": 2,
            "queue_sessions_cleaned": 2,
        }
        assert fake.preview_calls == []  # DELETE never previews -- GET does
        assert len(fake.apply_calls) == 1
        assert fake.apply_calls[0][0] == "root-1"

    @pytest.mark.anyio
    async def test_no_workspace_query_param_needed(
        self, client: httpx.AsyncClient
    ) -> None:
        """The delete route takes no workspace query param at all -- the
        session id alone is enough to reach the service."""
        fake = _FakeDeletionService(result=_sample_result())
        _override_delete_service(fake)

        response = await client.delete("/sessions/root-1")

        assert response.status_code == 200
        assert fake.apply_calls == [("root-1", None)]

    @pytest.mark.anyio
    async def test_passes_the_authenticated_caller_as_requested_by(self) -> None:
        result = _sample_result()
        fake = _FakeDeletionService(result=result)
        _override_delete_service(fake)

        async with _client_with_scope_state({"contributor_id": "alice"}) as client:
            response = await client.delete("/sessions/root-1")

        assert response.status_code == 200
        assert fake.apply_calls == [("root-1", "alice")]

    @pytest.mark.anyio
    async def test_unknown_session_returns_404(self, client: httpx.AsyncClient) -> None:
        fake = _FakeDeletionService(result=None)
        _override_delete_service(fake)

        response = await client.delete("/sessions/does-not-exist")

        assert response.status_code == 404

    @pytest.mark.anyio
    async def test_conflict_when_sessions_still_receiving_data(
        self, client: httpx.AsyncClient
    ) -> None:
        fake = _FakeDeletionService(
            apply_error=RuntimeError("sessions still draining: ['sub-1']")
        )
        _override_delete_service(fake)

        response = await client.delete("/sessions/root-1")

        assert response.status_code == 409
        assert "still draining" in response.json()["detail"]

    @pytest.mark.anyio
    async def test_ambiguous_session_id_returns_409(
        self, client: httpx.AsyncClient
    ) -> None:
        """A session id found in more than one workspace is a 409, not a 500
        or a silent guess."""
        fake = _FakeDeletionService(
            apply_error=AmbiguousSessionError("root-1", ["ws1", "ws2"])
        )
        _override_delete_service(fake)

        response = await client.delete("/sessions/root-1")

        assert response.status_code == 409
        assert "more than one workspace" in response.json()["detail"]

    @pytest.mark.anyio
    async def test_refuses_a_caller_who_fails_require_write(
        self, client: httpx.AsyncClient
    ) -> None:
        fake = _FakeDeletionService(result=_sample_result())
        _override_delete_service(fake)
        app.dependency_overrides[require_write] = _reject_write

        response = await client.delete("/sessions/root-1")

        assert response.status_code == 403
