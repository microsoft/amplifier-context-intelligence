"""Pydantic request/response models for the Context Intelligence Server."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator


class EventRequest(BaseModel):
    """Inbound event payload from an Amplifier client.

    workspace is mandatory — events without a workspace are invalid.
    The Amplifier client must always supply workspace on every event.
    Events without workspace (e.g. an incorrectly configured hook) are
    rejected at the endpoint with HTTP 422.
    """

    event: str
    workspace: str
    idempotency_key: str | None = None
    data: dict[str, Any]

    @field_validator("workspace")
    @classmethod
    def workspace_must_not_be_empty(cls, v: str) -> str:
        """Reject blank workspace — a workspace is always a non-empty project slug."""
        if not v or not v.strip():
            raise ValueError("workspace must not be empty")
        return v


class EventResponse(BaseModel):
    """Response returned after an event is accepted."""

    status: str = "queued"
    session_id: str | None = None


class StatusResponse(BaseModel):
    """Server health and metrics response."""

    status: str
    uptime_seconds: float
    active_sessions: int


class CypherRequest(BaseModel):
    """Request body for proxying a Cypher query to Neo4j."""

    query: str
    params: dict[str, Any] = Field(default_factory=dict)
    workspace: str | None = None
