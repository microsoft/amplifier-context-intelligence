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

    working_dir is OPTIONAL — the bundle hook emits it as a top-level envelope
    field alongside workspace, but older clients and re-imported archives may
    omit it.  It is declared here for contract + validation only: the ingest
    endpoint persists the RAW request body to the durable queue, so the field
    reaches the drainer whether or not this model names it.  Absent/None leaves
    the Session node's working_dir unset for a later event to populate;
    populate-if-missing means an already-set value is never overwritten.
    """

    event: str
    workspace: str
    working_dir: str | None = None
    idempotency_key: str | None = None
    data: dict[str, Any]

    @field_validator("workspace")
    @classmethod
    def workspace_must_not_be_empty(cls, v: str) -> str:
        """Reject blank workspace — a workspace is always a non-empty project slug."""
        if not v or not v.strip():
            raise ValueError("workspace must not be empty")
        return v

    @field_validator("working_dir")
    @classmethod
    def working_dir_must_not_be_blank(cls, v: str | None) -> str | None:
        """Allow ``None`` (working_dir is optional, unlike workspace) but reject
        blank/whitespace-only strings.

        ``None`` means "this client did not report a working directory" — which
        is NOT the same as "the working directory is the empty string".  A
        whitespace-only value (e.g. ``"   "``) is never a legitimate path and
        must not reach the Session node verbatim.
        """
        if v is not None and not v.strip():
            raise ValueError("working_dir must not be blank")
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
