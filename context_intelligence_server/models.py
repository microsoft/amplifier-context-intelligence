"""Pydantic request/response models for the Context Intelligence Server."""

from typing import Any

from pydantic import BaseModel


class EventRequest(BaseModel):
    """Inbound event payload from an Amplifier client."""

    event: str
    workspace: str
    data: dict[str, Any]


class EventResponse(BaseModel):
    """Response returned after an event is accepted."""

    status: str = "queued"
    session_id: str | None = None


class StatusResponse(BaseModel):
    """Server health and metrics response."""

    status: str
    uptime_seconds: float
    active_sessions: int
