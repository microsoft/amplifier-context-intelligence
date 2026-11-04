"""Pydantic request/response models for the Context Intelligence Server."""

from __future__ import annotations

import re
from typing import Any, Literal

from context_intelligence_server.queue_manager import is_safe_session_id
from pydantic import BaseModel, ConfigDict, Field, field_validator


_LOWER_SHA256 = re.compile(r"[0-9a-f]{64}")


def _require_nonempty_workspace(value: str) -> str:
    """Return a meaningful workspace name or reject an empty value."""
    if not value or not value.strip():
        raise ValueError("workspace must not be empty")
    return value


def _require_lower_sha256(value: str, error_message: str) -> str:
    """Return a lowercase SHA-256 digest or reject an incompatible value."""
    if not _LOWER_SHA256.fullmatch(value):
        raise ValueError(error_message)
    return value


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
        return _require_nonempty_workspace(v)

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


class NativeRecoverySource(BaseModel):
    """Immutable, path-free descriptor for one native capture."""

    model_config = ConfigDict(extra="forbid")

    protocol: Literal["native-recovery-source-v1"]
    session_id: str
    source_stream_sha256: str
    source_sha256: str
    record_count: int = Field(ge=0)

    @field_validator("session_id")
    @classmethod
    def nonempty_session_id(cls, value: str) -> str:
        if not is_safe_session_id(value):
            raise ValueError("source.session_id is invalid")
        return value

    @field_validator("source_stream_sha256", "source_sha256")
    @classmethod
    def lower_sha256(cls, value: str) -> str:
        return _require_lower_sha256(
            value, "source hashes must be lowercase SHA-256 hex"
        )


class NativeRecoveryOrigin(BaseModel):
    """Identity of one ordinal within a source-capability namespace."""

    model_config = ConfigDict(extra="forbid")

    ordinal: int = Field(ge=0)
    source_line_sha256: str

    @field_validator("source_line_sha256")
    @classmethod
    def lower_sha256(cls, value: str) -> str:
        return _require_lower_sha256(
            value, "origin hashes must be lowercase SHA-256 hex"
        )


class RecoveryEventRequest(BaseModel):
    """A recovery envelope that deliberately excludes local working-directory data."""

    model_config = ConfigDict(extra="forbid")

    event: str
    workspace: str
    idempotency_key: str | None = None
    data: dict[str, Any]
    source: NativeRecoverySource
    origin: NativeRecoveryOrigin

    @field_validator("workspace")
    @classmethod
    def workspace_must_not_be_empty(cls, value: str) -> str:
        return _require_nonempty_workspace(value)


class RecoveryAdmissionRequest(BaseModel):
    """The exact identity and canonical payload digest for one future recovery event."""

    model_config = ConfigDict(extra="forbid")

    workspace: str
    source: NativeRecoverySource
    origin: NativeRecoveryOrigin
    payload_sha256: str

    @field_validator("workspace")
    @classmethod
    def workspace_must_not_be_empty(cls, value: str) -> str:
        return _require_nonempty_workspace(value)

    @field_validator("payload_sha256")
    @classmethod
    def lower_payload_sha256(cls, value: str) -> str:
        return _require_lower_sha256(
            value, "payload_sha256 must be lowercase SHA-256 hex"
        )


class RecoveryAdmissionPermitResponse(BaseModel):
    """A short-lived, single-use recovery admission permit."""

    status: Literal["permit"] = "permit"
    permit: str
    expires_at: float


class RecoveryAdmissionDuplicateResponse(BaseModel):
    """An exact durable recovery receipt acknowledged without a permit."""

    status: Literal["duplicate"] = "duplicate"


class LegacyNativeRecoveryOrigin(BaseModel):
    """Former public-origin shape, accepted only in explicit lease mode."""

    session_id: str
    ordinal: int = Field(ge=0)
    source_line_sha256: str
    source_stream_sha256: str

    @field_validator("session_id")
    @classmethod
    def nonempty_session_id(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("origin.session_id must not be empty")
        return value

    @field_validator("source_line_sha256", "source_stream_sha256")
    @classmethod
    def lower_sha256(cls, value: str) -> str:
        return _require_lower_sha256(
            value, "origin hashes must be lowercase SHA-256 hex"
        )


class LegacyRecoveryEventRequest(BaseModel):
    """Lease-only request retained during the explicit upgrade transition."""

    model_config = ConfigDict(extra="forbid")

    event: str
    workspace: str
    idempotency_key: str | None = None
    data: dict[str, Any]
    origin: LegacyNativeRecoveryOrigin

    @field_validator("workspace")
    @classmethod
    def workspace_must_not_be_empty(cls, value: str) -> str:
        return _require_nonempty_workspace(value)


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
