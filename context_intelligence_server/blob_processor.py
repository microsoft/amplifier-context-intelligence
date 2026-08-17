"""blob_processor — In-Place Transform for event data blob offloading.

Identifies large blob fields in event data, writes them to the blob store,
and replaces the field value with a ``ci-blob://`` URI reference in-place.

No deepcopy is performed — the caller (server) owns the deserialized JSON
object exclusively, so in-place mutation is safe and avoids extra allocation.
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from context_intelligence_server.blob_store import BlobStore

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BLOB_FIELDS: frozenset[str] = frozenset(
    {"raw", "result", "messages", "mount_plan", "context_snapshot", "debug"}
)


# ---------------------------------------------------------------------------
# Blob-ref carrier allowlist -- single source of truth (WS-5 runtime tripwire)
# ---------------------------------------------------------------------------
#
# Every ``ci-blob://`` URI minted below (``process_event_data``) is written
# into ``data``, which ``DefaultHandler`` always persists wholesale as the
# JSON-serialized ``data`` property on the Event node
# (handlers/data_layer_1/default.py). The blob-reclaim reference scan
# (``routers.admin._scan_referenced_uris``) enumerates every ``ci-blob://``
# reference anywhere in the graph by walking a FIXED allowlist of node
# properties -- never an all-property/all-node scan (this codebase has scar
# tissue from a 1.3M-node AllNodesScan stall). A blob whose reference lives
# on a node property the scan doesn't know about is invisible to it and can
# be deleted as a false orphan.
#
# BLOB_REF_CARRIER_PROPERTIES is THE single source of truth for that
# allowlist, imported by ``routers.admin`` to build the scan's Cypher
# directly from this tuple (so the query text can never drift from it) and
# checked here, at the mint site, via :func:`assert_carrier_registered`.
#
# Adding a new carrier (a future field-lifter/enricher that promotes a
# blob-ref-shaped value onto a new node property) means adding its name
# here. Forgetting to is now a fail-closed error, not a silent GC hole.
BLOB_REF_CARRIER_PROPERTIES: tuple[str, ...] = (
    "data",
    "tool_input",
    "prompt",
    "response",
)

# Defensive validation, run once at import time: every carrier name must be
# a legal Cypher property identifier, because routers.admin interpolates
# these names directly into a Cypher query string. Guards against a future
# careless addition (e.g. containing a space or backtick) turning into a
# broken or injectable query rather than a loud, immediate import error.
_VALID_CARRIER_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_carrier_names(names: tuple[str, ...]) -> None:
    for name in names:
        if not _VALID_CARRIER_NAME_RE.match(name):
            raise ValueError(
                f"BLOB_REF_CARRIER_PROPERTIES entry {name!r} is not a valid "
                "Cypher property identifier -- refusing to load (this tuple "
                "is interpolated directly into a Cypher query by "
                "routers.admin._scan_referenced_uris)"
            )


_validate_carrier_names(BLOB_REF_CARRIER_PROPERTIES)


class UnregisteredBlobCarrierError(RuntimeError):
    """A ``ci-blob://`` reference is destined for a node property that is
    not in :data:`BLOB_REF_CARRIER_PROPERTIES`.

    This is the WS-5 runtime tripwire: it converts a silent reclaim-GC hole
    (a live blob deleted as an orphan because its carrier property was never
    added to the allowlist) into a loud, immediate failure at the point the
    omission is introduced -- not after a live blob is gone.
    """


def assert_carrier_registered(property_name: str) -> None:
    """Fail loud if *property_name* is not a registered blob-ref carrier.

    Cheap (single tuple-membership check) and safe to call on every
    ``process_event_data`` invocation. Raises
    :class:`UnregisteredBlobCarrierError` -- deliberately NOT caught by the
    per-field ``except Exception`` below, so it propagates out of
    ``process_event_data``, through ``pipeline.process_event``'s outer
    handler (which logs and re-raises), and the event is dead-lettered
    instead of silently minting an unprotected blob reference.
    """
    if property_name not in BLOB_REF_CARRIER_PROPERTIES:
        raise UnregisteredBlobCarrierError(
            f"ci-blob:// reference destined for node property {property_name!r} "
            f"is not in BLOB_REF_CARRIER_PROPERTIES {BLOB_REF_CARRIER_PROPERTIES!r} "
            "-- the blob-reclaim scan (context_intelligence_server.routers.admin) "
            "will not see refs stored there and could delete this blob as an "
            f"orphan. Add {property_name!r} to BLOB_REF_CARRIER_PROPERTIES "
            "(context_intelligence_server/blob_processor.py) before shipping."
        )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _lift_raw_fields(data: dict[str, Any]) -> None:
    """Mutate *data* in-place to promote selected fields from ``raw`` before offloading.

    Lifted fields:
    - ``stop_reason``: copied to top-level if not already present.
    - ``finish_reason``: copied to top-level if not already present.
    - ``raw.usage``: merged into top-level ``usage``; existing top-level keys win
      on collision.

    Does nothing if ``raw`` is absent or not a dict.
    """
    raw = data.get("raw")
    if not isinstance(raw, dict):
        return

    # Promote stop_reason and finish_reason (only if not already set)
    for field in ("stop_reason", "finish_reason"):
        if field in raw and field not in data:
            data[field] = raw[field]

    # Merge raw.usage into top-level usage (existing keys win)
    raw_usage = raw.get("usage")
    if isinstance(raw_usage, dict):
        top_usage: dict[str, Any] = data.get("usage") or {}
        # New keys from raw_usage; existing top-level keys take precedence
        merged = {**raw_usage, **top_usage}
        data["usage"] = merged


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def process_event_data(
    data: dict[str, Any],
    blob_store: BlobStore,
    session_id: str,
    node_id: str,
) -> None:
    """Offload blob fields from *data* to *blob_store*, mutating *data* in-place.

    For each field in :data:`BLOB_FIELDS`:

    - If absent or ``None``, skip.
    - Otherwise write the value to *blob_store* with key ``{node_id}__{field_name}``.
    - On success, replace the field with ``{'$blob_ref': uri}``.
    - On failure, replace the field with ``{'$blob_error': 'write failed: <reason>'}``.

    :func:`_lift_raw_fields` is called first to promote ``stop_reason``,
    ``finish_reason``, and ``usage`` from ``raw`` before it is offloaded.

    Returns ``None``.
    """
    _lift_raw_fields(data)

    # WS-5 runtime tripwire: every ci-blob:// URI minted below lands in
    # `data`, which DefaultHandler always persists wholesale onto the Event
    # node's "data" property. Fail loud, BEFORE any blob is written, if that
    # destination is ever missing from the allowlist the reclaim scan reads
    # (see BLOB_REF_CARRIER_PROPERTIES above). Deliberately outside the
    # per-field try/except below so it is never downgraded to a swallowed
    # $blob_error -- it propagates out of process_event_data and dead-letters
    # the event instead of silently minting an unprotected blob reference.
    assert_carrier_registered("data")

    for field_name in BLOB_FIELDS:
        value = data.get(field_name)
        if value is None:
            # Absent or explicitly None — skip
            continue

        key = f"{node_id}__{field_name}"
        try:
            ref = await blob_store.write(session_id, key, value)
            data[field_name] = {"$blob_ref": ref.uri}
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "blob_offload_failed session=%s field=%s node=%s: %s",
                session_id,
                field_name,
                node_id,
                exc,
            )
            data[field_name] = {"$blob_error": f"write failed: {exc}"}
