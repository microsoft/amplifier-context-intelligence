"""blob_processor — In-Place Transform for event data blob offloading.

Identifies large blob fields in event data, writes them to the blob store,
and replaces the field value with a ``ci-blob://`` URI reference in-place.

No deepcopy is performed — the caller (server) owns the deserialized JSON
object exclusively, so in-place mutation is safe and avoids extra allocation.
"""

from __future__ import annotations

import logging
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

    for field_name in BLOB_FIELDS:
        value = data.get(field_name)
        if value is None:
            # Absent or explicitly None — skip
            continue

        key = f"{node_id}__{field_name}"
        try:
            uri = await blob_store.write(session_id, key, value)
            data[field_name] = {"$blob_ref": uri}
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "blob_offload_failed session=%s field=%s node=%s: %s",
                session_id, field_name, node_id, exc,
            )
            data[field_name] = {"$blob_error": f"write failed: {exc}"}
