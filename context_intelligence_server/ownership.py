"""Ownership edge integrity checker.

Enforces single-parent semantics on ownership edge types (HAS_RUN, HAS_STEP,
TRIGGERED).  Each destination node may have at most one owner edge of a given
type; a second owner evicts the first with a last-writer-wins strategy.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Ownership edge types
# ---------------------------------------------------------------------------

OWNERSHIP_EDGE_TYPES: frozenset[str] = frozenset({"HAS_RUN", "HAS_STEP", "TRIGGERED"})


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _find_owner_in_buffer(graph: Any, dst_id: str, edge_type: str) -> str | None:
    """Return the src_id of an existing ownership edge of *edge_type* → *dst_id*.

    Scans the in-memory edge buffer of *graph*, which may be either:
    - ``GraphState``     — exposes ``_edges: dict[tuple[str, str], dict]``
    - ``Neo4jGraphStore``— exposes ``_edge_buffer: dict[tuple, dict]``

    Returns the src_id string if found, or ``None`` when no matching edge exists.
    """
    # Support both GraphState (_edges) and Neo4jGraphStore (_edge_buffer)
    if hasattr(graph, "_edges"):
        buffer = graph._edges
    elif hasattr(graph, "_edge_buffer"):
        buffer = graph._edge_buffer
    else:
        return None

    for (src_id, d_id), data in buffer.items():
        if d_id == dst_id and data.get("type") == edge_type:
            return src_id

    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def check_ownership(
    graph: Any,
    dst_id: str,
    edge_type: str,
    new_src_id: str,
) -> bool:
    """Enforce single-parent semantics before upserting an ownership edge.

    Only acts on edge types in ``OWNERSHIP_EDGE_TYPES``; all others pass
    through immediately.

    Algorithm (last-writer-wins):
    1. If *edge_type* is not an ownership edge type → return True.
    2. Scan the graph buffer for an existing edge of *edge_type* → *dst_id*.
    3. If no existing edge → return True (safe to proceed with upsert).
    4. If existing edge has the same source as *new_src_id* → return True
       (idempotent; upsert will be a no-op or update in place).
    5. If existing edge has a *different* source:
       - Remove the old edge via ``graph.remove_edge()``.
       - Log a WARNING about the ownership mutation.
       - Return True (caller may proceed to upsert the new edge).

    Args:
        graph:      The graph store instance (GraphState or Neo4jGraphStore).
        dst_id:     The destination node ID of the edge being upserted.
        edge_type:  The relationship type (e.g. ``"HAS_RUN"``).
        new_src_id: The source node ID that will own *dst_id* after the upsert.

    Returns:
        Always ``True`` — ownership checks are advisory; the caller proceeds
        regardless.  The return value is kept for future extensibility.
    """
    if edge_type not in OWNERSHIP_EDGE_TYPES:
        return True

    existing_src = _find_owner_in_buffer(graph, dst_id, edge_type)

    if existing_src is None:
        # No existing owner — safe to proceed
        return True

    if existing_src == new_src_id:
        # Same owner — idempotent upsert, no action needed
        return True

    # Different owner — evict old edge with last-writer-wins
    graph.remove_edge(existing_src, dst_id)
    logger.warning(
        "Ownership mutation: %s edge to %s reassigned from %s to %s",
        edge_type,
        dst_id,
        existing_src,
        new_src_id,
    )
    return True
