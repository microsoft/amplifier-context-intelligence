"""Regression test: touch_session must NOT write the shared-root ancestor node.

The shared-root write was the deadlock hot spot — every child event walked the
parent_id chain and SET last_updated on the one shared :Session root node, so
many independent writers contended on that single node's exclusive lock.  The
fix updates only the direct session node, never the ancestor/root chain.
"""

from __future__ import annotations

from typing import Any

from context_intelligence_server.services import HookStateService


class FakeGraph:
    """Minimal async graph store that records which nodes get upserted."""

    def __init__(self, nodes: dict[str, dict[str, Any]]) -> None:
        self.nodes = nodes
        self.touched: list[str] = []
        self.workspace = "test"

    async def get_node(self, node_id: str) -> dict[str, Any] | None:
        return self.nodes.get(node_id)

    async def upsert_node(self, node_id: str, data: dict[str, Any]) -> None:
        self.touched.append(node_id)
        self.nodes.setdefault(node_id, {}).update(data)


async def test_touch_session_updates_only_direct_node() -> None:
    """Touching a child must update only the child, never the shared root."""
    graph = FakeGraph(
        {
            "c": {
                "labels": ["Session"],
                "session_id": "c",
                "parent_id": "root",
                "last_updated": "2026-06-11T00:00:00+00:00",
            },
            "root": {
                "labels": ["Session"],
                "session_id": "root",
                "last_updated": "2026-06-11T00:00:00+00:00",
            },
        }
    )
    services = HookStateService(workspace="test", graph_store=graph)

    await services.touch_session("c", "2026-06-11T12:00:00+00:00")

    assert "root" not in graph.touched
    assert graph.touched == ["c"]
