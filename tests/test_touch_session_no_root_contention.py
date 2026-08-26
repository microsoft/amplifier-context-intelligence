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
    """Minimal async graph store that records which nodes get upserted.

    Conforms fully to the ``GraphStore`` Protocol: the ``graph_store``
    constructor parameter is typed as ``GraphStore | None``, so a fake
    passed to it must structurally satisfy the Protocol even though this
    test only exercises get_node/upsert_node. The extra members are
    no-ops -- this test's behavior is unchanged.
    """

    def __init__(self, nodes: dict[str, dict[str, Any]]) -> None:
        self.nodes = nodes
        self.touched: list[str] = []
        self.workspace = "test"
        self.created_by: str | None = None

    async def get_node(self, node_id: str) -> dict[str, Any] | None:
        return self.nodes.get(node_id)

    async def upsert_node(self, node_id: str, data: dict[str, Any]) -> None:
        self.touched.append(node_id)
        self.nodes.setdefault(node_id, {}).update(data)

    async def upsert_edge(self, src_id: str, dst_id: str, data: dict[str, Any]) -> None:
        pass

    async def get_edge(self, src_id: str, dst_id: str) -> dict[str, Any] | None:
        return None

    async def find_delegation_by_sub_session(
        self, sub_session_id: str, workspace: str
    ) -> dict[str, Any] | None:
        return None

    def discard_buffer(self) -> None:
        pass

    async def flush(self) -> None:
        pass

    async def close(self) -> None:
        pass


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
