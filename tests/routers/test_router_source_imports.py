"""Tests that verify router source files live in context_intelligence_server/routers/.

These tests verify task-9: version.py moved from handlers/ to routers/.
"""

from __future__ import annotations


class TestRouterSourceImports:
    """Router source files must be importable from the routers package."""

    def test_version_router_importable_from_routers(self) -> None:
        """version router must be importable from context_intelligence_server.routers.version."""
        from context_intelligence_server.routers.version import router  # noqa: F401

        assert router is not None


class TestMainImportsFromRouters:
    """main.py must import routers from the routers package, not handlers."""

    def test_main_imports_version_router_from_routers(self) -> None:
        """main.py must use routers.version for version_router (not handlers.version)."""
        import ast
        from pathlib import Path

        main_src = (
            Path(__file__).parent.parent.parent
            / "context_intelligence_server"
            / "main.py"
        ).read_text(encoding="utf-8")
        tree = ast.parse(main_src)

        # Collect all imports aliased as version_router
        version_router_imports = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
            and node.names
            and any(alias.asname == "version_router" for alias in node.names)
        ]
        assert version_router_imports, "version_router not imported in main.py"
        for imp in version_router_imports:
            assert imp.module == "context_intelligence_server.routers.version", (
                f"version_router imported from '{imp.module}' but expected "
                "'context_intelligence_server.routers.version'"
            )
