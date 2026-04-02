"""Tests that verify router source files live in context_intelligence_server/routers/.

These tests verify task-9: skills.py and version.py moved from handlers/ to routers/.
"""

from __future__ import annotations


class TestRouterSourceImports:
    """Router source files must be importable from the routers package."""

    def test_skill_registry_importable_from_routers(self) -> None:
        """SkillRegistry must be importable from context_intelligence_server.routers.skills."""
        from context_intelligence_server.routers.skills import SkillRegistry  # noqa: F401

        assert SkillRegistry is not None

    def test_skills_router_importable_from_routers(self) -> None:
        """skills router must be importable from context_intelligence_server.routers.skills."""
        from context_intelligence_server.routers.skills import router  # noqa: F401

        assert router is not None

    def test_version_router_importable_from_routers(self) -> None:
        """version router must be importable from context_intelligence_server.routers.version."""
        from context_intelligence_server.routers.version import router  # noqa: F401

        assert router is not None


class TestMainImportsFromRouters:
    """main.py must import SkillRegistry and routers from the routers package, not handlers."""

    def test_main_imports_skill_registry_from_routers(self) -> None:
        """main.py must use routers.skills for SkillRegistry (not handlers.skills)."""
        import ast
        from pathlib import Path

        main_src = (
            Path(__file__).parent.parent.parent
            / "context_intelligence_server"
            / "main.py"
        ).read_text(encoding="utf-8")
        tree = ast.parse(main_src)

        # Collect all imports of SkillRegistry
        skill_registry_imports = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
            and node.names
            and any(alias.name == "SkillRegistry" for alias in node.names)
        ]
        assert skill_registry_imports, "SkillRegistry not imported in main.py"
        for imp in skill_registry_imports:
            assert imp.module == "context_intelligence_server.routers.skills", (
                f"SkillRegistry imported from '{imp.module}' but expected "
                "'context_intelligence_server.routers.skills'"
            )

    def test_main_imports_skills_router_from_routers(self) -> None:
        """main.py must use routers.skills for skills_router (not handlers.skills)."""
        import ast
        from pathlib import Path

        main_src = (
            Path(__file__).parent.parent.parent
            / "context_intelligence_server"
            / "main.py"
        ).read_text(encoding="utf-8")
        tree = ast.parse(main_src)

        # Collect all imports aliased as skills_router
        skills_router_imports = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
            and node.names
            and any(alias.asname == "skills_router" for alias in node.names)
        ]
        assert skills_router_imports, "skills_router not imported in main.py"
        for imp in skills_router_imports:
            assert imp.module == "context_intelligence_server.routers.skills", (
                f"skills_router imported from '{imp.module}' but expected "
                "'context_intelligence_server.routers.skills'"
            )

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
