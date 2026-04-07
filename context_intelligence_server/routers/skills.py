"""SkillRegistry — load skill files and compute ETags."""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path

from fastapi import APIRouter
from fastapi.requests import Request
from fastapi.responses import Response

logger = logging.getLogger(__name__)

router = APIRouter()


class SkillRegistry:
    """Registry of skill definitions loaded from a directory of skill packages.

    Each skill lives in its own subdirectory containing a ``SKILL.md`` file.
    The skill is keyed by the subdirectory name. An ETag (SHA-256 hex digest)
    is computed per file for cache-validation support.
    """

    def __init__(self) -> None:
        self._content: dict[str, str] = {}
        self._etags: dict[str, str] = {}

    def load_from_dir(self, skills_dir: Path) -> None:
        """Load all ``<skill-name>/SKILL.md`` packages from *skills_dir*.

        Skills are discovered using the pattern ``*/SKILL.md`` so that each skill
        must live in its own subdirectory.  Packages are processed in sorted order.
        Each file's content is stored keyed by the parent directory name (the skill
        name); the corresponding ETag is the SHA-256 hex digest of the UTF-8
        encoded content.

        Args:
            skills_dir: Directory to scan for ``<skill-name>/SKILL.md`` packages.
        """
        for skill_path in sorted(skills_dir.glob("*/SKILL.md")):
            content = skill_path.read_text(encoding="utf-8")
            etag = hashlib.sha256(content.encode("utf-8")).hexdigest()
            stem = skill_path.parent.name  # directory name = skill name
            self._content[stem] = content
            self._etags[stem] = etag

        logger.info(
            "SkillRegistry: loaded %d skill(s) from %s", len(self._content), skills_dir
        )

    def get(self, skill_name: str) -> tuple[str, str] | None:
        """Return ``(content, etag)`` for *skill_name*, or ``None`` if not found."""
        content = self._content.get(skill_name)
        if content is None:
            return None
        return content, self._etags[skill_name]

    @property
    def skill_names(self) -> frozenset[str]:
        """Return a frozenset of all registered skill names."""
        return frozenset(self._content)


@router.get("/skills/{skill_name}")
async def get_skill(skill_name: str, request: Request) -> Response:
    """Return skill content as Markdown with an ETag for cache validation.

    This endpoint is intentionally unauthenticated.

    Returns:
        200: Skill content with ``Content-Type: text/markdown`` and ``ETag`` header.
        304: When ``If-None-Match`` matches the current ETag (not modified).
        404: When the skill is not found in the registry.
    """
    skill_registry = request.app.state.skill_registry
    result = skill_registry.get(skill_name)
    if result is None:
        return Response(status_code=404, content=f"Skill not found: {skill_name}")

    content, etag = result

    if_none_match = request.headers.get("if-none-match")
    if if_none_match == etag:
        return Response(status_code=304)

    return Response(
        content=content,
        media_type="text/markdown; charset=utf-8",
        headers={"ETag": etag},
    )
