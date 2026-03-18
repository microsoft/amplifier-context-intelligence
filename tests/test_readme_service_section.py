"""Tests for the 'Running as a System Service' section in README.md.

Validates that README.md:
- Contains exactly one 'Running as a System Service' section
- Links to docs/service-setup.md
- Is positioned between 'Running Without Docker' and 'Feeding Events into the Server'
"""

from pathlib import Path

# tests/ -> amplifier-context-intelligence/ -> repo root
SUBMODULE_ROOT = Path(__file__).parent.parent
README_PATH = SUBMODULE_ROOT / "README.md"


def _content() -> str:
    """Return README.md content."""
    return README_PATH.read_text()


def _lines() -> list[str]:
    """Return README.md lines (0-indexed)."""
    return _content().splitlines()


# ---------------------------------------------------------------------------
# Existence checks
# ---------------------------------------------------------------------------


def test_readme_exists():
    """README.md must exist."""
    assert README_PATH.exists(), f"File not found: {README_PATH}"


# ---------------------------------------------------------------------------
# Section presence
# ---------------------------------------------------------------------------


def test_system_service_section_present():
    """README.md must contain a 'Running as a System Service' section heading."""
    assert "## Running as a System Service" in _content(), (
        "Section '## Running as a System Service' not found in README.md"
    )


def test_system_service_section_appears_exactly_once():
    """'Running as a System Service' heading must appear exactly once."""
    count = _content().count("## Running as a System Service")
    assert count == 1, (
        f"'## Running as a System Service' appears {count} time(s); expected exactly 1"
    )


# ---------------------------------------------------------------------------
# Link to service-setup.md
# ---------------------------------------------------------------------------


def test_system_service_section_links_to_service_setup():
    """The section must contain a relative link to docs/service-setup.md."""
    assert "docs/service-setup.md" in _content(), (
        "No relative link to docs/service-setup.md found in README.md"
    )


# ---------------------------------------------------------------------------
# Positioning checks
# ---------------------------------------------------------------------------


def test_system_service_section_after_running_without_docker():
    """'Running as a System Service' must appear after 'Running Without Docker'."""
    content = _content()
    pos_without_docker = content.find("## Running Without Docker")
    pos_service = content.find("## Running as a System Service")
    assert pos_without_docker != -1, "'## Running Without Docker' not found"
    assert pos_service != -1, "'## Running as a System Service' not found"
    assert pos_service > pos_without_docker, (
        "'Running as a System Service' must appear after 'Running Without Docker'"
    )


def test_system_service_section_before_feeding_events():
    """'Running as a System Service' must appear before 'Feeding Events into the Server'."""
    content = _content()
    pos_service = content.find("## Running as a System Service")
    pos_feeding = content.find("## Feeding Events into the Server")
    assert pos_service != -1, "'## Running as a System Service' not found"
    assert pos_feeding != -1, "'## Feeding Events into the Server' not found"
    assert pos_service < pos_feeding, (
        "'Running as a System Service' must appear before 'Feeding Events into the Server'"
    )


def test_localhost_confirm_before_service_section():
    """The localhost confirmation line must appear before 'Running as a System Service'."""
    content = _content()
    pos_localhost = content.find(
        "Open [http://localhost:8000](http://localhost:8000) to confirm the server is running."
    )
    pos_service = content.find("## Running as a System Service")
    assert pos_localhost != -1, "localhost confirmation line not found"
    assert pos_service != -1, "'## Running as a System Service' not found"
    assert pos_localhost < pos_service, (
        "localhost confirmation line must appear before 'Running as a System Service'"
    )
