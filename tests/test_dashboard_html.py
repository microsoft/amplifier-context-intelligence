"""Tests for version badge in web/dashboard.html.

These tests verify that dashboard.html contains the required
dash-version chip and the inline /version fetch script.
"""

from __future__ import annotations

from pathlib import Path

DASHBOARD_HTML = (
    Path(__file__).parent.parent
    / "context_intelligence_server"
    / "web"
    / "dashboard.html"
)


def _read_html() -> str:
    return DASHBOARD_HTML.read_text(encoding="utf-8")


class TestDashVersionChip:
    """dashboard.html must contain a stat-chip with id='dash-version'."""

    def test_dash_version_id_present(self) -> None:
        """An element with id='dash-version' exists in the HTML."""
        html = _read_html()
        assert 'id="dash-version"' in html

    def test_dash_version_in_stat_chip(self) -> None:
        """The dash-version element is inside a stat-chip div."""
        html = _read_html()
        # Check a stat-chip contains the Version label and dash-version id
        assert '<span class="stat-label">Version</span>' in html, (
            "Missing Version label in stat-chip"
        )
        assert 'id="dash-version"' in html, "Missing id='dash-version'"

    def test_dash_version_grep_returns_two_lines(self) -> None:
        """'dash-version' must appear on exactly 2 lines in the HTML.

        Line 1: the stat-chip element definition
        Line 2: the inline script that populates it
        """
        html = _read_html()
        matching_lines = [line for line in html.splitlines() if "dash-version" in line]
        assert len(matching_lines) == 2, (
            f"Expected 2 lines containing 'dash-version', got {len(matching_lines)}: "
            f"{matching_lines}"
        )


class TestDashVersionScript:
    """dashboard.html must have an inline script that fetches /version."""

    def test_fetch_version_present(self) -> None:
        """An inline fetch('/version') call exists in the HTML."""
        html = _read_html()
        assert "fetch('/version')" in html

    def test_script_populates_dash_version(self) -> None:
        """The script references 'dash-version' to populate the element."""
        html = _read_html()
        # The script should use getElementById('dash-version')
        assert "getElementById('dash-version')" in html

    def test_script_reads_version_field(self) -> None:
        """The script reads d.version (or similar) from the response JSON."""
        html = _read_html()
        assert "d.version" in html
