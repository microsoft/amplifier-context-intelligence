"""Tests for package configuration and metadata."""

from context_intelligence_server import __version__


def test_package_version():
    """Package version should be 0.1.0."""
    assert __version__ == "0.1.0"
