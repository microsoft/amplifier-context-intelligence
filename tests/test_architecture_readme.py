"""Tests for docs/architecture/README.md — the entry point for architecture docs.

TDD phase: These tests FAIL before the file is created.
"""

import pathlib

# Project root
PROJECT_ROOT = pathlib.Path(__file__).parent.parent
README = PROJECT_ROOT / "docs" / "architecture" / "README.md"


def test_readme_exists() -> None:
    assert README.exists(), "docs/architecture/README.md must exist"


def _content() -> str:
    return README.read_text()


def test_readme_has_title() -> None:
    assert "# Architecture Diagrams" in _content(), (
        "README must have title '# Architecture Diagrams'"
    )


def test_readme_embeds_pipeline_flow_svg() -> None:
    assert "![Pipeline Flow](./01-pipeline-flow.svg)" in _content(), (
        "README must embed Pipeline Flow SVG"
    )


def test_readme_embeds_handler_architecture_svg() -> None:
    assert "![Handler Architecture](./02-handler-architecture.svg)" in _content(), (
        "README must embed Handler Architecture SVG"
    )


def test_readme_embeds_graph_model_svg() -> None:
    assert "![Graph Model](./03-graph-model.svg)" in _content(), (
        "README must embed Graph Model SVG"
    )


def test_readme_embeds_default_handler_flow_svg() -> None:
    assert "![DefaultHandler Flow](./04-default-handler-flow.svg)" in _content(), (
        "README must embed DefaultHandler Flow SVG"
    )


def test_readme_has_four_diagram_sections() -> None:
    content = _content()
    sections = [
        "Pipeline Flow",
        "Handler Architecture",
        "Graph Model",
        "DefaultHandler Flow",
    ]
    for section in sections:
        assert section in content, f"README must contain section '{section}'"


def test_readme_references_dot_source_files() -> None:
    content = _content()
    dot_files = [
        "01-pipeline-flow.dot",
        "02-handler-architecture.dot",
        "03-graph-model.dot",
        "04-default-handler-flow.dot",
    ]
    for dot_file in dot_files:
        assert dot_file in content, (
            f"README must reference source DOT file '{dot_file}'"
        )


def test_readme_has_regenerating_svgs_section() -> None:
    assert "Regenerating SVGs" in _content(), (
        "README must have a 'Regenerating SVGs' section"
    )


def test_readme_has_shell_loop_command() -> None:
    content = _content()
    assert "for f in docs/architecture/*.dot" in content, (
        "README must contain the shell loop command for regenerating SVGs"
    )
    assert 'dot -Tsvg "$f"' in content, "README must contain the dot -Tsvg command"
    assert '"${f%.dot}.svg"' in content, (
        "README must contain the output substitution in the shell loop"
    )


def test_readme_has_svg_lifecycle_note() -> None:
    content = _content()
    # The note states SVG files exist only to be embedded in this README
    assert "SVG" in content, "README must mention SVG files"
    # Check for some variant of the lifecycle note about SVGs being for embedding
    embedded_note = any(
        phrase in content
        for phrase in [
            "only to be embedded",
            "exist only to be embedded",
            "embedded in this README",
        ]
    )
    assert embedded_note, (
        "README must contain note that SVG files exist only to be embedded in this README"
    )
