"""Tests for docs/architecture/README.md — the entry point for architecture docs.

Diagrams are rendered to PNG (the tracked artifact) and embedded in this README.
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


def test_readme_embeds_durable_ingest_queue_png() -> None:
    assert (
        "![Durable Ingest Queue & Drain Loop](./05-durable-ingest-queue.png)"
        in _content()
    ), "README must embed the Durable Ingest Queue PNG (the headline diagram)"


def test_readme_embeds_pipeline_flow_png() -> None:
    assert "![Pipeline Flow](./01-pipeline-flow.png)" in _content(), (
        "README must embed Pipeline Flow PNG"
    )


def test_readme_embeds_handler_architecture_png() -> None:
    assert "![Handler Architecture](./02-handler-architecture.png)" in _content(), (
        "README must embed Handler Architecture PNG"
    )


def test_readme_embeds_graph_model_png() -> None:
    assert "![Graph Model](./03-graph-model.png)" in _content(), (
        "README must embed Graph Model PNG"
    )


def test_readme_embeds_default_handler_flow_png() -> None:
    assert "![DefaultHandler Flow](./04-default-handler-flow.png)" in _content(), (
        "README must embed DefaultHandler Flow PNG"
    )


def test_readme_has_diagram_sections() -> None:
    content = _content()
    sections = [
        "Durable Ingest Queue",
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
        "05-durable-ingest-queue.dot",
    ]
    for dot_file in dot_files:
        assert dot_file in content, (
            f"README must reference source DOT file '{dot_file}'"
        )


def test_readme_has_regenerating_pngs_section() -> None:
    assert "Regenerating PNGs" in _content(), (
        "README must have a 'Regenerating PNGs' section"
    )


def test_readme_has_shell_loop_command() -> None:
    content = _content()
    assert "for f in docs/architecture/*.dot" in content, (
        "README must contain the shell loop command for regenerating PNGs"
    )
    assert 'dot -Tpng "$f"' in content, "README must contain the dot -Tpng command"
    assert '"${f%.dot}.png"' in content, (
        "README must contain the output substitution in the shell loop"
    )


def test_readme_has_png_lifecycle_note() -> None:
    content = _content()
    # The note states PNG files exist only to be embedded in this README
    assert "PNG" in content, "README must mention PNG files"
    # Check for some variant of the lifecycle note about PNGs being for embedding
    embedded_note = any(
        phrase in content
        for phrase in [
            "only to be embedded",
            "exist only to be embedded",
            "embedded in this README",
        ]
    )
    assert embedded_note, (
        "README must contain note that PNG files exist only to be embedded in this README"
    )
