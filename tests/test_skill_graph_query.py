"""Tests for context-intelligence-graph-query.md skill file — Sections 1 and 2.

TDD phase: These tests define what the rewritten skill file must contain.
They FAIL before the file is rewritten.
"""

import pathlib

SKILL_FILE = (
    pathlib.Path(__file__).parent.parent
    / "context_intelligence_server"
    / "skills"
    / "context-intelligence-graph-query.md"
)


def _content() -> str:
    return SKILL_FILE.read_text()


# ---------------------------------------------------------------------------
# YAML Frontmatter
# ---------------------------------------------------------------------------


def test_skill_file_exists() -> None:
    assert SKILL_FILE.exists(), "Skill file must exist"


def test_frontmatter_version_2() -> None:
    assert "version: 2.0.0" in _content(), (
        "Frontmatter must specify version: 2.0.0"
    )


def test_frontmatter_name() -> None:
    assert "name: context-intelligence-graph-query" in _content(), (
        "Frontmatter must have name: context-intelligence-graph-query"
    )


def test_frontmatter_description_mentions_property_graph() -> None:
    content = _content()
    assert "property graph" in content.lower(), (
        "Frontmatter description must mention 'property graph'"
    )


# ---------------------------------------------------------------------------
# Section 1 — Two Layers
# ---------------------------------------------------------------------------


def test_section_1_heading() -> None:
    assert "Section 1" in _content(), (
        "File must contain Section 1"
    )


def test_section_1_data_layer_1_explained() -> None:
    assert "Data layer 1" in _content(), (
        "Section 1 must explain Data layer 1"
    )


def test_section_1_data_layer_2_explained() -> None:
    assert "Data layer 2" in _content(), (
        "Section 1 must explain Data layer 2"
    )


def test_section_1_layer_identification_double_underscore() -> None:
    content = _content()
    assert "data layer 1" in content.lower(), (
        "Section 1 must mention data layer 1 identification"
    )
    assert "__" in content, (
        "Section 1 must mention __ (double underscore) as data layer 1 separator"
    )


def test_section_1_layer_identification_double_colon() -> None:
    content = _content()
    assert "::" in content, (
        "Section 1 must mention :: (double colon) as data layer 2 separator"
    )


def test_section_1_plain_ids_mentioned() -> None:
    content = _content()
    # ToolCall uses provider tool_call_id, Orchestrator uses name string
    assert "ToolCall" in content or "tool_call_id" in content, (
        "Section 1 must mention ToolCall using plain IDs"
    )
    assert "Orchestrator" in content, (
        "Section 1 must mention Orchestrator using plain IDs"
    )


# ---------------------------------------------------------------------------
# Section 2 — Schema Reference
# ---------------------------------------------------------------------------


def test_section_2_heading() -> None:
    assert "Section 2" in _content(), (
        "File must contain Section 2"
    )


def test_section_2_data_layer_1_nodes_table() -> None:
    content = _content()
    assert "Data Layer 1 Nodes" in content or "Data layer 1 Nodes" in content, (
        "Section 2 must have Data Layer 1 Nodes table"
    )
    assert ":Session" in content, "Data Layer 1 Nodes table must include Session"
    assert ":Event" in content, "Data Layer 1 Nodes table must include Event"


def test_section_2_data_layer_1_edges_table() -> None:
    content = _content()
    assert "Data Layer 1 Edges" in content or "Data layer 1 Edges" in content, (
        "Section 2 must have Data Layer 1 Edges table"
    )
    assert "HAS_FORK" in content, "Data Layer 1 Edges table must include HAS_FORK"
    assert "HAS_TOOL_CALL" in content, "Data Layer 1 Edges table must include HAS_TOOL_CALL"
    assert "HAS_EVENT" in content, "Data Layer 1 Edges table must include HAS_EVENT"


def test_section_2_data_layer_2_entity_types_table() -> None:
    content = _content()
    assert "Data Layer 2 Entity Types" in content or "Data layer 2 Entity Types" in content, (
        "Section 2 must have Data Layer 2 Entity Types table"
    )
    # All 10 entities must be present
    entities = [
        "OrchestratorRun",
        "Iteration",
        "ContentBlock",
        "ToolCall",
        "Prompt",
        "Cancellation",
        "ContextCompaction",
        "MountPlan",
        "Orchestrator",
    ]
    for entity in entities:
        assert entity in content, (
            f"Data Layer 2 Entity Types table must include {entity}"
        )


def test_section_2_data_layer_2_entity_sst_labels() -> None:
    content = _content()
    assert "SST_EVENT" in content, "Entity types table must include SST_EVENT label"
    assert "SST_THING" in content, "Entity types table must include SST_THING label"
    assert "SST_CONCEPT" in content, "Entity types table must include SST_CONCEPT label"


def test_section_2_data_layer_2_edge_types_table() -> None:
    content = _content()
    assert "Data Layer 2 Edge Types" in content or "Data layer 2 Edge Types" in content, (
        "Section 2 must have Data Layer 2 Edge Types table"
    )
    # Key edges must be present
    edges = [
        "HAS_EXECUTION",
        "FORKED",
        "HAS_ATTRIBUTE",
        "HAS_PART",
        "HAS_COMPACTION",
        "HAS_SUBSESSION",
        "CAUSED",
        "PARALLEL_EXECUTION",
        "TRIGGERS",
        "ENABLES",
    ]
    for edge in edges:
        assert edge in content, (
            f"Data Layer 2 Edge Types table must include {edge}"
        )


def test_section_2_edge_types_sst_semantic_property() -> None:
    content = _content()
    assert "sst_semantic" in content, (
        "Data Layer 2 Edge Types table must reference sst_semantic property"
    )
    assert "CONTAINS" in content, "sst_semantic values must include CONTAINS"
    assert "LEADS_TO" in content, "sst_semantic values must include LEADS_TO"
    assert "EXPRESSES" in content, "sst_semantic values must include EXPRESSES"
    assert "NEAR" in content, "sst_semantic values must include NEAR"


def test_data_blob_uri_mentioned() -> None:
    content = _content()
    assert "ci-blob://" in content, (
        "Skill must mention ci-blob:// URI references"
    )
    assert "JSON string" in content, (
        "Skill must note that 'data' is a JSON string, not a Cypher map"
    )
