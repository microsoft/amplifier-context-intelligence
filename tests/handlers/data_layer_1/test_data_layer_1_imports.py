"""Tests verifying data_layer_1 source files are at correct locations with proper imports.

These tests validate that after task-3 migration:
- DefaultHandler is importable from data_layer_1.default
- FieldLifter classes are importable from data_layer_1.field_lifters
- pipeline.py can import DefaultHandler from the new location
"""


def test_default_handler_importable_from_data_layer_1():
    """DefaultHandler must be importable from data_layer_1.default."""
    from context_intelligence_server.handlers.data_layer_1.default import (  # noqa: F401
        DefaultHandler,
    )


def test_field_lifter_importable_from_data_layer_1():
    """FieldLifter base class must be importable from data_layer_1.field_lifters."""
    from context_intelligence_server.handlers.data_layer_1.field_lifters import (  # noqa: F401
        FieldLifter,
    )


def test_field_lifter_subclasses_importable_from_data_layer_1():
    """All FieldLifter subclasses must be importable from data_layer_1.field_lifters."""
    from context_intelligence_server.handlers.data_layer_1.field_lifters import (  # noqa: F401
        ArtifactLifter,
        DelegateLifter,
        LlmLifter,
        PromptLifter,
        RecipeLifter,
        SessionLifter,
        SkillLifter,
        ToolLifter,
        UniversalLifter,
    )


def test_pipeline_setup_handlers_importable():
    """setup_handlers from pipeline.py must be importable (uses DefaultHandler from new path)."""
    from context_intelligence_server.pipeline import setup_handlers  # noqa: F401
