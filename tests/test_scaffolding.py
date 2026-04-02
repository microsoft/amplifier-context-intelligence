"""Tests for directory scaffolding — verifies new sub-packages exist with correct docstrings."""

import importlib


def test_data_layer_1_package_importable():
    """data_layer_1 package can be imported."""
    mod = importlib.import_module("context_intelligence_server.handlers.data_layer_1")
    assert mod is not None


def test_data_layer_1_package_docstring():
    """data_layer_1 __init__.py has the expected docstring."""
    mod = importlib.import_module("context_intelligence_server.handlers.data_layer_1")
    assert mod.__doc__ is not None
    assert "Data layer 1" in mod.__doc__
    assert "raw event capture handlers" in mod.__doc__
    assert "DefaultHandler" in mod.__doc__
    assert "FieldLifter" in mod.__doc__


def test_data_layer_2_package_importable():
    """data_layer_2 package can be imported."""
    mod = importlib.import_module("context_intelligence_server.handlers.data_layer_2")
    assert mod is not None


def test_data_layer_2_package_docstring():
    """data_layer_2 __init__.py has the expected docstring."""
    mod = importlib.import_module("context_intelligence_server.handlers.data_layer_2")
    assert mod.__doc__ is not None
    assert "Data layer 2" in mod.__doc__
    assert "semantic enrichment handlers" in mod.__doc__
    assert "SessionHandler" in mod.__doc__
    assert "ToolCallHandler" in mod.__doc__


def test_routers_package_importable():
    """routers package can be imported."""
    mod = importlib.import_module("context_intelligence_server.routers")
    assert mod is not None


def test_routers_package_docstring():
    """routers __init__.py has the expected docstring."""
    mod = importlib.import_module("context_intelligence_server.routers")
    assert mod.__doc__ is not None
    assert "FastAPI" in mod.__doc__
    assert "HTTP endpoint" in mod.__doc__


def test_test_data_layer_1_pkg_exists():
    """tests/handlers/data_layer_1/__init__.py exists."""
    import os

    path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "tests",
        "handlers",
        "data_layer_1",
        "__init__.py",
    )
    # __file__ is already in tests/, so adjust:
    path = os.path.join(
        os.path.dirname(__file__), "handlers", "data_layer_1", "__init__.py"
    )
    assert os.path.exists(path), f"Expected {path} to exist"


def test_test_data_layer_2_pkg_exists():
    """tests/handlers/data_layer_2/__init__.py exists."""
    import os

    path = os.path.join(
        os.path.dirname(__file__), "handlers", "data_layer_2", "__init__.py"
    )
    assert os.path.exists(path), f"Expected {path} to exist"


def test_test_routers_pkg_exists():
    """tests/routers/__init__.py exists."""
    import os

    path = os.path.join(os.path.dirname(__file__), "routers", "__init__.py")
    assert os.path.exists(path), f"Expected {path} to exist"
