"""Tests for blob_processor — BLOB_FIELDS constant, _lift_raw_fields, process_event_data.

14 tests covering:
1.  BLOB_FIELDS constant is exactly the expected frozenset
2.  process_event_data mutates data in-place (same object identity)
3.  process_event_data returns None
4.  $blob_ref substitution on successful write
5.  $blob_error on failed write (no blocking)
6.  Absent fields are skipped (not added to data)
7.  None fields are skipped (not written to blob store)
8.  Blob key format is {node_id}__{field_name}
9.  _lift_raw_fields promotes stop_reason from raw to top-level
10. _lift_raw_fields promotes finish_reason from raw to top-level
11. _lift_raw_fields merges raw.usage into top-level usage
12. _lift_raw_fields handles missing raw gracefully (no-op)
13. _lift_raw_fields does not overwrite existing top-level stop_reason/finish_reason
14. _lift_raw_fields: existing top-level usage keys win on collision
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from context_intelligence_server.blob_processor import (
    BLOB_FIELDS,
    _lift_raw_fields,
    process_event_data,
)


# ---------------------------------------------------------------------------
# 1. BLOB_FIELDS constant
# ---------------------------------------------------------------------------


def test_blob_fields_constant() -> None:
    """BLOB_FIELDS is exactly the specified frozenset."""
    assert BLOB_FIELDS == frozenset(
        {"raw", "result", "messages", "mount_plan", "context_snapshot", "debug"}
    )
    assert isinstance(BLOB_FIELDS, frozenset)


# ---------------------------------------------------------------------------
# 2. In-place mutation verification
# ---------------------------------------------------------------------------


async def test_process_event_data_mutates_in_place() -> None:
    """process_event_data mutates the same dict object — no deepcopy."""
    data: dict[str, Any] = {"raw": {"stop_reason": "end_turn"}, "other": "value"}
    original_id = id(data)

    blob_store = AsyncMock()
    blob_store.write = AsyncMock(return_value="ci-blob://sess/node__raw")

    await process_event_data(data, blob_store, "sess", "node")

    # Same dict object must have been modified
    assert id(data) == original_id
    # The raw field must have been replaced (mutation happened)
    assert "$blob_ref" in data["raw"]


# ---------------------------------------------------------------------------
# 3. None return value
# ---------------------------------------------------------------------------


async def test_process_event_data_returns_none() -> None:
    """process_event_data returns None."""
    data: dict[str, Any] = {"result": {"answer": 42}}
    blob_store = AsyncMock()
    blob_store.write = AsyncMock(return_value="ci-blob://sess/node__result")

    result = await process_event_data(data, blob_store, "sess", "node")

    assert result is None


# ---------------------------------------------------------------------------
# 4. $blob_ref substitution on successful write
# ---------------------------------------------------------------------------


async def test_blob_ref_substitution_on_successful_write() -> None:
    """Fields present and non-None are replaced with {'$blob_ref': uri} on success."""
    data: dict[str, Any] = {
        "raw": {"event": "tool_call"},
        "result": {"answer": 42},
    }
    blob_store = AsyncMock()

    # Use a function-based side_effect so the returned URI always matches
    # the actual key argument, regardless of BLOB_FIELDS frozenset iteration order.
    async def _write(session_id: str, key: str, value: object) -> str:
        return f"ci-blob://{session_id}/{key}"

    blob_store.write = AsyncMock(side_effect=_write)

    await process_event_data(data, blob_store, "sess", "node")

    assert data["raw"] == {"$blob_ref": "ci-blob://sess/node__raw"}
    assert data["result"] == {"$blob_ref": "ci-blob://sess/node__result"}


# ---------------------------------------------------------------------------
# 5. $blob_error on failed write (no blocking)
# ---------------------------------------------------------------------------


async def test_blob_error_on_failed_write() -> None:
    """Failed writes produce {'$blob_error': 'write failed: <reason>'} without blocking."""
    data: dict[str, Any] = {"messages": [{"role": "user", "content": "hi"}]}
    blob_store = AsyncMock()
    blob_store.write = AsyncMock(side_effect=OSError("disk full"))

    await process_event_data(data, blob_store, "sess", "node")

    assert "$blob_error" in data["messages"]
    assert "write failed:" in data["messages"]["$blob_error"]
    assert "disk full" in data["messages"]["$blob_error"]


# ---------------------------------------------------------------------------
# 6. Absent fields are skipped
# ---------------------------------------------------------------------------


async def test_absent_fields_are_skipped() -> None:
    """Fields in BLOB_FIELDS that are absent from data are not added."""
    data: dict[str, Any] = {"other_field": "untouched"}
    blob_store = AsyncMock()
    blob_store.write = AsyncMock(return_value="ci-blob://sess/node__something")

    await process_event_data(data, blob_store, "sess", "node")

    # No BLOB_FIELDS keys should have been added
    for field in BLOB_FIELDS:
        assert field not in data
    # blob_store.write should never have been called
    blob_store.write.assert_not_called()


# ---------------------------------------------------------------------------
# 7. None fields are skipped
# ---------------------------------------------------------------------------


async def test_none_fields_are_skipped() -> None:
    """Fields present in data but set to None are skipped without writing to blob store."""
    data: dict[str, Any] = {
        "raw": None,
        "result": None,
        "messages": None,
    }
    blob_store = AsyncMock()
    blob_store.write = AsyncMock(return_value="ci-blob://sess/node__x")

    await process_event_data(data, blob_store, "sess", "node")

    # None fields must remain None (not replaced)
    assert data["raw"] is None
    assert data["result"] is None
    assert data["messages"] is None
    blob_store.write.assert_not_called()


# ---------------------------------------------------------------------------
# 8. Blob key format is {node_id}__{field_name}
# ---------------------------------------------------------------------------


async def test_blob_key_format() -> None:
    """write() is called with key '{node_id}__{field_name}'."""
    data: dict[str, Any] = {
        "raw": {"x": 1},
        "debug": {"trace": "verbose"},
    }
    blob_store = AsyncMock()
    blob_store.write = AsyncMock(
        side_effect=[
            "ci-blob://my-session/my-node__raw",
            "ci-blob://my-session/my-node__debug",
        ]
    )

    await process_event_data(data, blob_store, "my-session", "my-node")

    # Collect all keys used in write calls
    call_keys = [call.args[1] for call in blob_store.write.call_args_list]
    assert "my-node__raw" in call_keys
    assert "my-node__debug" in call_keys


# ---------------------------------------------------------------------------
# 9. _lift_raw_fields promotes stop_reason from raw to top-level
# ---------------------------------------------------------------------------


def test_lift_raw_fields_promotes_stop_reason() -> None:
    """_lift_raw_fields lifts stop_reason from raw to top-level when not set."""
    data: dict[str, Any] = {"raw": {"stop_reason": "end_turn", "other": "x"}}
    _lift_raw_fields(data)
    assert data["stop_reason"] == "end_turn"


# ---------------------------------------------------------------------------
# 10. _lift_raw_fields promotes finish_reason from raw to top-level
# ---------------------------------------------------------------------------


def test_lift_raw_fields_promotes_finish_reason() -> None:
    """_lift_raw_fields lifts finish_reason from raw to top-level when not set."""
    data: dict[str, Any] = {"raw": {"finish_reason": "stop", "other": "x"}}
    _lift_raw_fields(data)
    assert data["finish_reason"] == "stop"


# ---------------------------------------------------------------------------
# 11. _lift_raw_fields merges raw.usage into top-level usage
# ---------------------------------------------------------------------------


def test_lift_raw_fields_merges_usage() -> None:
    """_lift_raw_fields merges raw.usage into top-level usage dict."""
    data: dict[str, Any] = {
        "raw": {"usage": {"input_tokens": 10, "output_tokens": 5}},
    }
    _lift_raw_fields(data)
    assert data["usage"] == {"input_tokens": 10, "output_tokens": 5}


# ---------------------------------------------------------------------------
# 12. _lift_raw_fields handles missing raw gracefully (no-op)
# ---------------------------------------------------------------------------


def test_lift_raw_fields_no_raw_is_noop() -> None:
    """_lift_raw_fields does nothing when raw is absent."""
    data: dict[str, Any] = {"result": {"answer": 42}}
    _lift_raw_fields(data)
    # data unchanged
    assert data == {"result": {"answer": 42}}


# ---------------------------------------------------------------------------
# 13. _lift_raw_fields does not overwrite existing top-level stop_reason/finish_reason
# ---------------------------------------------------------------------------


def test_lift_raw_fields_does_not_overwrite_existing() -> None:
    """_lift_raw_fields skips stop_reason/finish_reason if already set at top-level."""
    data: dict[str, Any] = {
        "stop_reason": "already_set",
        "finish_reason": "already_set",
        "raw": {"stop_reason": "new_value", "finish_reason": "new_value"},
    }
    _lift_raw_fields(data)
    assert data["stop_reason"] == "already_set"
    assert data["finish_reason"] == "already_set"


# ---------------------------------------------------------------------------
# 14. _lift_raw_fields: existing top-level usage keys win on collision
# ---------------------------------------------------------------------------


def test_lift_raw_fields_usage_existing_keys_win() -> None:
    """When merging raw.usage into top-level usage, existing top-level keys win."""
    data: dict[str, Any] = {
        "usage": {"input_tokens": 99, "cache_tokens": 7},
        "raw": {"usage": {"input_tokens": 10, "output_tokens": 5}},
    }
    _lift_raw_fields(data)
    # input_tokens was already set — existing wins
    assert data["usage"]["input_tokens"] == 99
    # output_tokens was new from raw
    assert data["usage"]["output_tokens"] == 5
    # cache_tokens was only in existing
    assert data["usage"]["cache_tokens"] == 7


# ---------------------------------------------------------------------------
# 15. blob write failure emits WARNING log
# ---------------------------------------------------------------------------


async def test_blob_write_failure_logs_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Failed blob writes must emit a WARNING log with session_id, field_name, and node_id."""
    import logging

    data: dict[str, Any] = {"messages": [{"role": "user", "content": "hi"}]}
    blob_store = AsyncMock()
    blob_store.write = AsyncMock(side_effect=OSError("disk full"))
    with caplog.at_level(logging.WARNING):
        await process_event_data(data, blob_store, "sess-abc", "node-xyz")
    assert "blob_offload_failed" in caplog.text
    assert "sess-abc" in caplog.text
    assert "messages" in caplog.text
    assert "node-xyz" in caplog.text
    assert "disk full" in caplog.text
