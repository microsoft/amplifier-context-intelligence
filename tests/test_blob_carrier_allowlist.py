"""Tests for the WS-5 blob-carrier allowlist runtime tripwire.

Covers the single source of truth shared by the mint path
(``blob_processor.BLOB_REF_CARRIER_PROPERTIES`` /
``blob_processor.assert_carrier_registered``) and the reclaim-scan path
(``routers.admin._BLOB_REF_CARRIER_PROPERTIES`` / ``_BLOB_REF_SCAN_QUERY``):

1.  The allowlist is exactly the current 4-item tuple (regression lock).
2.  ``routers.admin`` imports the SAME object -- no local re-declaration to
    drift out of sync.
3.  The generated Cypher scan query references exactly the allowlist's
    properties -- no more, no less (mint/scan agreement, structurally).
4.  ``assert_carrier_registered`` is non-vacuous: it raises for an
    unregistered property and is a no-op for a registered one.
5.  ``process_event_data`` (the real mint call path) propagates the
    tripwire's exception -- fail-closed, BEFORE any blob is written -- when
    its destination carrier ("data") is not registered, and completes
    normally when it is.
"""

from __future__ import annotations

import re
from typing import Any
from unittest.mock import AsyncMock

import pytest
from context_intelligence_server.blob_processor import (
    BLOB_REF_CARRIER_PROPERTIES,
    UnregisteredBlobCarrierError,
    assert_carrier_registered,
    process_event_data,
)
from context_intelligence_server.routers.admin import (
    _BLOB_REF_CARRIER_PROPERTIES as admin_carrier_properties,
)
from context_intelligence_server.routers.admin import (
    _BLOB_REF_SCAN_QUERY,
)

# ---------------------------------------------------------------------------
# 1. Regression lock -- current 4-item allowlist
# ---------------------------------------------------------------------------


def test_carrier_properties_locked() -> None:
    """BLOB_REF_CARRIER_PROPERTIES is exactly the specified 4-item tuple."""
    assert BLOB_REF_CARRIER_PROPERTIES == ("data", "tool_input", "prompt", "response")


# ---------------------------------------------------------------------------
# 2. admin.py imports the SAME allowlist -- no local re-declaration
# ---------------------------------------------------------------------------


def test_admin_imports_same_allowlist_object() -> None:
    """routers.admin re-exports blob_processor's tuple by identity, not a
    hand-copied duplicate -- proves there is exactly ONE allowlist object."""
    assert admin_carrier_properties is BLOB_REF_CARRIER_PROPERTIES


# ---------------------------------------------------------------------------
# 3. Generated scan query references exactly the allowlist's properties
# ---------------------------------------------------------------------------


def test_scan_query_matches_allowlist_exactly() -> None:
    """The Cypher query built for the reclaim scan mentions exactly the
    properties in BLOB_REF_CARRIER_PROPERTIES -- no more, no less.

    This is the structural lock that makes mint/scan drift impossible: if a
    property is ever added to (or removed from) the allowlist without the
    query being regenerated from it, this test fails.
    """
    referenced_props = set(re.findall(r"n\.(\w+)", _BLOB_REF_SCAN_QUERY))
    assert referenced_props == set(BLOB_REF_CARRIER_PROPERTIES)


# ---------------------------------------------------------------------------
# 4. assert_carrier_registered -- non-vacuous tripwire
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("carrier", list(BLOB_REF_CARRIER_PROPERTIES))
def test_assert_carrier_registered_passes_for_registered_carrier(
    carrier: str,
) -> None:
    """A registered carrier (each of the current 4) does not trip the guard."""
    assert_carrier_registered(carrier)  # must not raise


def test_assert_carrier_registered_raises_for_unregistered_carrier() -> None:
    """An unregistered carrier property trips the guard immediately.

    Proves the tripwire is non-vacuous: it actually fires. Simulates the
    exact future scenario described in WS-5 -- a new field-lifter/enricher
    promoting a value onto a brand-new node property ("artifact_content")
    that nobody added to BLOB_REF_CARRIER_PROPERTIES.
    """
    with pytest.raises(UnregisteredBlobCarrierError, match="artifact_content"):
        assert_carrier_registered("artifact_content")


# ---------------------------------------------------------------------------
# 5. process_event_data -- the real mint call path
# ---------------------------------------------------------------------------


async def test_process_event_data_fails_closed_when_data_carrier_unregistered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If "data" is (hypothetically) removed from the allowlist, the mint
    call fails loud BEFORE writing any blob -- not after.

    Regression target: this is the exact failure mode WS-5 exists to catch
    -- a carrier property silently dropping out of the allowlist while the
    mint path keeps writing to it. blob_store.write must never be called:
    the guard fires before any blob is persisted, matching "fail loud at the
    source, not after a live blob is deleted."
    """
    import context_intelligence_server.blob_processor as blob_processor_module

    monkeypatch.setattr(
        blob_processor_module,
        "BLOB_REF_CARRIER_PROPERTIES",
        ("tool_input", "prompt", "response"),  # "data" removed
    )

    data: dict[str, Any] = {"result": {"answer": 42}}
    blob_store = AsyncMock()
    blob_store.write = AsyncMock(return_value="ci-blob://sess/node__result")

    with pytest.raises(UnregisteredBlobCarrierError, match="'data'"):
        await process_event_data(data, blob_store, "sess", "node")

    blob_store.write.assert_not_called()
    # data must be untouched -- the guard fired before any mutation/write
    assert data == {"result": {"answer": 42}}


async def test_process_event_data_succeeds_when_data_carrier_registered() -> None:
    """Sanity/non-regression: with the real (unmodified) allowlist, the mint
    path completes normally -- the guard does not false-trip on the
    ordinary, correctly-registered path."""
    data: dict[str, Any] = {"result": {"answer": 42}}
    blob_store = AsyncMock()
    blob_store.write = AsyncMock(return_value="ci-blob://sess/node__result")

    await process_event_data(data, blob_store, "sess", "node")

    assert data["result"] == {"$blob_ref": "ci-blob://sess/node__result"}
