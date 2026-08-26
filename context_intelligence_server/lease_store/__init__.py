"""Writer-lease persistence behind a backend-neutral Protocol."""

from __future__ import annotations

from context_intelligence_server.lease_store.factory import create_lease_store
from context_intelligence_server.lease_store.protocol import LeaseRecord, LeaseStore

__all__ = ["LeaseRecord", "LeaseStore", "create_lease_store"]
