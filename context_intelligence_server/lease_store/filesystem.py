"""Filesystem-backed writer-lease store.

The lease is one atomically-replaced ``.writer.lease`` file in a directory
resolved lazily via *dir_source* -- resolved per operation (a cheap attribute
read, zero syscalls) so the store, like the detector it serves, constructs
nothing at build time and reflects a directory the tests may re-point.
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from pathlib import Path

from context_intelligence_server.lease_store.protocol import LeaseRecord

LEASE_FILENAME = ".writer.lease"
LEASE_TMP_FILENAME = ".writer.lease.tmp"
_LEASE_VERSION = 1


class FileSystemLeaseStore:
    """A ``LeaseStore`` backed by a single atomically-written file on disk."""

    def __init__(self, dir_source: Callable[[], Path]) -> None:
        self._dir_source = dir_source

    def _path(self) -> Path:
        return self._dir_source() / LEASE_FILENAME

    def read(self) -> LeaseRecord | None:
        try:
            text = self._path().read_text(encoding="utf-8")
        except FileNotFoundError:
            # A missing lease means "free directory", not a share fault.
            return None
        try:
            data = json.loads(text.strip())
            return LeaseRecord(
                owner=str(data["owner"]),
                host=str(data.get("host", "")),
                pid=int(data.get("pid", 0)),
                started_at=float(data.get("started_at", 0.0)),
                heartbeat=float(data["heartbeat"]),
                revision=data.get("revision"),
                server_version=str(data.get("server_version", "")),
                lease_version=int(data.get("lease_version", -1)),
            )
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            # Torn/malformed lease is treated as fresh-and-foreign, same
            # strength as a genuine live peer.
            return LeaseRecord(
                owner="",
                host="",
                pid=0,
                started_at=0.0,
                heartbeat=0.0,
                revision=None,
                server_version="",
                lease_version=-1,
                unreadable=True,
            )

    def write(self, record: LeaseRecord) -> None:
        directory = self._dir_source()
        payload = {
            "lease_version": record.lease_version,
            "owner": record.owner,
            "host": record.host,
            "pid": record.pid,
            "started_at": record.started_at,
            "heartbeat": record.heartbeat,
            "revision": record.revision,
            "server_version": record.server_version,
        }
        tmp = directory / LEASE_TMP_FILENAME
        tmp.write_text(
            json.dumps(payload, separators=(",", ":")) + "\n", encoding="utf-8"
        )
        os.replace(tmp, directory / LEASE_FILENAME)

    def delete_if_owned(self, owner: str) -> None:
        rec = self.read()
        if rec is not None and not rec.unreadable and rec.owner == owner:
            try:
                self._path().unlink()
            except FileNotFoundError:
                pass
