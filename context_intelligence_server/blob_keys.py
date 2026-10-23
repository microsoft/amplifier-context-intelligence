"""Single logical-name -> physical-name map for blob storage.

Pure and stdlib-only (plus ``utils.fold_name``): no I/O, no locks.

Ordinary names keep their historical layout -- ``ci-blob://<session>/<name>``
stored as ``<name>.json``.  A logical write name that cannot be one filename
component (path separators, ASCII controls) is mapped to a flat, structural
address instead::

    ci-blob://<session>/~ci-key-v1/<64 lowercase hex>   ->  ~ci-key-v1~<hex>.blob

The ``.blob`` suffix and the three-part URI keep the mapped namespace disjoint
from every ordinary ``.json`` name, including names that merely look like the
marker.  Nothing here URL-decodes, case-folds or Unicode-normalises.
"""

from __future__ import annotations

import hashlib
import os
import re
from dataclasses import dataclass

from context_intelligence_server.utils import fold_name

__all__ = [
    "LOCK",
    "MAPPED_MARKER",
    "MAX_KEY_BYTES",
    "MAX_SESSION_BYTES",
    "METADATA",
    "BlobAddress",
    "address_from_filename",
    "component",
    "exact_address",
    "legacy_address",
    "parse_address",
    "parse_uri",
    "read_candidates",
    "write_address",
]

LOCK = ".ci-blob-retention.lock"
METADATA = ".ci-blob-retention"
MAPPED_MARKER = "~ci-key-v1"

_SCHEME = "ci-blob://"
_DIGEST_DOMAIN = b"ci-blob-event-key:v1\x00"
MAX_SESSION_BYTES = (
    255  # NAME_MAX: nothing is ever appended to a session directory name
)
MAX_KEY_BYTES = 242  # NAME_MAX - 13, the ``.<8 chars>.tmp`` suffix of a temp file
_HEX64 = re.compile(r"[0-9a-f]{64}")
_MAPPED_FILENAME = re.compile(re.escape(MAPPED_MARKER) + r"~([0-9a-f]{64})\.blob")


def component(value: str) -> str:
    """Accept one unambiguous URI/filesystem component, never a path.

    URIs are split by literal scheme-stripping and never URL-decoded, so
    ``%``, ``?`` and ``#`` are ordinary bytes (legacy ids carry them); only
    path separators, controls, ``.``/``..`` and the reserved names are unsafe.
    """
    if (
        not value
        or value in (".", "..", LOCK, METADATA)
        or any(c in value for c in "/\\")
        or any(ord(c) < 32 or ord(c) == 127 for c in value)
    ):
        raise ValueError(f"Unsafe blob component: {value!r}")
    return value


def _legacy_backslash_supported() -> bool:
    """True only where a literal backslash is one ordinary filename byte."""
    return os.sep == "/" and os.altsep is None and os.name != "nt"


def _ordinary_stem(value: str) -> str:
    """Validate an ordinary physical stem; allow the pre-upgrade POSIX backslash."""
    if "\\" in value and _legacy_backslash_supported():
        component(value.replace("\\", "_"))
        return value
    return component(value)


@dataclass(frozen=True)
class BlobAddress:
    """A validated physical address: pure formatting, never folding.

    ``name`` is the ordinary physical stem, or the 64-hex digest when
    ``mapped``.
    """

    session: str
    name: str
    mapped: bool = False

    def __post_init__(self) -> None:
        component(self.session)
        if self.mapped:
            if not _HEX64.fullmatch(self.name):
                raise ValueError(f"Invalid mapped blob digest: {self.name!r}")
        else:
            _ordinary_stem(self.name)

    @property
    def uri(self) -> str:
        if self.mapped:
            return f"{_SCHEME}{self.session}/{MAPPED_MARKER}/{self.name}"
        return f"{_SCHEME}{self.session}/{self.name}"

    @property
    def stem(self) -> str:
        """The physical filename without its extension (the temp-name base)."""
        return f"{MAPPED_MARKER}~{self.name}" if self.mapped else self.name

    @property
    def filename(self) -> str:
        return f"{self.stem}.blob" if self.mapped else f"{self.stem}.json"


def _mapped(session: str, logical_name: str) -> BlobAddress:
    digest = hashlib.sha256(_DIGEST_DOMAIN + logical_name.encode("utf-8")).hexdigest()
    return BlobAddress(session, digest, mapped=True)


def write_address(session_id: str, logical_name: str) -> BlobAddress:
    """Where a logical write name is stored; raises ``ValueError`` if invalid."""
    session = fold_name(component(session_id), MAX_SESSION_BYTES)
    if logical_name in ("", ".", "..", LOCK, METADATA):
        raise ValueError(f"Unsafe blob key: {logical_name!r}")
    logical_name.encode("utf-8")  # a lone surrogate stays a UnicodeEncodeError
    folded = fold_name(logical_name, MAX_KEY_BYTES)
    try:
        return BlobAddress(session, component(folded))
    except ValueError:
        return _mapped(session, logical_name)


def exact_address(uri: str) -> BlobAddress:
    """Parse *uri* without folding (what is on disk is what is addressed)."""
    if not uri.startswith(_SCHEME):
        raise ValueError(f"Invalid blob URI: {uri!r}")
    parts = uri[len(_SCHEME) :].split("/")
    if len(parts) == 2:
        return BlobAddress(parts[0], parts[1])
    if len(parts) == 3 and parts[1] == MAPPED_MARKER:
        return BlobAddress(parts[0], parts[2], mapped=True)
    raise ValueError(f"Invalid blob URI: {uri!r}")


def parse_address(uri: str) -> BlobAddress:
    """Parse *uri*; ordinary names fold exactly as the store always has."""
    addr = exact_address(uri)
    if addr.mapped:
        return addr
    return BlobAddress(
        fold_name(addr.session, MAX_SESSION_BYTES),
        fold_name(addr.name, MAX_KEY_BYTES),
    )


def parse_uri(uri: str) -> tuple[str, str]:
    """``(session, handle)``: the ordinary name, or ``~ci-key-v1/<digest>``."""
    addr = exact_address(uri)
    return addr.session, f"{MAPPED_MARKER}/{addr.name}" if addr.mapped else addr.name


def address_from_filename(session: str, filename: str) -> BlobAddress | None:
    """Recognise a scanned directory entry; never fold or remap it.

    Returns ``None`` for unknown files and raises ``ValueError`` for a
    recognised ordinary ``.json`` whose stem is invalid.
    """
    if filename.endswith(".json"):
        return BlobAddress(session, filename[: -len(".json")])
    match = _MAPPED_FILENAME.fullmatch(filename)
    if match:
        return BlobAddress(session, match[1], mapped=True)
    return None


def legacy_address(session_id: str, logical_name: str) -> BlobAddress | None:
    """The pre-mapping physical address, or ``None`` if it was never creatable.

    Used for reads and expiry checks only; nothing is ever created here.
    """
    session = fold_name(component(session_id), MAX_SESSION_BYTES)
    try:
        return BlobAddress(session, fold_name(logical_name, MAX_KEY_BYTES))
    except ValueError:
        return None


def read_candidates(uri: str) -> tuple[BlobAddress, ...]:
    """Physical addresses to try, in order, for *uri*."""
    addr = parse_address(uri)
    if addr.mapped or "\\" not in addr.name:
        return (addr,)
    canonical = write_address(addr.session, addr.name)
    return (addr,) if canonical == addr else (addr, canonical)
