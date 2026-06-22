"""Credential generation and persistence for the Context Intelligence Server."""

import hashlib
import secrets
from pathlib import Path
from typing import Any

import yaml


def generate_credentials(
    path: Path,
    *,
    neo4j_url: str,
    neo4j_user: str,
    neo4j_password: str | None = None,
) -> str:
    """Generate credentials and write them to a YAML file.

    If *neo4j_password* is ``None``, a cryptographically random password is
    generated.  A new API bearer token is always auto-generated; only its
    SHA-256 hex digest is stored in the file (under the ``api_keys`` block).
    The raw token is **not** written to the file — it is returned to the caller,
    who must print it prominently so the operator can capture it.

    The digest derivation is ``hashlib.sha256(token.encode()).hexdigest()``,
    which is the same expression used by ``auth.py`` ``_resolve_token``.

    Returns:
        The raw bearer token.  The caller is responsible for displaying it
        (e.g. ``print(f"API token: {raw_token}")``) because it is not recoverable
        from the written file.
    """
    raw_token = secrets.token_urlsafe(32)
    digest = hashlib.sha256(raw_token.encode()).hexdigest()
    creds: dict[str, Any] = {
        "neo4j_url": neo4j_url,
        "neo4j_user": neo4j_user,
        "neo4j_password": neo4j_password or secrets.token_urlsafe(32),
        "api_keys": {digest: {"id": "owner"}},
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.dump(creds, default_flow_style=False, sort_keys=False))
    return raw_token


def read_credentials(path: Path) -> dict[str, Any]:
    """Read credentials from a YAML file.

    Raises:
        FileNotFoundError: If the file does not exist.
    """
    if not path.exists():
        raise FileNotFoundError(f"Credentials file not found: {path}")
    data = yaml.safe_load(path.read_text())
    if not isinstance(data, dict):
        raise ValueError(f"Invalid credentials file (expected YAML mapping): {path}")
    return data
