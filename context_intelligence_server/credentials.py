"""Credential generation and persistence for the Context Intelligence Server."""

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
) -> dict[str, str]:
    """Generate credentials and write them to a YAML file.

    If *neo4j_password* is ``None``, a cryptographically random password is
    generated.  The API bearer token (``api_key``) is always auto-generated.

    Returns the credentials dict that was written.
    """
    creds: dict[str, str] = {
        "neo4j_url": neo4j_url,
        "neo4j_user": neo4j_user,
        "neo4j_password": neo4j_password or secrets.token_urlsafe(32),
        "api_key": secrets.token_urlsafe(32),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.dump(creds, default_flow_style=False, sort_keys=False))
    return creds


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
