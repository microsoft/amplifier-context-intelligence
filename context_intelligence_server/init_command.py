"""``context-intelligence-server-init`` CLI command.

Handles first-run configuration for standalone (non-Docker) deployments.
Writes Neo4j connection details and an auto-generated API bearer token
to the server YAML config file.
"""

import argparse
import secrets
import sys
from pathlib import Path
from typing import Any

import yaml

_DEFAULT_CONFIG_PATH = Path("server-config.yaml")
_DEFAULT_BLOB_PATH = "~/.local/share/context-intelligence/blobs"
_DEFAULT_LOG_PATH = "~/.local/share/context-intelligence/logs/server.jsonl"
_DEFAULT_CURSOR_PATH = "~/.local/share/context-intelligence/cursors"


def run_init(
    *,
    config_path: Path,
    neo4j_url: str,
    neo4j_user: str,
    neo4j_password: str,
    blob_path: str | None = None,
    log_path: str | None = None,
    cursor_path: str | None = None,
    server_host: str | None = None,
    server_port: int | None = None,
) -> dict[str, str]:
    """Write server config with Neo4j credentials and a generated API key.

    If *config_path* already exists, existing keys are preserved and only
    the provided keys are overwritten.  Optional path/host/port params are
    only written when explicitly supplied (not ``None``).

    Returns a dict with the generated ``api_key`` (and all written keys).
    """
    existing: dict[str, Any] = {}
    if config_path.exists():
        loaded = yaml.safe_load(config_path.read_text())
        if isinstance(loaded, dict):
            existing = loaded

    api_key = secrets.token_urlsafe(32)

    existing["neo4j_url"] = neo4j_url
    existing["neo4j_user"] = neo4j_user
    existing["neo4j_password"] = neo4j_password
    existing["api_key"] = api_key

    if blob_path is not None:
        existing["blob_path"] = str(Path(blob_path).expanduser())
    if log_path is not None:
        existing["log_path"] = str(Path(log_path).expanduser())
    if cursor_path is not None:
        existing["cursor_path"] = str(Path(cursor_path).expanduser())
    if server_host is not None:
        existing["server_host"] = server_host
    if server_port is not None:
        existing["server_port"] = server_port

    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        yaml.dump(existing, default_flow_style=False, sort_keys=False)
    )

    return {"api_key": api_key, "config_path": str(config_path)}


def main(argv: list[str] | None = None) -> None:
    """CLI entry point for ``context-intelligence-server-init``."""
    parser = argparse.ArgumentParser(
        description="Initialize Context Intelligence Server configuration.",
    )
    parser.add_argument(
        "--config-path",
        type=Path,
        default=_DEFAULT_CONFIG_PATH,
        help="Path to the server config YAML file (default: server-config.yaml in CWD).",
    )
    parser.add_argument(
        "--neo4j-url",
        type=str,
        default="bolt://localhost:7687",
        help="Neo4j bolt URL (default: bolt://localhost:7687).",
    )
    parser.add_argument(
        "--neo4j-user",
        type=str,
        default="neo4j",
        help="Neo4j username (default: neo4j).",
    )
    parser.add_argument(
        "--neo4j-password",
        type=str,
        default=None,
        help="Neo4j password. If omitted, you will be prompted interactively.",
    )
    parser.add_argument(
        "--blob-path",
        type=str,
        default=_DEFAULT_BLOB_PATH,
        help=f"Path for blob storage (default: {_DEFAULT_BLOB_PATH}).",
    )
    parser.add_argument(
        "--log-path",
        type=str,
        default=_DEFAULT_LOG_PATH,
        help=f"Path for server log file (default: {_DEFAULT_LOG_PATH}).",
    )
    parser.add_argument(
        "--cursor-path",
        type=str,
        default=_DEFAULT_CURSOR_PATH,
        help=f"Path for cursor storage (default: {_DEFAULT_CURSOR_PATH}).",
    )
    parser.add_argument(
        "--server-host",
        type=str,
        default="0.0.0.0",
        help="Server bind host (default: 0.0.0.0).",
    )
    parser.add_argument(
        "--server-port",
        type=int,
        default=8000,
        help="Server bind port (default: 8000).",
    )

    args = parser.parse_args(argv)

    neo4j_password = args.neo4j_password
    if neo4j_password is None:
        try:
            neo4j_password = input("Neo4j password: ")
        except (EOFError, KeyboardInterrupt):
            print("\nAborted.")
            sys.exit(1)

    result = run_init(
        config_path=args.config_path,
        neo4j_url=args.neo4j_url,
        neo4j_user=args.neo4j_user,
        neo4j_password=neo4j_password,
        blob_path=args.blob_path,
        log_path=args.log_path,
        cursor_path=args.cursor_path,
        server_host=args.server_host,
        server_port=args.server_port,
    )

    print(f"Config written to: {result['config_path']}")
    print(f"API key: {result['api_key']}")


if __name__ == "__main__":
    main()
