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


def run_init(
    *,
    config_path: Path,
    neo4j_url: str,
    neo4j_user: str,
    neo4j_password: str,
) -> dict[str, str]:
    """Write server config with Neo4j credentials and a generated API key.

    If *config_path* already exists, existing keys are preserved and only
    the Neo4j + auth keys are overwritten.

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
        default="neo4j://localhost:7687",
        help="Neo4j bolt URL (default: neo4j://localhost:7687).",
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
    )

    print(f"Config written to: {result['config_path']}")
    print(f"API key: {result['api_key']}")


if __name__ == "__main__":
    main()
