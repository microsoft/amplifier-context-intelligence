"""Lazy console dispatch: blob preview never imports the ASGI app or drivers."""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from pathlib import Path


def main(argv: list[str] | None = None) -> None:
    argv = sys.argv[1:] if argv is None else argv
    if not argv or argv[0] != "blobs":
        from context_intelligence_server.main import main as legacy_main
        legacy_main(argv)
        return

    from context_intelligence_server import blob_retention as retention

    parser = argparse.ArgumentParser(prog="context-intelligence-server blobs")
    commands = parser.add_subparsers(dest="command", required=True)
    prune = commands.add_parser("prune", help="Preview storage-age blob expiry; no graph changes.")
    prune.add_argument("--older-than", default="30d", help="Positive integer s/m/h/d/w (default: 30d).")
    prune.add_argument("--blob-root", type=Path, help="Existing blob root; otherwise use existing settings.")
    prune.add_argument("--apply", action="store_true", help="Lock writers out and ask for interactive confirmation.")
    args = parser.parse_args(argv[1:])

    try:
        cutoff = retention.cutoff_for(retention.duration_ns(args.older_than))
        config = os.environ.get("AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE")
        if config is not None and (not config or not Path(config).is_file()):
            raise ValueError("Explicit server config file does not exist")
        root = args.blob_root
        if root is None:
            from context_intelligence_server.config import get_settings
            root = Path(get_settings().blob_path)

        def confirm(report: retention.Report) -> bool:
            print(report.text(), flush=True)
            print("Old unpatched writers do not honor this lock. Stop them before apply.\n"
                  "Graph/session metadata and queue files are kept.", flush=True)
            if not sys.stdin.isatty():
                raise retention.RetentionError("Apply requires an interactive terminal; no --yes bypass")
            try:
                return input("Type 'prune' to expire these payloads: ") == "prune"
            except EOFError:
                return False

        report = retention.apply(root, cutoff, confirm) if args.apply else retention.preview(root, cutoff)
        print(report.text())
        if report.errors or (args.apply and report.changed):
            raise SystemExit(1)
    except (OSError, ValueError, retention.RetentionError, sqlite3.Error) as exc:
        print(f"Blob prune failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()