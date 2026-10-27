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
    prune.add_argument("--job-id", help="Online only: inspect/confirm this existing preview instead of creating another.")
    status = commands.add_parser("prune-status", help="Read cached online job status.")
    status.add_argument("job_id", nargs="?")
    cancel = commands.add_parser("prune-cancel", help="Request online cancellation after the active durable batch.")
    cancel.add_argument("job_id")
    scheduled = commands.add_parser("prune-scheduled", help="Unattended online-only prune with bounded cancellation.")
    scheduled.add_argument("--online", action="store_true", required=True)
    scheduled.add_argument("--older-than", required=True)
    scheduled.add_argument("--server-url", required=True)
    scheduled.add_argument("--admin-key-file", type=Path)
    scheduled.add_argument("--dry-run", action="store_true", help="Preview and cancel without starting deletion.")
    scheduled.add_argument("--max-runtime", type=float, default=3600)
    scheduled.add_argument("--cancel-timeout", type=float, default=120)
    for command in (prune, status, cancel):
        command.add_argument("--online", action="store_true", help="Use the running server's configured blob root.")
        command.add_argument("--server-url", help="Server base URL; remote credentials require HTTPS.")
        command.add_argument("--admin-key-file", type=Path, help="Private same-owner 0600/0400 raw admin token file.")
    args = parser.parse_args(argv[1:])

    if args.command == "prune-scheduled":
        _scheduled(args)
        return

    try:
        if args.online:
            if getattr(args, "blob_root", None) is not None:
                parser.error("--blob-root cannot be used with --online; only the server's configured root is allowed")
            _online(args)
            return
        if args.command != "prune":
            parser.error("prune-status and prune-cancel require --online")
        if args.server_url or args.admin_key_file or args.job_id:
            parser.error("--server-url, --admin-key-file and --job-id require --online")
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


def _scheduled(args) -> None:
    import json
    import signal
    from dataclasses import asdict

    from context_intelligence_server.retention_client import RetentionClient, admin_credential, server_url
    from context_intelligence_server.retention_scheduler import (
        ScheduledResult, ScheduledTermination, _pacific_now, run_scheduled,
    )

    previous = signal.getsignal(signal.SIGTERM)
    terminated = False

    def terminate(signum, frame):
        nonlocal terminated
        if not terminated:
            terminated = True
            raise ScheduledTermination()

    result = ScheduledResult()
    try:
        signal.signal(signal.SIGTERM, terminate)
        server_url(args.server_url)  # Refuse insecure transport before reading credentials.
        client = RetentionClient(args.server_url, admin_credential(args.admin_key_file))
        result = run_scheduled(
            client, older_than=args.older_than, dry_run=args.dry_run,
            max_runtime=args.max_runtime, cancel_timeout=args.cancel_timeout,
        )
    except ScheduledTermination:
        result.error_code, result.exit_code = "sigterm", 143
    except KeyboardInterrupt:
        result.error_code, result.exit_code = "interrupted", 130
    except Exception:
        # Exception strings may contain key-file paths, URLs or server text.
        result.error_code = "client_setup_failed"
    finally:
        signal.signal(signal.SIGTERM, previous)
        result.ended_at = _pacific_now()
    print(json.dumps(asdict(result), sort_keys=True), flush=True)
    raise SystemExit(result.exit_code)


def _online(args) -> None:
    from context_intelligence_server.retention_client import (
        ClientError, RetentionClient, admin_credential,
    )
    from context_intelligence_server.blob_retention import Report, duration_ns

    try:
        settings = None
        if not args.server_url or not (
            args.admin_key_file or os.environ.get("ADMIN_API_KEY")
            or os.environ.get("AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_ADMIN_API_KEY")
        ):
            from context_intelligence_server.config import get_settings
            settings = get_settings()
        url = args.server_url or f"http://127.0.0.1:{settings.server_port}"
        # Validate transport before resolving credentials.
        from context_intelligence_server.retention_client import server_url
        server_url(url)
        client = RetentionClient(url, admin_credential(args.admin_key_file, settings))
        if args.command == "prune-status":
            job = client.status(args.job_id)
        elif args.command == "prune-cancel":
            job = client.cancel(args.job_id)
        else:
            duration_ns(args.older_than)
            job = client.status(args.job_id) if args.job_id else client.preview(args.older_than)
            print(f"Preview job: {job['id']} (server runs scan in background)", flush=True)
            job = client.wait_preview(job)
        print(f"Job: {job['id']}\nState: {job['state']}\n{Report(**job['report']).text()}", flush=True)
        if job.get("error"):
            print(f"Job error: {job['error']}", file=sys.stderr)
        if args.command == "prune" and args.apply and job["state"] == "ready":
            print("Live estimate, not an immutable manifest. Apply rescans the same cutoff; "
                  "new or changed files are retained. Graph and queue state are kept.", flush=True)
            if not sys.stdin.isatty():
                raise ClientError("Apply requires an interactive terminal; no --yes bypass")
            try:
                confirmed = input("Type 'prune' to expire these payloads: ") == "prune"
            except EOFError:
                confirmed = False
            if not confirmed:
                client.cancel(job["id"])
                print("Prune not confirmed; preview cancelled, no payloads deleted.")
                return
            job = client.start(job["id"], "prune")
            print(f"Apply accepted: {job['id']}; runs in the server background. "
                  "Disconnecting does NOT cancel it.", flush=True)
        print(f"Status: context-intelligence-server blobs prune-status --online {job['id']} "
              f"--server-url {url}\nCancel: context-intelligence-server blobs prune-cancel --online "
              f"{job['id']} --server-url {url}", flush=True)
        if args.command == "prune" and job["state"] in {"previewing", "ready"}:
            print(f"Confirm this same preview when ready: context-intelligence-server blobs prune --online "
                  f"--job-id {job['id']} --apply --server-url {url}", flush=True)
        if job["state"] in {"failed", "interrupted"} or job["report"]["errors"]:
            raise SystemExit(1)
    except KeyboardInterrupt:
        print("Client interrupted. Remote work is NOT automatically cancelled; use prune-status/prune-cancel.", file=sys.stderr)
        raise SystemExit(130) from None
    except (ClientError, OSError, ValueError) as exc:
        print(f"Online blob prune failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()