"""Reusable synchronous online-retention client; no data-key fallback."""

from __future__ import annotations

import ipaddress
import json
import os
import stat
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path


class ClientError(RuntimeError):
    pass


def server_url(value: str) -> str:
    try:
        if any(ord(c) <= 32 or ord(c) == 127 for c in value) or "\\" in value:
            raise ValueError()
        parsed = urllib.parse.urlsplit(value)
        if (parsed.scheme not in {"http", "https"} or not parsed.hostname
                or parsed.username is not None or parsed.password is not None
                or parsed.query or parsed.fragment or "%" in parsed.netloc
                or any(part in {".", ".."} for part in parsed.path.split("/"))
                or "%" in parsed.path):
            raise ValueError()
        port = parsed.port
        if port is not None and not 1 <= port <= 65535:
            raise ValueError()
        host = parsed.hostname
        try:
            loopback = ipaddress.ip_address(host).is_loopback
        except ValueError:
            loopback = host.lower() == "localhost"
        if parsed.scheme == "http" and not loopback:
            raise ClientError("Remote admin credentials require HTTPS; HTTP is allowed only for loopback")
    except ValueError as exc:
        raise ClientError("Invalid server URL: use http(s)://host[:port] without credentials, query, or fragment") from exc
    return value.rstrip("/")


def admin_credential(key_file: Path | None = None, settings=None) -> str:
    if key_file is not None:
        fd = os.open(key_file, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK)
        with os.fdopen(fd, "rb") as stream:
            st = os.fstat(stream.fileno())
            if (not stat.S_ISREG(st.st_mode) or st.st_nlink != 1
                    or st.st_uid != os.geteuid() or stat.S_IMODE(st.st_mode) not in {0o400, 0o600}
                    or st.st_size > 8192):
                raise ClientError("Admin key file must be same-owner, regular, not a symlink, mode 0600/0400, at most 8192 bytes")
            raw = stream.read(8193).decode("utf-8").strip()
    else:
        raw = (os.environ.get("ADMIN_API_KEY")
               or os.environ.get("AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_ADMIN_API_KEY")
               or (getattr(settings, "admin_api_key", None) if settings is not None else None))
    if not raw:
        raise ClientError(
            "A raw admin bearer token is required: set ADMIN_API_KEY (or "
            "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_ADMIN_API_KEY), configure raw admin_api_key, "
            "or pass --admin-key-file PATH. A digest-only config cannot supply a token; data keys are never used."
        )
    if len(raw) > 8192 or any(ord(c) <= 32 or ord(c) >= 127 for c in raw):
        raise ClientError("Invalid raw admin bearer token")
    return raw


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ClientError("Admin request redirect refused")


class RetentionClient:
    def __init__(self, url: str, credential: str, *, timeout: float = 10):
        self.url = server_url(url)
        self._credential = credential
        self.timeout = timeout
        # Never inherit ambient proxy settings for an admin credential.
        self._opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())

    @staticmethod
    def _id(job_id: str) -> str:
        try:
            return str(uuid.UUID(job_id))
        except (ValueError, AttributeError) as exc:
            raise ClientError("Job id must be a UUID") from exc

    def _request(self, method: str, suffix: str = "", body: dict | None = None) -> dict:
        request = urllib.request.Request(
            self.url + "/admin/blobs/prune" + suffix,
            data=json.dumps(body).encode() if body is not None else None,
            headers={"Authorization": f"Bearer {self._credential}", "Content-Type": "application/json"},
            method=method,
        )
        try:
            with self._opener.open(request, timeout=self.timeout) as response:
                data = response.read(1_048_577)
            if len(data) > 1_048_576:
                raise ClientError("Oversized admin response")
            result = json.loads(data)
            if not isinstance(result, dict):
                raise ClientError("Invalid admin response")
            return result
        except urllib.error.HTTPError as exc:
            exc.close()
            raise ClientError(f"Admin request failed (HTTP {exc.code}); verify admin authority and current job status") from None
        except (urllib.error.URLError, TimeoutError, OSError, ValueError) as exc:
            raise ClientError(f"Admin request failed ({type(exc).__name__}); remote job may still be running; query prune-status") from None

    def preview(self, older_than: str) -> dict:
        return self._request("POST", body={"older_than": older_than})

    def status(self, job_id: str | None = None) -> dict:
        return self._request("GET", "/" + self._id(job_id) if job_id else "")

    def start(self, job_id: str, confirmation: str) -> dict:
        return self._request("POST", "/" + self._id(job_id) + "/start", {"confirmation": confirmation})

    def cancel(self, job_id: str) -> dict:
        return self._request("POST", "/" + self._id(job_id) + "/cancel", {})

    def wait_preview(self, job: dict, *, timeout: float = 120, interval: float = 0.25) -> dict:
        deadline = time.monotonic() + timeout
        while job["state"] == "previewing" and time.monotonic() < deadline:
            time.sleep(min(interval, max(0, deadline - time.monotonic())))
            job = self.status(job["id"])
        return job