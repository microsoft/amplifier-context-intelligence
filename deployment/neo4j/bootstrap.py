"""Retained Neo4j guest bootstrap. Rendering/importing never executes guest work."""
from __future__ import annotations

import argparse
import base64
from contextlib import contextmanager
import grp
import hashlib
import http.client
import io
import ipaddress
import json
import os
from pathlib import Path
import pwd
import re
import signal
import stat
import subprocess
import sys
import tempfile
import time
import urllib.parse
import urllib.error
import urllib.request
import uuid
import zipfile


VERSIONS = {"neo4j": "5.26.27", "apoc": "5.26.27", "gds": "2.13.11"}
SECRET_BASE = "https://kv-context-intel.vault.azure.net/secrets/context-intelligence-neo4j-"
HELPER = "/usr/local/lib/neo4j-bootstrap.py"
MANIFEST = "/etc/neo4j-bootstrap.json"
HTTP = "http://127.0.0.1:7474/db/neo4j/tx/commit"
ENV = {"PATH": "/usr/sbin:/usr/bin:/sbin:/bin", "LANG": "C",
       "DEBIAN_FRONTEND": "noninteractive"}
SHAPE = {
    "schema_version": None, "versions": dict.fromkeys(VERSIONS),
    "apt": dict.fromkeys(("neo4j_package_version", "key_url", "key_sha256")),
    "storage": dict.fromkeys((
        "lun", "partition", "filesystem_uuid", "filesystem", "mount", "data_dir",
        "transaction_logs_dir", "database_store", "system_store", "store_markers",
        "plugins_dir", "uid", "gid")),
    "config": dict.fromkeys(("neo4j_conf", "sha256")),
    "identity": dict.fromkeys(("kind", "client_id")),
    "credentials": dict.fromkeys(("username_secret_url", "password_secret_url", "version_policy")),
    "plugins": None,
}
PLUGIN_KEYS = {"name", "version", "filename", "sha256", "url", "bundled_path"}
SAFE_CODES = frozenset("""
manifest manifest.keys manifest.size schema_version versions
apt apt.neo4j_package_version apt.key_url apt.key_sha256 storage config identity credentials plugins
storage.lun storage.partition storage.filesystem_uuid storage.filesystem storage.mount
storage.data_dir storage.transaction_logs_dir storage.database_store storage.system_store
storage.store_markers storage.plugins_dir storage.uid storage.gid storage.stores
storage.device storage.parent storage.mount_parent storage.mountpoint storage.unmounted
storage.store storage.ownership storage.marker storage.fstab_conflict
config.sha256 config.credentials config.settings config.required config.bolt config.allowlist
config.unrestricted config.database config.memory config.installed
identity.kind identity.client_id identity.selected identity.token identity.resource
credentials.version_policy credentials.username_secret_url credentials.password_secret_url
credentials.unusable credentials.username
plugins.names plugins.keys plugins.filename plugins.sha256 plugins.url plugins.bundled_path
plugins.zip plugins.directory plugins.symlink plugins.unexpected plugins.file plugins.missing
plugins.bundle plugins.parent plugins.filesystem cloud_config.size guest.command guest.root
guest.os guest.already_running path.symlink path.canonical path.root_trust
http.status http.size packages.policy packages.id_collision packages.account packages.key
packages.java packages.neo4j readiness.errors readiness.results readiness.versions readiness.http
readiness.timeout readiness.port_occupied readiness.peer readiness.connection
cleanup.stop cleanup.mask
""".split())


class _Refused(ValueError):
    """Field-name-only validation failure."""

    def __init__(self, code: str):
        self.code = code if code in SAFE_CODES else "REFUSED"
        super().__init__(self.code)


class _Starting(Exception):
    """Only explicitly retryable availability failures, never response contents."""


def _require(ok: bool, field: str) -> None:
    if not ok:
        raise _Refused(field)


def _text(value: object) -> bool:
    return isinstance(value, str) and bool(value) and value == value.strip()


def _digest(value: object) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def _url(value: object) -> bool:
    if not _text(value) or re.search(r"[\s\\%]", str(value)):
        return False
    u = urllib.parse.urlsplit(str(value))
    return (u.scheme == "https" and bool(u.hostname) and u.netloc == u.hostname
            and not u.query and not u.fragment and u.path.startswith("/"))


def _path(value: object) -> bool:
    return (isinstance(value, str) and value.startswith("/") and value != "/"
            and re.fullmatch(r"/[A-Za-z0-9_./-]+", value) is not None
            and str(Path(value)) == value and ".." not in Path(value).parts)


def _inside(path: str, parent: str) -> bool:
    return Path(path).is_relative_to(parent) and path != parent


def _config(inputs: dict) -> None:
    c, s = inputs["config"], inputs["storage"]
    text = c["neo4j_conf"]
    _require(isinstance(text, str) and bool(text.strip()) and _digest(c["sha256"]), "config")
    _require(hashlib.sha256(text.encode("utf-8")).hexdigest() == c["sha256"], "config.sha256")
    _require(re.search(r"(?i)(password|username|credential|secret|token)\s*[:=]", text) is None,
             "config.credentials")
    # Explicit supported settings: no JVM flags, APOC network import, credentials,
    # environment expansion, command substitution, or executable extension hooks.
    allowed = {
        "dbms.security.auth_enabled", "server.directories.data",
        "server.directories.transaction.logs.root", "server.directories.plugins",
        "server.http.enabled", "server.http.listen_address", "server.https.enabled",
        "server.bolt.enabled", "server.bolt.listen_address",
        "dbms.security.procedures.allowlist", "dbms.security.procedures.unrestricted",
        "server.memory.heap.initial_size", "server.memory.heap.max_size",
        "server.memory.pagecache.size", "initial.dbms.default_database",
    }
    settings = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, sep, value = line.partition("=")
        key, value = key.strip(), value.strip()
        _require(bool(sep) and key in allowed and key not in settings, "config.settings")
        _require(bool(value) and not re.search(r"[$`\\\x00-\x1f]", value), "config.settings")
        settings[key] = value
    required = {
        "dbms.security.auth_enabled": "true",
        "server.directories.data": s["data_dir"],
        "server.directories.transaction.logs.root": s["transaction_logs_dir"],
        "server.directories.plugins": s["plugins_dir"],
        "server.http.enabled": "true", "server.http.listen_address": "127.0.0.1:7474",
        "server.https.enabled": "false", "server.bolt.enabled": "true",
    }
    _require(all(settings.get(k) == v for k, v in required.items()), "config.required")
    address, sep, port = settings.get("server.bolt.listen_address", "").rpartition(":")
    _require(bool(sep) and port == "7687", "config.bolt")
    ip = ipaddress.IPv4Address(address)
    _require(address == "0.0.0.0" or (ip.is_private and not ip.is_loopback
             and not ip.is_link_local and not ip.is_multicast and not ip.is_reserved), "config.bolt")
    allow = set(settings.get("dbms.security.procedures.allowlist", "").split(","))
    _require({"apoc.*", "gds.*"}.issubset(allow), "config.allowlist")
    _require(all(re.fullmatch(r"(?:apoc|gds|db|dbms)\.[A-Za-z0-9_.*]+", x)
                 for x in allow), "config.allowlist")
    unrestricted = set(settings.get("dbms.security.procedures.unrestricted", "").split(","))
    _require("gds.*" in unrestricted and all(
        x == "gds.*" or re.fullmatch(r"apoc\.[A-Za-z0-9_.]+", x)
        for x in unrestricted), "config.unrestricted")
    _require(settings.get("initial.dbms.default_database", "neo4j") == "neo4j", "config.database")
    for key, value in settings.items():
        if key.startswith("server.memory."):
            _require(re.fullmatch(r"[1-9][0-9]*[kKmMgG]", value) is not None, "config.memory")


def _validate_fields(inputs: dict, candidate: bool) -> None:
    _require(type(inputs) is dict and inputs.keys() == SHAPE.keys(), "manifest.keys")
    for key, shape in SHAPE.items():
        if isinstance(shape, dict):
            _require(type(inputs[key]) is dict and inputs[key].keys() == shape.keys(), key)
    _require(type(inputs["schema_version"]) is int and inputs["schema_version"] == 1, "schema_version")
    _require(inputs["versions"] == VERSIONS, "versions")
    s, a, c, ident, cred = (inputs[k] for k in ("storage", "apt", "config", "identity", "credentials"))
    checks = {
        "lun": lambda v: type(v) is int and v == 0,
        "partition": lambda v: type(v) is int and 0 <= v <= 128,
        "filesystem_uuid": lambda v: isinstance(v, str) and str(uuid.UUID(v)) == v,
        "filesystem": lambda v: v in ("ext4", "xfs"),
        "uid": lambda v: type(v) is int and 0 < v < 2**31,
        "gid": lambda v: type(v) is int and 0 < v < 2**31,
    }
    for key, value in s.items():
        if value is None and candidate and key != "lun":
            continue
        if key in checks:
            _require(checks[key](value), "storage." + key)
        elif key == "store_markers":
            _require(type(value) is list and len(value) >= 2 and len(set(value)) == len(value)
                     and all(_path(p) for p in value), "storage.store_markers")
        else:
            _require(_path(value), "storage." + key)
    if s["mount"] is not None:
        mount = s["mount"]
        _require(mount not in ("/etc", "/usr", "/var", "/var/lib", "/home", "/root",
                              "/boot", "/dev", "/proc", "/sys", "/run", "/tmp"),
                 "storage.mount")
        _require(not any(Path(mount).is_relative_to(p) for p in
                         ("/etc", "/usr", "/boot", "/dev", "/proc", "/sys", "/run", "/root")),
                 "storage.mount")
        for key in ("data_dir", "transaction_logs_dir", "database_store", "system_store", "plugins_dir"):
            if s[key] is not None:
                _require(_inside(s[key], mount), "storage." + key)
    if s["data_dir"] and s["database_store"] and s["system_store"]:
        _require(s["database_store"] == s["data_dir"] + "/databases/neo4j"
                 and s["system_store"] == s["data_dir"] + "/databases/system", "storage.stores")
    if s["store_markers"] is not None:
        _require(bool(s["database_store"]) and bool(s["system_store"]), "storage.stores")
        _require(all(any(_inside(p, s[k]) for k in ("database_store", "system_store"))
                     for p in s["store_markers"]), "storage.store_markers")
        _require(all(any(_inside(p, s[k]) for p in s["store_markers"])
                     for k in ("database_store", "system_store")), "storage.store_markers")
    if s["plugins_dir"] and s["data_dir"] and s["transaction_logs_dir"]:
        _require(not any(Path(s["plugins_dir"]).is_relative_to(s[k])
                         or Path(s[k]).is_relative_to(s["plugins_dir"])
                         for k in ("data_dir", "transaction_logs_dir")), "storage.plugins_dir")
    for key, value in a.items():
        if value is None and candidate:
            continue
        valid = (_url(value) if key == "key_url" else _digest(value) if key == "key_sha256"
                 else isinstance(value, str) and re.fullmatch(r"(?:[1-9][0-9]*:)?5\.26\.27", value))
        _require(bool(valid), "apt." + key)
    if ident["kind"] is not None or not candidate:
        _require(ident["kind"] in ("user-assigned", "system-assigned"), "identity.kind")
    if ident["client_id"] is not None or not candidate:
        _require(isinstance(ident["client_id"], str)
                 and str(uuid.UUID(ident["client_id"])) == ident["client_id"]
                 and uuid.UUID(ident["client_id"]).int != 0, "identity.client_id")
    policy = cred["version_policy"]
    _require((candidate and policy is None) or policy in ("latest", "pinned"), "credentials.version_policy")
    for key, suffix in (("username_secret_url", "user"), ("password_secret_url", "password")):
        url = cred[key]
        base = SECRET_BASE + suffix
        _require(isinstance(url, str) and _url(url), "credentials." + key)
        _require(url == base if policy != "pinned" else
                 re.fullmatch(re.escape(base) + r"/[0-9a-f]{32}", url) is not None, "credentials." + key)
    plugins = inputs["plugins"]
    _require(type(plugins) is list and len(plugins) == 2, "plugins")
    _require([p.get("name") for p in plugins if type(p) is dict] == ["apoc", "gds"], "plugins.names")
    for p in plugins:
        _require(p.keys() == PLUGIN_KEYS and p["version"] == VERSIONS[p["name"]], "plugins.keys")
        for key in ("filename", "sha256", "url"):
            value = p[key]
            if candidate and value is None:
                continue
            if key == "filename":
                pattern = (r"apoc-5\.26\.27-core\.jar" if p["name"] == "apoc"
                           else r"neo4j-graph-data-science-2\.13\.11\.jar")
                _require(isinstance(value, str) and re.fullmatch(pattern, value) is not None, "plugins.filename")
            else:
                _require(_digest(value) if key == "sha256" else _url(value), "plugins." + key)
        if p["bundled_path"] is not None:
            _require(p["name"] == "apoc" and p["filename"] is not None and
                     p["bundled_path"] in ("/var/lib/neo4j/labs/" + p["filename"],
                                           "/usr/share/neo4j/labs/" + p["filename"]), "plugins.bundled_path")
    if c["neo4j_conf"] is not None or c["sha256"] is not None or not candidate:
        _config(inputs)


def _validate(inputs: dict, candidate: bool) -> None:
    try:
        _validate_fields(inputs, candidate)
    except _Refused:
        raise
    except Exception:
        raise _Refused("manifest") from None


def validate_inputs(inputs: dict) -> None:
    """Validate complete nonsecret production inputs; errors contain field names only."""
    _validate(inputs, False)


def render_cloud_init(inputs: dict, *, candidate: bool = False) -> str:
    """Render deterministic, self-contained JSON cloud-config (also valid YAML)."""
    _validate(inputs, candidate)
    config = {
        "write_files": [
            {"path": HELPER, "owner": "root:root", "permissions": "0600",
             "content": Path(__file__).read_text(encoding="utf-8")},
            {"path": MANIFEST, "owner": "root:root", "permissions": "0600",
             "content": json.dumps(inputs, sort_keys=True, indent=2) + "\n"},
        ],
        "runcmd": [["/usr/bin/python3", "-I", HELPER, "bootstrap", "--manifest", MANIFEST]],
    }
    label = "# REVIEW CANDIDATE INCOMPLETE INPUTS BOOTSTRAP WILL REFUSE\n" if candidate else ""
    result = "#cloud-config\n" + label + json.dumps(config, sort_keys=True, indent=2) + "\n"
    _require(len(result.encode("utf-8")) <= 65535, "cloud_config.size")
    return result


def _run(args: list[str], *, check: bool = True, timeout: int = 120) -> subprocess.CompletedProcess:
    result = subprocess.run(args, check=False, capture_output=True, text=True,
                            env=ENV.copy(), timeout=timeout, stdin=subprocess.DEVNULL)
    _require(not check or result.returncode == 0, "guest.command")
    return result


def _safe_path(path: Path) -> None:
    for p in (path, *path.parents):
        _require(not p.is_symlink(), "path.symlink")
    _require(path.resolve() == path, "path.canonical")


def _root_controlled(path: Path, *, missing_ok: bool = False) -> None:
    """Root must control replacement at every level, including the filesystem root."""
    _require(path.is_absolute() and ".." not in path.parts, "path.root_trust")
    for p in reversed((path, *path.parents)):
        try:
            st = p.lstat()
        except FileNotFoundError:
            _require(missing_ok, "path.root_trust")
            continue
        _require(st.st_uid == 0 and st.st_mode & 0o022 == 0
                 and (stat.S_ISDIR(st.st_mode) or (p == path and stat.S_ISREG(st.st_mode))),
                 "path.root_trust")


def _atomic(path: Path, data: bytes, mode: int = 0o600) -> None:
    _safe_path(path)
    _safe_path(path.parent)
    fd, name = tempfile.mkstemp(prefix=".neo4j-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as out:
            out.write(data)
            out.flush()
            os.fsync(out.fileno())
            os.fchmod(out.fileno(), mode)
        os.replace(name, path)
    finally:
        Path(name).unlink(missing_ok=True)


def _host() -> None:
    _require(os.geteuid() == 0, "guest.root")
    release = Path("/etc/os-release").read_text(encoding="utf-8")
    fields = dict(line.split("=", 1) for line in release.splitlines() if "=" in line)
    _require(fields.get("ID", "").strip('"') == "ubuntu"
             and fields.get("VERSION_ID", "").strip('"') == "22.04", "guest.os")


def _storage(inputs: dict, *, allow_mount: bool = False) -> None:
    s = inputs["storage"]
    disk = Path("/dev/disk/azure/scsi1/lun0").resolve(strict=True)
    device = Path("/dev/disk/azure/scsi1/lun0" +
                  (f"-part{s['partition']}" if s["partition"] else "")).resolve(strict=True)
    _require(stat.S_ISBLK(disk.stat().st_mode) and stat.S_ISBLK(device.stat().st_mode), "storage.device")
    if s["partition"]:
        parent = _run(["/usr/bin/lsblk", "-dnro", "PKNAME", str(device)]).stdout.strip()
        _require(Path("/dev", parent).resolve() == disk, "storage.parent")
        number = Path("/sys/class/block", device.name, "partition").read_text(encoding="utf-8").strip()
        _require(number == str(s["partition"]), "storage.partition")
    else:
        _require(device == disk, "storage.device")
    fields = dict(line.split("=", 1) for line in
                  _run(["/usr/sbin/blkid", "-p", "-o", "export", str(device)]).stdout.splitlines()
                  if "=" in line)
    _require(fields.get("UUID") == s["filesystem_uuid"]
             and fields.get("TYPE") == s["filesystem"], "storage.filesystem")
    mount = Path(s["mount"])
    _safe_path(mount)
    probe = _run(["/usr/bin/findmnt", "-J", "-M", str(mount),
                  "-o", "SOURCE,UUID,FSTYPE,TARGET"], check=False)
    if probe.returncode == 1 and allow_mount:
        _require(mount.parent.is_dir(), "storage.mount_parent")
        if not mount.exists():
            mount.mkdir(mode=0o755)
        st = mount.stat()
        _require(st.st_uid == 0 and st.st_mode & 0o022 == 0
                 and not any(mount.iterdir()), "storage.mountpoint")
        _run(["/usr/bin/mount", "-t", s["filesystem"], "-o", "nodev,nosuid",
              "UUID=" + s["filesystem_uuid"], str(mount)])
        probe = _run(["/usr/bin/findmnt", "-J", "-M", str(mount),
                      "-o", "SOURCE,UUID,FSTYPE,TARGET"])
    _require(probe.returncode == 0, "storage.unmounted")
    entries = json.loads(probe.stdout)["filesystems"]
    _require(len(entries) == 1, "storage.mount")
    entry = entries[0]
    _require(entry.get("uuid") == s["filesystem_uuid"] and entry.get("fstype") == s["filesystem"]
             and entry.get("target") == str(mount)
             and Path(entry.get("source", "")).resolve() == device, "storage.mount")
    for key in ("data_dir", "transaction_logs_dir", "database_store", "system_store"):
        p = Path(s[key])
        _safe_path(p)
        _require(p.is_dir(), "storage.store")
        st = p.stat()
        _require(p.is_dir() and st.st_uid == s["uid"] and st.st_gid == s["gid"]
                 and st.st_dev == mount.stat().st_dev, "storage.ownership")
    for name in s["store_markers"]:
        p = Path(name)
        _safe_path(p)
        _require(p.is_file(), "storage.marker")
        st = p.stat()
        _require(stat.S_ISREG(st.st_mode) and st.st_size > 0 and st.st_uid == s["uid"]
                 and st.st_gid == s["gid"] and st.st_dev == mount.stat().st_dev, "storage.marker")
    _safe_path(Path(s["plugins_dir"]))
    if Path(s["plugins_dir"]).exists():
        _require(Path(s["plugins_dir"]).stat().st_dev == mount.stat().st_dev, "plugins.filesystem")


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ValueError("http.redirect")


def _fetch(url: str, *, headers: dict | None = None, data: bytes | None = None,
           limit: int = 1048576, timeout: float = 10) -> bytes:
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())
    request = urllib.request.Request(url, headers=headers or {}, data=data)
    with opener.open(request, timeout=timeout) as response:
        _require(response.status == 200 and response.geturl() == url, "http.status")
        body = response.read(limit + 1)
    _require(len(body) <= limit, "http.size")
    return body


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    _require(remaining > 0, "readiness.timeout")
    return min(10, remaining)


def _credentials(inputs: dict, *, deadline: float | None = None) -> tuple[str, str]:
    client_id = inputs["identity"]["client_id"]
    url = ("http://169.254.169.254/metadata/identity/oauth2/token?"
           + urllib.parse.urlencode({"api-version": "2018-02-01",
                                     "resource": "https://vault.azure.net", "client_id": client_id}))
    timeout = lambda: _remaining(deadline) if deadline is not None else 10
    token = json.loads(_fetch(url, headers={"Metadata": "true"}, limit=32768, timeout=timeout()))
    _require(str(token.get("client_id", "")).lower() == client_id, "identity.selected")
    _require(token.get("resource") in ("https://vault.azure.net", "https://vault.azure.net/"),
             "identity.resource")
    access = token.get("access_token")
    _require(_text(access) and "\n" not in access and "\r" not in access
             and token.get("token_type", "").lower() == "bearer", "identity.token")
    values = []
    for key in ("username_secret_url", "password_secret_url"):
        secret = json.loads(_fetch(inputs["credentials"][key] + "?api-version=7.4",
                                   headers={"Authorization": "Bearer " + access}, limit=32768,
                                   timeout=timeout()))
        value = secret.get("value")
        _require(secret.get("attributes", {}).get("enabled") is True
                 and isinstance(value, str) and 0 < len(value.encode("utf-8")) <= 16384
                 and not any(ch in value for ch in ("\r", "\n", "\x00")), "credentials.unusable")
        values.append(value)
    _require(":" not in values[0], "credentials.username")
    return values[0], values[1]


def _jar(data: bytes, expected: str) -> None:
    _require(hashlib.sha256(data).hexdigest() == expected, "plugins.sha256")
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        entries = archive.infolist()
        _require(bool(entries) and sum(i.file_size for i in entries) <= 512 * 1024**2
                 and all(not i.flag_bits & 1 for i in entries), "plugins.zip")
        _require(archive.testzip() is None and "META-INF/MANIFEST.MF" in archive.namelist(),
                 "plugins.zip")


def _check_plugins(inputs: dict, *, missing_ok: bool = False) -> list[dict]:
    directory = Path(inputs["storage"]["plugins_dir"])
    _safe_path(directory)
    _root_controlled(directory, missing_ok=missing_ok)
    expected = {p["filename"]: p for p in inputs["plugins"]}
    if directory.exists():
        st = directory.stat()
        _require(directory.is_dir() and st.st_uid == 0 and st.st_gid == inputs["storage"]["gid"]
                 and st.st_mode & 0o055 == 0o055 and st.st_mode & 0o022 == 0,
                 "plugins.directory")
        for path in directory.iterdir():
            _require(not path.is_symlink(), "plugins.symlink")
            if path.suffix.lower() == ".jar":
                _require(path.name in expected, "plugins.unexpected")
    missing = []
    for p in inputs["plugins"]:
        path = directory / p["filename"]
        if path.exists():
            _require(path.is_file() and path.stat().st_size <= 100 * 1024**2
                     and path.stat().st_mode & 0o444 == 0o444
                     and path.stat().st_mode & 0o022 == 0
                     and path.stat().st_uid == 0
                     and path.stat().st_gid == inputs["storage"]["gid"], "plugins.file")
            _jar(path.read_bytes(), p["sha256"])
        else:
            missing.append(p)
    _require(missing_ok or not missing, "plugins.missing")
    return missing


def ensure_plugins(inputs: dict) -> None:
    """Reuse verified JARs or publish hash-checked bundled/downloaded JARs without clobbering."""
    validate_inputs(inputs)
    _host()
    _storage(inputs)
    missing = _check_plugins(inputs, missing_ok=True)
    staged = []
    directory = Path(inputs["storage"]["plugins_dir"])
    try:
        # Verify every missing artifact before publishing any of them.
        payloads = []
        for p in missing:
            data = None
            if p["bundled_path"]:
                bundled = Path(p["bundled_path"])
                _safe_path(bundled)
                if bundled.exists():
                    _require(bundled.is_file() and bundled.stat().st_size <= 100 * 1024**2,
                             "plugins.bundle")
                    raw = bundled.read_bytes()
                    if hashlib.sha256(raw).hexdigest() == p["sha256"]:
                        _jar(raw, p["sha256"])
                        data = raw
            if data is None:
                data = _fetch(p["url"], limit=100 * 1024**2)
                _jar(data, p["sha256"])
            payloads.append((p, data))
        if not missing:
            return
        if not directory.exists():
            _require(directory.parent.is_dir(), "plugins.parent")
            directory.mkdir(mode=0o755)
            os.chown(directory, 0, inputs["storage"]["gid"])
            directory.chmod(0o755)
        _safe_path(directory)
        _require(directory.stat().st_dev == Path(inputs["storage"]["mount"]).stat().st_dev,
                 "plugins.filesystem")
        for p, data in payloads:
            fd, name = tempfile.mkstemp(prefix=".neo4j-jar-", dir=directory)
            staged.append(Path(name))
            with os.fdopen(fd, "wb") as out:
                out.write(data)
                out.flush()
                os.fsync(out.fileno())
                os.fchown(out.fileno(), 0, inputs["storage"]["gid"])
                os.fchmod(out.fileno(), 0o644)
            os.link(name, directory / p["filename"])
    finally:
        for path in staged:
            path.unlink(missing_ok=True)


def guard(inputs: dict) -> None:
    """Every-start guard: root, mounted stores, exact config/JARs and both vault secrets."""
    validate_inputs(inputs)
    _host()
    _port_free()
    _storage(inputs)
    path = Path("/etc/neo4j/neo4j.conf")
    _safe_path(path)
    _root_controlled(path)
    _require(hashlib.sha256(path.read_bytes()).hexdigest() == inputs["config"]["sha256"],
             "config.installed")
    _check_plugins(inputs)
    _credentials(inputs)


def _versions_response(body: bytes) -> None:
    result = json.loads(body)
    errors = result.get("errors")
    if (isinstance(errors, list) and errors
            and all(isinstance(e, dict) and isinstance(e.get("code"), str)
                    and e["code"].startswith("Neo.TransientError.") for e in errors)):
        raise _Starting()
    _require(result.get("errors") == [], "readiness.errors")
    results = result.get("results")
    _require(type(results) is list and len(results) == 3, "readiness.results")
    expected = [
        (["name", "versions", "edition"], ["Neo4j Kernel", [VERSIONS["neo4j"]], "community"]),
        (["apoc_version"], [VERSIONS["apoc"]]),
        (["gds_version"], [VERSIONS["gds"]]),
    ]
    for row, (columns, values) in zip(results, expected):
        _require(row.get("columns") == columns and type(row.get("data")) is list
                 and len(row["data"]) == 1 and row["data"][0].get("row") == values,
                 "readiness.versions")


def _tcp_rows(table: str) -> list[list[str]]:
    lines = Path("/proc/net/" + table).read_text(encoding="utf-8").splitlines()
    _require(bool(lines) and "local_address" in lines[0], "readiness.peer")
    rows = [line.split() for line in lines[1:]]
    _require(all(len(r) >= 10 and re.fullmatch(r"[0-9A-F]+:[0-9A-F]{4}", r[1])
                 and re.fullmatch(r"[0-9A-F]+:[0-9A-F]{4}", r[2])
                 and re.fullmatch(r"[0-9A-F]{2}", r[3]) and r[9].isdigit()
                 for r in rows), "readiness.peer")
    return rows


def _port_free() -> None:
    try:
        _require(not any(r[1].endswith(":1D32") and r[3] != "06"
                         for table in ("tcp", "tcp6") for r in _tcp_rows(table)),
                 "readiness.port_occupied")
    except OSError:
        raise _Refused("readiness.peer") from None


def _in_unit(pid: str, group: str) -> bool:
    lines = Path("/proc", pid, "cgroup").read_text(encoding="utf-8").splitlines()
    for line in lines:
        _, controllers, path = line.split(":", 2)
        if controllers in ("", "name=systemd"):
            return path == group or path.startswith(group + "/")
    return False


def _connected_peer(sock, deadline: float) -> None:
    """Attest the accepted server socket of this TCP connection, not its listener."""
    try:
        _require(sock.getpeername() == ("127.0.0.1", 7474)
                 and sock.getsockname()[0] == "127.0.0.1", "readiness.peer")
        show = _run(["/usr/bin/systemctl", "show", "neo4j.service",
                     "--property=MainPID", "--property=ControlGroup"], timeout=_remaining(deadline))
        fields = dict(line.split("=", 1) for line in show.stdout.splitlines() if "=" in line)
        pid, group = fields.get("MainPID", ""), fields.get("ControlGroup", "")
        _require(pid.isdigit() and int(pid) > 0 and group == "/system.slice/neo4j.service"
                 and _in_unit(pid, group), "readiness.peer")
        remote = f"0100007F:{sock.getsockname()[1]:04X}"
        end = min(deadline, time.monotonic() + 1)
        while True:
            matches = [r for r in _tcp_rows("tcp")
                       if r[1] == "0100007F:1D32" and r[2] == remote and r[3] == "01"]
            _require(len(matches) <= 1, "readiness.peer")
            if matches and matches[0][9] != "0":
                inode = "socket:[" + matches[0][9] + "]"
                for process in Path("/proc").iterdir():
                    _remaining(end)
                    if not process.name.isdigit():
                        continue
                    try:
                        if not _in_unit(process.name, group):
                            continue
                        for fd in (process / "fd").iterdir():
                            _remaining(end)
                            try:
                                owner = str(fd.readlink()) == inode
                            except FileNotFoundError:
                                continue
                            if owner:
                                _require(_in_unit(process.name, group) and _in_unit(pid, group),
                                         "readiness.peer")
                                return
                    except FileNotFoundError:
                        continue  # A disappearing PID cannot establish ownership.
            _require(time.monotonic() + 0.05 < end, "readiness.peer")
            time.sleep(0.05)  # The unit may not have accepted the socket yet.
    except _Refused:
        raise
    except (OSError, ValueError):
        raise _Refused("readiness.peer") from None


def _probe(payload: bytes, auth: str, deadline: float) -> bytes:
    conn = http.client.HTTPConnection("127.0.0.1", 7474, timeout=_remaining(deadline))
    try:
        conn.connect()  # No headers/credentials before connected-peer attestation.
        conn.auto_open = 0  # Never reconnect (or resend auth) to an unattested socket.
        sock = conn.sock
        _connected_peer(sock, deadline)
        _require(conn.sock is sock and sock.fileno() >= 0
                 and sock.getpeername() == ("127.0.0.1", 7474), "readiness.connection")
        sock.settimeout(_remaining(deadline))
        conn.request("POST", "/db/neo4j/tx/commit", body=payload,
                     headers={"Authorization": "Basic " + auth, "Content-Type": "application/json"})
        sock.settimeout(_remaining(deadline))
        response = conn.getresponse()
        if response.status in (502, 503):
            raise _Starting()
        _require(response.status == 200, "readiness.http")
        sock.settimeout(_remaining(deadline))
        body = response.read(1048577)
        _remaining(deadline)
        _require(len(body) <= 1048576, "http.size")
        return body
    finally:
        conn.close()


@contextmanager
def _readiness_deadline():
    # Linux guest CLI is single-threaded. A socket inactivity timeout alone does
    # not bound a response which trickles headers/body forever.
    def expired(*_):
        raise _Refused("readiness.timeout")
    previous = signal.getsignal(signal.SIGALRM)
    signal.signal(signal.SIGALRM, expired)
    try:
        signal.setitimer(signal.ITIMER_REAL, 120)
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def readiness(inputs: dict) -> None:
    """Authenticated localhost HTTP metadata probe; not an algorithm or application E2E test."""
    validate_inputs(inputs)
    _host()
    with _readiness_deadline():
        _await_ready(inputs)


def _await_ready(inputs: dict) -> None:
    deadline = time.monotonic() + 120
    user, password = _credentials(inputs, deadline=deadline)
    auth = base64.b64encode((user + ":" + password).encode("utf-8")).decode("ascii")
    statements = [
        "CALL dbms.components() YIELD name, versions, edition RETURN name, versions, edition",
        "RETURN apoc.version() AS apoc_version", "RETURN gds.version() AS gds_version",
    ]
    payload = json.dumps({"statements": [{"statement": s} for s in statements]}).encode("utf-8")
    while True:
        try:
            body = _probe(payload, auth, deadline)
            _versions_response(body)
            return
        except (_Starting, ConnectionError, TimeoutError):
            _require(deadline - time.monotonic() > 2, "readiness.timeout")
            time.sleep(2)


@contextmanager
def _policy(path: Path):
    _safe_path(path)
    existed = path.exists()
    if existed:
        st = path.stat()
        _require(stat.S_ISREG(st.st_mode) and st.st_uid == 0 and st.st_mode & 0o022 == 0,
                 "packages.policy")
        old, mode = path.read_bytes(), stat.S_IMODE(st.st_mode)
    try:
        _atomic(path, b"#!/bin/sh\nexit 101\n", 0o755)
        yield
    finally:
        if existed:
            _atomic(path, old, mode)
        else:
            path.unlink(missing_ok=True)


def _account(inputs: dict) -> None:
    uid, gid = (inputs["storage"][key] for key in ("uid", "gid"))
    for module, getter, by_id, number, field in (
        (grp, "getgrnam", "getgrgid", gid, "gr_gid"),
        (pwd, "getpwnam", "getpwuid", uid, "pw_uid"),
    ):
        try:
            entry = getattr(module, getter)("neo4j")
        except KeyError:
            try:
                getattr(module, by_id)(number)
            except KeyError:
                if module is grp:
                    _run(["/usr/sbin/groupadd", "--gid", str(gid), "neo4j"])
                else:
                    _run(["/usr/sbin/useradd", "--uid", str(uid), "--gid", str(gid),
                          "--no-create-home", "--home-dir", "/var/lib/neo4j",
                          "--shell", "/usr/sbin/nologin", "neo4j"])
            else:
                raise ValueError("packages.id_collision") from None
        else:
            _require(getattr(entry, field) == number and
                     (module is grp or entry.pw_gid == gid), "packages.account")


def _install(inputs: dict) -> str:
    _run(["/usr/bin/systemctl", "mask", "neo4j.service"])
    with _policy(Path("/usr/sbin/policy-rc.d")):
        _account(inputs)
        a = inputs["apt"]
        key = _fetch(a["key_url"], limit=1024 * 1024)
        _require(hashlib.sha256(key).hexdigest() == a["key_sha256"]
                 and key.startswith(b"-----BEGIN PGP PUBLIC KEY BLOCK-----")
                 and b"-----END PGP PUBLIC KEY BLOCK-----" in key, "packages.key")
        directory = Path("/etc/apt/keyrings")
        _safe_path(directory)
        if not directory.exists():
            directory.mkdir(mode=0o755)
            directory.chmod(0o755)
        _atomic(directory / "neo4j-bootstrap.asc", key, 0o644)
        _atomic(Path("/etc/apt/sources.list.d/neo4j-bootstrap.list"),
                b"deb [signed-by=/etc/apt/keyrings/neo4j-bootstrap.asc] https://debian.neo4j.com stable 5\n",
                0o644)
        _run(["/usr/bin/apt-get", "update"], timeout=600)
        _run(["/usr/bin/apt-get", "install", "-y", "--no-install-recommends",
              "openjdk-21-jre-headless", "neo4j=" + a["neo4j_package_version"]], timeout=900)
    files = _run(["/usr/bin/dpkg-query", "-L", "openjdk-21-jre-headless"]).stdout.splitlines()
    candidates = [Path(p) for p in files if re.fullmatch(r"/usr/lib/jvm/[A-Za-z0-9_.+-]+/bin/java", p)]
    _require(len(candidates) == 1, "packages.java")
    java = candidates[0]
    _safe_path(java.parent.parent)
    # java -version conventionally writes its version to stderr; never print it.
    version = _run([str(java), "-version"])
    _require(re.search(r'version "21(?:[."]|$)', version.stderr + version.stdout) is not None,
             "packages.java")
    installed = _run(["/usr/bin/dpkg-query", "-W", "-f=${Version}", "neo4j"]).stdout.strip()
    _require(installed == inputs["apt"]["neo4j_package_version"], "packages.neo4j")
    return str(java.parent.parent)


def _durable_mount(inputs: dict) -> None:
    s = inputs["storage"]
    path = Path("/etc/fstab")
    _safe_path(path)
    text = path.read_text(encoding="utf-8")
    desired = f"UUID={s['filesystem_uuid']} {s['mount']} {s['filesystem']} defaults,nodev,nosuid 0 2"
    found = False
    for line in text.splitlines():
        fields = line.split()
        if fields and not fields[0].startswith("#") and len(fields) >= 2:
            if fields[0] == "UUID=" + s["filesystem_uuid"] or fields[1] == s["mount"]:
                _require(line == desired, "storage.fstab_conflict")
                found = True
    if not found:
        _atomic(path, (text.rstrip("\n") + "\n" + desired + "\n").encode("utf-8"), 0o644)


def _unit(inputs: dict, java_home: str) -> None:
    directory = Path("/etc/systemd/system/neo4j.service.d")
    _safe_path(directory)
    _root_controlled(directory, missing_ok=True)
    _root_controlled(Path("/etc/neo4j/bootstrap-java.env"), missing_ok=True)
    directory.mkdir(mode=0o755, exist_ok=True)
    _atomic(Path("/etc/neo4j/bootstrap-java.env"), ("JAVA_HOME=" + java_home + "\n").encode("utf-8"))
    text = (
        "[Unit]\nRequiresMountsFor=" + inputs["storage"]["mount"] + "\n"
        "[Service]\nEnvironment=\"JAVA_HOME=" + java_home + "\"\n"
        "EnvironmentFile=/etc/neo4j/bootstrap-java.env\n"
        "TimeoutStartSec=240\n"
        "ExecStartPre=+/usr/bin/python3 -I " + HELPER + " guard --manifest " + MANIFEST + "\n"
        "ExecStartPost=+/usr/bin/python3 -I " + HELPER + " readiness --manifest " + MANIFEST + "\n"
    )
    _atomic(directory / "retained-bootstrap.conf", text.encode("utf-8"), 0o644)


def bootstrap(inputs: dict) -> None:
    """Guest-only installation. Strict validation precedes every side effect."""
    validate_inputs(inputs)
    _host()
    _port_free()
    active = _run(["/usr/bin/systemctl", "is-active", "neo4j.service"], check=False)
    processes = _run(["/usr/bin/pgrep", "-f", r"org\.neo4j\.(server|dbms)"], check=False)
    _require(active.returncode in (3, 4) and processes.returncode == 1, "guest.already_running")
    _storage(inputs, allow_mount=True)
    _durable_mount(inputs)
    java_home = _install(inputs)
    _storage(inputs)
    ensure_plugins(inputs)
    _root_controlled(Path("/etc/neo4j/neo4j.conf"), missing_ok=True)
    _atomic(Path("/etc/neo4j/neo4j.conf"), inputs["config"]["neo4j_conf"].encode("utf-8"), 0o644)
    # Establish credential availability before unmasking. Authentication against
    # the unchanged retained system store is proven by ExecStartPost, never reset.
    guard(inputs)
    _unit(inputs, java_home)
    _run(["/usr/bin/systemctl", "daemon-reload"])
    _run(["/usr/bin/systemctl", "unmask", "neo4j.service"])
    started = False
    try:
        _run(["/usr/bin/systemctl", "enable", "neo4j.service"])
        started = True
        _run(["/usr/bin/systemctl", "start", "neo4j.service"], timeout=270)
    except Exception as failure:
        codes = []
        try:
            if started:
                try:
                    result = _run(["/usr/bin/systemctl", "stop", "neo4j.service"], check=False)
                    if result.returncode != 0:
                        codes.append("cleanup.stop")
                except Exception:
                    codes.append("cleanup.stop")
        finally:
            try:
                result = _run(["/usr/bin/systemctl", "mask", "neo4j.service"], check=False)
                if result.returncode != 0:
                    codes.append("cleanup.mask")
            except Exception:
                codes.append("cleanup.mask")
        failure.cleanup_codes = tuple(codes)
        raise


def main(argv: list[str] | None = None) -> int:
    """CLI with fixed redacted errors. No command is selected implicitly."""
    stage = "arguments"
    try:
        parser = argparse.ArgumentParser(description=__doc__)
        commands = parser.add_subparsers(dest="command", required=True)
        for name in ("validate", "render", "bootstrap", "guard", "readiness"):
            sub = commands.add_parser(name)
            sub.add_argument("--manifest", required=True)
            if name == "render":
                sub.add_argument("--output", required=True)
                sub.add_argument("--candidate", action="store_true")
        # Suppress argparse's echo of untrusted argv values.
        import contextlib
        with contextlib.redirect_stderr(io.StringIO()):
            try:
                args = parser.parse_args(argv)
            except SystemExit as exc:
                if exc.code == 0:
                    return 0
                raise ValueError("arguments") from None
        stage = args.command
        path = Path(args.manifest).expanduser()
        if stage in ("bootstrap", "guard", "readiness"):
            _require(os.geteuid() == 0, "guest.root")
            for trusted in (Path(__file__).absolute(), path.absolute()):
                _root_controlled(trusted)
                _require(stat.S_ISREG(trusted.lstat().st_mode), "path.root_trust")
            for fixed in ("/etc/neo4j/neo4j.conf", "/etc/neo4j/bootstrap-java.env",
                          "/etc/systemd/system/neo4j.service.d/retained-bootstrap.conf"):
                _root_controlled(Path(fixed), missing_ok=True)
        _require(path.stat().st_size <= 32768, "manifest.size")
        inputs = json.loads(path.read_text(encoding="utf-8"))
        if stage == "render":
            content = render_cloud_init(inputs, candidate=args.candidate)
            output = Path(args.output).expanduser().absolute()
            _atomic(output, content.encode("utf-8"))
        else:
            {"validate": validate_inputs, "bootstrap": bootstrap, "guard": guard,
             "readiness": readiness}[stage](inputs)
        print(json.dumps({"stage": stage, "status": "ok"}))
        return 0
    except Exception as exc:
        diagnostic = {"stage": stage, "code": exc.code if isinstance(exc, _Refused)
                      and exc.code in SAFE_CODES else "REFUSED"}
        cleanup = [c for c in getattr(exc, "cleanup_codes", ()) if c in ("cleanup.stop", "cleanup.mask")]
        if cleanup:
            diagnostic["cleanup"] = cleanup
        print(json.dumps(diagnostic), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())