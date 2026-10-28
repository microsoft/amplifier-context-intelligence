"""Offline tests only; never execute guest commands or contact any endpoint."""
import importlib.util
import ast
import base64
from contextlib import ExitStack, redirect_stderr, redirect_stdout
import copy
import hashlib
import http.client
import io
import json
import os
import signal
import socket
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import zipfile


HERE = Path(__file__).resolve().parent


def load():
    spec = importlib.util.spec_from_file_location("retained_bootstrap", HERE / "bootstrap.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def jar(label="fixture"):
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as archive:
        archive.writestr(zipfile.ZipInfo("META-INF/MANIFEST.MF"), "Manifest-Version: 1.0\n")
        archive.writestr(zipfile.ZipInfo("fixture.txt"), label)
    return out.getvalue()


def example():
    return json.loads((HERE / "inputs.example.json").read_text(encoding="utf-8"))


def complete(root="/mnt/retained"):
    """Synthetic inputs only; no identifiers or hashes from a real deployment."""
    m = example()
    s = m["storage"]
    s.update(partition=1, filesystem_uuid="aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
             filesystem="ext4", mount=root, data_dir=root + "/data",
             transaction_logs_dir=root + "/transactions",
             database_store=root + "/data/databases/neo4j",
             system_store=root + "/data/databases/system",
             store_markers=[root + "/data/databases/neo4j/neostore",
                            root + "/data/databases/system/neostore"],
             plugins_dir=root + "/plugins", uid=1000, gid=1000)
    m["apt"] = {"neo4j_package_version": "5.26.27",
                "key_url": "https://artifacts.example.invalid/key.asc", "key_sha256": "a" * 64}
    m["identity"] = {"kind": "user-assigned",
                     "client_id": "aaaaaaaa-0000-4000-8000-000000000001"}
    m["credentials"]["version_policy"] = "latest"
    for p in m["plugins"]:
        p["filename"] = ("apoc-5.26.27-core.jar" if p["name"] == "apoc"
                         else "neo4j-graph-data-science-2.13.11.jar")
        p["sha256"] = hashlib.sha256(jar(p["name"])).hexdigest()
        p["url"] = "https://artifacts.example.invalid/" + p["filename"]
    set_config(m)
    return m


def set_config(m, extra=""):
    s = m["storage"]
    conf = "\n".join([
        "dbms.security.auth_enabled=true",
        "server.directories.data=" + s["data_dir"],
        "server.directories.transaction.logs.root=" + s["transaction_logs_dir"],
        "server.directories.plugins=" + s["plugins_dir"],
        "server.http.enabled=true", "server.http.listen_address=127.0.0.1:7474",
        "server.https.enabled=false", "server.bolt.enabled=true",
        "server.bolt.listen_address=0.0.0.0:7687",
        "dbms.security.procedures.allowlist=apoc.*,gds.*,db.*,dbms.*",
        "dbms.security.procedures.unrestricted=gds.*",
    ]) + extra
    m["config"] = {"neo4j_conf": conf, "sha256": hashlib.sha256(conf.encode()).hexdigest()}


def response():
    return {"errors": [], "results": [
        {"columns": ["name", "versions", "edition"],
         "data": [{"row": ["Neo4j Kernel", ["5.26.27"], "community"]}]},
        {"columns": ["apoc_version"], "data": [{"row": ["5.26.27"]}]},
        {"columns": ["gds_version"], "data": [{"row": ["2.13.11"]}]},
    ]}


class OfflineTest(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        # Any accidental real guest boundary, even in a failure path, fails the test.
        self.stack.enter_context(patch("subprocess.run", side_effect=AssertionError("guest command escaped")))
        self.stack.enter_context(patch("urllib.request.OpenerDirector.open",
                                       side_effect=AssertionError("network escaped")))
        self.stack.enter_context(patch("http.client.HTTPConnection.connect",
                                       side_effect=AssertionError("HTTP escaped")))
        self.stack.enter_context(patch("socket.create_connection",
                                       side_effect=AssertionError("socket escaped")))
        self.b = load()
        self.stack.enter_context(patch.object(signal, "setitimer"))
        self.stack.enter_context(patch.object(signal, "signal"))
        self.stack.enter_context(patch.object(signal, "getsignal", return_value=signal.SIG_DFL))
        self.real_port_free = self.b._port_free
        self.mock("_port_free")  # Guest proc inspection is opt-in to synthetic fixtures only.

    def mock(self, name, **kwargs):
        return self.stack.enter_context(patch.object(self.b, name, **kwargs))


class AvailabilityTests(unittest.TestCase):
    def test_bootstrap_module_exists(self):
        self.assertTrue(
            (HERE / "bootstrap.py").is_file(),
            "retained-store bootstrap implementation is missing",
        )


class ValidationTests(OfflineTest):
    def test_complete_manifest(self):
        self.b.validate_inputs(complete())

    def test_null_example_is_not_deployable(self):
        with self.assertRaises(ValueError):
            self.b.validate_inputs(example())
        with self.assertRaises(ValueError):
            self.b.render_cloud_init(example())

    def test_candidate_bootstrap_refuses_before_any_guest_boundary(self):
        host = self.mock("_host", side_effect=AssertionError("host touched"))
        with self.assertRaises(ValueError):
            self.b.bootstrap(example())
        host.assert_not_called()

    def test_bad_inputs_field_names_only(self):
        cases = [("partition", True), ("lun", 1), ("filesystem", "btrfs"),
                 ("filesystem_uuid", "PASSWORD_CANARY"), ("uid", False),
                 ("mount", "/"), ("plugins_dir", "/tmp/outside"),
                 ("data_dir", "/mnt/retained/../escape")]
        for key, value in cases:
            with self.subTest(key=key):
                m = complete()
                m["storage"][key] = value
                with self.assertRaises(ValueError) as caught:
                    self.b.validate_inputs(m)
                self.assertNotIn("PASSWORD_CANARY", str(caught.exception))
        m = complete()
        m["identity"]["client_id"] = "PASSWORD_CANARY"
        with self.assertRaises(ValueError) as caught:
            self.b.validate_inputs(m)
        self.assertNotIn("PASSWORD_CANARY", str(caught.exception))

    def test_unknown_keys_and_candidate_secrets_refused(self):
        for candidate in (True, False):
            m = example() if candidate else complete()
            m["credentials"]["password"] = "PASSWORD_CANARY"
            with self.assertRaises(ValueError):
                self.b.render_cloud_init(m, candidate=candidate)
        m = example()
        m["storage"]["uid"] = True
        with self.assertRaises(ValueError):
            self.b.render_cloud_init(m, candidate=True)

    def test_versions_and_package_pin(self):
        for package in ("latest", "1:5.26.0", "5.26.270", "1:5.26.270",
                        "5.26.27;touch /tmp/x", "5.26.27\n"):
            m = complete()
            m["apt"]["neo4j_package_version"] = package
            with self.assertRaises(ValueError):
                self.b.validate_inputs(m)
        m = complete()
        m["apt"]["neo4j_package_version"] = "1:5.26.27"
        self.b.validate_inputs(m)
        m["versions"]["neo4j"] = "5.26.22"
        with self.assertRaises(ValueError):
            self.b.validate_inputs(m)

    def test_https_artifact_rules(self):
        for url in ("http://example.invalid/a", "https://u:p@example.invalid/a",
                    "https://example.invalid/a?token=x", "https://example.invalid/a#frag",
                    "https://example.invalid/a\n", "https://example.invalid/%0a"):
            for field in ("key_url", "plugin"):
                m = complete()
                if field == "plugin":
                    m["plugins"][0]["url"] = url
                else:
                    m["apt"][field] = url
                with self.subTest(url=url, field=field), self.assertRaises(ValueError):
                    self.b.validate_inputs(m)

    def test_both_exact_secret_refs_and_explicit_version_policy(self):
        m = complete()
        self.b.validate_inputs(m)
        for key in ("username_secret_url", "password_secret_url"):
            bad = copy.deepcopy(m)
            bad["credentials"][key] = bad["credentials"][key].replace("context-intelligence-", "")
            with self.assertRaises(ValueError):
                self.b.validate_inputs(bad)
        m["credentials"]["version_policy"] = "pinned"
        with self.assertRaises(ValueError):
            self.b.validate_inputs(m)
        for key in ("username_secret_url", "password_secret_url"):
            m["credentials"][key] += "/" + "a" * 32
        self.b.validate_inputs(m)

    def test_config_integrity_and_dangerous_settings(self):
        for extra in ("\nserver.jvm.additional=-Dpassword=CANARY",
                      "\napoc.import.file.enabled=true",
                      "\ndbms.security.auth_enabled=false",
                      "\nserver.http.listen_address=0.0.0.0:7474",
                      "\n# password=PASSWORD_CANARY"):
            m = complete()
            set_config(m, extra)
            with self.subTest(extra=extra), self.assertRaises(ValueError):
                self.b.validate_inputs(m)
        m = complete()
        m["config"]["sha256"] = "a" * 64
        with self.assertRaises(ValueError):
            self.b.validate_inputs(m)

    def test_unrestricted_wildcard_and_public_bolt_refused(self):
        for old, new in (("unrestricted=gds.*", "unrestricted=*"),
                         ("unrestricted=gds.*", "unrestricted=gds.*,apoc.*"),
                         ("0.0.0.0:7687", "8.8.8.8:7687"),
                         ("data=/mnt/retained/data", "data=/tmp/data")):
            m = complete()
            text = m["config"]["neo4j_conf"].replace(old, new)
            m["config"] = {"neo4j_conf": text, "sha256": hashlib.sha256(text.encode()).hexdigest()}
            with self.assertRaises(ValueError):
                self.b.validate_inputs(m)

    def test_candidate_determinism_self_containment_and_size(self):
        rendered = self.b.render_cloud_init(example(), candidate=True)
        self.assertEqual(rendered, self.b.render_cloud_init(example(), candidate=True))
        self.assertLessEqual(len(rendered.encode()), 65535)
        self.assertIn("REVIEW CANDIDATE INCOMPLETE INPUTS BOOTSTRAP WILL REFUSE", rendered)
        config = json.loads("\n".join(line for line in rendered.splitlines() if not line.startswith("#")))
        self.assertNotIn("packages", config)
        self.assertNotIn("package_update", config)
        files = config["write_files"]
        self.assertEqual(len(files), 2)
        self.assertTrue(all(f["permissions"] == "0600" and f["owner"] == "root:root" for f in files))
        self.assertEqual(files[0]["content"], (HERE / "bootstrap.py").read_text(encoding="utf-8"))
        self.assertEqual(json.loads(files[1]["content"]), example())
        self.assertEqual(config["runcmd"], [["/usr/bin/python3", "-I", self.b.HELPER, "bootstrap",
                                            "--manifest", self.b.MANIFEST]])

    def test_checked_in_cloud_config_is_exact_regeneration(self):
        self.assertEqual((HERE / "cloud-init.yaml").read_text(encoding="utf-8"),
                         self.b.render_cloud_init(example(), candidate=True))

    def test_invalid_render_does_not_create_or_replace_output(self):
        with tempfile.TemporaryDirectory() as root:
            manifest, output = Path(root) / "m.json", Path(root) / "out.yaml"
            manifest.write_text(json.dumps(example()), encoding="utf-8")
            for prior in (None, b"original"):
                if prior is not None:
                    output.write_bytes(prior)
                with redirect_stderr(io.StringIO()), redirect_stdout(io.StringIO()):
                    rc = self.b.main(["render", "--manifest", str(manifest), "--output", str(output)])
                self.assertEqual(rc, 1)
                self.assertEqual(output.read_bytes() if output.exists() else None, prior)


class PluginTests(OfflineTest):
    def setUp(self):
        super().setUp()
        self.temp = self.stack.enter_context(tempfile.TemporaryDirectory())
        self.m = complete(self.temp)
        self.directory = Path(self.m["storage"]["plugins_dir"])
        self.mock("_storage")
        self.mock("_host")
        # Guest ownership change only: publishing/ZIP/hash/temp files are real.
        self.stack.enter_context(patch.object(self.b.os, "fchown"))
        self.stack.enter_context(patch.object(self.b.os, "chown"))
        self.metadata = {}
        real_stat = Path.stat
        def guest_stat(p, *a, **k):
            st = list(real_stat(p, *a, **k))
            st[4], st[5] = 0, 1000
            if str(p) == "/tmp":
                st[0] &= ~0o022
            for key, value in self.metadata.get(str(p), {}).items():
                st[key] = value
            return os.stat_result(st)
        self.stack.enter_context(patch.object(Path, "stat", guest_stat))
        self.stack.enter_context(patch.object(Path, "lstat", lambda p: guest_stat(p, follow_symlinks=False)))

    def install_existing(self):
        self.directory.mkdir(exist_ok=True)
        self.directory.chmod(0o755)
        for p in self.m["plugins"]:
            path = self.directory / p["filename"]
            path.write_bytes(jar(p["name"]))
            path.chmod(0o644)

    def test_reuse_zero_fetch_and_unchanged_files(self):
        self.install_existing()
        before = {p.name: (p.read_bytes(), p.stat().st_mtime_ns, p.stat().st_ino)
                  for p in self.directory.iterdir()}
        fetch = self.mock("_fetch", side_effect=AssertionError("must reuse"))
        self.b.ensure_plugins(self.m)
        self.assertEqual(before, {p.name: (p.read_bytes(), p.stat().st_mtime_ns, p.stat().st_ino)
                                  for p in self.directory.iterdir()})
        fetch.assert_not_called()

    def test_missing_both_download_and_real_hash_zip_validation(self):
        seen = []
        def fetch(url, **kwargs):
            seen.append(url)
            return jar("apoc" if "apoc-" in url else "gds")
        self.mock("_fetch", side_effect=fetch)
        self.b.ensure_plugins(self.m)
        self.assertEqual(seen, [p["url"] for p in self.m["plugins"]])
        for p in self.m["plugins"]:
            self.assertEqual((self.directory / p["filename"]).read_bytes(), jar(p["name"]))
        self.assertFalse(list(self.directory.glob(".neo4j-jar-*")))

    def test_new_plugin_directory_mode_even_with_restrictive_umask(self):
        self.mock("_fetch", side_effect=[jar("apoc"), jar("gds")])
        previous = os.umask(0o077)
        try:
            self.b.ensure_plugins(self.m)
        finally:
            os.umask(previous)
        self.assertEqual(stat.S_IMODE(self.directory.stat().st_mode), 0o755)

    def test_bundle_then_gds_fetch(self):
        bundle = Path(self.temp) / "bundle.jar"
        bundle.write_bytes(jar("apoc"))
        approved = "/usr/share/neo4j/labs/apoc-5.26.27-core.jar"
        self.m["plugins"][0]["bundled_path"] = approved
        real_bytes, real_exists, real_stat, real_file = Path.read_bytes, Path.exists, Path.stat, Path.is_file
        def read(p):
            return bundle.read_bytes() if str(p) == approved else real_bytes(p)
        def exists(p):
            return True if str(p) == approved else real_exists(p)
        def getstat(p, *args, **kwargs):
            return real_stat(bundle, *args, **kwargs) if str(p) == approved else real_stat(p, *args, **kwargs)
        def is_file(p):
            return True if str(p) == approved else real_file(p)
        with patch.object(Path, "read_bytes", read), patch.object(Path, "exists", exists), \
                patch.object(Path, "stat", getstat), patch.object(Path, "is_file", is_file), \
                patch.object(self.b, "_safe_path"):
            fetch = self.mock("_fetch", return_value=jar("gds"))
            self.b.ensure_plugins(self.m)
            self.assertEqual(fetch.call_args.args[0], self.m["plugins"][1]["url"])
            self.assertEqual(fetch.call_count, 1)

    def test_missing_bundle_falls_back_to_exact_url(self):
        self.m["plugins"][0]["bundled_path"] = "/usr/share/neo4j/labs/apoc-5.26.27-core.jar"
        # An absent optional bundle is not a reason to skip approved APOC provisioning.
        fetch = self.mock("_fetch", side_effect=[jar("apoc"), jar("gds")])
        self.b.ensure_plugins(self.m)
        self.assertEqual(fetch.call_args_list[0].args[0], self.m["plugins"][0]["url"])

    def test_mismatched_bundle_uses_exact_approved_fallback(self):
        approved = "/usr/share/neo4j/labs/apoc-5.26.27-core.jar"
        bundle = Path(self.temp) / "bundle.jar"
        bundle.write_bytes(jar("wrong-bundle"))
        self.m["plugins"][0]["bundled_path"] = approved
        read, exists, getstat, is_file = Path.read_bytes, Path.exists, Path.stat, Path.is_file
        with patch.object(Path, "read_bytes", lambda p: read(bundle) if str(p) == approved else read(p)), \
                patch.object(Path, "exists", lambda p: True if str(p) == approved else exists(p)), \
                patch.object(Path, "stat", lambda p, *a, **k: getstat(bundle if str(p) == approved else p, *a, **k)), \
                patch.object(Path, "is_file", lambda p: True if str(p) == approved else is_file(p)), \
                patch.object(self.b, "_safe_path"):
            fetch = self.mock("_fetch", side_effect=[jar("apoc"), jar("gds")])
            self.b.ensure_plugins(self.m)
        self.assertEqual(fetch.call_args_list[0].args[0], self.m["plugins"][0]["url"])
        self.assertEqual((self.directory / self.m["plugins"][0]["filename"]).read_bytes(), jar("apoc"))

    def test_bad_hash_and_corrupt_zip_publish_nothing(self):
        for data in (b"not-a-jar", jar("other")):
            with self.subTest(data=data[:10]):
                self.mock("_fetch", side_effect=[jar("apoc"), data])
                with self.assertRaises(ValueError):
                    self.b.ensure_plugins(self.m)
                self.assertFalse(self.directory.exists())
        broken = b"not-a-zip"
        self.m["plugins"][0]["sha256"] = hashlib.sha256(broken).hexdigest()
        self.mock("_fetch", return_value=broken)
        with self.assertRaises(zipfile.BadZipFile):
            self.b.ensure_plugins(self.m)
        self.assertFalse(self.directory.exists())

    def test_unexpected_symlink_and_existing_mismatch_are_not_overwritten(self):
        self.install_existing()
        fetch = self.mock("_fetch", side_effect=AssertionError("unexpected fetch"))
        extra = self.directory / "unexpected.jar"
        extra.write_bytes(jar())
        with self.assertRaises(ValueError):
            self.b.ensure_plugins(self.m)
        extra.unlink()
        path = self.directory / self.m["plugins"][0]["filename"]
        original = path.read_bytes()
        path.write_bytes(b"mismatch")
        with self.assertRaises(ValueError):
            self.b.ensure_plugins(self.m)
        self.assertEqual(path.read_bytes(), b"mismatch")
        path.unlink()
        path.symlink_to(Path(self.temp) / "absent")
        with self.assertRaises(ValueError):
            self.b.ensure_plugins(self.m)
        self.assertTrue(path.is_symlink())
        fetch.assert_not_called()
        self.assertNotEqual(original, b"mismatch")

    def test_guard_fails_before_directory_creation(self):
        self.mock("_storage", side_effect=ValueError("storage.marker"))
        fetch = self.mock("_fetch")
        with self.assertRaises(ValueError):
            self.b.ensure_plugins(self.m)
        self.assertFalse(self.directory.exists())
        fetch.assert_not_called()

    def test_atomic_publish_cannot_clobber_racing_destination(self):
        self.mock("_fetch", side_effect=[jar("apoc"), jar("gds")])
        link = os.link
        def race(src, dst):
            Path(dst).write_bytes(b"racing-owner")
            link(src, dst)
        with patch.object(self.b.os, "link", side_effect=race):
            with self.assertRaises(FileExistsError):
                self.b.ensure_plugins(self.m)
        self.assertEqual((self.directory / self.m["plugins"][0]["filename"]).read_bytes(), b"racing-owner")
        self.assertFalse(list(self.directory.glob(".neo4j-jar-*")))

    def test_service_owned_jar_or_directory_is_untrusted_without_repair(self):
        self.install_existing()
        before = {p: p.read_bytes() for p in self.directory.iterdir()}
        for path in (self.directory, self.directory / self.m["plugins"][0]["filename"]):
            self.metadata[str(path)] = {4: 1000}
            with self.subTest(path=path), self.assertRaises(ValueError):
                self.b.ensure_plugins(self.m)
            self.metadata.clear()
        self.assertEqual(before, {p: p.read_bytes() for p in self.directory.iterdir()})
        self.b.os.chown.assert_not_called()

    def test_plugin_parent_and_mount_ancestry_root_controlled_even_when_missing(self):
        for mode, uid in ((stat.S_IFDIR | 0o775, 0), (stat.S_IFDIR | 0o755, 1000)):
            self.metadata[self.temp] = {0: mode, 4: uid}
            fetch = self.mock("_fetch", side_effect=[jar("apoc"), jar("gds")])
            with self.subTest(mode=mode, uid=uid), self.assertRaises(ValueError):
                self.b.ensure_plugins(self.m)
            fetch.assert_not_called()
            self.assertFalse(self.directory.exists())

    def test_plugin_parent_symlink_refused(self):
        elsewhere = Path(self.temp) / "elsewhere"
        elsewhere.mkdir()
        self.directory.symlink_to(elsewhere, target_is_directory=True)
        fetch = self.mock("_fetch")
        with self.assertRaises(ValueError):
            self.b.ensure_plugins(self.m)
        fetch.assert_not_called()

    def test_second_publish_collision_keeps_first_and_never_overwrites_second(self):
        self.mock("_fetch", side_effect=[jar("apoc"), jar("gds")])
        link = os.link
        count = 0
        def race(src, dst):
            nonlocal count
            count += 1
            if count == 2:
                Path(dst).write_bytes(b"racing-owner")
            link(src, dst)
        with patch.object(self.b.os, "link", side_effect=race), self.assertRaises(FileExistsError):
            self.b.ensure_plugins(self.m)
        self.assertEqual((self.directory / self.m["plugins"][0]["filename"]).read_bytes(), jar("apoc"))
        self.assertEqual((self.directory / self.m["plugins"][1]["filename"]).read_bytes(), b"racing-owner")
        self.assertFalse(list(self.directory.glob(".neo4j-jar-*")))


class CredentialTests(OfflineTest):
    def secrets(self, *, enabled=True, value="PASSWORD_CANARY"):
        return [
            json.dumps({"client_id": complete()["identity"]["client_id"],
                        "access_token": "TOKEN_CANARY", "token_type": "Bearer",
                        "resource": "https://vault.azure.net"}).encode(),
            json.dumps({"value": "USER_CANARY", "attributes": {"enabled": True}}).encode(),
            json.dumps({"value": value, "attributes": {"enabled": enabled}}).encode(),
        ]

    def test_selected_identity_and_both_credentials_in_memory_only(self):
        fetch = self.mock("_fetch", side_effect=self.secrets())
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            self.assertEqual(self.b._credentials(complete()), ("USER_CANARY", "PASSWORD_CANARY"))
        calls = fetch.call_args_list
        self.assertIn("client_id=aaaaaaaa-0000-4000-8000-000000000001", calls[0].args[0])
        self.assertIn("resource=https%3A%2F%2Fvault.azure.net", calls[0].args[0])
        self.assertEqual(calls[0].kwargs["headers"], {"Metadata": "true"})
        for i, key in enumerate(("username_secret_url", "password_secret_url"), 1):
            self.assertEqual(calls[i].args[0], complete()["credentials"][key] + "?api-version=7.4")
            self.assertEqual(calls[i].kwargs["headers"], {"Authorization": "Bearer TOKEN_CANARY"})
        self.assertEqual(out.getvalue() + err.getvalue(), "")

    def test_identity_mismatch_never_reads_vault(self):
        fetch = self.mock("_fetch", return_value=b'{"client_id":"wrong","access_token":"TOKEN_CANARY"}')
        with self.assertRaises(ValueError):
            self.b._credentials(complete())
        self.assertEqual(fetch.call_count, 1)

    def test_disabled_missing_and_unauthorized_fail(self):
        for values in (self.secrets(enabled=False), self.secrets(value=""),
                       [OSError("TOKEN_CANARY unauthorized")]):
            self.mock("_fetch", side_effect=values)
            with self.subTest(values=len(values)), self.assertRaises((ValueError, OSError)):
                self.b._credentials(complete())

    def test_readiness_authenticated_localhost_response_parser(self):
        self.mock("_host")
        self.mock("_fetch", side_effect=self.secrets())
        probe = self.mock("_probe", return_value=json.dumps(response()).encode())
        self.b.readiness(complete())
        request = probe.call_args
        encoded = request.args[1]
        self.assertEqual(base64.b64decode(encoded), b"USER_CANARY:PASSWORD_CANARY")
        statements = json.loads(request.args[0])["statements"]
        self.assertEqual(len(statements), 3)
        self.assertTrue(all("statement" in row and "PASSWORD_CANARY" not in str(row) for row in statements))

    def test_readiness_errors_missing_results_wrong_versions_edition(self):
        for mutate in (
            lambda r: r.update(errors=[{"message": "PASSWORD_CANARY"}]),
            lambda r: r.update(results=[]),
            lambda r: r["results"][1]["data"][0].update(row=["5.26.22"]),
            lambda r: r["results"][0]["data"][0].update(row=["Neo4j Kernel", ["5.26.27"], "enterprise"]),
        ):
            r = response()
            mutate(r)
            with self.assertRaises(ValueError):
                self.b._versions_response(json.dumps(r).encode())

    def test_readiness_polls_startup_within_deadline(self):
        self.mock("_host")
        self.mock("_credentials", return_value=("USER_CANARY", "PASSWORD_CANARY"))
        fetch = self.mock("_probe", side_effect=[ConnectionError("starting"), json.dumps(response()).encode()])
        with patch.object(self.b.time, "sleep") as sleep:
            self.b.readiness(complete())
        self.assertEqual(fetch.call_count, 2)
        sleep.assert_called_once()

    def test_readiness_failure_deadline_redacted(self):
        self.mock("_host")
        self.mock("_credentials", return_value=("USER_CANARY", "PASSWORD_CANARY"))
        self.mock("_probe", side_effect=ConnectionError("PASSWORD_CANARY"))
        with patch.object(self.b.time, "monotonic", side_effect=[0, 0, 121]), \
                patch.object(self.b.time, "sleep"):
            with self.assertRaises(ValueError) as caught:
                self.b.readiness(complete())
        self.assertNotIn("PASSWORD_CANARY", str(caught.exception))

    def test_fetch_no_proxy_no_redirect_and_bounded_body(self):
        class Reply:
            status = 200
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def geturl(self): return "https://artifacts.example.invalid/a"
            def read(self, limit):
                self.limit = limit
                return b"abc"
        class Opener:
            def open(self, request, timeout):
                self.request, self.timeout = request, timeout
                return reply
        reply, opener = Reply(), Opener()
        with patch.object(self.b.urllib.request, "build_opener", return_value=opener) as build:
            self.assertEqual(self.b._fetch(reply.geturl(), limit=5), b"abc")
        self.assertEqual(build.call_args.args[0].proxies, {})
        self.assertEqual(opener.timeout, 10)
        self.assertEqual(reply.limit, 6)
        with self.assertRaises(ValueError):
            self.b._NoRedirect().redirect_request(None, None, 302, "", {}, "https://elsewhere.invalid")

    def test_fetch_rejects_oversized_or_redirected_response(self):
        class Reply:
            status = 200
            url = "https://artifacts.example.invalid/a"
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def geturl(self): return self.url
            def read(self, limit): return b"x" * limit
        reply = Reply()
        opener = type("Opener", (), {"open": lambda *a, **k: reply})()
        with patch.object(self.b.urllib.request, "build_opener", return_value=opener):
            with self.assertRaises(ValueError):
                self.b._fetch(reply.url, limit=3)
            reply.url = "https://other.example.invalid/a"
            with self.assertRaises(ValueError):
                self.b._fetch("https://artifacts.example.invalid/a", limit=3)

    def test_cli_redacts_child_http_and_secret_errors(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "m.json"
            path.write_text(json.dumps(complete()), encoding="utf-8")
            self.mock("readiness", side_effect=OSError("USER_CANARY PASSWORD_CANARY TOKEN_CANARY"))
            self.mock("_root_controlled")
            out, err = io.StringIO(), io.StringIO()
            with patch.object(self.b.os, "geteuid", return_value=0), redirect_stdout(out), redirect_stderr(err):
                rc = self.b.main(["readiness", "--manifest", str(path)])
            self.assertEqual(rc, 1)
            self.assertEqual(err.getvalue(), '{"stage": "readiness", "code": "REFUSED"}\n')
            self.assertEqual(out.getvalue(), "")


class StorageAndServiceTests(OfflineTest):
    def test_guest_os_and_root_gates(self):
        with patch.object(self.b.os, "geteuid", return_value=1000):
            with self.assertRaises(ValueError):
                self.b._host()
        with patch.object(self.b.os, "geteuid", return_value=0):
            for release, expected in (('ID=ubuntu\nVERSION_ID="22.04"\n', True),
                                      ('ID=ubuntu\nVERSION_ID="24.04"\n', False),
                                      ('ID=debian\nVERSION_ID="12"\n', False)):
                with patch.object(Path, "read_text", return_value=release):
                    if expected:
                        self.b._host()
                    else:
                        with self.assertRaises(ValueError):
                            self.b._host()

    def test_guest_account_ids_existing_match_and_collision(self):
        run = self.mock("_run")
        with patch.object(self.b.grp, "getgrnam", return_value=SimpleNamespace(gr_gid=1000)), \
                patch.object(self.b.pwd, "getpwnam", return_value=SimpleNamespace(pw_uid=1000, pw_gid=1000)):
            self.b._account(complete())
        run.assert_not_called()
        with patch.object(self.b.grp, "getgrnam", side_effect=KeyError), \
                patch.object(self.b.grp, "getgrgid", return_value=SimpleNamespace(gr_name="unrelated")):
            with self.assertRaises(ValueError):
                self.b._account(complete())
        run.assert_not_called()

    def test_guest_account_creation_uses_explicit_ids_no_home_or_recursive_chown(self):
        run = self.mock("_run")
        with patch.object(self.b.grp, "getgrnam", side_effect=KeyError), \
                patch.object(self.b.grp, "getgrgid", side_effect=KeyError), \
                patch.object(self.b.pwd, "getpwnam", side_effect=KeyError), \
                patch.object(self.b.pwd, "getpwuid", side_effect=KeyError):
            self.b._account(complete())
        self.assertEqual(run.call_args_list[0].args[0], ["/usr/sbin/groupadd", "--gid", "1000", "neo4j"])
        self.assertIn("--no-create-home", run.call_args_list[1].args[0])
        self.assertTrue(all("chown" not in " ".join(c.args[0]) for c in run.call_args_list))

    def fixture_storage(self, root):
        m = complete(root)
        for key in ("data_dir", "transaction_logs_dir", "database_store", "system_store"):
            Path(m["storage"][key]).mkdir(parents=True, exist_ok=True)
        for path in m["storage"]["store_markers"]:
            Path(path).write_bytes(b"initialized-store")
        return m

    def storage_boundaries(self, m, *, fs="ext4", uuid=None, target=None, source="/dev/sdz1", partition="1"):
        real_resolve, real_stat, real_read = Path.resolve, Path.stat, Path.read_text
        def resolve(path, *a, **k):
            value = str(path)
            if value == "/dev/disk/azure/scsi1/lun0":
                return Path("/dev/sdz")
            if value == "/dev/disk/azure/scsi1/lun0-part1":
                return Path("/dev/sdz1")
            return real_resolve(path, *a, **k)
        def getstat(path, *a, **k):
            if str(path) in ("/dev/sdz", "/dev/sdz1"):
                values = list(real_stat(Path(m["storage"]["mount"])))
                values[0] = stat.S_IFBLK | 0o600
                return os.stat_result(values)
            return real_stat(path, *a, **k)
        def read(path, *a, **k):
            if str(path) == "/sys/class/block/sdz1/partition":
                return partition
            return real_read(path, *a, **k)
        self.stack.enter_context(patch.object(Path, "resolve", resolve))
        self.stack.enter_context(patch.object(Path, "stat", getstat))
        self.stack.enter_context(patch.object(Path, "read_text", read))
        def run(args, **k):
            if "lsblk" in args[0]:
                out = "sdz\n"
            elif "blkid" in args[0]:
                out = "UUID=" + m["storage"]["filesystem_uuid"] + "\nTYPE=" + fs + "\n"
            elif "findmnt" in args[0]:
                out = json.dumps({"filesystems": [{
                    "source": source, "uuid": uuid or m["storage"]["filesystem_uuid"],
                    "fstype": fs, "target": target or m["storage"]["mount"]}]})
            else:
                raise AssertionError("unexpected guest boundary")
            return subprocess.CompletedProcess(args, 0, out, "")
        return self.mock("_run", side_effect=run)

    def test_real_storage_markers_and_metadata_guard(self):
        with tempfile.TemporaryDirectory() as root:
            m = self.fixture_storage(root)
            self.storage_boundaries(m)
            self.b._storage(m)
            Path(m["storage"]["store_markers"][1]).unlink()
            with self.assertRaises(ValueError):
                self.b._storage(m)

    def test_wrong_uuid_fs_mount_source_partition_and_missing_marker(self):
        for kwargs in ({"uuid": "wrong"}, {"fs": "xfs"}, {"target": "/"}, {"source": "/dev/other"},
                       {"partition": "2"}):
            with self.subTest(kwargs=kwargs), tempfile.TemporaryDirectory() as root:
                m = self.fixture_storage(root)
                # Isolate mocked Path methods for each case.
                old = self.stack
                self.stack = ExitStack()
                try:
                    run = self.storage_boundaries(m, **kwargs)
                    with self.assertRaises(ValueError):
                        self.b._storage(m)
                    self.assertTrue(all("mount" not in c.args[0][0].split("/")[-1]
                                        for c in run.call_args_list if "findmnt" not in c.args[0][0]))
                finally:
                    self.stack.close()
                    self.stack = old

    def test_unmounted_guard_never_mounts_and_symlink_store_refused(self):
        with tempfile.TemporaryDirectory() as root:
            m = self.fixture_storage(root)
            run = self.storage_boundaries(m)
            original = run.side_effect
            def not_mounted(args, **kwargs):
                if "findmnt" in args[0]:
                    return subprocess.CompletedProcess(args, 1, "", "")
                return original(args, **kwargs)
            run.side_effect = not_mounted
            with self.assertRaises(ValueError):
                self.b._storage(m)
            self.assertFalse(any(c.args[0][0] == "/usr/bin/mount" for c in run.call_args_list))
            run.side_effect = original
            marker = Path(m["storage"]["store_markers"][0])
            marker.unlink()
            marker.symlink_to(m["storage"]["store_markers"][1])
            with self.assertRaises(ValueError):
                self.b._storage(m)

    def test_install_mask_policy_java_and_exact_pin_order_on_apt_failure_or_success(self):
        for fail, major in ((False, 21), (True, 21), (False, 20)):
            with self.subTest(fail=fail, java=major), tempfile.TemporaryDirectory() as root:
                m = complete()
                key = b"-----BEGIN PGP PUBLIC KEY BLOCK-----\nfixture\n-----END PGP PUBLIC KEY BLOCK-----\n"
                m["apt"]["key_sha256"] = hashlib.sha256(key).hexdigest()
                policy = Path(root) / "policy-rc.d"
                real_policy = self.b._policy
                events = []
                with ExitStack() as stack:
                    stack.enter_context(patch.object(self.b, "_policy", side_effect=lambda path: real_policy(policy)))
                    stack.enter_context(patch.object(self.b, "_safe_path"))
                    stack.enter_context(patch.object(self.b, "_account"))
                    stack.enter_context(patch.object(Path, "mkdir"))
                    stack.enter_context(patch.object(self.b, "_fetch", return_value=key))
                    atomic = self.b._atomic
                    def write(path, data, mode=0o600):
                        if path == policy:
                            atomic(path, data, mode)
                        else:
                            events.append("write")
                    stack.enter_context(patch.object(self.b, "_atomic", side_effect=write))
                    def run(args, **kwargs):
                        events.append(" ".join(args))
                        if "apt-get" in args[0]:
                            self.assertEqual(policy.read_bytes(), b"#!/bin/sh\nexit 101\n")
                            if fail and "install" in args:
                                raise ValueError("apt.failed")
                        out = ""
                        if "dpkg-query" in args[0]:
                            out = ("/usr/lib/jvm/java-21-fixture/bin/java\n"
                                   if "-L" in args else "5.26.27")
                        return subprocess.CompletedProcess(args, 0, out,
                                                           f'openjdk version "{major}.0.1"' if "-version" in args else "")
                    stack.enter_context(patch.object(self.b, "_run", side_effect=run))
                    if fail or major != 21:
                        with self.assertRaises(ValueError):
                            self.b._install(m)
                    else:
                        self.assertEqual(self.b._install(m), "/usr/lib/jvm/java-21-fixture")
                self.assertFalse(policy.exists())
                self.assertEqual(events[0], "/usr/bin/systemctl mask neo4j.service")
                command = next(e for e in events if "/usr/bin/apt-get install" in e)
                self.assertIn("openjdk-21-jre-headless neo4j=5.26.27", command)

    def test_first_unmounted_storage_mounts_uuid_then_rechecks_retained_stores(self):
        with tempfile.TemporaryDirectory() as root:
            m = self.fixture_storage(root)
            run = self.storage_boundaries(m)
            normal = run.side_effect
            mounted = False
            real_iter, real_stat = Path.iterdir, Path.stat
            def iterdir(p):
                return iter([]) if str(p) == root and not mounted else real_iter(p)
            def getstat(p, *a, **k):
                st = list(real_stat(p, *a, **k))
                if str(p) == root:
                    st[4] = 0
                    st[0] &= ~0o022
                return os.stat_result(st)
            def guest(args, **kwargs):
                nonlocal mounted
                if args[0] == "/usr/bin/findmnt" and not mounted:
                    return subprocess.CompletedProcess(args, 1, "", "")
                if args[0] == "/usr/bin/mount":
                    self.assertEqual(args, ["/usr/bin/mount", "-t", "ext4", "-o", "nodev,nosuid",
                                           "UUID=" + m["storage"]["filesystem_uuid"], root])
                    mounted = True
                    return subprocess.CompletedProcess(args, 0, "", "")
                return normal(args, **kwargs)
            run.side_effect = guest
            with patch.object(Path, "iterdir", iterdir), patch.object(Path, "stat", getstat):
                self.b._storage(m, allow_mount=True)
            self.assertTrue(mounted)
            self.assertEqual(sum(c.args[0][0] == "/usr/bin/findmnt" for c in run.call_args_list), 2)
            self.assertTrue(all(Path(p).read_bytes() == b"initialized-store" for p in m["storage"]["store_markers"]))

    def test_missing_store_before_any_package_or_plugin_or_write(self):
        self.mock("_host")
        self.mock("_run", side_effect=[subprocess.CompletedProcess([], 3, "", ""),
                                       subprocess.CompletedProcess([], 1, "", "")])
        self.mock("_storage", side_effect=ValueError("storage.marker"))
        writes = self.mock("_atomic", side_effect=AssertionError("write before guard"))
        install = self.mock("_install", side_effect=AssertionError("install before guard"))
        plugins = self.mock("ensure_plugins", side_effect=AssertionError("plugins before guard"))
        with self.assertRaises(ValueError):
            self.b.bootstrap(complete())
        writes.assert_not_called()
        install.assert_not_called()
        plugins.assert_not_called()

    def test_active_service_refused_never_stopped(self):
        self.mock("_host")
        run = self.mock("_run", side_effect=[subprocess.CompletedProcess([], 0, "", ""),
                                            subprocess.CompletedProcess([], 1, "", "")])
        storage = self.mock("_storage")
        with self.assertRaises(ValueError):
            self.b.bootstrap(complete())
        storage.assert_not_called()
        self.assertTrue(all("stop" not in c.args[0] for c in run.call_args_list))

    def test_bootstrap_start_order_and_no_data_chown(self):
        order = []
        self.mock("_host")
        def run(args, **kwargs):
            order.append(" ".join(args))
            rc = 3 if "is-active" in args else 1 if "pgrep" in args[0] else 0
            return subprocess.CompletedProcess(args, rc, "", "")
        self.mock("_run", side_effect=run)
        for name in ("_storage", "_durable_mount", "ensure_plugins", "_atomic", "guard", "_unit"):
            self.mock(name, side_effect=lambda *a, _name=name, **k: order.append(_name))
        self.mock("_install", side_effect=lambda *a: order.append("_install") or "/usr/lib/jvm/fixture")
        self.b.bootstrap(complete())
        self.assertLess(order.index("_storage"), order.index("_install"))
        self.assertLess(order.index("ensure_plugins"), order.index("guard"))
        self.assertLess(order.index("guard"), order.index("/usr/bin/systemctl unmask neo4j.service"))
        self.assertLess(order.index("_unit"), order.index("/usr/bin/systemctl start neo4j.service"))
        self.assertNotIn("chown -R", "\n".join(order))

    def test_policy_restore_exact_bytes_mode_on_success_and_failure(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "policy-rc.d"
            original = b"#!/bin/sh\n# original\nexit 0\n"
            path.write_bytes(original)
            path.chmod(0o751)
            # Ownership stat is a guest boundary. Only root uid is mocked.
            original_stat = Path.stat
            def root_stat(p, *args, **kwargs):
                st = original_stat(p, *args, **kwargs)
                fields = list(st)
                fields[4] = 0
                return os.stat_result(fields)
            with patch.object(Path, "stat", root_stat):
                for fail in (False, True):
                    try:
                        with self.b._policy(path):
                            self.assertEqual(path.read_bytes(), b"#!/bin/sh\nexit 101\n")
                            if fail:
                                raise RuntimeError("apt failed")
                    except RuntimeError:
                        pass
                    self.assertEqual(path.read_bytes(), original)
                    self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o751)
            path.unlink()
            with self.b._policy(path):
                self.assertTrue(path.exists())
            self.assertFalse(path.exists())

    def test_policy_symlink_refused(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "policy-rc.d"
            path.symlink_to(Path(root) / "original")
            with self.assertRaises(ValueError):
                with self.b._policy(path):
                    self.fail("symlink policy accepted")

    def test_unit_root_hooks_keep_vendor_commands_and_select_java(self):
        self.mock("_safe_path")
        self.stack.enter_context(patch.object(Path, "mkdir"))
        write = self.mock("_atomic")
        self.b._unit(complete(), "/usr/lib/jvm/java-21-fixture")
        unit = next(c.args[1].decode() for c in write.call_args_list
                    if str(c.args[0]).endswith(".conf"))
        self.assertIn("ExecStartPre=+/usr/bin/python3", unit)
        self.assertIn("ExecStartPost=+/usr/bin/python3", unit)
        self.assertNotIn("ExecStartPre=\n", unit)
        self.assertNotIn("ExecStartPost=\n", unit)
        self.assertIn("RequiresMountsFor=/mnt/retained", unit)
        self.assertIn("JAVA_HOME", unit)
        self.assertIn("EnvironmentFile=/etc/neo4j/bootstrap-java.env", unit)

    def test_every_start_guard_checks_all_boundaries(self):
        m = complete()
        events = []
        self.mock("_host", side_effect=lambda: events.append("root"))
        self.mock("_storage", side_effect=lambda inputs: events.append("storage"))
        self.mock("_check_plugins", side_effect=lambda inputs: events.append("plugins"))
        self.mock("_credentials", side_effect=lambda inputs: events.append("credentials"))
        self.mock("_safe_path")
        self.mock("_root_controlled")
        with patch.object(Path, "read_bytes", return_value=m["config"]["neo4j_conf"].encode()):
            self.b.guard(m)
        self.assertEqual(events, ["root", "storage", "plugins", "credentials"])
        events.clear()
        with patch.object(Path, "read_bytes", return_value=b"modified-conf"):
            with self.assertRaises(ValueError):
                self.b.guard(m)
        self.assertEqual(events, ["root", "storage"])

    def test_failed_start_masks_and_stops_only_attempted_service(self):
        self.mock("_host")
        for name in ("_storage", "_durable_mount", "ensure_plugins", "_atomic", "guard", "_unit"):
            self.mock(name)
        self.mock("_install", return_value="/usr/lib/jvm/java-21-fixture")
        commands = []
        def run(args, **kwargs):
            commands.append(args)
            if "start" in args:
                raise ValueError("failed-start")
            return subprocess.CompletedProcess(args, 3 if "is-active" in args else
                                               1 if "pgrep" in args[0] else 0, "", "")
        self.mock("_run", side_effect=run)
        with self.assertRaises(ValueError):
            self.b.bootstrap(complete())
        self.assertEqual(commands[-2:], [["/usr/bin/systemctl", "stop", "neo4j.service"],
                                        ["/usr/bin/systemctl", "mask", "neo4j.service"]])

    def test_fstab_exact_idempotence_and_conflict_no_write(self):
        self.mock("_safe_path")
        write = self.mock("_atomic")
        m = complete()
        expected = ("UUID=" + m["storage"]["filesystem_uuid"]
                    + " /mnt/retained ext4 defaults,nodev,nosuid 0 2")
        with patch.object(Path, "read_text", return_value="# original\n"):
            self.b._durable_mount(m)
        self.assertEqual(write.call_args.args[1], ("# original\n" + expected + "\n").encode())
        write.reset_mock()
        with patch.object(Path, "read_text", return_value=expected + "\n"):
            self.b._durable_mount(m)
        write.assert_not_called()
        with patch.object(Path, "read_text", return_value="UUID=other /mnt/retained ext4 defaults 0 2"):
            with self.assertRaises(ValueError):
                self.b._durable_mount(m)
        write.assert_not_called()


class CliAndSourceTests(OfflineTest):
    def test_import_is_side_effect_free_and_python310_parseable(self):
        source = (HERE / "bootstrap.py").read_text(encoding="utf-8")
        ast.parse(source, feature_version=(3, 10))
        with patch.object(Path, "read_text", side_effect=AssertionError("import read")), \
                patch.object(Path, "write_bytes", side_effect=AssertionError("import write")):
            load()

    def test_real_cli_exit_codes_no_args_validate_and_invalid_render(self):
        # Run only offline CLI paths, never bootstrap with complete inputs.
        # Undo the subprocess safety patch only for these exact child Python commands.
        with tempfile.TemporaryDirectory() as root:
            path, out = Path(root) / "input.json", Path(root) / "cloud.yaml"
            path.write_text(json.dumps(example()), encoding="utf-8")
            for args in ([], ["validate", "--manifest", str(path)],
                         ["render", "--manifest", str(path), "--output", str(out)],
                         ["--PASSWORD_CANARY"]):
                with self.subTest(args=args):
                    with patch("subprocess.run", self.real_run):
                        result = subprocess.run([sys.executable, "-B", str(HERE / "bootstrap.py"), *args],
                                                capture_output=True, text=True, timeout=15,
                                                env={"PATH": "/usr/bin:/bin", "PYTHONDONTWRITEBYTECODE": "1"})
                    self.assertEqual(result.returncode, 1)
                    self.assertNotIn("PASSWORD_CANARY", result.stdout + result.stderr)
                    self.assertNotIn("Traceback", result.stderr)
            self.assertFalse(out.exists())

    real_run = staticmethod(subprocess.run)

    def test_subprocess_environment_fixed_no_inherited_secrets(self):
        with patch.object(self.b.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, "", "")) as run:
            self.b._run(["/usr/bin/fixture", "version"])
        self.assertEqual(run.call_args.kwargs["env"], self.b.ENV)
        self.assertEqual(run.call_args.kwargs["stdin"], subprocess.DEVNULL)
        self.assertFalse(run.call_args.kwargs["check"])


def proc_tcp(local="0100007F:1D32", remote="0100007F:C350", state="01", inode="777"):
    return ("  sl  local_address rem_address st tx_queue rx_queue tr tm->when retrnsmt uid timeout inode\n"
            f"0: {local} {remote} {state} 0:0 00:0 0 1000 0 {inode}\n")


class ReviewFlowTests(OfflineTest):
    def flow(self):
        self.mock("_host")
        self.mock("_root_controlled", create=True)
        self.mock("_port_free", side_effect=self.real_port_free)
        for name in ("_storage", "_durable_mount", "ensure_plugins", "_atomic", "guard", "_unit"):
            self.mock(name)
        self.mock("_install", return_value="/usr/lib/jvm/java-21-fixture")
        # Synthetic empty proc tables: never inspect the host's sockets.
        read = Path.read_text
        self.stack.enter_context(patch.object(Path, "read_text",
            lambda p, *a, **k: proc_tcp(state="06") if str(p).startswith("/proc/") else read(p, *a, **k)))
        commands = []
        def run(args, **kwargs):
            commands.append(args)
            rc = 3 if "is-active" in args else 1 if "pgrep" in args[0] else 0
            return subprocess.CompletedProcess(args, rc, "", "")
        return commands, run

    def test_stop_timeout_still_masks_and_preserves_original_failure(self):
        commands, normal = self.flow()
        original = ValueError("start-canary")
        def run(args, **kwargs):
            result = normal(args, **kwargs)
            if "start" in args:
                raise original
            if "stop" in args:
                raise subprocess.TimeoutExpired(args, 120, output="PASSWORD_CANARY")
            if "mask" in args:
                raise OSError("TOKEN_CANARY")
            return result
        self.mock("_run", side_effect=run)
        with self.assertRaises(ValueError) as caught:
            self.b.bootstrap(complete())
        self.assertIs(caught.exception, original)
        self.assertEqual(commands[-1], ["/usr/bin/systemctl", "mask", "neo4j.service"])
        self.assertEqual(getattr(caught.exception, "cleanup_codes", ()), ("cleanup.stop", "cleanup.mask"))

    def test_occupied_port_before_storage_or_packages(self):
        m = complete()
        commands, run = self.flow()
        self.mock("_run", side_effect=run)
        with patch.object(Path, "read_text", return_value=proc_tcp(state="0A")):
            with self.assertRaises(ValueError):
                self.b.bootstrap(m)
        self.b._storage.assert_not_called()
        self.b._install.assert_not_called()
        self.assertFalse(any("start" in c or "stop" in c for c in commands))

    def test_every_start_occupied_port_refuses_before_credentials(self):
        m = complete()
        self.mock("_port_free", side_effect=self.real_port_free)
        self.mock("_host")
        self.mock("_storage")
        self.mock("_safe_path")
        self.mock("_check_plugins")
        creds = self.mock("_credentials")
        with patch.object(Path, "read_text", return_value=proc_tcp(state="0A")), \
                patch.object(Path, "read_bytes", return_value=m["config"]["neo4j_conf"].encode()):
            with self.assertRaises(ValueError):
                self.b.guard(m)
        creds.assert_not_called()

    def test_unit_isolated_python_and_vendor_stop_timeout(self):
        self.mock("_safe_path")
        self.stack.enter_context(patch.object(Path, "mkdir"))
        write = self.mock("_atomic")
        self.b._unit(complete(), "/usr/lib/jvm/java-21-fixture")
        unit = next(c.args[1].decode() for c in write.call_args_list if str(c.args[0]).endswith(".conf"))
        self.assertNotIn("TimeoutStopSec", unit)
        for hook in ("ExecStartPre", "ExecStartPost"):
            self.assertIn(hook + "=+/usr/bin/python3 -I ", unit)
        rendered = self.b.render_cloud_init(example(), candidate=True)
        config = json.loads("\n".join(l for l in rendered.splitlines() if not l.startswith("#")))
        self.assertEqual(config["runcmd"][0][:3], ["/usr/bin/python3", "-I", self.b.HELPER])

    def test_imds_missing_wrong_resource_never_reads_vault(self):
        for resource in (None, "https://management.azure.com", "https://vault.azure.net.evil"):
            with self.subTest(resource=resource):
                token = {"client_id": complete()["identity"]["client_id"], "access_token": "TOKEN_CANARY",
                         "token_type": "Bearer"}
                if resource is not None:
                    token["resource"] = resource
                fetch = self.mock("_fetch", side_effect=[json.dumps(token).encode(),
                    b'{"value":"USER_CANARY","attributes":{"enabled":true}}',
                    b'{"value":"PASSWORD_CANARY","attributes":{"enabled":true}}'])
                with self.assertRaises(ValueError):
                    self.b._credentials(complete())
                self.assertEqual(fetch.call_count, 1)

    def test_cli_reports_only_allowlisted_code(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "manifest.json"
            m = complete()
            m["storage"]["uid"] = False
            path.write_text(json.dumps(m), encoding="utf-8")
            err = io.StringIO()
            with redirect_stderr(err), redirect_stdout(io.StringIO()):
                self.assertEqual(self.b.main(["validate", "--manifest", str(path)]), 1)
            self.assertEqual(json.loads(err.getvalue()), {"stage": "validate", "code": "storage.uid"})
            m["PASSWORD_CANARY"] = "TOKEN_CANARY"
            path.write_text(json.dumps(m), encoding="utf-8")
            err = io.StringIO()
            with redirect_stderr(err), redirect_stdout(io.StringIO()):
                self.b.main(["validate", "--manifest", str(path)])
            self.assertEqual(json.loads(err.getvalue())["code"], "manifest.keys")
            self.assertNotIn("CANARY", err.getvalue())

    def test_cli_cleanup_codes_preserve_original_code_and_redact_unknown_exceptions(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "m.json"
            path.write_text(json.dumps(complete()), encoding="utf-8")
            self.mock("_root_controlled")
            failures = [self.b._Refused("guest.command"), self.b._Refused("PASSWORD_CANARY"),
                        OSError("USER_CANARY PASSWORD_CANARY TOKEN_CANARY"),
                        subprocess.TimeoutExpired(["PASSWORD_CANARY"], 10, output="TOKEN_CANARY")]
            for i, failure in enumerate(failures):
                failure.cleanup_codes = ("cleanup.stop", "cleanup.mask", "PASSWORD_CANARY")
                handler = self.mock("bootstrap", side_effect=failure)
                err = io.StringIO()
                with patch.object(self.b.os, "geteuid", return_value=0), \
                        redirect_stderr(err), redirect_stdout(io.StringIO()):
                    self.assertEqual(self.b.main(["bootstrap", "--manifest", str(path)]), 1)
                self.assertEqual(json.loads(err.getvalue()), {
                    "stage": "bootstrap", "code": "guest.command" if i == 0 else "REFUSED",
                    "cleanup": ["cleanup.stop", "cleanup.mask"]})
                handler.assert_called_once()
                self.assertNotIn("CANARY", err.getvalue())

    def test_both_vault_failures_block_bootstrap_before_start(self):
        real_guard = self.b.guard
        commands, run = self.flow()
        self.mock("_run", side_effect=run)
        self.mock("guard", side_effect=real_guard)
        self.mock("_safe_path")
        self.mock("_check_plugins")
        token = json.dumps({"client_id": complete()["identity"]["client_id"], "access_token": "TOKEN_CANARY",
                            "token_type": "Bearer", "resource": "https://vault.azure.net"}).encode()
        good_user = b'{"value":"USER_CANARY","attributes":{"enabled":true}}'
        for replies in ([token, b'{"value":"","attributes":{"enabled":true}}'],
                        [token, b'{"value":"USER_CANARY","attributes":{"enabled":false}}'],
                        [token, OSError("USERNAME_CANARY")],
                        [token, good_user, OSError("PASSWORD_CANARY")]):
            self.mock("_fetch", side_effect=replies)
            with patch.object(Path, "read_bytes", return_value=complete()["config"]["neo4j_conf"].encode()):
                with self.assertRaises((ValueError, OSError)):
                    self.b.bootstrap(complete())
            self.assertFalse(any("start" in c or "unmask" in c for c in commands))

    def test_plugin_failure_blocks_bootstrap_before_activation(self):
        commands, run = self.flow()
        self.mock("_run", side_effect=run)
        for failure in (ValueError("plugins.sha256"), OSError("download"), FileExistsError("second-publish")):
            self.mock("ensure_plugins", side_effect=failure)
            with self.assertRaises(type(failure)):
                self.b.bootstrap(complete())
            self.assertFalse(any("start" in c or "unmask" in c for c in commands))


class ConnectedPeerTests(OfflineTest):
    def setUp(self):
        super().setUp()
        self.mock("_host")
        self.mock("_credentials", return_value=("USER_CANARY", "PASSWORD_CANARY"))
        self.requests = []
        self.statuses = [200]
        self.bodies = [json.dumps(response()).encode()]
        self.group = "/system.slice/neo4j.service"
        self.pid_group = self.group
        self.inode = "777"
        self.rows = proc_tcp()
        self.change_socket = False
        self.missing_proc = False
        self.read_limits = []
        self.timeouts = []
        self.connections = []
        outer = self
        class Sock:
            def getsockname(self): return ("127.0.0.1", 50000)
            def getpeername(self): return ("127.0.0.1", 7474)
            def fileno(self): return 7
            def settimeout(self, value): outer.timeouts.append(value)
        class Reply:
            def __init__(self):
                self.status = outer.statuses.pop(0)
                self.body = outer.bodies.pop(0)
            def read(self, size):
                outer.read_limits.append(size)
                return self.body
        class Connection:
            def __init__(self, host, port, timeout):
                outer.assertEqual((host, port), ("127.0.0.1", 7474))
                outer.timeouts.append(timeout)
                self.sock = None
                self.auto_open = 1
                outer.connections.append(self)
            def connect(self): self.sock = Sock()
            def request(self, method, path, body, headers):
                outer.assertEqual(self.auto_open, 0)
                outer.requests.append((method, path, body, headers))
            def getresponse(self): return Reply()
            def close(self): self.sock = None
        self.stack.enter_context(patch.object(http.client, "HTTPConnection", Connection))
        def run(args, **kwargs):
            self.assertEqual(args[:2], ["/usr/bin/systemctl", "show"])
            self.assertIn("neo4j.service", args)
            return subprocess.CompletedProcess(args, 0,
                "MainPID=11\nControlGroup=" + self.group + "\n", "")
        self.mock("_run", side_effect=run)
        # Old unsafe path is also trapped, so RED can observe missing attestation.
        self.legacy = self.mock("_fetch", return_value=json.dumps(response()).encode())
        real_read = Path.read_text
        def read(path, *a, **k):
            if str(path).startswith("/proc/"):
                if self.missing_proc:
                    raise PermissionError("PASSWORD_CANARY")
                if str(path) == "/proc/net/tcp":
                    return self.rows
                if str(path) == "/proc/net/tcp6":
                    return proc_tcp().splitlines()[0] + "\n"
                if str(path) in ("/proc/11/cgroup", "/proc/12/cgroup"):
                    return "0::" + self.pid_group + "\n"
                if str(path) == "/proc/22/cgroup":
                    return "0::/user.slice/untrusted.scope\n"
                raise AssertionError("unexpected proc read " + str(path))
            return real_read(path, *a, **k)
        def iterdir(path):
            if str(path) == "/proc":
                return iter([Path("/proc/11"), Path("/proc/12"), Path("/proc/22")])
            if str(path).endswith("/fd"):
                return iter([path / "9"])
            raise AssertionError("unexpected proc enumeration")
        def readlink(path):
            if self.change_socket:
                self.connections[-1].sock = Sock()
            return Path("socket:[" + self.inode + "]")
        self.stack.enter_context(patch.object(Path, "read_text", read))
        self.stack.enter_context(patch.object(Path, "iterdir", iterdir))
        self.stack.enter_context(patch.object(Path, "readlink", readlink))
        ticks = iter(i / 100 for i in range(100000))
        self.stack.enter_context(patch.object(self.b.time, "monotonic", side_effect=lambda: next(ticks)))
        self.stack.enter_context(patch.object(self.b.time, "sleep"))

    def test_connected_peer_attestation_precedes_basic_header(self):
        self.b.readiness(complete())
        self.legacy.assert_not_called()
        self.assertEqual(len(self.requests), 1)
        method, path, body, headers = self.requests[0]
        self.assertEqual((method, path), ("POST", "/db/neo4j/tx/commit"))
        self.assertEqual(base64.b64decode(headers["Authorization"].split()[1]), b"USER_CANARY:PASSWORD_CANARY")
        self.assertEqual(len(json.loads(body)["statements"]), 3)
        self.assertTrue(all(0 < t <= 10 for t in self.timeouts))

    def test_wrong_pid_or_cgroup_or_tuple_never_sends_credentials(self):
        for field, value in (("inode", "888"), ("pid_group", "/user.slice/fake"),
                             ("group", "/user.slice/fake"),
                             ("rows", proc_tcp(remote="0100007F:C351")),
                             ("rows", proc_tcp() + proc_tcp().splitlines()[1] + "\n"),
                             ("rows", proc_tcp(local="00000000:1D32")),
                             ("missing_proc", True)):
            with self.subTest(field=field, value=value):
                old = getattr(self, field)
                setattr(self, field, value)
                with self.assertRaises(ValueError):
                    self.b.readiness(complete())
                self.assertEqual(self.requests, [])
                self.legacy.assert_not_called()
                setattr(self, field, old)

    def test_socket_turnover_before_request_refused(self):
        self.change_socket = True
        with self.assertRaises(ValueError):
            self.b.readiness(complete())
        self.assertEqual(self.requests, [])
        self.legacy.assert_not_called()

    def test_503_then_ready_and_transient_database_unavailable_then_ready(self):
        for status, body in ((503, b"PASSWORD_CANARY"),
            (200, json.dumps({"errors": [{"code": "Neo.TransientError.General.DatabaseUnavailable",
                                         "message": "PASSWORD_CANARY"}]}).encode())):
            self.requests.clear()
            self.statuses = [status, 200]
            self.bodies = [body, json.dumps(response()).encode()]
            self.b.readiness(complete())
            self.assertEqual(len(self.requests), 2)
            self.legacy.assert_not_called()

    def test_auth_wrong_versions_and_redirect_are_fatal_without_retry(self):
        bad = response()
        bad["results"][1]["data"][0]["row"] = ["5.26.22"]
        for status, body in ((401, b"PASSWORD_CANARY"), (403, b"PASSWORD_CANARY"),
                             (302, b"redirect"), (200, json.dumps(bad).encode()),
                             (200, b'{"errors":[{"code":"Neo.ClientError.Security.Unauthorized"}]}')):
            self.requests.clear()
            self.statuses = [status, 200]
            self.bodies = [body, json.dumps(response()).encode()]
            with self.assertRaises(ValueError):
                self.b.readiness(complete())
            self.assertEqual(len(self.requests), 1)
            self.assertEqual(len(self.statuses), 1)

    def test_slow_response_is_interrupted_by_total_deadline(self):
        handlers, timers = [], []
        # A mock alarm fires while request I/O has not returned. No real host timer.
        signal_module = __import__("signal")
        with patch.object(signal_module, "signal", side_effect=lambda sig, handler: handlers.append(handler)), \
                patch.object(signal_module, "setitimer", side_effect=lambda *args: timers.append(args)), \
                patch.object(signal_module, "getsignal", return_value=signal_module.SIG_DFL):
            def slow(*args):
                self.assertTrue(handlers, "total deadline must be armed before I/O")
                handlers[0](None, None)
                self.fail("deadline did not interrupt blocked response")
            self.mock("_probe", side_effect=slow)
            with self.assertRaises(ValueError) as caught:
                self.b.readiness(complete())
        self.assertEqual(str(caught.exception), "readiness.timeout")
        self.assertEqual(timers[0][1], 120)
        self.assertEqual(timers[-1][1], 0)


class GuestTrustTests(OfflineTest):
    def test_guest_cli_rejects_untrusted_service_override_directory(self):
        with tempfile.TemporaryDirectory() as root:
            manifest = Path(root) / "m.json"
            manifest.write_text(json.dumps(complete()), encoding="utf-8")
            real = Path.lstat
            def lstat(p):
                try:
                    st = list(real(p))
                except FileNotFoundError:
                    st = list(real(Path(root)))
                st[4] = 0
                st[0] &= ~0o022
                if str(p) == "/etc/systemd/system/neo4j.service.d":
                    st[4] = 1000
                return os.stat_result(st)
            for stage in ("bootstrap", "guard", "readiness"):
                handler = self.mock(stage)
                err = io.StringIO()
                with patch.object(Path, "lstat", lstat), patch.object(self.b.os, "geteuid", return_value=0), \
                        redirect_stderr(err), redirect_stdout(io.StringIO()):
                    self.assertEqual(self.b.main([stage, "--manifest", str(manifest)]), 1)
                handler.assert_not_called()
                self.assertEqual(json.loads(err.getvalue())["code"], "path.root_trust")

    def test_guest_cli_checks_helper_manifest_ancestors_before_loading(self):
        for stage in ("bootstrap", "guard", "readiness"):
            for bad in ("manifest", "parent", "helper", "symlink", "nonroot"):
                with self.subTest(stage=stage, bad=bad), tempfile.TemporaryDirectory() as root:
                    manifest = Path(root) / "m.json"
                    manifest.write_text(json.dumps(complete()), encoding="utf-8")
                    original_stat = Path.lstat
                    def lstat(p):
                        st = list(original_stat(p))
                        st[4] = 0
                        st[0] &= ~0o022
                        target = (p == manifest if bad in ("manifest", "symlink")
                                  else p == manifest.parent if bad == "parent"
                                  else p == Path(self.b.__file__))
                        if target:
                            if bad == "symlink":
                                st[0] = stat.S_IFLNK | 0o777
                            else:
                                st[4] = 1000
                        return os.stat_result(st)
                    handler = self.mock(stage)
                    with patch.object(Path, "lstat", lstat), \
                            patch.object(self.b.os, "geteuid", return_value=1000 if bad == "nonroot" else 0), \
                            patch.object(Path, "read_text", side_effect=AssertionError("loaded unsafe manifest")):
                        err = io.StringIO()
                        with redirect_stderr(err), redirect_stdout(io.StringIO()):
                            self.assertEqual(self.b.main([stage, "--manifest", str(manifest)]), 1)
                    handler.assert_not_called()
                    self.assertEqual(json.loads(err.getvalue())["code"],
                                     "guest.root" if bad == "nonroot" else "path.root_trust")

    def test_ordinary_user_render_validate_remain_offline(self):
        with tempfile.TemporaryDirectory() as root:
            manifest, out = Path(root) / "m.json", Path(root) / "out.yaml"
            manifest.write_text(json.dumps(complete()), encoding="utf-8")
            with patch.object(self.b.os, "geteuid", return_value=1000), \
                    redirect_stderr(io.StringIO()), redirect_stdout(io.StringIO()):
                self.assertEqual(self.b.main(["validate", "--manifest", str(manifest)]), 0)
                self.assertEqual(self.b.main(["render", "--manifest", str(manifest), "--output", str(out)]), 0)
            self.assertTrue(out.is_file())


if __name__ == "__main__":
    unittest.main()