"""Shipping image coverage, with a credential-free delivery-contract boundary.

Public checks inspect the delivery contract, execute secret-file handling with
non-secret fixtures, and build without secrets to prove failure at that boundary.
These are not evidence of genuine SDK installation or token authenticity.

The positive build/run/status case is marked ``mise``. It requires Docker and
explicit CFS_UV_CONFIG_FILE / CFS_TOKEN_FILE inputs outside the build context;
missing trusted-tier prerequisites fail, never skip. Only a genuine private-feed
build can prove that the production image actually builds, boots, and serves.
"""

from __future__ import annotations

import importlib.metadata
import json
import os
import pathlib
import re
import runpy
import shlex
import shutil
import socket
import stat
import subprocess
import sys
import time
import tomllib
from collections.abc import Generator
from contextlib import contextmanager
from types import ModuleType, SimpleNamespace
from typing import Any

import httpx
import pytest
import yaml

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
IMAGE_TAG = "context-intelligence-server:integration-test"

# A cold build (base image pull + dependency install) far exceeds the global
# 30s pytest-timeout, so override it generously for this one test.
BUILD_AND_BOOT_TIMEOUT = 900


def _build_workflow() -> dict[str, Any]:
    return yaml.load(
        (
            PROJECT_ROOT
            / ".github/workflows/context-intelligence-api-build-deploy.yaml"
        ).read_text(),
        Loader=yaml.BaseLoader,
    )


def _build_step(name: str) -> dict[str, Any]:
    steps = [
        step
        for step in _build_workflow()["jobs"]["build"]["steps"]
        if step.get("name") == name
    ]
    assert len(steps) == 1, f"Required build step absent: {name}"
    return steps[0]


def _run_cfs_step(
    name: str, temp_dir: pathlib.Path, **overrides: str
) -> subprocess.CompletedProcess[str]:
    """Execute the shipping workflow script with non-secret local fixtures."""
    return subprocess.run(
        [
            "bash",
            "--noprofile",
            "--norc",
            "-e",
            "-o",
            "pipefail",
            "-c",
            _build_step(name)["run"],
        ],
        env={
            **os.environ,
            "RUNNER_TEMP": str(temp_dir),
            "CFS_AZURE_CLIENT_ID": "fixture-client",
            "CFS_AZURE_TENANT_ID": "fixture-tenant",
            "CFS_PYTHON_INDEX_URL": "https://packages.example.test/simple/",
            "CFS_ACCESSTOKEN": "fixture-access-token",
            **overrides,
        },
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )


def test_mise_requirement_pins_only_the_approved_release_hash() -> None:
    text = (PROJECT_ROOT / "requirements-mise.txt").read_text()
    assert any(line.startswith("#") for line in text.splitlines())
    requirement = " ".join(
        line
        for line in text.replace("\\\n", "").splitlines()
        if line.strip() and not line.startswith("#")
    )
    assert shlex.split(requirement) == [
        "mise==2.7.0",
        "--hash=sha256:aa08b4b17e5e76022ed2e06e356c817d7ea01e15d1fdb097f274bf7959056af0",
    ]
    assert "://" not in text and "@" not in text


def test_runtime_dependencies_supply_cffi_without_resolving_mise_or_pyjwt() -> None:
    project = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text())
    dependencies = project["project"]["dependencies"]
    assert "cffi>=1.16.0" in dependencies
    assert not any(dep.lower().startswith("pyjwt") for dep in dependencies)
    resolver_inputs = [
        *dependencies,
        *project["dependency-groups"]["dev"],
        *(
            dep
            for extra in project["project"].get("optional-dependencies", {}).values()
            for dep in extra
        ),
    ]
    assert not any(dep.lower().startswith("mise") for dep in resolver_inputs)
    lock = tomllib.loads((PROJECT_ROOT / "uv.lock").read_text())
    packages = {package["name"]: package for package in lock["package"]}
    assert "mise" not in packages
    assert all(
        package["source"].get("registry", "https://pypi.org/simple")
        == "https://pypi.org/simple"
        for package in packages.values()
    ), "The public PR lock must not reference private package sources"
    runtime = packages["context-intelligence-server"]["dependencies"]
    assert {"name": "cffi"} in runtime
    assert not any(dep["name"] == "pyjwt" for dep in runtime)


def test_docker_python_acquisition_requires_the_sole_cfs_config_and_secrets() -> None:
    dockerfile = (PROJECT_ROOT / "Dockerfile").read_text()
    logical = re.sub(r"\\\n\s*", " ", dockerfile)
    assert re.findall(r"(?m)^FROM (.+)$", logical) == [
        "mcr.microsoft.com/azurelinux/base/python:3.12"
    ]
    assert re.findall(r"(?m)^ARG (.+)$", logical) == ["OS_PATCH_EPOCH"]
    assert re.search(r"tdnf -y install [^&\n]*\bicu\b", logical)
    assert "COPY pyproject.toml requirements-mise.txt ." in dockerfile
    installs = [
        line
        for line in logical.splitlines()
        if line.startswith("RUN ") and "pip install" in line
    ]
    assert len(installs) == 2
    for instruction in installs:
        assert (
            "--mount=type=secret,id=cfs_config,target=/run/secrets/cfs.toml,"
            "required=true,mode=0400"
        ) in instruction
        assert (
            "--mount=type=secret,id=cfs_token,env=UV_INDEX_CFS_PASSWORD,required=true"
        ) in instruction
        assert instruction.count(
            "UV_INDEX_CFS_USERNAME=cfs uv --config-file /run/secrets/cfs.toml pip install"
        ) == instruction.count("pip install")
    assert "--no-deps --require-hashes --only-binary=:all:" in logical
    assert "-r requirements-mise.txt" in logical
    assert "from mise import Mise" in logical
    assert "version('mise') == '2.7.0'" in logical
    assert "Mise()" in logical
    assert "tdnf -y remove python3-pip python3-setuptools python3-wheel" in logical
    assert '"setuptools>=79"' in logical
    assert 'ENTRYPOINT ["./docker-entrypoint.sh"]' in dockerfile
    assert not re.search(r"(?m)^(ARG|ENV)\s+.*(CFS|PASSWORD|TOKEN)", logical)
    for forbidden in (
        "pypi.org",
        "--extra-index",
        "--index-url",
        "--default-index",
        "--find-links",
        "--no-build-isolation",
    ):
        assert forbidden not in logical


def test_protected_build_uses_pinned_cfs_actions_and_buildkit_secret_files() -> None:
    workflow = _build_workflow()
    assert set(workflow["on"]) == {"push", "schedule", "workflow_dispatch"}
    assert workflow["on"]["push"]["branches"] == ["main"]
    assert "requirements-mise.txt" in workflow["on"]["push"]["paths"]
    build = workflow["jobs"]["build"]
    assert build["environment"] == "production"
    assert build["if"] == "github.ref == 'refs/heads/main'"
    login = _build_step("Log in to Azure for CFS")
    assert login["uses"] == "Azure/login@a641126d1b8aa4d1fa005f4f92df94a3a4c4c906"
    assert login["with"] == {
        "client-id": "${{ vars.CFS_AZURE_CLIENT_ID }}",
        "tenant-id": "${{ vars.CFS_AZURE_TENANT_ID }}",
        "allow-no-subscriptions": "true",
    }
    assert _build_step("Set up CFS")["uses"] == (
        "1es-actions/setup-cfs@ca9f87a2de7ac289f170105e79a9d2e68ac6ce32"
    )
    assert _build_step("Check CFS configuration")["env"] == {
        "CFS_AZURE_CLIENT_ID": "${{ vars.CFS_AZURE_CLIENT_ID }}",
        "CFS_AZURE_TENANT_ID": "${{ vars.CFS_AZURE_TENANT_ID }}",
        "CFS_PYTHON_INDEX_URL": "${{ secrets.CFS_PYTHON_INDEX_URL }}",
    }
    push = _build_step("Build and push Docker image")["with"]
    assert push["build-args"].splitlines() == [
        "OS_PATCH_EPOCH=${{ steps.epoch.outputs.day }}"
    ]
    assert push["secret-files"].splitlines() == [
        "cfs_config=${{ runner.temp }}/cfs-uv.toml",
        "cfs_token=${{ runner.temp }}/cfs-token",
    ]
    assert push["push"] == "true"
    assert push["cache-from"] == "type=registry,ref=${{ env.IMAGE_NAME }}:buildcache"
    assert push["cache-to"] == (
        "type=registry,ref=${{ env.IMAGE_NAME }}:buildcache,mode=max"
    )
    names = [step.get("name") for step in build["steps"]]
    ordered = [
        "Check CFS configuration",
        "Log in to Azure for CFS",
        "Set up CFS",
        "Write CFS BuildKit secrets",
        "Build and push Docker image",
        "Smoke test native MISE",
        "Scan image for vulnerabilities",
        "Generate SBOM",
        "Attest build provenance",
    ]
    assert [names.index(name) for name in ordered] == sorted(
        names.index(name) for name in ordered
    )
    assert workflow["jobs"]["deploy"]["needs"] == "build"
    assert workflow["jobs"]["deploy"]["if"] == "github.event_name != 'schedule'"


@pytest.mark.parametrize(
    "missing", ["CFS_AZURE_CLIENT_ID", "CFS_AZURE_TENANT_ID", "CFS_PYTHON_INDEX_URL"]
)
def test_cfs_preflight_rejects_missing_configuration_without_printing_values(
    tmp_path: pathlib.Path, missing: str
) -> None:
    result = _run_cfs_step("Check CFS configuration", tmp_path, **{missing: ""})
    assert result.returncode != 0
    assert missing in result.stdout + result.stderr
    assert "fixture-client" not in result.stdout + result.stderr
    assert "fixture-tenant" not in result.stdout + result.stderr
    assert "https://packages.example.test" not in result.stdout + result.stderr
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("CFS_ACCESSTOKEN", ""),
        ("CFS_PYTHON_INDEX_URL", ""),
        ("CFS_PYTHON_INDEX_URL", "http://packages.example.test/simple/"),
        ("CFS_PYTHON_INDEX_URL", "https://user:password@packages.example.test/simple/"),
        ("CFS_PYTHON_INDEX_URL", "https://packages.example.test/simple/?key=value"),
        ("CFS_PYTHON_INDEX_URL", "https://packages.example.test/simple/#fragment"),
        ("CFS_PYTHON_INDEX_URL", "https://packages.example.test/simple/?"),
        ("CFS_PYTHON_INDEX_URL", "https://[invalid/simple/"),
        ("CFS_PYTHON_INDEX_URL", "https://packages.example.test/\nsimple/"),
    ],
)
def test_cfs_secret_writer_rejects_missing_token_or_unsafe_url(
    tmp_path: pathlib.Path, field: str, value: str
) -> None:
    result = _run_cfs_step("Write CFS BuildKit secrets", tmp_path, **{field: value})
    assert result.returncode != 0
    assert field in result.stdout + result.stderr
    assert "fixture-access-token" not in result.stdout + result.stderr
    if value:
        assert value not in result.stdout + result.stderr
    assert not list(tmp_path.iterdir())


def test_cfs_secret_files_are_private_single_index_and_always_cleaned(
    tmp_path: pathlib.Path,
) -> None:
    result = _run_cfs_step("Write CFS BuildKit secrets", tmp_path)
    assert result.returncode == 0, result.stderr
    assert result.stdout == "" and result.stderr == ""
    config = tmp_path / "cfs-uv.toml"
    token = tmp_path / "cfs-token"
    assert stat.S_IMODE(config.stat().st_mode) == 0o600
    assert stat.S_IMODE(token.stat().st_mode) == 0o600
    assert tomllib.loads(config.read_text()) == {
        "index": [
            {
                "name": "cfs",
                "url": "https://packages.example.test/simple/",
                "default": True,
                "authenticate": "always",
            }
        ]
    }
    assert token.read_text() == "fixture-access-token"
    assert _build_step("Remove CFS BuildKit secrets")["if"] == "always()"
    cleaned = _run_cfs_step("Remove CFS BuildKit secrets", tmp_path)
    assert cleaned.returncode == 0, cleaned.stderr
    assert not list(tmp_path.iterdir())


def test_native_image_smoke_uses_built_digest_and_sanitized_adapter_rejection() -> None:
    smoke = _build_step("Smoke test native MISE")
    assert smoke["env"]["MISE_IMAGE"] == (
        "${{ env.IMAGE_NAME }}@${{ steps.push.outputs.digest }}"
    )
    script = smoke["run"]
    assert (
        'docker run --rm --network none --entrypoint python3 -i "$MISE_IMAGE" -'
        in script
    )
    assert 'version("mise") == "2.7.0"' in script
    assert "MiseTokenValidator(client_id, tenant_id)" in script
    assert "validator.validate(" in script and '"not-a-jwt"' in script
    assert "except TokenValidationError as error:" in script
    assert 'str(error) == "Token validation failed"' in script
    assert (
        "error.__cause__ is None" in script and "error.__suppress_context__" in script
    )
    assert "client_id not in rendered and tenant_id not in rendered" in script
    assert 'raise AssertionError("MISE accepted a malformed bearer")' in script


def _run_image_smoke_with_native_double(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
    *,
    status: int = 401,
    claims: list[Any] | None = None,
    configuration_error: int = 0,
    failure_stage: str | None = None,
    failure: Exception | None = None,
) -> list[str]:
    """Execute the exact gate with the real adapter; doubles prove gate logic only."""
    events: list[str] = []

    class Client:
        def __init__(self) -> None:
            if failure_stage == "constructor":
                assert failure is not None
                raise failure
            events.append("construct")

        @contextmanager
        def configure(
            self, configuration_json: str, section: str
        ) -> Generator[SimpleNamespace, None, None]:
            assert section == "AzureAd"
            assert json.loads(configuration_json) == {
                "MiseVersion": "2.0",
                "AzureAd": {
                    "Instance": "https://login.microsoftonline.com/",
                    "ClientId": "aaaaaaaa-0000-0000-0000-000000000001",
                    "TenantId": "bbbbbbbb-0000-0000-0000-000000000002",
                    "ValidTenantIds": ["bbbbbbbb-0000-0000-0000-000000000002"],
                    "Audiences": [
                        "aaaaaaaa-0000-0000-0000-000000000001",
                        "api://aaaaaaaa-0000-0000-0000-000000000001",
                    ],
                    "Protocols": {
                        "Bearer": {
                            "TokenTypes": {
                                "AccessToken": {"UserToken": True, "AppToken": True}
                            }
                        }
                    },
                },
            }
            if failure_stage == "configure":
                assert failure is not None
                raise failure
            events.append("configure")
            try:
                yield SimpleNamespace(error_code=configuration_error)
            finally:
                events.append("configuration released")

        @contextmanager
        def validate(
            self, *, validation_input: SimpleNamespace
        ) -> Generator[SimpleNamespace, None, None]:
            assert events[-1] == "configuration released"
            assert vars(validation_input) == {
                "authorization_header": "Bearer not-a-jwt",
                "original_uri_header": "https://service.example.test/events",
                "original_method_header": "POST",
            }
            if failure_stage == "validate":
                assert failure is not None
                raise failure
            events.append("validate")
            try:
                yield SimpleNamespace(
                    http_response_status_code=status,
                    subject_claims=[] if claims is None else claims,
                    error_description="fixture native diagnostic",
                )
            finally:
                events.append("validation released")

    sdk = ModuleType("mise")
    monkeypatch.setattr(sdk, "Mise", Client, raising=False)
    monkeypatch.setattr(sdk, "MiseValidationInput", SimpleNamespace, raising=False)
    monkeypatch.setitem(sys.modules, "mise", sdk)
    original_version = importlib.metadata.version
    monkeypatch.setattr(
        importlib.metadata,
        "version",
        lambda name: "2.7.0" if name == "mise" else original_version(name),
    )
    shell = _build_step("Smoke test native MISE")["run"]
    source = shell.split("<<'PY'\n", 1)[1].rsplit("\nPY", 1)[0]
    script = tmp_path / "image-smoke.py"
    script.write_text(source)
    runpy.run_path(str(script))
    return events


def _run_isolated_image_smoke_failure(
    tmp_path: pathlib.Path, stage: str, failure_type: type[Exception]
) -> subprocess.CompletedProcess[str]:
    """Capture the exact script's real process output, not just its exception."""
    bootstrap = """
import builtins
import pathlib
import runpy
import sys

import pytest

tests = runpy.run_path(sys.argv[1])
with pytest.MonkeyPatch.context() as patch:
    tests["_run_image_smoke_with_native_double"](
        patch,
        pathlib.Path(sys.argv[2]),
        failure_stage=sys.argv[3],
        failure=getattr(builtins, sys.argv[4])("MISE_NATIVE_DIAGNOSTIC_CANARY"),
    )
"""
    return subprocess.run(
        [
            sys.executable,
            "-c",
            bootstrap,
            str(pathlib.Path(__file__).resolve()),
            str(tmp_path),
            stage,
            failure_type.__name__,
        ],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )


def test_native_image_smoke_does_not_accept_a_sanitized_native_type_error(
    tmp_path: pathlib.Path,
) -> None:
    result = _run_isolated_image_smoke_failure(tmp_path, "validate", TypeError)
    assert result.returncode != 0
    for stream in (result.stdout, result.stderr):
        assert "MISE_NATIVE_DIAGNOSTIC_CANARY" not in stream
        assert "Traceback" not in stream
    assert result.stdout == ""
    assert result.stderr == "Native MISE smoke failed\n"


def test_native_image_smoke_checks_native_rejection_then_sanitized_adapter(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    events = _run_image_smoke_with_native_double(monkeypatch, tmp_path)
    assert events == [
        "construct",
        "configure",
        "configuration released",
        "validate",
        "validation released",
        "construct",
        "configure",
        "configuration released",
        "validate",
        "validation released",
    ]


@pytest.mark.parametrize("status", [200, 403, 500])
def test_native_image_smoke_requires_exact_native_401(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path, status: int
) -> None:
    with pytest.raises(SystemExit, match="^Native MISE smoke failed$"):
        _run_image_smoke_with_native_double(monkeypatch, tmp_path, status=status)


def test_native_image_smoke_rejects_native_401_with_subject_claims(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    with pytest.raises(SystemExit, match="^Native MISE smoke failed$"):
        _run_image_smoke_with_native_double(
            monkeypatch,
            tmp_path,
            claims=[SimpleNamespace(name="oid", value="fixture-subject")],
        )


def test_native_image_smoke_requires_successful_native_configuration(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    with pytest.raises(SystemExit, match="^Native MISE smoke failed$"):
        _run_image_smoke_with_native_double(
            monkeypatch, tmp_path, configuration_error=1
        )


@pytest.mark.parametrize(
    ("stage", "failure_type"),
    [("constructor", OSError), ("configure", OSError), ("validate", RuntimeError)],
)
def test_native_image_smoke_fails_without_logging_native_diagnostics(
    tmp_path: pathlib.Path,
    stage: str,
    failure_type: type[Exception],
) -> None:
    result = _run_isolated_image_smoke_failure(tmp_path, stage, failure_type)
    assert result.returncode != 0
    for stream in (result.stdout, result.stderr):
        assert "MISE_NATIVE_DIAGNOSTIC_CANARY" not in stream
        assert "Traceback" not in stream
    assert result.stdout == ""
    assert result.stderr == "Native MISE smoke failed\n"


def test_pr_ci_is_sdk_free_application_boundary_coverage() -> None:
    text = (PROJECT_ROOT / ".github/workflows/ci.yml").read_text()
    for forbidden in (
        "id-token",
        "CFS_",
        "setup-cfs@",
        "Azure/login@",
        "pull_request_target",
        "secrets.",
    ):
        assert forbidden not in text
    workflow = yaml.load(text, Loader=yaml.BaseLoader)
    commands = [
        step["run"]
        for job in workflow["jobs"].values()
        for step in job["steps"]
        if "pytest" in step.get("run", "")
    ]
    assert len(commands) == 2
    for command in commands:
        args = shlex.split(command)
        assert "not mise" in args[args.index("-m") + 1]


@pytest.mark.timeout(BUILD_AND_BOOT_TIMEOUT)
def test_image_build_fails_at_the_required_cfs_secret_boundary() -> None:
    assert "id=cfs_config" in (PROJECT_ROOT / "Dockerfile").read_text()
    if shutil.which("docker") is None:
        pytest.skip("Docker CLI unavailable for the credential-free negative build")
    info = subprocess.run(
        ["docker", "info"], capture_output=True, text=True, timeout=15, check=False
    )
    if info.returncode != 0:
        pytest.skip("Docker daemon unavailable for the credential-free negative build")
    result = subprocess.run(
        [
            "docker",
            "build",
            "--platform",
            "linux/amd64",
            "--no-cache",
            "--progress=plain",
            "-f",
            "Dockerfile",
            ".",
        ],
        cwd=PROJECT_ROOT,
        env={**os.environ, "DOCKER_BUILDKIT": "1"},
        capture_output=True,
        text=True,
        timeout=BUILD_AND_BOOT_TIMEOUT - 30,
        check=False,
    )
    output = result.stdout + result.stderr
    assert result.returncode != 0, (
        "A production image must not build without CFS secrets"
    )
    assert re.search(r"secret cfs_(config|token).*not found", output), output


def _get_free_port() -> int:
    """Return a free TCP port on localhost."""
    with socket.socket() as s:
        s.bind(("", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def built_image() -> Generator[Any, None, None]:
    """Build the single production image through explicitly supplied CFS secrets."""
    secret_files: dict[str, pathlib.Path] = {}
    for secret_id, variable in (
        ("cfs_config", "CFS_UV_CONFIG_FILE"),
        ("cfs_token", "CFS_TOKEN_FILE"),
    ):
        supplied = os.environ.get(variable)
        if not supplied:
            pytest.fail(f"The trusted image tier requires {variable}", pytrace=False)
        path = pathlib.Path(supplied).resolve()
        if not path.is_file() or path.is_relative_to(PROJECT_ROOT):
            pytest.fail(
                f"{variable} must be a file outside the build context", pytrace=False
            )
        if stat.S_IMODE(path.stat().st_mode) & 0o077:
            pytest.fail(f"{variable} must not be group/world accessible", pytrace=False)
        secret_files[secret_id] = path

    try:
        import docker  # type: ignore[import-untyped]
        from docker.errors import DockerException  # type: ignore[import-untyped]
    except ImportError:
        pytest.fail("The trusted image tier requires the docker package", pytrace=False)

    try:
        client = docker.from_env()
        client.ping()
    except DockerException:
        pytest.fail(
            "The trusted image tier requires a reachable Docker daemon", pytrace=False
        )

    try:
        result = subprocess.run(
            [
                "docker",
                "build",
                "--platform",
                "linux/amd64",
                "--load",
                "--secret",
                f"id=cfs_config,src={secret_files['cfs_config']}",
                "--secret",
                f"id=cfs_token,src={secret_files['cfs_token']}",
                "--tag",
                IMAGE_TAG,
                ".",
            ],
            cwd=PROJECT_ROOT,
            env={**os.environ, "DOCKER_BUILDKIT": "1"},
            capture_output=True,
            text=True,
            timeout=BUILD_AND_BOOT_TIMEOUT - 120,
            check=False,
        )
        if result.returncode != 0:
            pytest.fail(
                "CFS production image build failed (private build output withheld)",
                pytrace=False,
            )
        image = client.images.get(IMAGE_TAG)
        try:
            yield image
        finally:
            client.images.remove(image.id, force=True)
    finally:
        client.close()


@pytest.mark.mise
@pytest.mark.timeout(BUILD_AND_BOOT_TIMEOUT)
def test_image_builds_runs_and_serves_status(built_image: Any) -> None:
    """The built image runs and serves a real HTTP response on /status.

    Proves end-to-end that the image is real: the container starts, the FastAPI
    server boots past its fail-closed auth gate (via ALLOW_UNAUTHENTICATED, the
    documented dev/test opt-out), and the auth-exempt /status endpoint answers
    over the network. /status always returns 200 and soft-fails Neo4j, so we
    assert on the JSON body (which proves the real handler ran), not just the
    status code.
    """
    import docker  # type: ignore[import-untyped]

    client = docker.from_env()
    host_port = _get_free_port()

    container = client.containers.run(
        IMAGE_TAG,
        environment={
            "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_ALLOW_UNAUTHENTICATED": "true"
        },
        ports={"8000/tcp": host_port},
        detach=True,
        remove=True,
    )

    try:
        url = f"http://127.0.0.1:{host_port}/status"
        deadline = time.monotonic() + 90
        last_error: Exception | str | None = None
        body: dict[str, Any] | None = None

        while time.monotonic() < deadline:
            try:
                resp = httpx.get(url, timeout=2.0)
                if resp.status_code == 200:
                    body = resp.json()
                    break
                last_error = f"HTTP {resp.status_code}"
            except Exception as exc:  # connection refused while booting
                last_error = exc
            time.sleep(1)

        if body is None:
            logs = container.logs(tail=50).decode("utf-8", errors="replace")
            pytest.fail(
                f"Server never served 200 on /status within 90s "
                f"(last error: {last_error}).\nContainer logs (tail):\n{logs}"
            )

        # The real /status handler ran: it always reports Neo4j connectivity.
        # No Neo4j is wired up in this test, so it must report a disconnected
        # state — which proves the endpoint actually probed rather than being a
        # static stub.
        assert isinstance(body, dict), (
            f"/status must return a JSON object, got: {body!r}"
        )
        assert "neo4j_connected" in body, (
            f"/status body must include neo4j_connected (real handler); got keys: {list(body)}"
        )
        assert body["neo4j_connected"] is False, (
            "with no Neo4j wired up, /status must report neo4j_connected=False "
            f"(soft-fail), got: {body['neo4j_connected']!r}"
        )
    finally:
        try:
            container.stop(timeout=5)
        except Exception:
            pass
