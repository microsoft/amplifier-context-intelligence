"""Credential-free delivery contracts plus an explicitly selected real image tier.

Contract tests run in the public unit lane despite this file's directory. Only
the build/run test requires ``integration and image``. None proves configured
MISE sidecar conformance, positive live-token validation, or S360 clearance.
"""

from __future__ import annotations

import ast
import base64
import hashlib
import http.client
import json
import pathlib
import re
import runpy
import shutil
import subprocess
import sys
import time
import tomllib
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import httpx
import pytest
import yaml

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
BUILD_AND_BOOT_TIMEOUT = 900


def _text(path: str) -> str:
    return (PROJECT_ROOT / path).read_text(encoding="utf-8")


def _workflow(filename: str) -> dict[str, Any]:
    return yaml.load(_text(f".github/workflows/{filename}"), Loader=yaml.BaseLoader)


def _build_workflow() -> dict[str, Any]:
    return _workflow("context-intelligence-api-build-deploy.yaml")


def _step(job: dict[str, Any], name: str) -> dict[str, Any]:
    matches = [step for step in job["steps"] if step.get("name") == name]
    assert len(matches) == 1, f"Required step absent: {name}"
    return matches[0]


def _python_blocks(script: str) -> list[str]:
    return re.findall(r"<<'PY'\n(.*?)\nPY(?:\n|$)", script, flags=re.DOTALL)


def test_public_lock_is_sdk_free_and_matches_server_version() -> None:
    project = tomllib.loads(_text("pyproject.toml"))
    packages = {p["name"]: p for p in tomllib.loads(_text("uv.lock"))["package"]}
    assert project["project"]["version"] == "6.11.2"
    assert packages["context-intelligence-server"]["version"] == "6.11.2"
    assert not {"mise", "pyjwt", "cffi", "pycparser"} & packages.keys()
    assert not (PROJECT_ROOT / "requirements-mise.txt").exists()
    dependencies = project["project"]["dependencies"]
    assert not any(dep.startswith("httpx") for dep in dependencies)
    assert "httpx>=0.27.0" in project["dependency-groups"]["dev"]
    assert all(
        p["source"].get("registry", "https://pypi.org/simple")
        == "https://pypi.org/simple"
        for p in packages.values()
    )
    markers = project["tool"]["pytest"]["ini_options"]["markers"]
    assert {m.split(":")[0] for m in markers} == {"integration", "neo4j", "image"}
    assert project["tool"]["pytest"]["ini_options"]["timeout"] == 30


def test_docker_uses_hashed_public_lock_and_preserves_hardening() -> None:
    dockerfile = _text("Dockerfile")
    logical = re.sub(r"[ \t]+", " ", re.sub(r"\\\n\s*", " ", dockerfile))
    assert re.findall(r"(?m)^FROM (.+)$", logical) == [
        "mcr.microsoft.com/azurelinux/base/python:3.12"
    ]
    assert re.findall(r"(?m)^ARG (.+)$", logical) == ["OS_PATCH_EPOCH"]
    assert "tdnf -y update" in logical
    assert "tdnf -y install ca-certificates curl" in logical
    assert not re.search(r"tdnf -y install [^&\n]*\bicu\b", logical)
    assert "COPY pyproject.toml uv.lock ." in dockerfile
    assert (
        "uv --no-config export --locked --no-dev --no-emit-project "
        "--format requirements-txt --output-file /tmp/server-requirements.txt"
    ) in logical
    assert (
        "uv --no-config pip install --system --no-cache "
        "--default-index https://pypi.org/simple --require-hashes "
        "-r /tmp/server-requirements.txt"
    ) in logical
    assert (
        "uv --no-config pip install --system --no-cache "
        "--default-index https://pypi.org/simple --no-deps ."
    ) in logical
    assert "rm -f /tmp/server-requirements.txt" in logical
    assert "tdnf -y remove python3-pip python3-setuptools python3-wheel" in logical
    for package in ("pip", "setuptools", "wheel"):
        assert f"/usr/lib/python3.12/site-packages/{package}-*.dist-info" in logical
    assert (
        "uv --no-config pip install --system --no-cache "
        '--default-index https://pypi.org/simple "setuptools>=79"'
    ) in logical
    for forbidden in ("cfs", "secret", "requirements-mise", "Mise()", "from mise"):
        assert forbidden not in dockerfile
    assert 'ENTRYPOINT ["./docker-entrypoint.sh"]' in dockerfile
    assert 'CMD ["context-intelligence-server"]' in dockerfile
    assert (
        "COPY context_intelligence_server/ context_intelligence_server/" in dockerfile
    )


def test_build_release_security_gates_stay_enabled_without_sdk_credentials() -> None:
    workflow = _build_workflow()
    assert set(workflow["on"]) == {"push", "schedule", "workflow_dispatch"}
    assert workflow["on"]["push"]["branches"] == ["main"]
    paths = workflow["on"]["push"]["paths"]
    assert "amplifier-online.yaml" in paths
    assert "requirements-mise.txt" not in paths
    build = workflow["jobs"]["build"]
    assert build["if"] == "github.ref == 'refs/heads/main'"
    assert build["environment"] == "production"
    push = _step(build, "Build and push Docker image")["with"]
    assert push["push"] == "true"
    assert "secret-files" not in push
    assert push["build-args"] == "OS_PATCH_EPOCH=${{ steps.epoch.outputs.day }}\n"
    assert push["cache-from"] == "type=registry,ref=${{ env.IMAGE_NAME }}:buildcache"
    assert push["cache-to"].endswith(":buildcache,mode=max")
    scan = _step(build, "Scan image for vulnerabilities")["with"]
    assert scan["exit-code"] == "1" and scan["severity"] == "CRITICAL,HIGH"
    names = [s.get("name") for s in build["steps"]]
    ordered = [
        "Build and push Docker image",
        "Smoke test server image (SDK-free)",
        "Scan image for vulnerabilities",
        "Generate SBOM",
        "Attest build provenance",
    ]
    assert [names.index(n) for n in ordered] == sorted(names.index(n) for n in ordered)
    assert workflow["permissions"]["id-token"] == "write"
    assert workflow["permissions"]["attestations"] == "write"
    deploy = workflow["jobs"]["deploy"]
    assert deploy["needs"] == "build"
    assert deploy["if"] == "github.event_name != 'schedule'"
    assert deploy["environment"] == "production"
    assert deploy["permissions"]["id-token"] == "write"
    script = _step(deploy, "Deploy via provisioner")["run"]
    assert (
        '"${{ vars.AO_PROVISIONER_URL }}/deploy/projects/${{ vars.AO_PROJECT_NAME }}"'
        in script
    )
    assert (
        "'{image: $image, service: $service, ghcr_token: $ghcr, manifest: $manifest}'"
        in script
    )
    assert "exit 1" not in script
    text = _text(".github/workflows/context-intelligence-api-build-deploy.yaml")
    assert not any(s in text for s in ("CFS_", "setup-cfs@", "Smoke test native MISE"))
    assert not any(
        s.get("run", "").strip() in ("exit 1", "false")
        for job in workflow["jobs"].values()
        for s in job["steps"]
    )


def test_image_smoke_is_digest_bound_offline_packaging_not_sidecar_proof() -> None:
    smoke = _step(
        _build_workflow()["jobs"]["build"], "Smoke test server image (SDK-free)"
    )
    assert (
        smoke["env"]["SERVER_IMAGE"]
        == "${{ env.IMAGE_NAME }}@${{ steps.push.outputs.digest }}"
    )
    script = smoke["run"]
    assert 'timeout 120 docker pull "$SERVER_IMAGE"' in script
    assert "--network none" in script and "--pull never" in script
    assert "trap cleanup EXIT" in script
    assert "docker rm -f" in script and "--label" in script
    (source,) = _python_blocks(script)
    ast.parse(source)
    for required in (
        'version("context-intelligence-server")',
        '"6.11.2"',
        'find_spec("mise") is None',
        'find_spec("jwt") is None',
        "HTTPConnection",
        "create_asgi_app",
        'auth_mode="static"',
    ):
        assert required in source
    assert ".validate(" not in source


def test_exact_image_smoke_python_constructs_static_app_without_http(
    tmp_path: pathlib.Path,
) -> None:
    """Execute packaging logic locally, NOT a Docker/image attestation."""
    smoke = _step(
        _build_workflow()["jobs"]["build"], "Smoke test server image (SDK-free)"
    )
    (source,) = _python_blocks(smoke["run"])
    # Isolated process avoids the test harness's imported app/settings caches.
    result = subprocess.run(
        [sys.executable, "-c", source],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert (
        "SDK-free static application construction passed; no sidecar proof"
        in result.stdout
    )


def test_public_ci_runs_contracts_without_container_or_privileged_credentials() -> None:
    text = _text(".github/workflows/ci.yml")
    for forbidden in (
        "id-token",
        "CFS_",
        "setup-cfs@",
        "login@",
        "pull_request_target",
        "secrets.",
    ):
        assert forbidden not in text
    workflow = _workflow("ci.yml")
    assert workflow["jobs"]["integration-tests"]["continue-on-error"] == "true"
    commands = [
        s["run"]
        for job in workflow["jobs"].values()
        for s in job["steps"]
        if "pytest" in s.get("run", "")
    ]
    assert commands == [
        "uv run --locked pytest tests/ -v -m "
        "'not neo4j and not integration and not image'",
        "uv run --locked pytest tests/integration/ -v -m 'not neo4j and not image'",
    ]


def test_collection_keeps_contracts_public_and_other_integrations_marked() -> None:
    hook = runpy.run_path(str(PROJECT_ROOT / "tests/integration/conftest.py"))[
        "pytest_collection_modifyitems"
    ]
    items = []
    marks: list[list[str]] = [[], ["integration", "image"], [], [], []]
    paths = [
        "tests/integration/test_docker_image.py",
        "tests/integration/test_docker_image.py",
        "tests/integration/test_pipeline.py",
        r"tests\integration\test_pipeline.py",
        "tests/test_auth.py",
    ]
    for path, item_marks in zip(paths, marks, strict=True):
        items.append(
            SimpleNamespace(
                path=pathlib.Path(path),
                fspath=path,
                add_marker=lambda marker, target=item_marks: target.append(marker.name),
            )
        )
    hook(items)
    assert marks == [[], ["integration", "image"], ["integration"], ["integration"], []]


def test_manual_acceptance_checks_sidecar_and_expected_identity_before_tokens() -> None:
    workflow = _workflow("m2-auth-acceptance.yml")
    assert set(workflow["on"]) == {"workflow_dispatch"}
    gate = workflow["jobs"]["acceptance-gate"]
    steps = gate["steps"]
    prerequisite = _step(gate, "Check configured localhost sidecar prerequisites")
    assert steps.index(prerequisite) < next(
        i for i, s in enumerate(steps) if "login@" in s.get("uses", "")
    )
    (source,) = _python_blocks(prerequisite["run"])
    ast.parse(source)
    for required in ("127.0.0.1", "5000", "/readyz", "/ValidateRequest", "401"):
        assert required in source
    resolver = _step(
        gate, "STEP A.2 (THE GATE): real EntraResolver.resolve() against the live token"
    )
    (source,) = _python_blocks(resolver["run"])
    ast.parse(source)
    assert "original_uri=" in source and "original_method=" in source
    assert 'os.environ["ACCEPTANCE_EXPECTED_OID"]' in source
    assert "expected_identity_map = {expected_oid: expected_created_by}" in source
    assert "identity_map=expected_identity_map" in source
    assert "service_identity_map=expected_identity_map" in source
    assert "contributor_id == expected_created_by" in source
    text = _text(".github/workflows/m2-auth-acceptance.yml")
    assert "appid or azp or oid" not in text
    assert "SKIPPED — missing" not in text


@pytest.mark.parametrize(
    ("validated_oid_matches", "validated_roles", "accepted"),
    [(True, ["Contributor"], True), (False, ["Contributor"], False), (True, [], False)],
)
def test_exact_manual_resolver_block_checks_validated_service_identity(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    validated_oid_matches: bool,
    validated_roles: list[str],
    accepted: bool,
) -> None:
    """Real resolver policy with an injected validator, not live-token proof."""
    from context_intelligence_server import auth  # noqa: PLC0415

    expected_oid = "aaaaaaaa-0000-0000-0000-000000000001"
    tenant_id = "bbbbbbbb-0000-0000-0000-000000000002"
    bearer = "opaque-workflow-test-bearer"
    for name, value in {
        "TOKEN": bearer,
        "AZURE_CLIENT_ID": "cccccccc-0000-0000-0000-000000000003",
        "AZURE_TENANT_ID": tenant_id,
        "ACCEPTANCE_EXPECTED_OID": expected_oid,
        "ACCEPTANCE_CONTRIBUTOR_ID": "fixture-contributor",
        "SERVICE_DATA_ROLE": "Contributor",
        "READER_ROLE": "Reader",
        "ADMIN_ROLE": "IdentityAdmin",
    }.items():
        monkeypatch.setenv(name, value)
    calls: list[tuple[str, str, str]] = []

    class AcceptedClaimsValidator:
        def validate(
            self,
            token: str,
            *,
            original_uri: str,
            original_method: str,
        ) -> dict[str, Any]:
            calls.append((token, original_uri, original_method))
            return {
                "tid": tenant_id,
                "oid": expected_oid
                if validated_oid_matches
                else "dddddddd-0000-0000-0000-000000000004",
                "roles": validated_roles,
                "idtyp": "app",
            }

    # Only replace the authenticity boundary: execute the unchanged workflow
    # source and real EntraResolver, including its separate service-map lookup.
    monkeypatch.setattr(auth, "MiseTokenValidator", AcceptedClaimsValidator)
    gate = _workflow("m2-auth-acceptance.yml")["jobs"]["acceptance-gate"]
    (source,) = _python_blocks(
        _step(
            gate,
            "STEP A.2 (THE GATE): real EntraResolver.resolve() against the live token",
        )["run"]
    )
    namespace: dict[str, Any] = {}
    if accepted:
        exec(compile(source, "<acceptance-resolver>", "exec"), namespace)  # noqa: S102 - trusted checked-in workflow
        assert (
            namespace["contributor_id"],
            namespace["roles"],
            namespace["is_service"],
        ) == ("fixture-contributor", ["Contributor"], True)
        output = capsys.readouterr().out
        assert (
            "Resolver ACCEPTED the live token; mapped identity matches; is_service=True"
            in output
        )
        assert expected_oid not in output and bearer not in output
    else:
        with pytest.raises(
            SystemExit, match="Live MISE/resolver acceptance failed"
        ) as error:
            exec(compile(source, "<acceptance-resolver>", "exec"), namespace)  # noqa: S102 - trusted checked-in workflow
        assert error.value.__cause__ is None and error.value.__suppress_context__
        assert isinstance(error.value.__context__, auth.AuthError)
        assert error.value.__context__.status_code == 403
        assert capsys.readouterr().out == ""
    assert calls == [(bearer, "https://service.example.test/events", "POST")]


@pytest.mark.parametrize(
    ("statuses", "success"),
    [([200, 401], True), ([503], False), ([200, 200], False), ([200, 403], False)],
)
def test_exact_sidecar_prerequisite_requires_health_and_negative_rejection(
    monkeypatch: pytest.MonkeyPatch,
    statuses: list[int],
    success: bool,
) -> None:
    """Doubles prove manual gate decisions only, not configured MISE behavior."""
    status_iter = iter(statuses)
    requests: list[tuple[str, str]] = []
    closed: list[bool] = []

    class Connection:
        def __init__(self, host: str, port: int, timeout: int) -> None:
            assert (host, port, timeout) == ("127.0.0.1", 5000, 3)

        def request(self, method: str, path: str, *, headers: dict[str, str]) -> None:
            requests.append((method, path))
            assert headers["Authorization"] == "Bearer not-a-jwt"

        def getresponse(self) -> SimpleNamespace:
            return SimpleNamespace(status=next(status_iter))

        def close(self) -> None:
            closed.append(True)

    monkeypatch.setattr(http.client, "HTTPConnection", Connection)
    gate = _workflow("m2-auth-acceptance.yml")["jobs"]["acceptance-gate"]
    (source,) = _python_blocks(
        _step(gate, "Check configured localhost sidecar prerequisites")["run"]
    )
    if success:
        exec(compile(source, "<sidecar-prerequisite>", "exec"), {})  # noqa: S102 - trusted checked-in workflow, no input interpolation
        assert requests == [("GET", "/readyz"), ("POST", "/ValidateRequest")]
    else:
        with pytest.raises(SystemExit, match="prerequisite missing or failed"):
            exec(compile(source, "<sidecar-prerequisite>", "exec"), {})  # noqa: S102 - trusted checked-in workflow
    assert len(closed) == len(requests)


def test_manual_preflight_missing_expected_identity_fails_without_values(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    for name in ("AZURE_CLIENT_ID", "AZURE_TENANT_ID"):
        monkeypatch.setenv(name, "aaaaaaaa-0000-0000-0000-000000000001")
    monkeypatch.setenv("ACCEPTANCE_EXPECTED_OID", "")
    preflight = _workflow("m2-auth-acceptance.yml")["jobs"]["preflight"]
    (source,) = _python_blocks(preflight["steps"][0]["run"])
    with pytest.raises(SystemExit, match="Missing/invalid ACCEPTANCE_EXPECTED_OID"):
        exec(compile(source, "<acceptance-preflight>", "exec"), {})  # noqa: S102 - trusted checked-in workflow
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize("idtyp", [None, "app"])
def test_manual_shape_accepts_optional_idtyp_with_expected_mapped_oid(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    idtyp: str | None,
) -> None:
    expected_oid = "aaaaaaaa-0000-0000-0000-000000000001"
    claims = {
        "idtyp": idtyp,
        "oid": expected_oid,
        "roles": ["Contributor"],
        "azp": "fixture-app",
    }
    payload = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
    token = f"fixture.{payload}.not-a-signature"
    for name, value in {
        "TOKEN": token,
        "ACCEPTANCE_EXPECTED_OID": expected_oid,
        "SERVICE_DATA_ROLE": "Contributor",
        "READER_ROLE": "Reader",
        "ADMIN_ROLE": "IdentityAdmin",
    }.items():
        monkeypatch.setenv(name, value)
    gate = _workflow("m2-auth-acceptance.yml")["jobs"]["acceptance-gate"]
    (source,) = _python_blocks(
        _step(gate, "STEP A.1 (THE GATE): decode + assert real token shape")["run"]
    )
    exec(compile(source, "<acceptance-shape>", "exec"), {})  # noqa: S102 - trusted checked-in workflow
    output = capsys.readouterr().out
    assert "ALL CLAIM-SHAPE ASSERTIONS PASSED" in output
    assert expected_oid not in output and token not in output


def test_workflow_embedded_python_and_shell_parse() -> None:
    for filename in (
        "ci.yml",
        "context-intelligence-api-build-deploy.yaml",
        "m2-auth-acceptance.yml",
    ):
        workflow = _workflow(filename)
        for job in workflow["jobs"].values():
            for step in job["steps"]:
                if "run" not in step:
                    continue
                script = step["run"]
                for source in _python_blocks(script):
                    ast.parse(source)
                result = subprocess.run(
                    ["bash", "-n"],
                    input=script,
                    text=True,
                    capture_output=True,
                    timeout=5,
                    check=False,
                )
                assert result.returncode == 0, result.stderr


def test_manifest_keeps_storage_and_declares_unprovisioned_sidecar() -> None:
    # Compare comment-stripped YAML text, not a fixture containing deployment IDs.
    # This guards all existing data, including inline comments, without copying it.
    text = _text("amplifier-online.yaml")
    data_lines = "\n".join(
        line
        for line in text.splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    )
    assert hashlib.sha256(data_lines.encode()).hexdigest() == (
        "c24b98c586b92d4c0321d8e189c806c6a990294b257d3a805b4c380d9d99168e"
    )
    manifest = yaml.safe_load(text)
    assert manifest["services"]["api"]["volume"]["size_gib"] == 100
    assert "sidecars" not in manifest["services"]["api"]
    assert "needs external sidecar wiring not provisioned here" in text
    assert "review only; not merge ready" in text
    assert "PyJWT" not in text


def test_explicit_image_tier_fails_not_skips_when_docker_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(shutil, "which", lambda _command: None)
    with pytest.raises(pytest.fail.Exception, match="requires Docker"):
        test_image_builds_runs_and_serves_authenticated_static_mode()


@pytest.mark.integration
@pytest.mark.image
@pytest.mark.timeout(BUILD_AND_BOOT_TIMEOUT)
def test_image_builds_runs_and_serves_authenticated_static_mode() -> None:
    """Real build and HTTP smoke; not a real MISE sidecar test. Opt in explicitly."""
    if shutil.which("docker") is None:
        pytest.fail("The selected image tier requires Docker", pytrace=False)
    name = f"ci-image-test-{uuid4().hex}"
    tag = f"context-intelligence-server:{name}"

    def docker(
        *args: str,
        timeout: int = 15,
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["docker", *args],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=check,
        )

    docker("info", timeout=15)
    try:
        docker(
            "build",
            "--platform",
            "linux/amd64",
            "--label",
            f"ci.image-test={name}",
            "--tag",
            tag,
            ".",
            timeout=660,
        )
        # Do not run first-boot token-printing here. The unchanged entrypoint is
        # covered separately; this test boots the shipping command with a known,
        # throwaway static key, never allow_unauthenticated.
        container_id = docker(
            "run",
            "--detach",
            "--name",
            name,
            "--label",
            f"ci.image-test={name}",
            "--pull",
            "never",
            "--publish",
            "127.0.0.1::8000",
            "--memory",
            "1g",
            "--cpus",
            "1",
            "--entrypoint",
            "context-intelligence-server",
            "--env",
            "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_AUTH_MODE=static",
            "--env",
            "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_API_KEY=image-test-throwaway-key",
            "--env",
            "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_ALLOW_UNAUTHENTICATED=false",
            "--env",
            "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J_URL=bolt://127.0.0.1:1",
            tag,
        ).stdout.strip()
        binding = json.loads(docker("inspect", container_id).stdout)[0][
            "NetworkSettings"
        ]["Ports"]["8000/tcp"]
        assert binding[0]["HostIp"] == "127.0.0.1"
        base = f"http://127.0.0.1:{binding[0]['HostPort']}"
        deadline = time.monotonic() + 90
        with httpx.Client(timeout=2, trust_env=False) as client:
            while True:
                try:
                    response = client.get(f"{base}/status")
                    if response.status_code == 200:
                        break
                except httpx.TransportError:
                    pass
                if time.monotonic() >= deadline:
                    pytest.fail("Image did not serve /status within 90s", pytrace=False)
                time.sleep(0.5)
            assert response.json()["neo4j_connected"] is False
            expected = tomllib.loads(_text("pyproject.toml"))["project"]["version"]
            assert client.get(f"{base}/version").json() == {"version": expected}
            assert client.post(f"{base}/events", json={}).status_code == 401
    finally:
        # Name is unique even when Docker starts a container then the client fails.
        cleanup_errors: list[str] = []
        for args in (("rm", "--force", name), ("image", "rm", "--force", tag)):
            try:
                result = docker(*args, check=False)
                if result.returncode != 0 and "No such" not in result.stderr:
                    cleanup_errors.append(" ".join(args))
            except subprocess.TimeoutExpired:
                cleanup_errors.append(" ".join(args))
        assert not cleanup_errors, f"Owned image test cleanup failed: {cleanup_errors}"
