"""Real integration test for the Docker image — build it, run it, hit it.

This is the *reality stamp* for the container. It replaces a suite of brittle
text-matching tests (`tests/test_docker_infrastructure.py`) that only asserted
substrings existed in the Dockerfile — those pass even if the image never builds
and prove nothing about actual behaviour.

Here we do what a user does:
  1. Build the image from the Dockerfile (proves the Azure Linux base pulls,
     `uv pip install` resolves, and the layered build succeeds).
  2. Run the container (proves the entrypoint runs and the FastAPI server boots
     past its fail-closed auth gate).
  3. Curl `/status` over the network (proves the server actually serves).

One assertion here validates more than every string-match test combined.

Cost: a cold `docker build` plus container start-up. Budget a few minutes; this
is deliberately placed in `tests/integration/` (auto-marked ``integration``) so
it runs in the dedicated integration CI job, not the fast unit sweep. It is
skipped entirely when Docker is not available.
"""

from __future__ import annotations

import pathlib
import socket
import time
from collections.abc import Generator
from typing import Any

import httpx
import pytest

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
IMAGE_TAG = "context-intelligence-server:integration-test"

# A cold build (base image pull + dependency install) far exceeds the global
# 30s pytest-timeout, so override it generously for this one test.
BUILD_AND_BOOT_TIMEOUT = 900


def _get_free_port() -> int:
    """Return a free TCP port on localhost."""
    with socket.socket() as s:
        s.bind(("", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def built_image() -> Generator[Any, None, None]:
    """Build the server image from the Dockerfile once for this module.

    Skips the whole module if the ``docker`` package or a reachable Docker
    daemon is unavailable, mirroring the house pattern in tests/neo4j/conftest.py.
    """
    try:
        import docker  # type: ignore[import-untyped]
        from docker.errors import BuildError, DockerException  # type: ignore[import-untyped]
    except ImportError:
        pytest.skip("docker package not installed — skip Docker image test")

    try:
        client = docker.from_env()
        client.ping()
    except DockerException as exc:
        pytest.skip(f"Docker daemon not reachable — skip Docker image test: {exc}")

    try:
        image, _logs = client.images.build(
            path=str(PROJECT_ROOT),
            tag=IMAGE_TAG,
            rm=True,
            forcerm=True,
        )
    except BuildError as exc:
        # A build failure is a REAL product failure — surface it loudly, do not skip.
        log_tail = "\n".join(
            line.get("stream", "").rstrip()
            for line in exc.build_log
            if isinstance(line, dict) and line.get("stream")
        )
        pytest.fail(f"Docker image build failed:\n{log_tail}\n\nError: {exc}")

    yield image

    # Best-effort cleanup — never fail the suite on teardown.
    try:
        client.images.remove(image.id, force=True)
    except Exception:
        pass


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
