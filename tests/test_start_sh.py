"""Tests for the start.sh host-side wrapper script.

TDD phase: These tests define the EXPECTED state after adding start.sh (Task 1),
which:
  - Generates credentials on the HOST before docker compose starts
  - Creates both credentials.yaml and neo4j-auth.env if they don't exist
  - Skips credential generation if BOTH files already exist (idempotent)
  - Re-generates if either file is missing (self-healing)
  - Then delegates to docker compose up -d
"""

import os
import pathlib
import stat
import subprocess

import pytest
import yaml

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
START_SH = PROJECT_ROOT / "start.sh"


@pytest.fixture()
def isolated_home(tmp_path: pathlib.Path):
    """tmp home dir with a no-op docker stub and matching env."""
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_docker = fake_bin / "docker"
    fake_docker.write_text("#!/bin/bash\nexit 0\n")
    fake_docker.chmod(0o755)

    env = os.environ.copy()
    env["HOME"] = str(tmp_path)
    env["PATH"] = str(fake_bin) + ":" + env.get("PATH", "")
    return tmp_path, env


def test_start_sh_exists() -> None:
    """start.sh must exist in the project root."""
    assert START_SH.exists(), "start.sh must exist in the project root"


def test_start_sh_is_executable() -> None:
    """start.sh must have the executable bit set."""
    assert START_SH.exists(), "start.sh must exist"
    assert os.access(START_SH, os.X_OK), "start.sh must be executable (chmod +x)"


def test_start_sh_syntax_is_valid_bash() -> None:
    """bash -n must return 0 (no syntax errors)."""
    result = subprocess.run(
        ["bash", "-n", str(START_SH)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"bash -n reported syntax errors:\n{result.stderr}"


def test_start_sh_starts_with_bash_shebang() -> None:
    """Script must start with #!/bin/bash."""
    content = START_SH.read_text()
    first_line = content.splitlines()[0]
    assert first_line == "#!/bin/bash", (
        f"First line must be '#!/bin/bash', got: {first_line!r}"
    )


def test_start_sh_has_strict_mode() -> None:
    """Script must contain 'set -euo pipefail'."""
    content = START_SH.read_text()
    assert "set -euo pipefail" in content, "Script must contain 'set -euo pipefail'"


def test_first_run_creates_credentials_files(
    isolated_home: tuple[pathlib.Path, dict],
) -> None:
    """On first run, both credentials.yaml and neo4j-auth.env are created."""
    fake_home, env = isolated_home
    data_dir = fake_home / "amplifier-context-intelligence-server-data-store"

    # Verify neither file exists before running
    assert not (data_dir / "credentials.yaml").exists()
    assert not (data_dir / "neo4j-auth.env").exists()

    result = subprocess.run(
        ["bash", str(START_SH)],
        capture_output=True,
        text=True,
        env=env,
    )
    assert result.returncode == 0, (
        f"start.sh failed on first run:\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )

    assert (data_dir / "credentials.yaml").exists(), (
        "credentials.yaml must be created on first run"
    )
    assert (data_dir / "neo4j-auth.env").exists(), (
        "neo4j-auth.env must be created on first run"
    )


def test_first_run_credentials_yaml_has_expected_keys(
    isolated_home: tuple[pathlib.Path, dict],
) -> None:
    """credentials.yaml must contain neo4j_url, neo4j_user, neo4j_password, api_key."""
    fake_home, env = isolated_home

    result = subprocess.run(
        ["bash", str(START_SH)],
        capture_output=True,
        text=True,
        env=env,
    )
    assert result.returncode == 0, (
        f"start.sh failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )

    data_dir = fake_home / "amplifier-context-intelligence-server-data-store"
    creds = yaml.safe_load((data_dir / "credentials.yaml").read_text())

    assert "neo4j_url" in creds, "credentials.yaml must contain 'neo4j_url'"
    assert "neo4j_user" in creds, "credentials.yaml must contain 'neo4j_user'"
    assert "neo4j_password" in creds, "credentials.yaml must contain 'neo4j_password'"
    assert "api_key" in creds, "credentials.yaml must contain 'api_key'"
    assert creds["neo4j_url"] == "neo4j://neo4j:7687", (
        "neo4j_url must be 'neo4j://neo4j:7687'"
    )
    assert creds["neo4j_user"] == "neo4j", "neo4j_user must be 'neo4j'"


def test_first_run_neo4j_auth_env_format(
    isolated_home: tuple[pathlib.Path, dict],
) -> None:
    """neo4j-auth.env must contain NEO4J_AUTH=neo4j/<password>."""
    fake_home, env = isolated_home

    result = subprocess.run(
        ["bash", str(START_SH)],
        capture_output=True,
        text=True,
        env=env,
    )
    assert result.returncode == 0, (
        f"start.sh failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )

    data_dir = fake_home / "amplifier-context-intelligence-server-data-store"
    auth_content = (data_dir / "neo4j-auth.env").read_text()
    assert auth_content.startswith("NEO4J_AUTH=neo4j/"), (
        f"neo4j-auth.env must start with 'NEO4J_AUTH=neo4j/', got: {auth_content!r}"
    )


def test_subsequent_run_does_not_overwrite_credentials(
    isolated_home: tuple[pathlib.Path, dict],
) -> None:
    """On subsequent run, existing credentials.yaml and neo4j-auth.env are NOT overwritten."""
    fake_home, env = isolated_home
    data_dir = fake_home / "amplifier-context-intelligence-server-data-store"
    data_dir.mkdir(parents=True)

    # Pre-create credentials with sentinel values
    sentinel_creds = "neo4j_password: SENTINEL_PASSWORD\napi_key: SENTINEL_KEY\n"
    sentinel_auth = "NEO4J_AUTH=neo4j/SENTINEL_PASSWORD\n"
    (data_dir / "credentials.yaml").write_text(sentinel_creds)
    (data_dir / "neo4j-auth.env").write_text(sentinel_auth)

    result = subprocess.run(
        ["bash", str(START_SH)],
        capture_output=True,
        text=True,
        env=env,
    )
    assert result.returncode == 0, (
        f"start.sh failed on subsequent run:\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )

    # Files must not have been overwritten
    assert (data_dir / "credentials.yaml").read_text() == sentinel_creds, (
        "credentials.yaml must NOT be overwritten on subsequent run"
    )
    assert (data_dir / "neo4j-auth.env").read_text() == sentinel_auth, (
        "neo4j-auth.env must NOT be overwritten on subsequent run"
    )


def test_data_subdirectories_created_on_first_run(
    isolated_home: tuple[pathlib.Path, dict],
) -> None:
    """blobs, logs, and neo4j subdirectories must be created on first run."""
    fake_home, env = isolated_home
    data_dir = fake_home / "amplifier-context-intelligence-server-data-store"

    result = subprocess.run(
        ["bash", str(START_SH)],
        capture_output=True,
        text=True,
        env=env,
    )
    assert result.returncode == 0, (
        f"start.sh failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )

    assert (data_dir / "blobs").is_dir(), "blobs subdirectory must be created"
    assert (data_dir / "logs").is_dir(), "logs subdirectory must be created"
    assert (data_dir / "neo4j").is_dir(), "neo4j subdirectory must be created"


def test_credentials_yaml_mode_600(
    isolated_home: tuple[pathlib.Path, dict],
) -> None:
    """credentials.yaml must have mode 600 after first run."""
    fake_home, env = isolated_home

    result = subprocess.run(
        ["bash", str(START_SH)],
        capture_output=True,
        text=True,
        env=env,
    )
    assert result.returncode == 0, (
        f"start.sh failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )

    data_dir = fake_home / "amplifier-context-intelligence-server-data-store"
    creds_file = data_dir / "credentials.yaml"
    file_mode = stat.S_IMODE(creds_file.stat().st_mode)
    assert file_mode == 0o600, (
        f"credentials.yaml must have mode 600, got {oct(file_mode)}"
    )
