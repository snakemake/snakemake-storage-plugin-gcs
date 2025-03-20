import pytest
import subprocess
import time
from typing import Generator

CONTAINER_NAME = "snakemake-gcs-server"


# mypy: ignore-errors
def is_docker_installed() -> bool:
    """Check if Docker is installed and available."""
    try:
        subprocess.run(
            ["docker", "--version"], check=True, capture_output=True, text=True
        )
        return True
    except FileNotFoundError:
        return False
    except subprocess.CalledProcessError:
        return False


def is_docker_running() -> bool:
    """Check if the Docker daemon is running."""
    try:
        subprocess.run(["docker", "ps"], check=True, capture_output=True, text=True)
        return True
    except subprocess.CalledProcessError:
        return False


@pytest.fixture(scope="session", autouse=True)
def setup_fake_gcs() -> Generator[None, None, None]:
    """
    Setup fixture to start the Fake GCS Server container before tests,
    and shut it down after tests are complete.
    """

    if not is_docker_installed():
        pytest.fail(
            "Docker is not installed! Please install Docker before running tests."
        )

    if not is_docker_running():
        pytest.fail(
            "Docker daemon is not running! Please start Docker before running tests."
        )

    # Run the Fake GCS Server container
    print("\n[pytest] Starting Fake GCS Server in Docker...")
    try:
        # Remove existing container if it exists
        subprocess.run(["docker", "rm", "-f", CONTAINER_NAME], check=True)
        # Start a new container
        subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "--name",
                CONTAINER_NAME,
                "-p",
                "4443:4443",
                "-v",
                "storage_data:/storage",
                "fsouza/fake-gcs-server",
                "-scheme",
                "http",
            ],
            check=True,
        )

        # Wait for it to be ready
        time.sleep(3)
        # verify that the container is running
        is_running = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Running}}", CONTAINER_NAME],
            check=True,
            capture_output=True,
            text=True,
        )
        if is_running.stdout.strip() == "false":
            pytest.fail("Failed to start Fake GCS Server: Container is not running.")
        print("[pytest] Fake GCS Server started.")

    except subprocess.CalledProcessError as e:
        pytest.fail(f"Failed to start Fake GCS Server: {e}")

    yield  # Run tests

    # Retrieve logs before stopping
    print("\n[pytest] Capturing Fake GCS Server logs before teardown...\n")
    try:
        logs = subprocess.run(
            ["docker", "logs", CONTAINER_NAME],
            check=True,
            capture_output=True,
            text=True,
        )
        print(logs.stderr)  # Print logs to console
    except subprocess.CalledProcessError:
        print("[pytest] Failed to retrieve logs from Fake GCS Server.")

    # Teardown: Stop and remove the container
    print("\n[pytest] Stopping Fake GCS Server...")
    subprocess.run(["docker", "rm", "-f", CONTAINER_NAME], check=True)

    is_stopped = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Running}}", CONTAINER_NAME],
        check=False,
        capture_output=True,
        text=True,
    )
    if is_stopped.stdout.strip() == "true":
        pytest.fail("Failed to stop Fake GCS Server: Container is still running.")
    print("[pytest] Fake GCS Server stopped.")


# def test_fake_gcs_server_starts_and_stops(setup_fake_gcs) -> None:
#     """Test that the fake GCS server starts up and stops properly"""
#     # Verify container is running
#     result = subprocess.run(
#         ["docker", "inspect", "-f", "{{.State.Running}}", CONTAINER_NAME],
#         check=True,
#         capture_output=True,
#         text=True,
#     )
#     assert result.stdout.strip() == "true"


if __name__ == "__main__":
    import pytest

    pytest.main(["-vv", "-s", __file__])
