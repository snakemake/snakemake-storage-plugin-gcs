import pytest
import subprocess
import time
from typing import Generator
import os
from google.auth.credentials import AnonymousCredentials
from google.cloud import storage
from google.api_core.exceptions import Conflict
import tempfile

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


@pytest.fixture(scope="session")
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

    # Check if the container is already running
    existing_container = subprocess.run(
        ["docker", "ps", "-q", "-f", f"name={CONTAINER_NAME}"],
        check=False,
        capture_output=True,
        text=True,
    )

    if existing_container.stdout.strip():
        print("[pytest] Fake GCS Server is already running.")
    else:
        # Run the Fake GCS Server container
        print("\n[pytest] Starting Fake GCS Server in Docker...")
        try:
            # Remove existing container if it exists
            try:
                subprocess.run(
                    ["docker", "rm", "-f", CONTAINER_NAME],
                    capture_output=True,
                    check=False,
                    text=True,
                )
            except subprocess.CalledProcessError:
                pass

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
                pytest.fail(
                    "Failed to start Fake GCS Server: Container is not running."
                )
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

    container_exists = subprocess.run(
        ["docker", "ps", "-a", "-q", "-f", f"name={CONTAINER_NAME}"],
        check=False,
        capture_output=True,
        text=True,
    )
    if container_exists.stdout.strip():
        pytest.fail("Failed to remove Fake GCS Server container.")
    print("[pytest] Fake GCS Server stopped.")


file_data = {
    "test-file.txt": "Hello World!",
    "test-file_2.txt": "Testing candidates",
    "test-file_3.txt": "What",
}


@pytest.fixture(scope="session")
def storage_client(setup_fake_gcs):
    os.environ.setdefault("STORAGE_EMULATOR_HOST", "http://localhost:4443")
    client = storage.Client(
        credentials=AnonymousCredentials(),
        project="test",
    )
    return client


@pytest.fixture(scope="session")
def test_bucket(storage_client):
    bucket = storage_client.bucket("snakemake-test-bucket")
    try:
        storage_client.create_bucket(bucket)
    except Conflict:
        pass

    # Test uploading blobs
    for file_name, contents in file_data.items():
        blob = bucket.blob(file_name)
        blob.upload_from_string(contents)
        assert blob.exists()

    yield bucket

    assert not bucket.blob("foo").exists()
    buckets = list(storage_client.list_buckets())
    assert any(b.name == "snakemake-test-bucket" for b in buckets)

    # Cleanup after tests
    for blob in bucket.list_blobs():
        blob.delete()
    bucket.delete()


def test_blob_operations(test_bucket):
    # Test listing blobs
    blobs = list(test_bucket.list_blobs())
    assert len(blobs) == len(file_data)

    # Test downloading blobs
    for blob in blobs:
        with tempfile.NamedTemporaryFile() as temp_file:
            blob.download_to_filename(temp_file.name)
            temp_file.seek(0)
            content = temp_file.read().decode()
            assert content == file_data[blob.name]


if __name__ == "__main__":
    import pytest

    pytest.main(["-vv", "-s", __file__])
