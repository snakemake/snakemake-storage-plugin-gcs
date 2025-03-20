import tempfile
import os
import pytest
from google.auth.credentials import AnonymousCredentials
from google.cloud import storage
from google.api_core.exceptions import Conflict

# mypy: ignore-errors


@pytest.fixture
def storage_client():
    os.environ.setdefault("STORAGE_EMULATOR_HOST", "http://localhost:4443")
    client = storage.Client(
        credentials=AnonymousCredentials(),
        project="test",
    )
    return client


@pytest.fixture
def test_bucket(storage_client):
    bucket = storage_client.bucket("snakemake-test-bucket")
    try:
        storage_client.create_bucket(bucket)
    except Conflict:
        pass
    yield bucket

    # Cleanup after tests
    for blob in bucket.list_blobs():
        blob.delete()
    bucket.delete()


def test_bucket_creation(test_bucket):
    assert test_bucket.exists()


def test_blob_operations(test_bucket):
    file_data = {
        "test-file.txt": "Hello World!",
        "test-file_2.txt": "Testing candidates",
        "test-file_3.txt": "What",
    }

    # Test uploading blobs
    for file_name, contents in file_data.items():
        blob = test_bucket.blob(file_name)
        blob.upload_from_string(contents)
        assert blob.exists()

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


def test_nonexistent_blob(test_bucket):
    assert not test_bucket.blob("foo").exists()


def test_bucket_listing(storage_client, test_bucket):
    buckets = list(storage_client.list_buckets())
    assert any(b.name == "snakemake-test-bucket" for b in buckets)
