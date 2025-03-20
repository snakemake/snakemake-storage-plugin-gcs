import os
import shutil
from typing import List, Optional, Type
import uuid
import pytest
from snakemake_interface_storage_plugins.tests import TestStorageBase
from snakemake_interface_storage_plugins.storage_provider import StorageProviderBase
from snakemake_interface_storage_plugins.settings import StorageProviderSettingsBase

from snakemake_storage_plugin_gcs import (
    StorageProvider,
    StorageProviderSettings,
)


# mypy: ignore-errors

# Use local fake server as outlined here:
# https://github.com/fsouza/fake-gcs-server
os.environ["STORAGE_EMULATOR_HOST"] = "http://localhost:4443"


class TestStorage(TestStorageBase):
    __test__ = True
    files_only = True  #

    @pytest.fixture(autouse=True)
    def setup_test_bucket(self, test_bucket):
        # This fixture will automatically run before each test method
        self.test_bucket = test_bucket

    def get_query(self, tmp_path) -> str:
        return "gs://snakemake-test-bucket/test-file.txt"

    def get_query_not_existing(self, tmp_path) -> str:
        bucket = uuid.uuid4().hex
        key = uuid.uuid4().hex
        return f"gs://{bucket}/{key}"

    def get_storage_provider_cls(self) -> Type[StorageProviderBase]:
        # Return the StorageProvider class of this plugin
        return StorageProvider

    def get_storage_provider_settings(self) -> Optional[StorageProviderSettingsBase]:
        # instantiate StorageProviderSettings of this plugin as appropriate
        return StorageProviderSettings(project="test")

    def get_example_args(self) -> List[str]:
        return []

    def test_storage_nonempty_directory(self, tmp_path):
        # make a directory
        tmpdir = "test_nonemptydir"

        # store the directory
        obj = self._get_obj(tmp_path, f"gs://snakemake-test-bucket/{tmpdir}")

        stored = False
        try:
            if not self.retrieve_only:
                obj.local_path().mkdir(parents=True, exist_ok=True)

                assert obj.is_directory()

                print(obj.local_path())
                print("Writing a file in the directory")
                # write a file in the directory
                with open(obj.local_path() / "testfile.txt", "w") as f:
                    f.write("test")
                    f.flush()

                assert obj.bucket.exists()
                assert obj.local_path().exists() and obj.local_path().is_dir()
                print("Storing the directory")

                obj.store_object()
                stored = True

            assert obj.exists()
            print(obj.mtime())
            print(obj.size())

            file = obj.local_path() / "testfile.txt"
            assert file.exists()
            print(file.read_text())

        finally:
            if not self.retrieve_only and stored and self.delete:
                obj.remove()
                shutil.rmtree(obj.local_path())

    def test_list_candidate_matches(self, tmp_path):
        obj = self._get_obj(tmp_path, "gs://snakemake-test-bucket/")
        candidates = list(obj.list_candidate_matches())
        # I think the previous test deletes the first test_object
        expected_matches = [
            "gs://snakemake-test-bucket/test-file_2.txt",
            "gs://snakemake-test-bucket/test-file_3.txt",
        ]
        assert candidates == expected_matches
