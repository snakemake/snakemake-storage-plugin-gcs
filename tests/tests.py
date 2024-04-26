import os
import shutil
from typing import List, Optional, Type
import uuid
from snakemake_interface_storage_plugins.tests import TestStorageBase
from snakemake_interface_storage_plugins.storage_provider import StorageProviderBase
from snakemake_interface_storage_plugins.settings import StorageProviderSettingsBase

from snakemake_storage_plugin_gcs import StorageProvider, StorageProviderSettings

# Use local fake server as outlined here:
# https://github.com/fsouza/fake-gcs-server
os.environ["STORAGE_EMULATOR_HOST"] = "http://localhost:4443"


class TestStorage(TestStorageBase):
    __test__ = True
    files_only = True  #

    def get_query(self, tmp_path) -> str:
        return "gcs://snakemake-test-bucket/test-file.txt"

    def get_query_not_existing(self, tmp_path) -> str:
        bucket = uuid.uuid4().hex
        key = uuid.uuid4().hex
        return f"gcs://{bucket}/{key}"

    def get_storage_provider_cls(self) -> Type[StorageProviderBase]:
        # Return the StorageProvider class of this plugin
        return StorageProvider

    def get_storage_provider_settings(self) -> Optional[StorageProviderSettingsBase]:
        # instantiate StorageProviderSettings of this plugin as appropriate
        return StorageProviderSettings(project="test")

    def get_example_args(self) -> List[str]:
        return []

    def test_storage_dbg(self, tmp_path):
        assert not (
            self.store_only and self.retrieve_only
        ), "store_only and retrieve_only may not be True at the same time"

        obj = self._get_obj(tmp_path, self.get_query(tmp_path))

        stored = False
        try:
            if not self.retrieve_only:
                print("Creating a local file")
                obj.local_path().parent.mkdir(parents=True, exist_ok=True)
                with open(obj.local_path(), "w") as f:
                    f.write("test")
                    f.flush()
                print("Storing the object")
                obj.store_object()
                stored = True
                print("Removing the local file")
                obj.local_path().unlink()

            assert obj.exists()
            print(obj.mtime())
            print(obj.size())

            if not self.store_only:
                obj.local_path().parent.mkdir(parents=True, exist_ok=True)
                obj.retrieve_object()

        finally:
            if not self.retrieve_only and stored and self.delete:
                print("Removing the object")
                obj.remove()

    def test_storage_nonempty_directory(self, tmp_path):
        # make a directory
        tmpdir = "test_nonemptydir"

        # store the directory
        obj = self._get_obj(tmp_path, f"gcs://snakemake-test-bucket/{tmpdir}")

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
