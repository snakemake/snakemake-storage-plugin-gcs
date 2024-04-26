import os
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
    files_only = False

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

    def test_storage_dbg(self, tmp_path):
        assert not (
            self.store_only and self.retrieve_only
        ), "store_only and retrieve_only may not be True at the same time"

        obj = self._get_obj(tmp_path, self.get_query(tmp_path))

        stored = False
        try:
            if not self.retrieve_only:
                obj.local_path().parent.mkdir(parents=True, exist_ok=True)
                with open(obj.local_path(), "w") as f:
                    f.write("test")
                    f.flush()
                obj.store_object()
                stored = True
                obj.local_path().unlink()

            assert obj.exists()
            print(obj.mtime())
            print(obj.size())

            if not self.store_only:
                obj.local_path().parent.mkdir(parents=True, exist_ok=True)
                obj.retrieve_object()

        finally:
            if not self.retrieve_only and stored and self.delete:
                obj.remove()
