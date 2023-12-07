from typing import List, Optional, Type
import uuid
from snakemake_interface_storage_plugins.tests import TestStorageBase
from snakemake_interface_storage_plugins.storage_provider import StorageProviderBase
from snakemake_interface_storage_plugins.settings import StorageProviderSettingsBase

from snakemake_storage_plugin_gcs import StorageProvider, StorageProviderSettings


class TestStorage(TestStorageBase):
    __test__ = True

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
        # Use local fake server as outlined here:
        # https://www.claritician.com/how-to-mock-google-cloud-storage-during-development
        return StorageProviderSettings(
            api_endpoint="http://localhost:5050",
            api_key="test",
        )

    def get_example_args(self) -> List[str]:
        return []
