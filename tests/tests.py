import os
from pathlib import Path
from unittest.mock import MagicMock, patch
from typing import List, Optional, Type
import uuid

from snakemake_interface_storage_plugins.tests import TestStorageBase
from snakemake_interface_storage_plugins.storage_provider import StorageProviderBase
from snakemake_interface_storage_plugins.settings import StorageProviderSettingsBase

from snakemake_storage_plugin_gcs import (
    StorageObject,
    StorageProvider,
    StorageProviderSettings,
)

# Use local fake server as outlined here:
# https://github.com/fsouza/fake-gcs-server
os.environ["STORAGE_EMULATOR_HOST"] = "http://localhost:4443"


class TestStorage(TestStorageBase):
    __test__ = True

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

    @patch("snakemake_storage_plugin_gcs.StorageObject.client")
    def test_list_candidate_matches(self, mock_client):
        mock_bucket = MagicMock()
        mock_bucket.name = "mybucket"
        mock_blob1 = MagicMock()
        mock_blob1.name = "path/to/file1.txt"
        mock_blob2 = MagicMock()
        mock_blob2.name = "path/to/file2.txt"
        mock_blob3 = MagicMock()
        mock_blob3.name = "path/to/another/file3.txt"

        settings = self.get_storage_provider_settings()
        storage_provider = StorageProvider(
            local_prefix=Path("./"),
            settings=settings,
        )

        mock_client.bucket.return_value = mock_bucket
        mock_client.list_blobs.return_value = [mock_blob1, mock_blob2, mock_blob3]
        storage_object = StorageObject(
            query="gs://mybucket/path/to/",
            keep_local=True,
            retrieve=True,
            provider=storage_provider,
        )
        # Call the list_candidate_matches method
        candidates = list(storage_object.list_candidate_matches())
        # Assert that the expected candidate matches are returned
        expected_matches = [
            "gs://mybucket/path/to/file1.txt",
            "gs://mybucket/path/to/file2.txt",
            "gs://mybucket/path/to/another/file3.txt",
        ]
        self.assertEqual(candidates, expected_matches)

        mock_client.bucket.assert_called_once_with(
            "mybucket", user_project=storage_object.provider.settings.project
        )
        mock_client.list_blobs.assert_called_once_with("mybucket", prefix="path/to/")
