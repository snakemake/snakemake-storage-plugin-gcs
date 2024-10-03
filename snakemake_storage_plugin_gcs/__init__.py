from dataclasses import dataclass, field
import re
from typing import Any, Iterable, List, Optional
from snakemake_interface_common.utils import lazy_property
from snakemake_interface_storage_plugins.settings import StorageProviderSettingsBase
from snakemake_interface_storage_plugins.storage_provider import (
    StorageProviderBase,
    StorageQueryValidationResult,
    ExampleQuery,
    QueryType,
)

from snakemake.exceptions import WorkflowError, CheckSumMismatchException
from snakemake_interface_storage_plugins.storage_object import (
    StorageObjectRead,
    StorageObjectWrite,
    StorageObjectGlob,
)
from snakemake_interface_storage_plugins.common import Operation
from snakemake_interface_storage_plugins.io import (
    IOCacheStorageInterface,
    get_constant_prefix,
    Mtime,
)
from snakemake_interface_common.logging import get_logger

from urllib.parse import urlparse
import base64
import os
from pathlib import Path
import google.cloud.exceptions
from google.cloud import storage
from google.cloud.storage import transfer_manager
from google.api_core import retry
from google_crc32c import Checksum

_RE_GCS_SCHEME = re.compile(r"^gcs://")


# Optional:
# Settings for the Google Storage plugin (e.g. host url, credentials).
# They will occur in the Snakemake CLI as --storage-<storage-plugin-name>-<param-name>
# Note from @vsoch - these are likely not complete!
@dataclass
class StorageProviderSettings(StorageProviderSettingsBase):
    project: Optional[str] = field(
        default=None,
        metadata={
            "help": "Google Cloud Project",
            "env_var": True,
            "required": True,
        },
    )
    keep_local: Optional[bool] = field(
        default=False,
        metadata={
            "help": "keep local copy of storage object(s)",
            "env_var": False,
            "required": False,
            "type": bool,
        },
    )
    stay_on_remote: Optional[bool] = field(
        default=False,
        metadata={
            "help": "The artifacts should stay on the remote ",
            "env_var": False,
            "required": False,
            "type": bool,
        },
    )
    retries: int = field(
        default=5,
        metadata={
            "help": "Google Cloud API retries",
            "env_var": False,
            "required": False,
            "type": int,
        },
    )


class Crc32cCalculator:
    """
    A wrapper to write a file and calculate a crc32 checksum.

    The Google Python client doesn't provide a way to stream a file being
    written, so we can wrap the file object in an additional class to
    do custom handling. This is so we don't need to download the file
    and then stream-read it again to calculate the hash.
    """

    def __init__(self, fileobj):
        self._fileobj = fileobj
        self.checksum = Checksum()

    def write(self, chunk):
        self._fileobj.write(chunk)
        self._update(chunk)

    def _update(self, chunk):
        """
        Given a chunk from the read in file, update the hexdigest
        """
        self.checksum.update(chunk)

    def hexdigest(self):
        """
        Return the hexdigest of the hasher.

        The Base64 encoded CRC32c is in big-endian byte order.
        See https://cloud.google.com/storage/docs/hashes-etags
        """
        return base64.b64encode(self.checksum.digest()).decode("utf-8")


def google_cloud_retry_predicate(ex):
    """
    Google cloud retry with specific Google Cloud errors.

    Given an exception from Google Cloud, determine if it's one in the
    listing of transient errors (determined by function
    google.api_core.retry.if_transient_error(exception)) or determine if
    triggered by a hash mismatch due to a bad download. This function will
    return a boolean to indicate if retry should be done, and is typically
    used with the google.api_core.retry.Retry as a decorator (predicate).

    Arguments:
      ex (Exception) : the exception passed from the decorated function
    Returns: boolean to indicate doing retry (True) or not (False)
    """
    from requests.exceptions import ReadTimeout

    # Most likely case is Google API transient error.
    if retry.if_transient_error(ex):
        return True
    # Timeouts should be considered for retry as well.
    if isinstance(ex, ReadTimeout):
        return True
    # Could also be checksum mismatch of download.
    if isinstance(ex, CheckSumMismatchException):
        return True
    return False


@retry.Retry(predicate=google_cloud_retry_predicate)
def download_blob(blob, filename):
    """
    Download and validate storage Blob to a blob_fil.

    Arguments:
      blob (storage.Blob) : the Google storage blob object
      blob_file (str)     : the file path to download to
    Returns: boolean to indicate doing retry (True) or not (False)
    """

    # create parent directories if necessary
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    # ideally we could calculate hash while streaming to file with provided function
    # https://github.com/googleapis/python-storage/issues/29
    with open(filename, "wb") as blob_file:
        parser = Crc32cCalculator(blob_file)
        blob.download_to_file(parser)
    os.sync()

    # **Important** hash can be incorrect or missing if not refreshed
    blob.reload()

    # Compute local hash and verify correct
    if parser.hexdigest() != blob.crc32c:
        os.remove(filename)
        raise CheckSumMismatchException("The checksum of %s does not match." % filename)
    return filename


# Required:
# Implementation of your storage provider
# settings are available via self.settings
class StorageProvider(StorageProviderBase):
    # For compatibility with future changes, you should not overwrite the __init__
    # method. Instead, use __post_init__ to set additional attributes and initialize
    # futher stuff.

    def __post_init__(self):
        self.client = storage.Client()

    @classmethod
    def is_valid_query(cls, query: str) -> StorageQueryValidationResult:
        """
        Return whether the given query is valid for this storage provider.
        I'm not sure I follow this logic so I'm copying what S3 does.
        """
        try:
            parsed = urlparse(query)
        except Exception as e:
            return StorageQueryValidationResult(
                query=query,
                valid=False,
                reason=f"cannot be parsed as URL ({e})",
            )

        if parsed.scheme != "gcs" and parsed.scheme != "gs":
            return StorageQueryValidationResult(
                query=query,
                valid=False,
                reason="must start with gcs or gs scheme (gs://... or gcs://...)",
            )
        return StorageQueryValidationResult(
            query=query,
            valid=True,
        )

    @classmethod
    def example_queries(cls) -> List[ExampleQuery]:
        """
        Return an example query with description for this storage provider.
        """
        return [
            ExampleQuery(
                query="gs://mybucket/myfile.txt",
                type=QueryType.ANY,
                description="A file in an google storage (GCS) bucket",
            ),
            ExampleQuery(
                query="gcs://mybucket/myfile.txt",
                type=QueryType.ANY,
                description="A file in an google storage (GCS) bucket (alternative "
                "query scheme)",
            ),
        ]

    def use_rate_limiter(self) -> bool:
        """Return False if no rate limiting is needed for this provider."""
        return False

    def default_max_requests_per_second(self) -> float:
        """Return the default maximum number of requests per second for this storage
        provider."""
        ...

    def rate_limiter_key(self, query: str, operation: Operation):
        """Return a key for identifying a rate limiter given a query and an operation.

        This is used to identify a rate limiter for the query.
        E.g. for a storage provider like http that would be the host name.
        For s3 it might be just the endpoint URL.
        """
        ...

    def list_objects(self, query: Any) -> Iterable[str]:
        """
        Return an iterator over all objects in the storage that match the query.

        This is optional and can raise a NotImplementedError() instead.
        """
        parsed = urlparse(query)
        bucket_name = parsed.netloc
        b = self.client.bucket(bucket_name, user_project=self.settings.project)
        return [k.name for k in b.list_blobs()]

    def postprocess_query(self, query: str) -> str:
        # normalize gcs:// to gs:// (the official scheme for google storage tools)
        return _RE_GCS_SCHEME.sub("gs://", query)


# Required:
# Implementation of storage object. If certain methods cannot be supported by your
# storage (e.g. because it is read-only see
# snakemake-storage-http for comparison), remove the corresponding base classes
# from the list of inherited items.
# Note from @vsoch - I have not worked on this in depth yet, only moved functions over.
# It should take logic from:
# https://github.com/snakemake/snakemake/tree/series-7/snakemake/remote
class StorageObject(StorageObjectRead, StorageObjectWrite, StorageObjectGlob):
    # For compatibility with future changes, you should not overwrite the __init__
    # method. Instead, use __post_init__ to set additional attributes and initialize
    # futher stuff.

    def __post_init__(self):
        parsed = urlparse(self.query)
        self.bucket_name = parsed.netloc
        self.key = parsed.path.lstrip("/")
        self._local_suffix = self._local_suffix_from_key(self.key)
        self._is_dir = None
        self.logger = get_logger()

    def cleanup(self):
        # Close any open connections, unmount stuff, etc.
        pass

    async def inventory(self, cache: IOCacheStorageInterface):
        """
        From this file, try to find as much existence and modification date
        information as possible. Only retrieve that information that comes for free
        given the current object.

        Using client.list_blobs(), we want to iterate over the objects in
        the "folder" of a bucket and store information about the IOFiles in the
        provided cache (snakemake.io.IOCache) indexed by bucket/blob name.
        This will be called by the first mention of a remote object, and
        iterate over the entire bucket once (and then not need to again).
        This includes:
         - cache.exist_remote
         - cache.mtime
         - cache.size
        """
        if self.get_inventory_parent() in cache.exists_in_storage:
            # bucket has been inventorized before, stop here
            return

        # check if bucket exists
        if not self.bucket.exists():
            cache.exists_in_storage[self.cache_key()] = False
            cache.exists_in_storage[self.get_inventory_parent()] = False
        else:
            subfolder = os.path.dirname(self.blob.name)
            for blob in self.client.list_blobs(self.bucket_name, prefix=subfolder):
                # By way of being listed, it exists. mtime is a datetime object
                key = self.cache_key(self._local_suffix_from_key(blob.name))
                cache.exists_in_storage[key] = True
                cache.mtime[key] = Mtime(storage=blob.updated.timestamp())
                cache.size[key] = blob.size
        #         # TODO cache "is directory" information

    def get_inventory_parent(self) -> Optional[str]:
        """
        Return the parent directory of this object.
        """
        return self.cache_key(self.bucket_name)

    def local_suffix(self) -> str:
        """
        Return a unique suffix for the local path, determined from self.query.
        """
        return self._local_suffix

    def _local_suffix_from_key(self, key: str) -> str:
        return f"{self.bucket_name}/{key}"

    @retry.Retry(predicate=google_cloud_retry_predicate)
    def exists(self) -> bool:
        """
        Return true if the object exists.
        """
        return self.bucket.exists() and (
            self.blob.exists() or any(self.directory_entries())
        )

    @retry.Retry(predicate=google_cloud_retry_predicate)
    def mtime(self) -> float:
        """
        Return the modification time
        """

        def get_mtime(blob):
            if blob.updated is None:
                blob.reload()
            return blob.updated.timestamp()

        if self.is_directory():
            entries = list(self.directory_entries())
            assert (
                entries
            ), f"bug: mtime called but directory does not seem to exist: {self.query}"
            return max(get_mtime(blob) for blob in entries)
        else:
            return get_mtime(self.blob)

    @retry.Retry(predicate=google_cloud_retry_predicate)
    def size(self) -> int:
        """
        Return the size in bytes
        """
        if self.is_directory():
            return 0
        else:
            # reload (see mtime) does not retrieve the size!
            return self.bucket.get_blob(self.key).size // 1024

    @retry.Retry(predicate=google_cloud_retry_predicate, deadline=600)
    def retrieve_object(self):
        """
        Ensure that the object is accessible locally under self.local_path()
        """
        # Create just a directory, or a file itself
        if self.is_directory():
            self._download_directory()
        else:
            download_blob(self.blob, self.local_path())

    # The following to methods are only required if the class inherits from
    # StorageObjectReadWrite.

    @retry.Retry(predicate=google_cloud_retry_predicate)
    def store_object(self):
        """
        Upload an object to storage

        TODO: note from vsoch - I'm not sure I read this function name right,
        but I didn't find an equivalent "upload" function so I thought this might
        be it. The original function comment is below.
        """
        # Ensure that the object is stored at the location specified by
        # self.local_path().
        try:
            self.ensure_bucket_exists()

            # Distinguish between single file, and folder
            local_object = self.local_path()
            if os.path.isdir(local_object):
                self.upload_directory(local_directory_path=local_object)
            else:
                self.blob.upload_from_filename(local_object)

        except google.cloud.exceptions.Forbidden as e:
            raise WorkflowError(
                e,
                "When running locally, make sure that you are authenticated "
                "via gcloud (see Snakemake documentation). When running in a "
                "kubernetes cluster, make sure that storage-rw is added to "
                "--scopes (see Snakemake documentation).",
            )

    def ensure_bucket_exists(self) -> None:
        """
        Check that the bucket exists, if not create it.
        """
        if not self.bucket.exists():
            self.client.create_bucket(self.bucket)

    def upload_directory(self, local_directory_path: Path):
        """
        Upload a directory to the storage.
        """
        self.ensure_bucket_exists()

        # if the local directory is empty, we need to create a blob
        # with no content to represent the directory
        if not os.listdir(local_directory_path):
            self.blob.upload_from_string(
                "", content_type="application/x-www-form-urlencoded;charset=UTF-8"
            )

        for root, _, files in os.walk(local_directory_path):
            for filename in files:
                relative_filepath = os.path.join(root, filename)
                local_prefix = self.provider.local_prefix.as_posix()

                # remove the prefix ("".snakemake/storage/gcs/{bucket_name}/)
                # this gives us the path to the file relative to the bucket
                bucket_file_path = (
                    relative_filepath.removeprefix(local_prefix)
                    .lstrip("/")
                    .removeprefix(self.bucket_name)
                    .lstrip("/")
                )

                blob = self.bucket.blob(bucket_file_path)
                blob.upload_from_filename(relative_filepath)

    @retry.Retry(predicate=google_cloud_retry_predicate)
    def remove(self) -> None:
        """
        Remove the object from the storage.
        """
        if self.is_directory():
            prefix = self.key
            if not prefix.endswith("/"):
                prefix += "/"
            blobs = self.client.list_blobs(self.bucket_name, prefix=prefix)
            for blob in blobs:
                blob.delete()
        else:
            self.blob.delete()

    # The following to methods are only required if the class inherits from
    # StorageObjectGlob.

    @retry.Retry(predicate=google_cloud_retry_predicate)
    def list_candidate_matches(self) -> Iterable[str]:
        """Return a list of candidate matches in the storage for the query."""
        # This is used by glob_wildcards() to find matches for wildcards in the query.
        prefix = get_constant_prefix(self.query)
        if prefix.startswith(f"gs://{self.bucket.name}"):
            prefix = prefix[6 + len(self.bucket.name) :].lstrip("/")

            return (
                f"gs://{self.bucket.name}/{item.name}"
                for item in self.bucket.list_blobs(prefix=prefix)
            )
        else:
            raise WorkflowError(
                f"GCS storage object {self.query} must start with gs:// or gcs://"
            )

    # Helper functions and properties not part of standard interface
    # TODO check parent class and determine if any of these are already implemented

    def directory_entries(self):
        """
        Get directory entries under a prefix.
        """
        prefix = self.key
        if not prefix.endswith("/"):
            prefix += "/"
        return self.client.list_blobs(self.bucket_name, prefix=prefix)

    @retry.Retry(predicate=google_cloud_retry_predicate)
    def is_directory(self):
        """
        Determine if a a file is a file or directory.
        """
        if self.local_path().exists() and self.local_path().is_dir():
            return True
        elif self.blob.exists():
            return False
        return any(self.directory_entries())

    @retry.Retry(predicate=google_cloud_retry_predicate)
    def _download_directory(self):
        """
        Handle download of a storage folder (assists retrieve_blob)
        """
        blob_names = [blob.name for blob in self.directory_entries()]
        results = transfer_manager.download_many_to_path(
            bucket=self.bucket,
            blob_names=blob_names,
            destination_directory=self.provider.local_prefix / self.bucket.name,
            create_directories=True,
        )
        for name, result in zip(blob_names, results):
            # The results list is either `None` or an exception for each blob in
            # the input list, in order.
            if isinstance(result, Exception):
                self.logger.error(
                    "Failed to download {} due to exception: {}".format(name, result)
                )

    @lazy_property
    def bucket(self):
        return self.client.bucket(
            self.bucket_name, user_project=self.provider.settings.project
        )

    @property
    def blob(self):
        return self.bucket.blob(self.key)

    @property
    def client(self):
        return self.provider.client

    # Note from @vsoch - functions removed include:
    # name
    # list (seems to be on provider now)
    # parse (added to parent)
