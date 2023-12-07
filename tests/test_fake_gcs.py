import tempfile
import os

from google.auth.credentials import AnonymousCredentials
from google.cloud import storage

# This endpoint assumes that you are using the default port 4443 from the container.
# If you are using a different port, please set the environment variable
# STORAGE_EMULATOR_HOST.
os.environ.setdefault("STORAGE_EMULATOR_HOST", "http://localhost:4443")


client = storage.Client(
    credentials=AnonymousCredentials(),
    project="test",
    # Alternatively instead of using the global env STORAGE_EMULATOR_HOST. You can
    # define it here.
    # This will set this client object to point to the local google cloud storage.
    # client_options={"api_endpoint": "http://localhost:4443"},
)

# List the Buckets
for bucket in client.list_buckets():
    print(f"Bucket: {bucket.name}\n")

    # List the Blobs in each Bucket
    for blob in bucket.list_blobs():
        print(f"Blob: {blob.name}")

        # Print the content of the Blob
        b = bucket.get_blob(blob.name)
        with tempfile.NamedTemporaryFile() as temp_file:
            s = b.download_to_filename(temp_file.name)
            temp_file.seek(0, 0)
            print(temp_file.read(), "\n")

# Create a new Bucket
bucket = client.bucket("snakemake-test-bucket")
client.create_bucket(bucket)
bucket.blob("test-file.txt").upload_from_string("Hello World!")


assert not bucket.blob("foo").exists()