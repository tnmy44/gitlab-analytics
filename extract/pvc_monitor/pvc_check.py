from google.cloud import storage
from yaml import load, safe_load, YAMLError, FullLoader
from os import environ as env

def list_buckets():
    # Initialize a GCS client
    scope = ["https://www.googleapis.com/auth/cloud-platform"]
    keyfile = load(env["GCP_SERVICE_CREDS"], Loader=FullLoader)
    credentials = service_account.Credentials.from_service_account_info(keyfile)
    scoped_credentials = credentials.with_scopes(scope)
    storage_client = storage.Client(credentials=scoped_credentials)
    # bucket_obj = storage_client.get_bucket(bucket)


    # List all buckets in the project
    buckets = list(storage_client.list_buckets())

    # Print details for each bucket
    for bucket in buckets:
        print(f"Bucket name: {bucket.name}")
        print(f"Bucket ID: {bucket.id}")
        print(f"Location: {bucket.location}")
        print(f"Storage Class: {bucket.storage_class}")
        print(f"Time Created: {bucket.time_created}")
        print(f"Owner: {bucket.owner}")

        # Get additional details
        bucket = client.get_bucket(bucket.name)
        print(f"Current Space Used: {bucket.usage() / (1024**3):.2f} GB")  # Convert bytes to GB
        print(f"Capacity: {bucket.quota / (1024**3):.2f} GB")  # Convert bytes to GB

        print("\n")

if __name__ == "__main__":
    # Replace 'your_project_id' with your actual Google Cloud project ID
    project_id = "gitlab-analysis"

    list_buckets()