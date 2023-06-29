import oci
import os
from logging import info, basicConfig, getLogger, error
from fire import Fire


config = {
    "user": os.environ["OCI_USER"],
    "key_content": os.environ["OCI_KEY_CONTENT"],
    "fingerprint": os.environ["OCI_FINGERPRINT"],
    "tenancy": os.environ["OCI_TENANCY"],
    "region": os.environ["OCI_REGION"],
}


reporting_namespace = "bling"

prefix_file = ""  #  For cost and usage files
destintation_path = "extract"

# Make a directory to receive reports
if not os.path.exists(destintation_path):
    os.mkdir(destintation_path)

# Get the list of reports
# config = oci.config.from_file(oci.config.DEFAULT_LOCATION, oci.config.DEFAULT_PROFILE)
reporting_bucket = config["tenancy"]
object_storage = oci.object_storage.ObjectStorageClient(config)
report_bucket_objects = oci.pagination.list_call_get_all_results(
    object_storage.list_objects,
    reporting_namespace,
    reporting_bucket,
    prefix=prefix_file,
)


def load_data():
    for o in report_bucket_objects.data.objects:
        info("Found file " + o.name)
        object_details = object_storage.get_object(
            reporting_namespace, reporting_bucket, o.name
        )
        filename = o.name.replace("/", "_")

        with open(destintation_path + "/" + filename, "wb") as f:
            for chunk in object_details.data.raw.stream(
                1024 * 1024, decode_content=False
            ):
                f.write(chunk)

        info("----> File " + o.name + " Downloaded")


if __name__ == "__main__":
    basicConfig(stream=sys.stdout, level=20)
    getLogger("snowflake.connector.cursor").disabled = True
    Fire(load_data)
    info("Complete.")
