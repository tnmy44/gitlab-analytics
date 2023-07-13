import oci
import os
import sys
from logging import info, basicConfig, getLogger, error
from fire import Fire


config = {
    "user": os.environ["OCI_USER"],
    "key_content": os.environ["OCI_KEY_CONTENT"],
    "fingerprint": os.environ["OCI_FINGERPRINT"],
    "tenancy": os.environ["OCI_TENANCY"],
    "region": os.environ["OCI_REGION"],
}

from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    snowflake_stage_load_copy_remove,
)

reporting_namespace = "bling"

prefix_file = ""  #  For cost and usage files
destintation_path = "extract"

# Make a directory to receive reports
if not os.path.exists(destintation_path):
    os.mkdir(destintation_path)

# Get the list of reports
reporting_bucket = config["tenancy"]
object_storage = oci.object_storage.ObjectStorageClient(config)
report_bucket_objects = oci.pagination.list_call_get_all_results(
    object_storage.list_objects,
    reporting_namespace,
    reporting_bucket,
    prefix=prefix_file,
)

snowflake_config_dict = os.environ.copy()
snowflake_engine = snowflake_engine_factory(snowflake_config_dict, "LOADER")


def load_data():
    for o in report_bucket_objects.data.objects:
        info("Found file " + o.name)
        object_details = object_storage.get_object(
            reporting_namespace, reporting_bucket, o.name
        )
        filename = o.name.replace("/", "_")

        if "cost" in filename:
            target_table = "oci_cost_report"
        if "usage" in filename:
            target_table = "oci_usage_report"

        with open(destintation_path + "/" + filename, "wb") as f:
            for chunk in object_details.data.raw.stream(
                1024 * 1024, decode_content=False
            ):
                f.write(chunk)

        snowflake_stage_load_copy_remove(
            f"{destintation_path}/{filename}",
            f"oci_reports.oci_report",
            f"oci_reports.{target_table}",
            snowflake_engine,
            "csv",
            on_error="ABORT_STATEMENT",
            file_format_options="SKIP_HEADER = 1",
        )

        info(f"File {o.name} loaded to table {target_table}")


if __name__ == "__main__":
    basicConfig(stream=sys.stdout, level=20)
    getLogger("snowflake.connector.cursor").disabled = True
    Fire(load_data)
    info("Complete.")
