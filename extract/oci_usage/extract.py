import oci
import os
import sys
from logging import info, basicConfig, getLogger
from fire import Fire
from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
)


config = {
    "user": os.environ["OCI_USER"],
    "key_content": os.environ["OCI_KEY_CONTENT"],
    "fingerprint": os.environ["OCI_FINGERPRINT"],
    "tenancy": os.environ["OCI_TENANCY"],
    "region": os.environ["OCI_REGION"],
}

reporting_namespace = "bling"

prefix_file = ""  # For cost and usage files
destination_path = "extract"

# Make a directory to receive reports
if not os.path.exists(destination_path):
    os.mkdir(destination_path)

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


def snowflake_stage_load_new_only_copy(
    file: str,
    stage: str,
    table_path: str,
    engine: Engine,
    type: str = "json",
    on_error: str = "abort_statement",
    file_format_options: str = "",
) -> None:
    """

    """

    file_name = os.path.basename(file)
    if file_name.endswith(".gz"):
        full_stage_file_path = f"{stage}/{file_name}"
    else:
        full_stage_file_path = f"{stage}/{file_name}.gz"
    remove_query = f"remove @{full_stage_file_path}"
    put_query = f"put 'file://{file}' @{stage} auto_compress=true;"

    if type == "json":
        copy_query = f"""copy into {table_path} (jsontext)
                         from @{full_stage_file_path}
                         file_format=(type='{type}'),
                         on_error='{on_error}';
                         """

    else:
        copy_query = f"""copy into {table_path}
                         from @{full_stage_file_path}
                         file_format=(type='{type}' {file_format_options}),
                         on_error='{on_error}';
                        """

    logging.basicConfig(stream=sys.stdout, level=20)

    try:
        connection = engine.connect()

        logging.info(
            f"Removing file from internal stage with full_stage_file_path: {full_stage_file_path}"
        )
        connection.execute(remove_query)
        logging.info("Query successfully run")

        logging.info("Writing to Snowflake.")
        connection.execute(put_query)
        logging.info("Query successfully run")
    finally:
        connection.close()
        engine.dispose()

    try:
        connection = engine.connect()

        logging.info(f"Copying to Table {table_path}.")
        connection.execute(copy_query)
        logging.info("Query successfully run")

        logging.info(f"Removing {file} from stage.")
        connection.execute(remove_query)
        logging.info("Query successfully run")
    finally:
        connection.close()
        engine.dispose()


def load_data():
    for o in report_bucket_objects.data.objects:
        info("Found file " + o.name)
        object_details = object_storage.get_object(
            reporting_namespace, reporting_bucket, o.name
        )
        filename = o.name.replace("/", "_")

        if "cost" in filename:
            target_table = "oci_cost_report"
        elif "usage" in filename:
            target_table = "oci_usage_report"

        with open(destination_path + "/" + filename, "wb") as f:
            for chunk in object_details.data.raw.stream(
                1024 * 1024, decode_content=False
            ):
                f.write(chunk)

        snowflake_stage_load_new_only_copy(
            f"{destination_path}/{filename}",
            "oci_reports.oci_report",
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
