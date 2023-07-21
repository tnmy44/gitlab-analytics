import oci
import os
import sys
from logging import info, basicConfig, getLogger
from fire import Fire
from sqlalchemy.engine.base import Engine
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


def rename_file(name: str) -> str:

    new_name = name.replace("/", "_")

    return new_name


def snowflake_stage_load_new_only_copy(
    file: str,
    stage: str,
    table_path: str,
    engine: Engine,
    type: str = "json",
    on_error: str = "abort_statement",
    file_format_options: str = "",
) -> None:
    """ """

    file_name = os.path.basename(file)
    if file_name.endswith(".gz"):
        full_stage_file_path = f"{stage}/{file_name}"
    else:
        full_stage_file_path = f"{stage}/{file_name}.gz"
    list_query = f"list @{stage}"
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

    basicConfig(stream=sys.stdout, level=20)

    try:
        connection = engine.connect()
        staged_files_i = connection.execute(list_query)
        staged_files = [staged_file.name for staged_file in staged_files_i]
        info(f"found staged files: {staged_files}")

        if file in staged_files:
            info(f"file: {file} already in stage: {stage}")
        else:
            connection.execute(put_query)
            info(f"{file } added to stage: {stage}")

    finally:
        connection.close()
        engine.dispose()

    try:
        connection = engine.connect()

        # info(f"Copying to Table {table_path}.")
        # connection.execute(copy_query)
        # info("Query successfully run")

        # info("Query successfully run")
    finally:
        connection.close()
        engine.dispose()


def load_data():
    for o in report_bucket_objects.data.objects:

        object_details = object_storage.get_object(
            reporting_namespace, reporting_bucket, o.name
        )
        filename = rename_file(o.name)
        info(f"Found extracted file: {filename}")
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
            "test.oci_report",
            f"test.{target_table}",
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
