import oci
import os
import sys
from logging import info, basicConfig, getLogger
from fire import Fire
from sqlalchemy.engine.base import Engine
from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
)

# methods


def rename_file(name: str) -> str:

    new_name = name.replace("/", "_")

    return new_name


def extract_files_from_oci(config, reporting_namespace, file_prefix, destination_path):
    info

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

    oci_extraction = {
        "oci_cost_report": [],
        "oci_usage_report": [],
    }

    for o in report_bucket_objects.data.objects:

        object_details = object_storage.get_object(
            reporting_namespace, reporting_bucket, o.name
        )
        filename = rename_file(o.name)
        info(f"Found extracted file: {filename}")

        with open(destination_path + "/" + filename, "wb") as f:
            for chunk in object_details.data.raw.stream(
                1024 * 1024, decode_content=False
            ):
                f.write(chunk)

        if "cost" in filename:
            oci_extraction["oci_cost_report"].append(filename)
        elif "usage" in filename:
            oci_extraction["oci_usage_report"].append(filename)

    return oci_extraction


def snowflake_copy_staged_files_into_table(
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

    try:
        connection = engine.connect()

        info(f"Copying to Table {table_path}.")
        connection.execute(copy_query)
        info("Query successfully run")

    finally:
        connection.close()


def snowflake_stage_put_copy_files(
    file_list: list,
    stage: str,
    table_path: str,
    engine: Engine,
    type: str = "json",
    on_error: str = "abort_statement",
    file_format_options: str = "",
) -> None:

    list_query = f"list @{stage}"

    info(f"checking for the following files: {file_list}")
    try:
        connection = engine.connect()
        staged_files_i = connection.execute(list_query)
        staged_files = [staged_file.name for staged_file in staged_files_i]
        info(f"found staged files: {staged_files}")
        new_files = [file for file in file_list if file not in staged_files]
        info(f"puting files: {staged_files} into stage: {stage}")
        loaded_files = []
        for file in new_files:
            put_query = f"put 'file://{file}' @{stage} auto_compress=true;"

            connection.execute(put_query)

            snowflake_copy_staged_files_into_table(
                file, stage, table_path, engine, type, on_error, file_format_options
            )

            info(f"File {file} loaded to table {table_path}")

    finally:
        connection.close()
        engine.dispose()
        engine.dispose()


# snowflake config
snowflake_config_dict = os.environ.copy()
snowflake_engine = snowflake_engine_factory(snowflake_config_dict, "LOADER")

# oci config

oci_config = {
    "user": os.environ["OCI_USER"],
    "key_content": os.environ["OCI_KEY_CONTENT"],
    "fingerprint": os.environ["OCI_FINGERPRINT"],
    "tenancy": os.environ["OCI_TENANCY"],
    "region": os.environ["OCI_REGION"],
}

reporting_namespace = "bling"

prefix_file = ""  # For cost and usage files
destination_path = "oci_report"


def load_data():
    info("running oci extraction")
    oci_extraction = extract_files_from_oci(
        oci_config, reporting_namespace, prefix_file, destination_path
    )

    for item in oci_extraction.items():
        target_table = item[0]
        oci_files = item[1]
        info(f"loading files {oci_files} into table: {target_table}")
        snowflake_stage_put_copy_files(
            oci_files,
            "test.oci_report",
            f"test.{target_table}",
            snowflake_engine,
            "csv",
            on_error="ABORT_STATEMENT",
            file_format_options="SKIP_HEADER = 1",
        )


if __name__ == "__main__":
    basicConfig(stream=sys.stdout, level=20)
    getLogger("snowflake.connector.cursor").disabled = True
    Fire(load_data)
    info("Complete.")
