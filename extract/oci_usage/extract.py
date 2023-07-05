import oci
import os
import sys
import logging
from fire import Fire
from snowflake.sqlalchemy import URL as snowflake_URL
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine


config = {
    "user": os.environ["OCI_USER"],
    "key_content": os.environ["OCI_KEY_CONTENT"],
    "fingerprint": os.environ["OCI_FINGERPRINT"],
    "tenancy": os.environ["OCI_TENANCY"],
    "region": os.environ["OCI_REGION"],
}

from gitlabdata.orchestration_utils import snowflake_engine_factory

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

def snowflake_csv_load_copy_remove(
    file: str, stage: str, table_path: str, engine: Engine, type: str = "json"
) -> None:
    """
    Upload file to stage, copy to table, remove file from stage on Snowflake
    """
    file_date = file.split(".")[0]
    print(file_date)
    put_query = f"put file://{file} @{stage} auto_compress=true;"

    copy_query = f"""
        copy into {table_path}
        from @{stage}
        """
    print(copy_query)
    remove_query = f"remove @{stage} pattern='.*.{type}.gz'"

    logging.basicConfig(stream=sys.stdout, level=20)

    try:
        connection = engine.connect()

        logging.info(f"Clearing {type} files from stage.")
        remove = connection.execute(remove_query)
        logging.info(remove)

        logging.info("Writing to Snowflake.")
        results = connection.execute(put_query)
        logging.info(results)
    finally:
        connection.close()
        engine.dispose()

    try:
        connection = engine.connect()

        logging.info(f"Copying to Table {table_path}.")
        copy_results = connection.execute(copy_query)
        logging.info(copy_results)

        logging.info(f"Removing {file} from stage.")
        remove = connection.execute(remove_query)
        logging.info(remove)
    finally:
        connection.close()
        engine.dispose()

def load_data():
    for o in report_bucket_objects.data.objects:
        logging.info("Found file " + o.name)
        object_details = object_storage.get_object(
            reporting_namespace, reporting_bucket, o.name
        )
        filename = o.name.replace("/", "_")

        if "cost" in filename:
            target_table ="oci_cost_report"
        if "usage" in filename:
            target_table ="oci_usage_report"

        with open(destintation_path + "/" + filename, "wb") as f:
            for chunk in object_details.data.raw.stream(
                1024 * 1024, decode_content=False
            ):
                f.write(chunk)

        snowflake_csv_load_copy_remove(
            f"{destintation_path}/{filename}",
            f"test.oci_report",
            f"test.{target_table}",
            snowflake_engine,
            on_error="ABORT_STATEMENT",
        )        

        logging.info(f"File {o.name} loaded to table {target_table}" )


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=20)
    logging.getLogger("snowflake.connector.cursor").disabled = True
    Fire(load_data)
    logging.info("Complete.")
