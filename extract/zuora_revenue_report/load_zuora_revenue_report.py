import sys
import logging
import pandas as pd
import io
import numpy as np

from os import environ as env
from fire import Fire
from typing import Dict
from yaml import load, FullLoader
from datetime import datetime
from time import time
from google.cloud import storage
from google.oauth2 import service_account
from google.cloud.storage.bucket import Bucket
from sqlalchemy.engine.base import Connection, Engine
from sheetload.sheetload_dataframe_utils import translate_column_names
from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    query_executor,
)


def get_gcs_storage_client():
    """Do the auth and return a usable gcs bucket object."""

    scope = ["https://www.googleapis.com/auth/cloud-platform"]
    keyfile = load(env["GCP_SERVICE_CREDS"], Loader=FullLoader)
    credentials = service_account.Credentials.from_service_account_info(keyfile)
    scoped_credentials = credentials.with_scopes(scope)
    storage_client = storage.Client(credentials=scoped_credentials)
    return storage_client


def get_files_for_report(bucket, output_file_name):
    """
    This function provide all the file under zuora_revenue_report.
    """
    source_bucket = get_gcs_storage_client().list_blobs(
        bucket, prefix="RAW_DB/staging/zuora_revenue_report", delimiter=None
    )
    file_list = [file.name for file in source_bucket]
    list_of_files_per_report = []
    for file in file_list:
        if output_file_name in file.lower():
            list_of_files_per_report.append(file)

    return list_of_files_per_report


def get_file_name_without_path(file_name):
    """separate file name from the bucket path"""
    return file_name.split("/")[-1]


def get_table_to_load(file_name, output_file_name) -> str:
    """This function provide information to which table to load the data and type of load i.e. it is for header or for body of the report"""

    if get_file_name_without_path(file_name).startswith("header"):
        table_to_load = output_file_name + "_header"
        type_of_load = "header"
    else:
        table_to_load = output_file_name + "_body"
        type_of_load = "body"

    print(f"This is the table we load file {file_name}:{table_to_load}")
    return table_to_load, type_of_load


def load_report_header_snow(schema, file_name, table_to_load, engine):

    copy_header_query = f"COPY INTO {schema}.{table_to_load} from (SELECT REGEXP_REPLACE(t.$1,'( ){1,}','_') AS metadata_column_name, \
     REGEXP_REPLACE(CONCAT(t.$2,COALESCE(t.$3,'')),'(=){1,}','') AS metadata_column_value ,GET(SPLIT(METADATA$FILENAME,'/'),4)  AS file_name \
     FROM @ZUORA_REVENUE_STAGING/{file_name} t ) file_format = (type = csv,field_DELIMITER=',') ;"
    results = query_executor(engine, copy_header_query)
    logging.info(results)


def data_frame_enricher(raw_df, file_name) -> pd.DataFrame:

    """Add _uploaded_at and _file_name to the data frame before persisting also doing some data cleansing."""
    raw_df["_uploaded_at"] = time()
    raw_df.loc[:, "_file_name"] = file_name
    raw_df.columns = [
        translate_column_names(str(column_name)) for column_name in raw_df.columns
    ]
    raw_df = raw_df.infer_objects()
    raw_df.replace("", np.nan, inplace=True)
    return raw_df


def load_report_body_snow(file_name, table_to_load, bucket, engine):
    """load body of report with proper data cleansing and adding metadata like uplodaed_at and file_name to the report."""
    raw_file_bucket = get_gcs_storage_client().bucket(bucket)
    raw_file_blob = raw_file_bucket.blob(file_name)
    raw_data = raw_file_blob.download_as_bytes()
    raw_df = pd.read_csv(io.BytesIO(raw_data))
    enriched_df = data_frame_enricher(raw_df, get_file_name_without_path(file_name))
    enriched_df.to_sql(
        name=table_to_load, con=engine, index=False, if_exists="append", chunksize=15000
    )
    logging.info(
        f"Successfully loaded {enriched_df.shape[0]} rows into {table_to_load}"
    )


def move_file_to_processed(bucket, file_name):
    source_bucket = get_gcs_storage_client.get_bucket(bucket)
    destination_bucket = get_gcs_storage_client.get_bucket(bucket)
    report_ran_date = file_name.split("/")[-2]
    file_name_without_path = file_name.split("/")[-1]
    destination_file_name = f"RAW_DB/processed/zuora_revenue_report/{report_ran_date}/{file_name_without_path}"
    file_name_without_bucket_prefix = "/".join(file_name.split("/")[3:])
    source_blob = source_bucket.blob(file_name_without_bucket_prefix)
    try:
        source_bucket.copy_blob(source_blob, destination_bucket, destination_file_name)
    except:
        logging.error(
            f"Source file {file_name} not found, Please ensure the direcotry is empty for next \
                            run else the file will be over written"
        )
        sys.exit(1)
    try:
        source_blob.delete()
    except:
        logging.error(
            f"{file_name} is not found , throwing this as error to ensure that we are not overwriting the files."
        )
        sys.exit(1)


def zuora_revenue_report_load(
    bucket: str,
    schema: str,
    output_file_name: str,
    conn_dict: Dict[str, str] = None,
) -> None:

    # Set some vars
    engine = snowflake_engine_factory(conn_dict or env, "LOADER", schema)
    # get the filename for the report type from bucket
    list_of_files = get_files_for_report(bucket, output_file_name)
    print(f"List of files to download for : {list_of_files}")
    for file_name in list_of_files:
        table_to_load, type_of_load = get_table_to_load(
            file_name.lower(), output_file_name
        )
        if type_of_load == "header":
            load_report_header_snow(schema, file_name, table_to_load, engine)
            move_file_to_processed(bucket, file_name)
        else:
            load_report_body_snow(file_name, table_to_load, bucket, engine)
            move_file_to_processed(bucket, file_name)


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=20)
    logging.getLogger("snowflake.connector.cursor").disabled = True
    # Copy all environment variables to dict.
    config_dict = env.copy()
    Fire(
        {
            "zuora_report_load": zuora_revenue_report_load,
        }
    )
