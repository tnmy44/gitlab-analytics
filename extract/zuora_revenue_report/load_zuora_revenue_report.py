import sys
import logging

from os import environ as env
from fire import Fire
from typing import Dict
from yaml import load, FullLoader
from datetime import datetime
from google.cloud import storage
from google.oauth2 import service_account
from google.cloud.storage.bucket import Bucket
from sqlalchemy.engine.base import Connection, Engine

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



def get_files_for_report(bucket,output_file_name):
    """
        This function provide all the file under zuora_revenue_report.
    """
    source_bucket = get_gcs_storage_client().list_blobs(bucket,prefix="RAW_DB/staging/zuora_revenue_report",delimiter=None)

    list_of_files_per_report=[]
    for file in source_bucket:
        if output_file_name in file:
            list_of_files_per_report.append(file)

    return list_of_files_per_report
    
def get_table_to_load(file_name,output_file_name) -> str:
    """ This function provide information to which table to load the data"""
    file_name_without_path=file_name.split("/")[-1]
    if file_name_without_path.startswith('header'):
           table_to_load=output_file_name+'_header'
           type_of_load='header'
    else:
            table_to_load=output_file_name+'_body'
            type_of_load='body'

    print(f"This is the table we load file {file_name}:{table_to_load}")
    return table_to_load,type_of_load

def load_report_header_snow(schema,file_name,table_to_load,engine):

    copy_header_query=f"COPY INTO {schema}.{table_to_load} from (SELECT REGEXP_REPLACE(t.$1,'( ){1,}','_') AS metadata_column_name, REGEXP_REPLACE(CONCAT(t.$2,COALESCE(t.$3,'')),'(=){1,}','') AS metadata_column_value ,                        GET(SPLIT(METADATA$FILENAME,'/'),4)  AS file_name FROM @raw.zuora_revenue.ZUORA_REVENUE_STAGING/{file_name} t ) file_format = (type = csv,field_DELIMITER=',') ;"
    print(copy_header_query)

def load_report_body_snow(chema,file_name,table_to_load,engine):
    copy_body_query=f""



def zuora_revenue_report_load(
    bucket: str,
    schema: str,
    output_file_name: str,
    conn_dict: Dict[str, str] = None,
) -> None:
    
    # Set some vars
    engine = snowflake_engine_factory(conn_dict or env, "LOADER", schema)
    # get the filename for the report type from bucket
    list_of_files=get_files_for_report(bucket,output_file_name)
    print(f"List of files to download for : {list_of_files}")
    for file_name in list_of_files:
        table_to_load,type_of_load=get_table_to_load(file_name,output_file_name)
        if type_of_load=='header':
            load_report_header_snow(schema,file_name,table_to_load,engine)
        else:
            load_report_body_snow(schema,file_name,table_to_load,engine)


        

    

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
