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
    print(type(storage_client))
    return storage_client



def get_files_for_report(bucket,output_file_name):
    source_bucket = get_gcs_storage_client().get_bucket(bucket,prefix="RAW_DB/staging/zuora_revenue_report")
    print(source_bucket)
    

    

def zuora_revenue_report_load(
    bucket: str,
    schema: str,
    output_file_name: str,
    conn_dict: Dict[str, str] = None,
) -> None:

    
    # Set some vars
    engine = snowflake_engine_factory(conn_dict or env, "LOADER", schema)
    
    if output_file_name.startswith('header'):
        table_to_load=output_file_name+'_header'
    else:
        table_to_load=output_file_name+'_body'

    get_files_for_report(bucket,output_file_name)

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
