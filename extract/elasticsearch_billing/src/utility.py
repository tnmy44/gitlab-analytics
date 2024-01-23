"""
Utility file
"""
import os
import sys
from datetime import date, datetime, timedelta
from logging import info, error
import requests

import pandas as pd

from gitlabdata.orchestration_utils import (
    dataframe_uploader,
    snowflake_engine_factory,
)

config_dict = os.environ.copy()
HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"ApiKey {config_dict['ELASTIC_SEARCH_BILLING_API_KEY']}",
}
base_url = "https://api.elastic-cloud.com/api/v1"
org_id = config_dict["ELASTIC_CLOUD_ORG_ID"]


def upload_to_snowflake(output_df, table_name):
    """
    This function will upload the dataframe to snowflake
    """
    try:
        loader_engine = snowflake_engine_factory(config_dict, "LOADER")
        dataframe_uploader(
            output_df,
            loader_engine,
            table_name=table_name,
            schema="elasticsearch_billing",
            if_exists="append",
            add_uploaded_at=True,
        )
        info(f"Uploaded {table_name} to Snowflake")
    except Exception as e:
        error(f"Error uploading to snowflake: {e}")
        sys.exit(1)


def get_response(url):
    """
    This function will get the response from the API
    """
    try:
        url = f"{base_url}{url}"
        response = requests.get(url, headers=HEADERS, timeout=60)
        data = response.json()
        return data
    except Exception as e:
        error(f"API connection failed: {e}")
        sys.exit(1)


def get_list_of_deployments():
    """
    This function will get the list of deployments from the API
    """
    deployments_url = f"/billing/costs/{org_id}/deployments"
    deployments_list = get_response(deployments_url)
    output_list = []
    for deployments in deployments_list:
        deployment_id = deployments
        output_list.append(deployment_id)
    return output_list


def prep_dataframe(output_list, columns_list):
    """
    This function will prepare the dataframe for itemised costs by deployment
    """
    output_df = pd.DataFrame(
        output_list,
        columns=columns_list,
    )
    return output_df


def get_extraction_start_date_end_date_backfill():
    """
    This function will set the extraction start date and end date
    """
    current_date = datetime.utcnow().date()
    start_date = config_dict["extraction_start_date"]
    extraction_start_date = datetime.strptime(start_date, "%Y-%m-%d")
    extraction_end_date = date(current_date.year, current_date.month, 1) - timedelta(
        days=1
    )
    return extraction_start_date, extraction_end_date
