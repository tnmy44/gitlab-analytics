"""
Utility file
"""
import os
import sys
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


def test_api_connection():
    """Check API response for 200 status code"""
    url = f"{base_url}/billing/costs/{org_id}"

    response = requests.get(url, headers=HEADERS, timeout=60)

    if response.status_code == 200:
        info("API connection successful")
        return True
    else:
        info(f"API connection failed with status code {response.status_code}")
        return False


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
    url = f"{base_url}{url}"
    response = requests.get(url, headers=HEADERS, timeout=60)
    data = response.json()
    return data


def upload_costs_overview_and_itemised_costs_respone_to_snowflake(
    extraction_start_date, extraction_end_date, data, table_name
):
    """
    This function will upload the json payload to snowflake
    """
    output_list = []
    row_list = [
        data,
        extraction_start_date,
        extraction_end_date,
    ]
    output_list.append(row_list)
    output_df = pd.DataFrame(
        output_list,
        columns=[
            "payload",
            "extraction_start_date",
            "extraction_end_date",
        ],
    )
    info("Uploading records to snowflake...")
    upload_to_snowflake(output_df, table_name)


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


def prep_dataframe_itemised_costs_by_deployment(output_list):
    """
    This function will prepare the dataframe for itemised costs by deployment
    """
    output_df = pd.DataFrame(
        output_list,
        columns=[
            "deployment_id",
            "payload",
            "extraction_start_date",
            "extraction_end_date",
        ],
    )
    return output_df
