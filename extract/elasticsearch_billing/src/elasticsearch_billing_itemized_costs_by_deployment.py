"""
Extract and load elasticsearch billing itemized costs by deployment
"""
import os
import sys
from datetime import datetime, timedelta
from logging import basicConfig, getLogger, info, error
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


def get_itemized_costs_by_deployments():
    """Retrieves the itemized costs for the given deployment from start of current month till present date"""

    info("Retrieving itemized costs by deployment")
    date_today = datetime.utcnow().date()

    extraction_start_date = date_today.replace(day=1)
    extraction_end_date = date_today - timedelta(days=1)

    # Get the list of deployments
    url = f"{base_url}/billing/costs/{org_id}/deployments"

    response = requests.get(url, headers=HEADERS, timeout=60)
    output_list = []
    for deployments in response.json()["deployments"]:
        deployment_id = deployments["deployment_id"]
        deployment_name = deployments["deployment_name"]
        info(f"Retrieving itemized costs for deployment {deployment_id}")
        url = f"{base_url}/billing/costs/{org_id}/deployments/{deployment_id}/items?start_date={extraction_start_date}&end_date={extraction_end_date}"

        response = requests.get(url, headers=HEADERS, timeout=60)

        data = response.json()
        row_list = [
            deployment_id,
            deployment_name,
            data,
            extraction_start_date,
            extraction_end_date,
        ]
        output_list.append(row_list)

    output_df = pd.DataFrame(
        output_list,
        columns=[
            "deployment_id",
            "deployment_name",
            "payload",
            "extraction_start_date",
            "extraction_end_date",
        ],
    )
    info("Uploading records to snowflake...")
    upload_to_snowflake(output_df)


def get_reconciliation_data():
    """
    Get reconciliation data from Elastic Cloud API,
    It is performed on 7 and 14th of every month for the previous month to capture any billing corrections
    """

    date_today = datetime.utcnow().date()
    output_list = []
    # if date_today day is 7 or 14 then set extraction_start_date as previous months start date and extraction_end_date as previous months end date
    if date_today.day in [7, 14]:
        info("Performing reconciliation...")
        current_months_first_day = date_today.replace(day=1)
        extraction_end_date = current_months_first_day - timedelta(days=1)
        extraction_start_date = extraction_end_date.replace(day=1)
        # Get the list of deployments
        url = f"{base_url}/billing/costs/{org_id}/deployments"

        response = requests.get(url, headers=HEADERS, timeout=60)

        for deployments in response.json()["deployments"]:
            deployment_id = deployments["deployment_id"]
            deployment_name = deployments["deployment_name"]
            info(f"Retrieving itemized costs for deployment {deployment_id}")
            url = f"{base_url}/billing/costs/{org_id}/deployments/{deployment_id}/items?start_date={extraction_start_date}&end_date={extraction_end_date}"
            response = requests.get(url, headers=HEADERS, timeout=60)
            data = response.json()
            # upload this data to snowflake
            row_list = [
                deployment_id,
                deployment_name,
                data,
                extraction_start_date,
                extraction_end_date,
            ]
            output_list.append(row_list)

        output_df = pd.DataFrame(
            output_list,
            columns=[
                "deployment_id",
                "deployment_name",
                "payload",
                "extraction_start_date",
                "extraction_end_date",
            ],
        )
        info("Uploading records to snowflake...")
        upload_to_snowflake(output_df)
    else:
        info("No reconciliation required")


def upload_to_snowflake(output_df):
    """
    This function will upload the dataframe to snowflake
    """
    try:
        loader_engine = snowflake_engine_factory(config_dict, "LOADER")
        dataframe_uploader(
            output_df,
            loader_engine,
            table_name="itemized_costs_by_deployment",
            schema="elasticsearch_billing",
            if_exists="append",
            add_uploaded_at=True,
        )
        info("Uploaded 'itemised_costs_by_deployment' to Snowflake")
    except Exception as e:
        error(f"Error uploading to snowflake: {e}")
        sys.exit(1)


def extract_load_billing_itemized_costs_by_deployment():
    """
    Extracts the itemized costs for the given deployment from start of current month till present date and perform reconciliation
    """
    info("Starting extraction of Elastic Search Billing Costs Overview")

    check_api_connection = test_api_connection()

    if check_api_connection:
        info(
            "Beginning extraction of Elastic Search Billing Itemized Costs By Deployment"
        )
        # Regular daily load from start of current month date till present date
        get_itemized_costs_by_deployments()
        # Capture reconciliation data for previous month
        get_reconciliation_data()
        info("Extraction of Elastic Search Billing Costs Overview completed")
    else:
        sys.exit(1)
