from logging import basicConfig, getLogger, info, error
import pandas as pd
import requests
import sys
from datetime import date, datetime, timedelta
import os

from gitlabdata.orchestration_utils import (
    dataframe_uploader,
    snowflake_engine_factory,
)

config_dict = os.environ.copy()


# test API connection
def test_api_connection(base_url, org_id):
    """Check API response for 200 status code"""
    url = f"{base_url}/billing/costs/{org_id}"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"ApiKey {config_dict['ELASTIC_SEARCH_BILLING_API_KEY']}",
    }
    response = requests.get(url, headers=headers, timeout=60)

    if response.status_code == 200:
        info("API connection successful")
        return True
    else:
        info(f"API connection failed with status code {response.status_code}")
        return False


# call API
def get_costs_overview(base_url, org_id):
    """Get costs overview from Elastic Cloud API from start of current month till present date"""

    info("Getting costs overview")
    date_today = datetime.utcnow().date()

    extraction_start_date = date_today.replace(day=1)
    extraction_end_date = date_today - timedelta(days=1)
    output_list = []
    url = f"{base_url}/billing/costs/{org_id}?from={extraction_start_date}&to={extraction_end_date}"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"ApiKey {config_dict['ELASTIC_SEARCH_BILLING_API_KEY']}",
    }

    response = requests.get(url, headers=headers, timeout=60)

    data = response.json()

    # upload this data to snowflake
    info("Uploading data to Snowflake")
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
    upload_to_snowflake(output_df)


def get_reconciliation_data(base_url, org_id):
    """Get reconciliation data from Elastic Cloud API"""

    info("Performing reconciliation...")
    date_today = datetime.utcnow().date()
    output_list = []
    # if date_today day is 7 or 14 then set extraction_start_date as previous months start date and extraction_end_date as previous months end date
    if date_today.day in [7, 14]:
        current_months_first_day = date_today.replace(day=1)
        extraction_end_date = current_months_first_day - timedelta(days=1)
        extraction_start_date = extraction_end_date.replace(day=1)
        url = f"{base_url}/billing/costs/{org_id}?from={extraction_start_date}&to={extraction_end_date}"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"ApiKey {config_dict['ELASTIC_SEARCH_BILLING_API_KEY']}",
        }
        response = requests.get(url, headers=headers, timeout=60)
        data = response.json()
        # upload this data to snowflake
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
            table_name="costs_overview",
            schema="elasticsearch_billing",
            if_exists="append",
            add_uploaded_at=True,
        )
        info("Uploaded 'costs_overview' to Snowflake")
    except Exception as e:
        error(f"Error uploading to snowflake: {e}")
        sys.exit(1)


# main function
if __name__ == "__main__":
    basicConfig(stream=sys.stdout, level=20)
    getLogger("snowflake.connector.cursor").disabled = True

    info("Starting extraction of Elastic Search Billing Costs Overview")

    base_url = "https://api.elastic-cloud.com/api/v1"

    org_id = config_dict["ELASTIC_CLOUD_ORG_ID"]

    check_api_connection = test_api_connection(base_url, org_id)

    if check_api_connection:
        # Regular daily load from start of current month date till present date
        get_costs_overview(base_url, org_id)
        # Capture reconciliation data for previous month
        get_reconciliation_data(base_url, org_id)
    else:
        sys.exit(1)
