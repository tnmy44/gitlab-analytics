"""
Extract and load Elasticsearch billing costs overview
"""
import os
import sys
from datetime import date, datetime, timedelta
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

def get_costs_overview():
    """Get costs overview from Elastic Cloud API from 2023-01-01 till previous months_end_date"""

    info("Getting costs overview")
    current_date = datetime.utcnow().date()

    extraction_start_date = date(2023, 1, 1)
    extraction_end_date = date(current_date.year, current_date.month, 1) - timedelta(days=1)

    print(f"{extraction_start_date} till {extraction_end_date}")
    output_list = []
    # iterate each month in between extraction_start_date and extraction_end_date and call API
    for month in range(extraction_start_date.month, extraction_end_date.month + 1):
      current_month = date(extraction_start_date.year, month, 1)
      start_date = current_month
      if month == extraction_end_date.month:
        end_date = extraction_end_date
      else:
        # update end_date to ending date of next month
        end_date = date(current_month.year, current_month.month+1, 1) - timedelta(days=1)
    
      print(f"{start_date} till {end_date}")

      url = f"{base_url}/billing/costs/{org_id}?from={start_date}&to={end_date}"
      response = requests.get(url, headers=HEADERS, timeout=60)

      data = response.json()
      row_list = [
        data,
        start_date,
        end_date,
        ]
      output_list.append(row_list)

    # upload this data to snowflake
    info("Uploading data to Snowflake")
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


def extract_load_billing_costs_overview_full_load():
    """
    Extract and load Elastic Search Billing costs overview from 2023-01-01 till previous months_end_date
    """

    info("Starting extraction of Elastic Search Billing Costs Overview")

    check_api_connection = test_api_connection()

    if check_api_connection:
        # Regular daily load from 2023-01-01 till previous months_end_date
        get_costs_overview()
        info("Extraction of Elastic Search Billing Costs Overview completed")
    else:
        sys.exit(1)
