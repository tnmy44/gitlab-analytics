"""
Utility file
"""
import os
import sys
from logging import info, error
import requests

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
