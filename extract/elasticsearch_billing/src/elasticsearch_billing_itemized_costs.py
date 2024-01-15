import json
from logging import basicConfig, getLogger, info
import requests
import sys
import time
from datetime import date, datetime, timedelta
import os

from gitlabdata.orchestration_utils import (
    snowflake_stage_load_copy_remove,
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
def get_itemized_costs(base_url, org_id):
    """Get Itemized costs from Elastic Cloud API"""

    date_today = datetime.utcnow().date()

    extraction_start_date = date_today.replace(day=1)
    extraction_end_date = date_today - timedelta(days=1)

    url = f"{base_url}/billing/costs/{org_id}/items?from={extraction_start_date}&to={extraction_end_date}"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"ApiKey {config_dict['ELASTIC_SEARCH_BILLING_API_KEY']}",
    }

    response = requests.get(url, headers=headers, timeout=60)

    data = response.json()

    # upload this data to snowflake


# main function
if __name__ == "__main__":
    basicConfig(stream=sys.stdout, level=20)
    getLogger("snowflake.connector.cursor").disabled = True

    info("Starting extraction of Elastic Search Billing Costs Overview")

    base_url = "https://api.elastic-cloud.com/api/v1"

    org_id = config_dict["ELASTIC_CLOUD_ORG_ID"]

    check_api_connection = test_api_connection(base_url, org_id)

    if check_api_connection:
        get_itemized_costs(base_url, org_id)
    else:
        sys.exit(1)
