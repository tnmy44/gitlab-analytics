"""
Extract and load Elasticsearch billing costs overview
"""
import os
from datetime import date, datetime, timedelta
from logging import info
import pandas as pd

from utility import (
    get_response,
    upload_costs_overview_and_itemised_costs_respone_to_snowflake,
    upload_to_snowflake,
)

config_dict = os.environ.copy()

org_id = config_dict["ELASTIC_CLOUD_ORG_ID"]
table_name = "costs_overview"


def get_costs_overview():
    """Get costs overview from Elastic Cloud API from start of current month till present date"""

    info("Getting costs overview")
    date_today = datetime.utcnow().date()

    extraction_start_date = date_today.replace(day=1)
    extraction_end_date = date_today - timedelta(days=1)
    costs_endpoint_url = (
        "/billing/costs/{org_id}?from={extraction_start_date}&to={extraction_end_date}"
    )
    data = get_response(costs_endpoint_url)
    upload_costs_overview_and_itemised_costs_respone_to_snowflake(
        extraction_start_date, extraction_end_date, data, table_name
    )


def get_reconciliation_data():
    """
    Get reconciliation data from Elastic Cloud API,
    It is performed on 7 and 14th of every month for the previous month to capture any billing corrections
    """

    date_today = datetime.utcnow().date()
    # if date_today day is 7 or 14 then set extraction_start_date as previous months start date and extraction_end_date as previous months end date
    if date_today.day in [7, 14]:
        info("Performing reconciliation...")
        current_months_first_day = date_today.replace(day=1)
        extraction_end_date = current_months_first_day - timedelta(days=1)
        extraction_start_date = extraction_end_date.replace(day=1)
        costs_endpoint_url = "/billing/costs/{org_id}?from={extraction_start_date}&to={extraction_end_date}"
        data = get_response(costs_endpoint_url)
        upload_costs_overview_and_itemised_costs_respone_to_snowflake(
            extraction_start_date, extraction_end_date, data, table_name
        )
    else:
        info("No reconciliation required")


def get_costs_overview_backfill():
    """
    Get costs overview from Elastic Cloud API from 2023-01-01 till previous months_end_date
    """

    info("Getting costs overview")
    current_date = datetime.utcnow().date()
    start_date = config_dict["extraction_start_date"]
    extraction_start_date = datetime.strptime(start_date, "%Y-%m-%d")
    extraction_end_date = date(current_date.year, current_date.month, 1) - timedelta(
        days=1
    )
    info(f"{extraction_start_date} till {extraction_end_date}")
    output_list = []
    # iterate each month in between extraction_start_date and extraction_end_date and call API
    for month in range(extraction_start_date.month, extraction_end_date.month + 1):
        current_month = date(extraction_start_date.year, month, 1)
        start_date = current_month
        if month == extraction_end_date.month:
            end_date = extraction_end_date
        else:
            # update end_date to ending date of next month
            end_date = date(current_month.year, current_month.month + 1, 1) - timedelta(
                days=1
            )
        info(f"{start_date} till {end_date}")
        costs_endpoint_url = "/billing/costs/{org_id}?from={extraction_start_date}&to={extraction_end_date}"
        data = get_response(costs_endpoint_url)
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
    upload_to_snowflake(output_df, table_name)


def extract_load_billing_costs_overview():
    """
    Extract and load Elastic Search Billing costs overview from start of current month till present date and perform reconciliation
    """

    info("Starting extraction of Elastic Search Billing Costs Overview")

    # Regular daily load from start of current month date till present date
    get_costs_overview()
    # Capture reconciliation data for previous month
    get_reconciliation_data()
    info("Extraction of Elastic Search Billing Costs Overview completed")
