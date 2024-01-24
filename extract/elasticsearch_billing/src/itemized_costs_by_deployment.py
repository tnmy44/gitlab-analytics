"""
Extract and load elasticsearch billing itemized costs by deployment
"""
import os
from datetime import date, datetime, timedelta
from logging import info

from utility import (
    get_list_of_deployments,
    get_response,
    prep_dataframe,
    upload_to_snowflake,
    get_extraction_start_date_end_date_backfill,
    get_extraction_start_date_end_date_recon,
    get_response_and_upload_costs_by_deployments,
)

config_dict = os.environ.copy()
org_id = config_dict["ELASTIC_CLOUD_ORG_ID"]
table_name = "itemized_costs_by_deployment"


def get_itemized_costs_by_deployments():
    """Retrieves the itemized costs for the given deployment from start of current month till present date"""

    info("Retrieving itemized costs by deployment")
    date_today = datetime.utcnow().date()

    extraction_start_date = date_today.replace(day=1)
    extraction_end_date = date_today - timedelta(days=1)

    # Get the list of deployments
    get_response_and_upload_costs_by_deployments(
        extraction_start_date, extraction_end_date, table_name
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
        (
            extraction_start_date,
            extraction_end_date,
        ) = get_extraction_start_date_end_date_recon(date_today)
        # Get the list of deployments
        get_response_and_upload_costs_by_deployments(
            extraction_start_date, extraction_end_date, table_name
        )
    else:
        info("No reconciliation required")


def get_itemized_costs_by_deployments_backfill():
    """Retrieves the itemized costs for the given deployment from 2023-01-01 till previous months_end_date"""
    info("Retrieving itemized costs by deployment")
    (
        extraction_start_date,
        extraction_end_date,
    ) = get_extraction_start_date_end_date_backfill()
    output_list = []
    info(f"{extraction_start_date} till {extraction_end_date}")
    # Get the list of deployments
    deployments_list = get_list_of_deployments()
    for deployments in deployments_list:
        deployment_id = deployments
        info(f"Retrieving itemized costs for deployment {deployment_id}")
        for month in range(extraction_start_date.month, extraction_end_date.month + 1):
            current_month = date(extraction_start_date.year, month, 1)
            start_date = current_month
            if month == extraction_end_date.month:
                end_date = extraction_end_date
            else:
                # update end_date to ending date of next month
                end_date = date(
                    current_month.year, current_month.month + 1, 1
                ) - timedelta(days=1)
            info(f"{start_date} till {end_date}")
            itemised_costs_by_deployments_url = f"/billing/costs/{org_id}/deployments/{deployment_id}/items?start_date={start_date}&end_date={end_date}"
            data = get_response(itemised_costs_by_deployments_url)
            row_list = [
                deployment_id,
                data,
                start_date,
                end_date,
            ]
            output_list.append(row_list)

    columns_list = [
        "deployment_id",
        "payload",
        "extraction_start_date",
        "extraction_end_date",
    ]
    output_df = prep_dataframe(output_list, columns_list)
    info("Uploading records to snowflake...")
    upload_to_snowflake(output_df, table_name)


def extract_load_billing_itemized_costs_by_deployment():
    """
    Extracts the itemized costs for the given deployment from start of current month till present date and perform reconciliation
    """

    info("Beginning extraction of Elastic Search Billing Itemized Costs By Deployment")
    # Regular daily load from start of current month date till present date
    get_itemized_costs_by_deployments()
    # Capture reconciliation data for previous month
    get_reconciliation_data()
    info("Extraction of Elastic Search Billing Costs Overview completed")
