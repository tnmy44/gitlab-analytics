"""
Functions to call various Kantata API endpoints
"""

from datetime import datetime, time
from logging import error, info
from typing import Optional

import requests
from gitlabdata.orchestration_utils import make_request
from kantata_utils import HEADERS

BASE_ENDPOINT = "https://api.mavenlink.com/api/v1"


def get_insight_reports(report_title_to_query: Optional[str] = None) -> dict:
    """
    This endpoint returns a list of all Insight Reports
    https://developer.kantata.com/tag/Insights-Reports

    If an optional `report_title_to_query` arg is passed in, will filter only for matching reports
    """
    get_insight_reports_endpoint = f"{BASE_ENDPOINT}/insights_reports"
    if report_title_to_query:
        query = {"title": report_title_to_query}
    else:
        query = {}
    info(
        f"Requesting {get_insight_reports_endpoint} to get all possible Insight Reports"
    )
    response = make_request(
        "GET", get_insight_reports_endpoint, headers=HEADERS, params=query
    )
    return response.json()


def get_scheduled_insight_reports() -> dict:
    """
    This endpoint returns a list of all Scheduled Insight Reports
    https://developer.kantata.com/tag/Insights-Reports
    """
    scheduled_insight_reports_endpoint = (
        f"{BASE_ENDPOINT}/scheduled_jobs/insights_report_exports"
    )
    info(
        f"Requesting {scheduled_insight_reports_endpoint} to get ALL scheduled report statuses"
    )
    response = make_request("GET", scheduled_insight_reports_endpoint, headers=HEADERS)
    return response.json()


def _calculate_scheduled_report_start_time() -> str:
    """
    The scheduled report start_time should be today's date, at 7:15 UTC
    That way the extract can run at 8UTC, and dbt at 8:45UTC
    """
    today = datetime.today().date()
    datetime_7_15_am = datetime.combine(today, time(7, 15))
    datetime_7_15_am_str = datetime_7_15_am.strftime("%Y-%m-%dT%H:%M:%SZ")
    return datetime_7_15_am_str


def create_scheduled_insight_report(
    title: str,
    description: str,
    report_external_identifier: str,
    cadence: str,
) -> dict:
    """
    This endpoint returns a list of all Scheduled Insight Reports
    https://developer.kantata.com/tag/Insights-Reports
    """
    scheduled_insight_reports_endpoint = (
        f"{BASE_ENDPOINT}/scheduled_jobs/insights_report_exports"
    )
    recurrence = {
        "cadence": cadence,
        "start_time": _calculate_scheduled_report_start_time(),
    }
    payload = {
        "insights_report_export": {
            "title": title,
            "description": description,
            "external_report_object_identifier": report_external_identifier,
            "recurrence": recurrence,
        }
    }
    info(
        f"POST to {scheduled_insight_reports_endpoint} endpoint to create new scheduled report '{title}'"
    )
    response = make_request(
        "POST", scheduled_insight_reports_endpoint, json=payload, headers=HEADERS
    )
    info(response.text)
    return response.json()


def get_latest_export(report_id: str) -> dict:
    """
    This endpoint returns the latest export for a particular scheduled report
    """
    try_count, max_try_count = 0, 3
    latest_export_endpoint = f"https://api.mavenlink.com/api/v1/scheduled_jobs/insights_report_exports/{report_id}/results/latest"
    info(
        f"Requesting {latest_export_endpoint} to get the latest export for report_id {report_id}"
    )
    while try_count < max_try_count:
        response = make_request("GET", latest_export_endpoint, headers=HEADERS)

        try:
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            error(f"Error: {e}. Response text: {response.text}")
            try_count += 1
            info(
                f"Will now attempt try_count: {try_count}, for a max of {max_try_count} tries"
            )

    raise RuntimeError(
        f"Failed to get latest export after {max_try_count} attempts. Aborting"
    )


# Unused endpoints ######
def get_scheduled_insight_report(report_id: str) -> dict:
    """
    This endpoint returns one insight report based on a report_id
    Not useful in our case, since we don't always know the report_id beforehand
    """
    scheduled_insight_report_endpoint = (
        f"{BASE_ENDPOINT}/scheduled_jobs/insights_report_exports/{report_id}"
    )

    info(
        f"Requesting {scheduled_insight_report_endpoint} to get report info for report_id {report_id}"
    )
    response = make_request("GET", scheduled_insight_report_endpoint, headers=HEADERS)
    info(response.text)
    return response.json()
