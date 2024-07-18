"""
Runs the main flow to extract data from Kantataa API
and then upload to Snowflake
"""

import gzip
import io
import sys
from logging import basicConfig, error, getLogger, info
from typing import Optional

import pandas as pd
from endpoints import (
    create_scheduled_insight_report,
    get_insight_reports,
    get_latest_export,
    get_scheduled_insight_reports,
)
from gitlabdata.orchestration_utils import make_request
from kantata_utils import (
    add_csv_file_extension,
    clean_string,
    config_dict,
    convert_timezone,
    process_args,
    upload_kantanta_to_snowflake,
)
from requests import Response


def retrieve_insight_report_external_identifier(
    report_name: str, insight_reports: list
) -> Optional[str]:
    """Retrieve the report 'external identifier'"""
    for report_d in insight_reports:
        if report_d.get("title", "") == report_name:
            return report_d["identifier"]
    return None


def retrieve_scheduled_insight_report(
    report_external_identifier: str, scheduled_insight_reports: list
) -> Optional[dict]:
    """
    Retrieve the scheduled_insight_report json that matches the specified report
    """
    for report_d in scheduled_insight_reports:
        if (
            report_d.get("external_report_object_identifier", "")
            == report_external_identifier
        ):
            return report_d
    return None


def has_valid_latest_export(scheduled_insight_report: dict) -> bool:
    """
    Evaluate if the latest report can be exported
    Valid latest export if:
        status=success
        and report was created between data_interval_start/data_interval_end
    Else not valid export
    """
    # return {} when latest_result value is None
    latest_result = scheduled_insight_report.get("latest_result") or {}
    report_title = scheduled_insight_report["title"]
    report_external_identifier = scheduled_insight_report[
        "external_report_object_identifier"
    ]
    if latest_result.get("status") != "success":
        # Generally this means new scheduled report, but raise error so not missed
        raise ValueError(
            f"Is this a just-scheduled report? Report_title {report_title} with report_external_identifier {report_external_identifier} cannot be exported because the status is {latest_result.get('status')}, full response: {scheduled_insight_report}"
        )
        return False

    latest_created_at = latest_result.get("created_at")  # in PST tz
    latest_created_at_utc = convert_timezone(latest_created_at)
    if (
        latest_created_at_utc < config_dict["data_interval_start"]
        or latest_created_at_utc >= config_dict["data_interval_end"]
    ):
        info(
            f"report_title {report_title} with report_external_identifier {report_external_identifier} cannot be exported because the latest_created_at_utc {latest_created_at_utc} is not between the Airflow execution_dates {config_dict['data_interval_start']} - {config_dict['data_interval_end']}"
        )
        return False

    return True


def process_latest_export_response(
    latest_export_response: Response, upload_file_name: str
) -> Optional[pd.DataFrame]:
    """
    If the latest export response is successful
    Then process the response by:
        1. Saving the contents of the response as .csv.gz to a file (to be uploaded to Snowflake stage)
        2. Returning a portion of the data as a dataframe
            - used for seeding the table if it doesn't exist
            - useful for previewing the data
    """
    if latest_export_response.status_code == 200:
        content = latest_export_response.content
        # Write the content to a file in binary mode
        with open(upload_file_name, "wb") as f:
            f.write(content)

        with gzip.GzipFile(fileobj=io.BytesIO(content)) as gz:
            df = pd.read_csv(gz, nrows=5)
            # df.rename(columns=lambda x: add_quotes_to_string(x.upper()), inplace=True)
            info(f"Sample of exported data, df.head:\n{df.head}")
            return df

    else:
        error(f"The download failed with this message: {latest_export_response.text}")
        latest_export_response.raise_for_status()
        return None


def download_report_from_latest_export(latest_export: dict) -> Response:
    """
    Get the url of the latest export (some MavenLink S3 bucket link), and pull it down
    """
    try:
        download_url = latest_export["url"]
    except IndexError:
        raise IndexError(
            f"\nLatest_export dict {latest_export} does not have valid download url"
        )
    response = make_request("GET", download_url)
    return response


def get_and_process_latest_export(scheduled_insight_report: dict, report_name: str):
    """
    Does the following:
        - Return the latest export response from endpoint
        - From the above response, obtain the download url
        - Request from download url, and save contents into a csv file
        - Upload csv file to Snowflake
    """

    latest_export = get_latest_export(scheduled_insight_report["id"])
    latest_export_response = download_report_from_latest_export(latest_export)

    snowflake_table_name = clean_string(report_name)
    info(f"snowflake_table_name: {snowflake_table_name}")
    upload_file_name = add_csv_file_extension(snowflake_table_name)
    df = process_latest_export_response(latest_export_response, upload_file_name)
    upload_kantanta_to_snowflake(df, snowflake_table_name, upload_file_name)


def main():
    """
    This function runs the main API flow:
        - get all available insight reports (scheduled and non-scheduled)
            - get the insight report identifier
        - get all scheduled API reports
        - If report is already scheduled, and it has a latest valid report, then extract
        - Else schedule the report
    """
    report_names = process_args()
    info(f"\nreport_names: {report_names}")

    insight_reports = get_insight_reports()
    scheduled_insight_reports = get_scheduled_insight_reports()
    for report_name in report_names:
        info(f"Processing report_name: {report_name}")
        report_external_identifier = retrieve_insight_report_external_identifier(
            report_name, insight_reports
        )
        info(f"With report_external_identifier: {report_external_identifier}")
        # if Kantata report exists
        if report_external_identifier:
            scheduled_insight_report = retrieve_scheduled_insight_report(
                report_external_identifier, scheduled_insight_reports
            )
            # if Kantata report is scheduled
            if scheduled_insight_report:
                if has_valid_latest_export(scheduled_insight_report):
                    get_and_process_latest_export(scheduled_insight_report, report_name)
            else:
                create_scheduled_insight_report(
                    report_name, report_name, report_external_identifier, "daily"
                )
        else:
            raise ValueError(
                f"\nNo report_external_identifier found for {report_name}, are you sure {report_name} exists?"
            )


if __name__ == "__main__":
    basicConfig(stream=sys.stdout, level=20)
    getLogger("snowflake.connector.cursor").disabled = True
    main()
    info("Complete.")
