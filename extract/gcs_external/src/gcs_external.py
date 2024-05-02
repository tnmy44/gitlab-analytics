import json
import logging
from os import environ as env
import fire
from google.cloud import bigquery

from big_query_client import BigQueryClient
from yaml import safe_load, YAMLError

config_dict = env.copy()


def get_export(export_name: str, config_path: str) -> dict:
    """
    retrieve export record attributes from gcs_external.yml
    """

    with open(config_path, "r") as yaml_file:
        try:
            stream = safe_load(yaml_file)
        except YAMLError as exc:
            print(exc)

    export = [
        export
        for export in stream["exports"]
        if (export_name is None or export.get("name") == export_name)
    ][0]

    return export


def get_billing_data_query(
    bucket_path: str,
    export: dict,
    export_date: str,
) -> str:
    """
    sql to run in bigquery for daily partition
    """
    if export["partition_date_part"] == "d":
        partition = export_date[0:10]
    elif export["partition_date_part"] == "m":
        partition = export_date[0:7]

    return f"""
        EXPORT DATA OPTIONS(
          uri='{bucket_path}/{partition}/*.parquet',
          format='PARQUET',
          overwrite=true
          ) AS
            {export['export_query']}
    """


def run_export(
    gcp_project: str,
    bucket_path: str,
    export_name: str,
    config_path: str,
):
    """
    run sql command in bigquery
    """

    export = get_export(export_name, config_path)

    export_date = config_dict["EXPORT_DATE"]
    sql_statement = get_billing_data_query(bucket_path, export, export_date)

    logging.info(sql_statement)

    credentials = json.loads(
        config_dict["GCP_MKTG_GOOG_ANALYTICS4_5E6DC7D6_CREDENTIALS"], strict=False
    )
    bq = BigQueryClient(credentials)
    result = bq.get_result_from_sql(
        sql_statement,
        project=gcp_project,
        job_config=bigquery.QueryJobConfig(use_legacy_sql=False),
    )


if __name__ == "__main__":
    logging.basicConfig(level=20)
    fire.Fire(run_export)
    logging.info("Complete.")
