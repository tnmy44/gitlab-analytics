import json
import logging
from os import environ as env
import fire
from google.cloud import bigquery

from big_query_client import BigQueryClient
from yaml import safe_load, YAMLError

config_dict = env.copy()

def get_billing_data_query(export: dict, export_date: str) -> str:
    """
    sql to run in bigquery for daily partition
    """
    if export["partition_date_part"] == "d":
        partition = export_date[0:10]
    elif export["partition_date_part"] == "m":
        partition = export_date[0:7]

    select_string = ", ".join(export["selected_columns"])

    return f"""
        EXPORT DATA OPTIONS(
          uri='{export['bucket_path']}/{partition}/*.parquet',
          format='PARQUET',
          overwrite=true
          ) AS
            SELECT {select_string}
            FROM `{export['table']}`
            WHERE {export['partition_column']} = '{partition}'
    """


def run_export(export: dict, gcp_project: str):
    """
    run sql command in bigquery
    """
    export_date = config_dict["EXPORT_DATE"]
    sql_statement = get_billing_data_query(export, export_date)

    logging.info(sql_statement)

    credentials = json.loads(config_dict["GCP_BILLING_ACCOUNT_CREDENTIALS"])
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
