import json
import logging
from os import environ as env
import fire
from google.cloud import bigquery

from big_query_client import BigQueryClient
from yaml import safe_load, YAMLError

config_dict = env.copy()


def get_billing_data_query(
    selected_columns: str,
    bucket_path: str,
    table: str,
    partition_column: str,
    partition: str,
) -> str:
    """
    sql to run in bigquery for daily partition
    """

    select_string = ", ".join(selected_columns)

    return f"""
        EXPORT DATA OPTIONS(
          uri='{bucket_path}/{partition}/*.parquet',
          format='PARQUET',
          overwrite=true
          ) AS
            SELECT {select_string}
            FROM `{table}`
            WHERE {partition_column} = '{partition}'
    """


def run_export(
    selected_columns: str,
    gcp_project: str,
    bucket_path: str,
    table: str,
    partition_column: str,
    partition: str,
):
    """
    run sql command in bigquery
    """
    export_date = config_dict["EXPORT_DATE"]
    sql_statement = get_billing_data_query(
        selected_columns,
        bucket_path,
        table,
        partition_column,
        partition,
    )

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
