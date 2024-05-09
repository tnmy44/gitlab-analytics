import json
import logging
from os import environ as env
import fire
from google.cloud import bigquery

from big_query_client import BigQueryClient
from yaml import safe_load, YAMLError

config_dict = env.copy()


def get_export(export_name: str, config_path: str) -> tuple[str, str, dict]:
    """
    retrieve export record attributes as well as gcp project and credentials from gcs_external.yml
    """

    with open(config_path, "r") as yaml_file:
        try:
            stream = safe_load(yaml_file)
        except YAMLError as exc:
            print(exc)

    project = stream["project"]
    gcp_credentials = stream["credentials"]
    export = [
        export
        for export in stream["exports"]
        if (export_name is None or export.get("name") == export_name)
    ][0]

    return project, gcp_credentials, export


def get_billing_data_query(
    export: dict,
) -> str:
    """
    sql to run in bigquery for daily partition
    """

    EXPORT_DATE = config_dict["EXPORT_DATE"]
    GIT_BRANCH = config_dict["GIT_BRANCH"]

    if GIT_BRANCH != "master":
        root_dir = export["bucket_path"].index("/", len("gs://"))
        export["bucket_path"] = (
            export["bucket_path"][:root_dir]
            + f"/BRANCH_TEST_{GIT_BRANCH}"
            + export["bucket_path"][root_dir:]
        )

    if export.get("partition_date_part") == None:
        partition = EXPORT_DATE
    elif export["partition_date_part"] == "d":
        partition = EXPORT_DATE[0:10]
    elif export["partition_date_part"] == "m":
        partition = EXPORT_DATE[0:7]

    export_query = export["export_query"].replace("{EXPORT_DATE}", partition)

    return f"""
        EXPORT DATA OPTIONS(
          uri='{export["bucket_path"]}/{partition}/*.parquet',
          format='PARQUET',
          overwrite=true
          ) AS
            {export_query}
    """


def run_export(
    config_path: str,
    export_name: str,
):
    """
    run sql command in bigquery
    """

    project, gcp_credentials, export = get_export(export_name, config_path)

    sql_statement = get_billing_data_query(export)

    logging.info(sql_statement)
    logging.info(f"authenticating with {gcp_credentials}")

    credentials = json.loads(config_dict[gcp_credentials], strict=False)
    bq = BigQueryClient(credentials)
    result = bq.get_result_from_sql(
        sql_statement,
        project=project,
        job_config=bigquery.QueryJobConfig(use_legacy_sql=False),
    )


if __name__ == "__main__":
    logging.basicConfig(level=20)
    fire.Fire(run_export)
    logging.info("Complete.")
