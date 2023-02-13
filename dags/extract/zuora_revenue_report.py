import os
from datetime import datetime, timedelta
from yaml import safe_load, YAMLError
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_utils import (
    DATA_IMAGE,
    clone_and_setup_extraction_cmd,
    gitlab_defaults,
    slack_failed_task,
)
from kube_secrets import (
    GCP_SERVICE_CREDS,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
)
from kubernetes_helpers import get_affinity, get_toleration

# Load the env vars into a dict and set Secrets
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = {
    "SNOWFLAKE_LOAD_DATABASE": "RAW"
    if GIT_BRANCH == "master"
    else f"{GIT_BRANCH.upper()}_RAW",
    "CI_PROJECT_DIR": "/analytics",
}

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(hours=24),
    "sla_miss_callback": slack_failed_task,
    "start_date": datetime(2021, 6, 1),
    "dagrun_timeout": timedelta(hours=6),
}

airflow_home = env["AIRFLOW_HOME"]
task_name = "zuora-revenue-report"

# Get all the report name for which tasks for loading needs to be created
with open(
    f"{airflow_home}/analytics/extract/zuora_revenue_report/src/zuora_report_api_list.yml",
    "r",
) as file:
    try:
        stream = safe_load(file)
    except YAMLError as exc:
        print(exc)


report_list = list(stream["report_list"].keys())


# Create the DAG  with one report at once
dag = DAG(
    "el_zuora_revenue_report",
    default_args=default_args,
    schedule_interval="0 5 * * *",
    concurrency=1,
)

start = DummyOperator(task_id="Start", dag=dag)

for report in report_list:
    file_name_to_load=stream["report_list"][report]["output_file_name"].lower()
    body_load_query_string=stream["report_list"][report]["load_column_name"]
    # Set the command for the container for loading the data
    container_cmd_load = f"""
        {clone_and_setup_extraction_cmd} &&
        python3 zuora_revenue_report/load_zuora_revenue_report.py zuora_report_load --bucket zuora_revpro_gitlab --schema zuora_revenue_report --output_file_name {file_name_to_load} --body_load_query "{body_load_query_string}"
        """
    task_identifier = f"el-{task_name}-{report.replace('_','-').lower()}-load"
    # Task 2
    zuora_revenue_load_run = KubernetesPodOperator(
        **gitlab_defaults,
        image=DATA_IMAGE,
        task_id=task_identifier,
        name=task_identifier,
        pool="default_pool",
        secrets=[
            GCP_SERVICE_CREDS,
            SNOWFLAKE_ACCOUNT,
            SNOWFLAKE_LOAD_ROLE,
            SNOWFLAKE_LOAD_USER,
            SNOWFLAKE_LOAD_WAREHOUSE,
            SNOWFLAKE_LOAD_PASSWORD,
        ],
        env_vars=pod_env_vars,
        affinity=get_affinity(False),
        tolerations=get_toleration(False),
        arguments=[container_cmd_load],
        dag=dag,
    )
    start >> zuora_revenue_load_run
