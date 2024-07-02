"""
Run kantata API export. The API returns all the data for each report
for that particular point in time.

That means that this DAG doesn't support backfilling by chunks, hence catchup=False
"""

import re
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator

from airflow_utils import (
    DATA_IMAGE_3_10,
    clone_and_setup_extraction_cmd,
    gitlab_defaults,
    slack_failed_task,
    gitlab_pod_env_vars,
)

from kube_secrets import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
    KANTATA_OAUTH_TOKEN,
)

from kubernetes_helpers import get_affinity, get_toleration


def get_task_name(report_name):
    """
    Need to clean the Kantata report name to make it a valid Airflow
    task name
    """
    # Replace '-' with '_'
    cleaned_name = report_name.replace("-", "_").lower()
    # Replace whitespace with '_'
    cleaned_name = re.sub(r"\s+", "_", cleaned_name)
    cleaned_name = re.sub(r"_+", "_", cleaned_name)
    # Remove all non-letter/number characters except '_'
    cleaned_name = re.sub(r"[^a-zA-Z0-9_]", "", cleaned_name)
    # Ensure the name doesn't start or end with '_'
    cleaned_name = cleaned_name.strip("_")
    return cleaned_name


KANTATA_REPORTS = [
    "Verify - Time Entry - Financial",
    "API Download of ! Rev QBR: Details by Project & User",
    "API Download: ! Forecast: FF + T&M [table] [week]",
    "API Download: !NC Project Budget Details",
    "API Download: !NC Project Details Table Including Custom Fields",
]
pod_env_vars = {**gitlab_pod_env_vars, **{}}

# Define the default arguments for the DAG
default_args = {
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(hours=24),
    "sla_miss_callback": slack_failed_task,
}

# Define the DAG
dag = DAG(
    "el_kantata_extract",
    default_args=default_args,
    # Run shortly before dbt dag which is at 8:45UTC
    schedule_interval="0 8 * * *",
    start_date=datetime(2024, 7, 8),
    catchup=False,
    max_active_runs=2,
)

dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

for kantata_report in KANTATA_REPORTS:
    kantata_extract_command = (
        f"{clone_and_setup_extraction_cmd} && "
        f"python kantata/src/kantata.py --reports '{kantata_report}'"
    )
    kantata_task_name = f"kantata_{get_task_name(kantata_report)}"

    kantata_task = KubernetesPodOperator(
        **gitlab_defaults,
        image=DATA_IMAGE_3_10,
        task_id=kantata_task_name,
        name=kantata_task_name,
        secrets=[
            SNOWFLAKE_ACCOUNT,
            SNOWFLAKE_LOAD_ROLE,
            SNOWFLAKE_LOAD_USER,
            SNOWFLAKE_LOAD_WAREHOUSE,
            SNOWFLAKE_LOAD_PASSWORD,
            KANTATA_OAUTH_TOKEN,
        ],
        env_vars={
            **pod_env_vars,
            "data_interval_start": "{{ data_interval_start }}",
            "data_interval_end": "{{ data_interval_end }}",
        },
        affinity=get_affinity("extraction"),
        tolerations=get_toleration("extraction"),
        arguments=[kantata_extract_command],
        dag=dag,
    )

    dummy_start >> kantata_task
