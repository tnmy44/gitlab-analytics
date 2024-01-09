"""
Run daily elastic search billing extract
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

from airflow_utils import (
    DATA_IMAGE,
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
    GCP_SERVICE_CREDS,
)

from kubernetes_helpers import get_affinity, get_toleration

env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = {**gitlab_pod_env_vars, **{}}

# Define the default arguments for the DAG
default_args = {
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(hours=24),
    "sla_miss_callback": slack_failed_task,
    "retries": 2,
    "description": "This DAG is extracts and loads elastic search billing data",
}

# Define the DAG
dag = DAG(
    f"el_elasticsearch_billing",
    default_args=default_args,
    schedule_interval="0 7 * * *",
    start_date=datetime(2023, 11, 7),
    catchup=False,
    max_active_runs=1,
    concurrency=1,
)

elasticsearch_billing_extract_command = (
    f"{clone_and_setup_extraction_cmd} && "
    f"python elastic_search_billing/src/elasticsearch_billing_costs_overview.py"
)

elasticsearch_billing_costs_overview_task = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE,
    task_id=f"el-costs-overview-extract-daily",
    name=f"el-costs-overview-extract-daily",
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
        SNOWFLAKE_LOAD_PASSWORD,
        GCP_SERVICE_CREDS,
    ],
    env_vars={
        **pod_env_vars,
        "logical_date": "{{ logical_date }}",
    },
    affinity=get_affinity("extraction"),
    tolerations=get_toleration("extraction"),
    arguments=[elasticsearch_billing_extract_command],
    dag=dag,
)

elasticsearch_billing_costs_overview_task
