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
    ELASTIC_SEARCH_BILLING_API_KEY,
    ELASTIC_CLOUD_ORG_ID,
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
    "description": "This DAG extracts and loads elastic search billing data",
}

# Define the DAG
dag = DAG(
    "el_elasticsearch_billing_full_load",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    concurrency=1,
)

elasticsearch_billing_costs_overview_extract_full_load_command = (
    f"{clone_and_setup_extraction_cmd} && "
    "python elasticsearch_billing/src/full_load/elasticsearch_billing_extraction_main.py extract_load_billing_costs_overview_full_load"
)

elasticsearch_billing_itemized_costs_extract_full_load_command = (
    f"{clone_and_setup_extraction_cmd} && "
    "python elasticsearch_billing/src/full_load/elasticsearch_billing_extraction_main.py extract_load_billing_itemized_costs_full_load"
)

elasticsearch_billing_deployments_itemized_costs_extract_full_load_command = (
    f"{clone_and_setup_extraction_cmd} && "
    "python elasticsearch_billing/src/full_load/elasticsearch_billing_extraction_main.py extract_load_billing_itemized_costs_by_deployment_full_load"
)

elasticsearch_billing_costs_overview_task = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE,
    task_id="el-costs-overview-extract-full-load",
    name="el-costs-overview-extract-full-load",
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
        SNOWFLAKE_LOAD_PASSWORD,
        ELASTIC_SEARCH_BILLING_API_KEY,
        ELASTIC_CLOUD_ORG_ID,
    ],
    env_vars={
        **pod_env_vars,
        "logical_date": "{{ logical_date }}",
    },
    affinity=get_affinity("extraction"),
    tolerations=get_toleration("extraction"),
    arguments=[elasticsearch_billing_costs_overview_extract_full_load_command],
    dag=dag,
)

elasticsearch_billing_itemized_costs_task = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE,
    task_id="el-itemized-costs-extract-full-load",
    name="el-itemized-costs-extract-full-load",
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
        SNOWFLAKE_LOAD_PASSWORD,
        ELASTIC_SEARCH_BILLING_API_KEY,
        ELASTIC_CLOUD_ORG_ID,
    ],
    env_vars={
        **pod_env_vars,
        "logical_date": "{{ logical_date }}",
    },
    affinity=get_affinity("extraction"),
    tolerations=get_toleration("extraction"),
    arguments=[elasticsearch_billing_itemized_costs_extract_full_load_command],
    dag=dag,
)

elasticsearch_billing_itemized_costs_by_deployments_task = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE,
    task_id="el-itemized-costs-by-deployments-extract-full-load",
    name="el-itemized-costs-by-deployments-extract-full-load",
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
        SNOWFLAKE_LOAD_PASSWORD,
        ELASTIC_SEARCH_BILLING_API_KEY,
        ELASTIC_CLOUD_ORG_ID,
    ],
    env_vars={
        **pod_env_vars,
        "logical_date": "{{ logical_date }}",
    },
    affinity=get_affinity("extraction"),
    tolerations=get_toleration("extraction"),
    arguments=[elasticsearch_billing_deployments_itemized_costs_extract_full_load_command],
    dag=dag,
)

elasticsearch_billing_costs_overview_task
elasticsearch_billing_itemized_costs_task
elasticsearch_billing_itemized_costs_by_deployments_task
