"""
Run daily Zendesk extract for tickets and ticket_audits tables
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
    ZENDESK_SENSITIVE_SERVICE_ACCOUNT_CREDENTIALS,
    ZENDESK_SENSITIVE_EXTRACTION_BUCKET_NAME,
)

from kubernetes_helpers import get_affinity, get_toleration

env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = {**gitlab_pod_env_vars, **{}}
TASK_SCHEDULE = "daily"

# Define the default arguments for the DAG
default_args = {
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(hours=24),
    "sla_miss_callback": slack_failed_task,
    "retries": 2,
    "description": "This DAG is extracts and transforms sensitive zendesk tickets and tickets_audits tables",
}

# Define the DAG
dag = DAG(
    f"zendesk_sensitive_extract",
    default_args=default_args,
    schedule_interval="0 7 * * *",
    start_date=datetime(2023, 11, 6),
    catchup=False,
    max_active_runs=1,
)

zendesk_ticket_audits_extract_command = (
    f"{clone_and_setup_extraction_cmd} && "
    f"python zendesk/src/zendesk_ticket_audits_refactor.py"
)

zendesk_tickets_extract_command = (
    f"{clone_and_setup_extraction_cmd} && "
    f"python zendesk/src/zendesk_tickets_refactor.py"
)

zendesk_extract_ticket_audits_task = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE,
    task_id=f"zendesk-ticket-audits-extract-{TASK_SCHEDULE}",
    name=f"zendesk-ticket-audits-extract-{TASK_SCHEDULE}",
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
        SNOWFLAKE_LOAD_PASSWORD,
        ZENDESK_SENSITIVE_SERVICE_ACCOUNT_CREDENTIALS,
        ZENDESK_SENSITIVE_EXTRACTION_BUCKET_NAME,
    ],
    env_vars={
        **pod_env_vars,
        "logical_date": "{{ logical_date }}",
        "task_schedule": TASK_SCHEDULE,
    },
    affinity=get_affinity("extraction"),
    tolerations=get_toleration("extraction"),
    arguments=[zendesk_ticket_audits_extract_command],
    dag=dag,
)

zendesk_extract_tickets_task = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE,
    task_id=f"zendesk-tickets-extract-{TASK_SCHEDULE}",
    name=f"zendesk-tickets-extract-{TASK_SCHEDULE}",
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
        SNOWFLAKE_LOAD_PASSWORD,
        ZENDESK_SENSITIVE_SERVICE_ACCOUNT_CREDENTIALS,
        ZENDESK_SENSITIVE_EXTRACTION_BUCKET_NAME,
    ],
    env_vars={
        **pod_env_vars,
        "logical_date": "{{ logical_date }}",
        "task_schedule": TASK_SCHEDULE,
    },
    affinity=get_affinity("extraction"),
    tolerations=get_toleration("extraction"),
    arguments=[zendesk_tickets_extract_command],
    dag=dag,
)

zendesk_extract_ticket_audits_task >> zendesk_extract_tickets_task
