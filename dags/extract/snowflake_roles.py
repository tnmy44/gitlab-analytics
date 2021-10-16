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
    SNOWFLAKE_LOAD_DATABASE,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_USER,
)
from kubernetes_helpers import get_affinity, get_toleration

# Load the env vars into a dict and set Secrets
env = os.environ.copy()

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2019, 1, 1),
    "dagrun_timeout": timedelta(hours=2),
}

# Set the command for the container
container_cmd = f"""
    {clone_and_setup_extraction_cmd} &&
    python3 snowflake/user_role_grants.py
"""

# Create the DAG
dag = DAG(
    "snowflake_roles_snapshot",
    default_args=default_args,
    schedule_interval="0 1 */1 * *",
)

# Task 1
snowflake_roles_snapshot = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE,
    task_id="snowflake-roles-snapshot",
    name="snowflake-roles-snapshot",
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_USER,
        SNOWFLAKE_LOAD_PASSWORD,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
    ],
    affinity=get_affinity(False),
    tolerations=get_toleration(False),
    env_vars=gitlab_pod_env_vars,
    arguments=[container_cmd],
    dag=dag,
)
