import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_utils import (
    DATA_IMAGE,
    clone_repo_cmd,
    gitlab_defaults,
    slack_failed_task,
    gitlab_pod_env_vars,
    clone_and_setup_extraction_cmd,
)
from kube_secrets import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_USER,
)

from kubernetes_helpers import get_affinity, get_toleration

# Load the env vars into a dict and set env vars
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]

# CLONE_DATE will be used to set the timestamp of when clone should
# CLONE_NAME_DATE date formatted to string to be used for clone name
# tomorrow_ds -  the day after the execution date as YYYY-MM-DD
# ds_nodash - the execution date as YYYYMMDD
pod_env_vars = {
    "CLONE_NAME_DATE": "{{ ds_nodash }}",
    "SNOWFLAKE_SYSADMIN_ROLE": "TRANSFORMER",
}

pod_env_vars = {**gitlab_pod_env_vars, **pod_env_vars}
pod_env_vars["BRANCH_NAME"] = env["GIT_BRANCH"].upper()

logging.info(pod_env_vars)

secrets = [
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_USER,
]

# Default arguments for the DAG
default_args = {
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2020, 6, 7),
    "dagrun_timeout": timedelta(hours=2),
}

# Create the DAG
#  DAG will be triggered at 06:59am UTC which is 23:59 PM PST
dag = DAG(
    "snowflake_table_clones",
    default_args=default_args,
    schedule_interval="0 7 * * *",
    catchup=False,
)

clone_table_config = [
    {
        "table_name": "mart_arr",
        "source_database": "PROD",
        "source_schema": "restricted_safe_common_mart_sales",
        "source_table": "mart_arr",
        "target_schema": "full_table_clones",
        "target_database": "RAW",
    },
]

for config in clone_table_config:
    target_table_name = f"'{config.get('table_name')}_$CLONE_NAME_DATE'"

    container_cmd = f"""
        {clone_repo_cmd} &&
        export PYTHONPATH="$CI_PROJECT_DIR/orchestration/:$PYTHONPATH" &&
        cd analytics/orchestration/ &&

    python3 manage_snowflake.py create-table-clone  \
        --source_database {config.get('source_database')}  \
        --source_schema {config.get('source_schema')}  \
        --source_table {config.get('source_table')} \
        --target_database {config.get('target_database')} \
        --target_schema {config.get('target_schema')}  \
        --target_table '{target_table_name}'"""

    clone_dag = KubernetesPodOperator(
        **gitlab_defaults,
        image=DATA_IMAGE,
        task_id=f"snowflake-clone-{config.get('source_table').replace('_', '-')}",
        name=f"snowflake-clone-{config.get('source_table').replace('_', '-')}",
        secrets=secrets,
        env_vars=pod_env_vars,
        arguments=[container_cmd],
        affinity=get_affinity("extraction"),
        tolerations=get_toleration("extraction"),
        dag=dag,
    )
