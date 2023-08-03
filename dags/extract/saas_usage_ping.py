"""
saas_usage_ping.py is responsible for orchestrating:
- usage ping combined metrics (sql + redis)
- usage ping namespace
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_utils import (
    DATA_IMAGE,
    clone_repo_cmd,
    gitlab_defaults,
    slack_failed_task,
    gitlab_pod_env_vars,
)
from kube_secrets import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_USER,
    GITLAB_ANALYTICS_PRIVATE_TOKEN,
)

# tomorrow_ds -  the day after the execution date as YYYY-MM-DD
# ds - the execution date as YYYY-MM-DD
pod_env_vars = {
    "RUN_DATE": "{{ next_ds }}",
    "SNOWFLAKE_SYSADMIN_ROLE": "TRANSFORMER",
    "SNOWFLAKE_LOAD_WAREHOUSE": "USAGE_PING",
}

pod_env_vars = {**gitlab_pod_env_vars, **pod_env_vars}


secrets = [
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_USER,
    GITLAB_ANALYTICS_PRIVATE_TOKEN,
]

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 0,
    "start_date": datetime(2020, 6, 7),
    "dagrun_timeout": timedelta(hours=8),
}

DAG_DESCRIPTION = (
    "This DAG run to calculate 2 types of metrics: "
    "- instance_combined_metrics (instance_sql_metrics and instance_redis_metrics) "
    "- instance_namespace_metrics "
)

# Create the DAG
#  Monday at 0700 UTC
dag = DAG("saas_usage_ping",
          default_args=default_args,
          concurrency=2,
          description=DAG_DESCRIPTION,
          schedule_interval="0 7 * * 1")

# Instance Level Usage Ping
instance_combined_metrics_cmd = f"""
    {clone_repo_cmd} &&
    cd analytics/extract/saas_usage_ping/ &&
    python3 transform_postgres_to_snowflake.py &&
    python3 usage_ping.py saas_instance_combined_metrics
"""

instance_combined_metrics_ping = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE,
    task_id="saas-instance-usage-ping-combined-metrics",
    name="saas-instance-usage-ping-combined-metrics",
    secrets=secrets,
    env_vars=pod_env_vars,
    arguments=[instance_combined_metrics_cmd],
    dag=dag,
)

# Namespace, Group, Project, User Level Usage Ping
namespace_cmd = f"""
    {clone_repo_cmd} &&
    cd analytics/extract/saas_usage_ping/ &&
    python3 instance_namespace_metrics.py saas_instance_namespace_metrics --ping_date=$RUN_DATE
"""


def get_task_name(current_chunk: int, no_of_tasks: int) -> str:
    """
    Generate task name
    """

    return f"saas-namespace-usage-ping-chunk-{current_chunk}-{no_of_tasks}"


def generate_task(current_chunk: int, no_of_tasks: int) -> None:
    """
    Generate tasks for namespace
    """
    task_id = task_name = get_task_name(current_chunk=current_chunk, no_of_tasks=no_of_tasks)

    return KubernetesPodOperator(
        **gitlab_defaults,
        image=DATA_IMAGE,
        task_id=task_id,
        name=task_name,
        secrets=secrets,
        env_vars=pod_env_vars,
        arguments=[namespace_cmd],
        dag=dag,
    )


[instance_combined_metrics_ping]

NO_OF_TASKS = 20
for i in range(1, NO_OF_TASKS + 1):
    generate_task(current_chunk=i, no_of_tasks=NO_OF_TASKS)
