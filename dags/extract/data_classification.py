"""
Unit to run data classification tasks
"""

import os
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_utils import (
    DATA_IMAGE,
    dbt_install_deps_cmd,
    clone_repo_cmd,
    dbt_install_deps_nosha_cmd,
    gitlab_defaults,
    gitlab_pod_env_vars,
    slack_failed_task,
)
from kube_secrets import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_USER,
)
from kubernetes_helpers import get_affinity, get_toleration

env = os.environ.copy()
pod_env_vars = {
    "RUN_DATE": "{{ next_ds }}",
}

pod_env_vars = {**gitlab_pod_env_vars, **pod_env_vars}

DAG_NAME = "data_classification"

DAG_DESCRIPTION = "This DAG run to identify data classification for MNPI and PII data."


secrets = [
    SNOWFLAKE_USER,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_WAREHOUSE,
]
default_args = {
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 0,
    "start_date": datetime(2019, 1, 1),
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(hours=24),
    "sla_miss_callback": slack_failed_task,
}


def get_command(task: str):
    """
    Get the execute command
    """
    if task == "extract_classification":
        operation = "EXTRACT"
    else:
        operation = "CLASSIFY"

    unset = Variable.get("DATA_CLASSIFICATION_UNSET", default_var="False")
    tagging_type = Variable.get(
        "DATA_CLASSIFICATION_TAGGING_TYPE", default_var="INCREMENTAL"
    )

    commands = {
        # "extract_classification": f"""{dbt_install_deps_cmd} && dbt --profiles-dir profile --target prod --quiet ls --models tag:mnpi+ --exclude tag:mnpi_exception config.database:$SNOWFLAKE_PREP_DATABASE config.schema:restricted_safe_common config.schema:restricted_safe_common_mapping config.schema:restricted_safe_common_mart_finance config.schema:restricted_safe_common_mart_sales config.schema:restricted_safe_common_mart_marketing config.schema:restricted_safe_common_mart_product config.schema:restricted_safe_common_prep config.schema:restricted_safe_legacy config.schema:restricted_safe_workspace_finance config.schema:restricted_safe_workspace_sales config.schema:restricted_safe_workspace_marketing config.schema:restricted_safe_workspace_engineering --output json > safe_models.json; ret=$?;""",
        "extract_classification": f"""{dbt_install_deps_cmd} && dbt --profiles-dir profile --target prod --quiet ls --models tag:mnpi+ --exclude tag:mnpi_exception config.database:$SNOWFLAKE_PREP_DATABASE config.schema:restricted_safe_common config.schema:restricted_safe_common_mapping config.schema:restricted_safe_common_mart_finance config.schema:restricted_safe_common_mart_sales config.schema:restricted_safe_common_mart_marketing config.schema:restricted_safe_common_mart_product config.schema:restricted_safe_common_prep config.schema:restricted_safe_legacy config.schema:restricted_safe_workspace_finance config.schema:restricted_safe_workspace_sales config.schema:restricted_safe_workspace_marketing config.schema:restricted_safe_workspace_engineering --output json > safe_models.json; && {clone_repo_cmd} && cd analytics/extract/data_classification/ && python3 extract.py --operation={operation} --date_from=$RUN_DATE --unset={unset} --tagging_type={tagging_type}""",
        "execute_classification": f"""{clone_repo_cmd} && cd analytics/extract/data_classification/ && python3 extract.py --operation={operation} --date_from=$RUN_DATE --unset={unset} --tagging_type={tagging_type}""",
    }
    return commands[task]


dag = DAG(
    DAG_NAME,
    default_args=default_args,
    schedule_interval="0 7 * * 1",
    concurrency=1,
    description=DAG_DESCRIPTION,
    catchup=False,
)

task_id = task_name = "extract_classification"


extract_classification = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE,
    task_id=task_id,
    name=task_name,
    secrets=secrets,
    env_vars=pod_env_vars,
    arguments=[get_command(task=task_id)],
    affinity=get_affinity("extraction"),
    tolerations=get_toleration("extraction"),
    dag=dag,
)

task_id = task_name = "execute_classification"

execute_classification = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE,
    task_id=task_id,
    name=task_name,
    secrets=secrets,
    env_vars=env,
    arguments=[get_command(task=task_id)],
    affinity=get_affinity("extraction"),
    tolerations=get_toleration("extraction"),
    dag=dag,
)

extract_classification >> execute_classification
