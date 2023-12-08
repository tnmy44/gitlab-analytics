import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.bash import BashOperator
from kubernetes_helpers import get_affinity, get_toleration
from airflow_utils import (
    DATA_IMAGE_3_10,
    clone_and_setup_extraction_cmd,
    gitlab_defaults,
    slack_failed_task,
    gitlab_pod_env_vars,
)

# Load the env vars into a dict and set Secrets
env = os.environ.copy()
pod_env_vars = gitlab_pod_env_vars

# Default arguments for the DAG
default_args = {
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(hours=12),
    "sla_miss_callback": slack_failed_task,
    "dagrun_timeout": timedelta(hours=6),
}

# Create the DAG
dag = DAG(
    "pvc_monitor",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    concurrency=1,
    catchup=False,
    start_date= datetime(2023, 11, 15),
)

# tableau Extract
pvc_monitor_cmd = f"""
    {clone_and_setup_extraction_cmd} &&
    apt-get update && 
    apt-get install jq -y && 
    curl -LO https://storage.googleapis.com/kubernetes-release/release/v1.26.4/bin/linux/amd64/kubectl
    chmod +x ./kubectl
    mv ./kubectl /usr/local/bin/kubectl
    cd ./pvc_monitor && 
    bash get_pvc_values.sh > pvc_values.csv
    python3 pvc_check.py
"""

get_pvc_values = BashOperator(
    task_id = 'other-get',
    bash_command = pvc_monitor_cmd
)


# having both xcom flag flavors since we're in an airflow version where one is being deprecated
tableau_workbook_migrate = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE_3_10,
    task_id="pvc-monitor",
    name="pvc-monitor",
    env_vars=pod_env_vars,
    affinity=get_affinity("extraction"),
    tolerations=get_toleration("extraction"),
    arguments=[pvc_monitor_cmd],
    do_xcom_push=True,
    dag=dag,
)


get_pvc_values >> tableau_workbook_migrate