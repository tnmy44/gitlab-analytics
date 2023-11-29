"""
## Info about DAG
This DAG is designed to perform adhoc full refresh of any required number of models.
Before running this DAG fill the variables for the refresh:
- DBT_MODEL_TO_FULL_REFRESH
- DBT_WAREHOUSE_FOR_FULL_REFRESH
- DBT_TYPE_FOR_FULL_REFRESH
All variables should be populated, or job will nto start.
"""
import os
import logging
from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.models import Variable
from airflow_utils import (
    DBT_IMAGE,
    dbt_install_deps_and_seed_nosha_cmd,
    gitlab_defaults,
    gitlab_pod_env_vars,
    slack_failed_task,
)
from kube_secrets import (
    GIT_DATA_TESTS_PRIVATE_KEY,
    GIT_DATA_TESTS_CONFIG,
    SALT,
    SALT_EMAIL,
    SALT_IP,
    SALT_NAME,
    SALT_PASSWORD,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_TRANSFORM_ROLE,
    SNOWFLAKE_TRANSFORM_SCHEMA,
    SNOWFLAKE_TRANSFORM_WAREHOUSE,
    SNOWFLAKE_USER,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
    MCD_DEFAULT_API_ID,
    MCD_DEFAULT_API_TOKEN,
    SNOWFLAKE_STATIC_DATABASE,
)

from kubernetes_helpers import get_affinity, get_toleration

# Load the env vars into a dict and set Secrets
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = {**gitlab_pod_env_vars, **{}}

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "start_date": datetime(2019, 1, 1, 0, 0, 0),
}

# Create the DAG
dag = DAG(
    "dbt_full_refresh",
    default_args=default_args,
    schedule_interval=None,
    description="Adhoc DBT FULL Refresh",
    catchup=False,
)
dag.doc_md = __doc__


def get_parameters(**kwargs):
    """
    Return parameter from the json file
    """
    dag_run = kwargs.get("dag_run")
    parameters = dag_run.conf["key"]

    return parameters


# read model for full-refresh from input json file
dbt_model_to_full_refresh = Variable.get("DBT_MODEL_TO_FULL_REFRESH")

# Read the warehouse from the input json file
dbt_warehouse_for_full_refresh = Variable.get("DBT_WAREHOUSE_FOR_FULL_REFRESH")

# Read the type from the input json file, values are either full-refresh or incremental
dbt_type_for_full_refresh = Variable.get("DBT_TYPE_FOR_FULL_REFRESH")

logging.info(
    f"Running  refresh for {dbt_model_to_full_refresh} "
    f"on warehouse {dbt_warehouse_for_full_refresh} "
    f"with type: {dbt_type_for_full_refresh}"
)

# dbt-full-refresh
dbt_full_refresh_cmd = f"""
    {dbt_install_deps_and_seed_nosha_cmd} &&
    export SNOWFLAKE_TRANSFORM_WAREHOUSE={dbt_warehouse_for_full_refresh} &&
    dbt --no-use-colors run --profiles-dir profile --target prod --models {dbt_model_to_full_refresh} --full-refresh ; ret=$?;
    montecarlo import dbt-run --manifest target/manifest.json --run-results target/run_results.json --project-name gitlab-analysis;
    python ../../orchestration/upload_dbt_file_to_snowflake.py results; exit $ret
"""
dbt_full_refresh = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id="dbt-full-refresh",
    name="dbt-full-refresh",
    secrets=[
        GIT_DATA_TESTS_PRIVATE_KEY,
        GIT_DATA_TESTS_CONFIG,
        SALT,
        SALT_EMAIL,
        SALT_IP,
        SALT_NAME,
        SALT_PASSWORD,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_USER,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_TRANSFORM_ROLE,
        SNOWFLAKE_TRANSFORM_WAREHOUSE,
        SNOWFLAKE_TRANSFORM_SCHEMA,
        SNOWFLAKE_LOAD_PASSWORD,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
        SNOWFLAKE_STATIC_DATABASE,
        MCD_DEFAULT_API_ID,
        MCD_DEFAULT_API_TOKEN,
        SNOWFLAKE_STATIC_DATABASE,
    ],
    env_vars=pod_env_vars,
    arguments=[dbt_full_refresh_cmd],
    affinity=get_affinity("dbt"),
    tolerations=get_toleration("dbt"),
    dag=dag,
)
