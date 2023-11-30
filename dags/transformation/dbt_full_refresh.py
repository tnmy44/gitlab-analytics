"""
## Info about DAG
This DAG is designed to perform adhoc full refresh of any required number of models.
Before running this DAG fill the variables for the refresh:
- DBT_MODEL_TO_FULL_REFRESH
- DBT_WAREHOUSE_FOR_FULL_REFRESH
- DBT_TYPE_FOR_FULL_REFRESH

All variables should be populated, or job will not start.
"""
import os
from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow_utils import (
    DBT_IMAGE,
    dbt_install_deps_and_seed_nosha_cmd,
    gitlab_defaults,
    gitlab_pod_env_vars,
    slack_failed_task,
)
from kube_secrets import (
    GIT_DATA_TESTS_CONFIG,
    GIT_DATA_TESTS_PRIVATE_KEY,
    MCD_DEFAULT_API_ID,
    MCD_DEFAULT_API_TOKEN,
    SALT,
    SALT_EMAIL,
    SALT_IP,
    SALT_NAME,
    SALT_PASSWORD,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_STATIC_DATABASE,
    SNOWFLAKE_TRANSFORM_ROLE,
    SNOWFLAKE_TRANSFORM_SCHEMA,
    SNOWFLAKE_TRANSFORM_WAREHOUSE,
    SNOWFLAKE_USER,
)
from kubernetes_helpers import get_affinity, get_toleration

# Load the env vars into a dict and set Secrets
env = os.environ.copy()
pod_env_vars = {**gitlab_pod_env_vars, **{}}

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "start_date": datetime(2019, 1, 1, 0, 0, 0),
}
params = {
    "DBT_MODEL_TO_FULL_REFRESH": Param("", type="string", min_length=3),
    "DBT_WAREHOUSE_FOR_FULL_REFRESH": Param(
        "TRANSFORMING_XS",
        type="string",
        enum=[
            "TRANSFORMING_XS",
            "TRANSFORMING_S",
            "TRANSFORMING_L",
            "TRANSFORMING_XL",
            "TRANSFORMING_4XL",
        ],
    ),
    "DBT_TYPE_FOR_FULL_REFRESH": Param(
        "--full-refresh",
        type="string",
        enum=["--full-refresh", ""],
        min_length=0,
        max_length=13,
    ),
}

# Create the DAG
dag = DAG(
    "dbt_full_refresh",
    default_args=default_args,
    schedule_interval=None,
    description="Adhoc DBT full or incremental refresh",
    catchup=False,
    params=params,
)
dag.doc_md = __doc__


dbt_full_refresh_cmd = f"""
    {dbt_install_deps_and_seed_nosha_cmd} &&
    export SNOWFLAKE_TRANSFORM_WAREHOUSE='{{ params.DBT_WAREHOUSE_FOR_FULL_REFRESH }}' &&
    dbt --no-use-colors run --profiles-dir profile --target prod --models '{{ params.DBT_MODEL_TO_FULL_REFRESH }}' '{{ params.DBT_TYPE_FOR_FULL_REFRESH }}' ; ret=$?;
    montecarlo import dbt-run --manifest target/manifest.json --run-results target/run_results.json --project-name gitlab-analysis;
    python ../../orchestration/upload_dbt_file_to_snowflake.py results; exit $ret
"""


check_parameters = BashOperator(
    dag=dag,
    task_id="check_parameters",
    bash_command="echo {{ params }}",
)


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

check_parameters >> dbt_full_refresh
