"""

"""
import os
from datetime import datetime
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_utils import (
    DBT_IMAGE,
    dbt_install_deps_nosha_cmd,
    gitlab_defaults,
    gitlab_pod_env_vars,
    partitions,
    slack_failed_task,
    run_command_test_exclude,
)

from kubernetes_helpers import get_affinity, get_toleration
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

# Load the env vars into a dict and set Secrets
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = {**gitlab_pod_env_vars, **{}}

if GIT_BRANCH in ["master", "main"]:
    target = "prod"
else:
    target = "ci"

dbt_secrets = [
    GIT_DATA_TESTS_CONFIG,
    GIT_DATA_TESTS_PRIVATE_KEY,
    SALT,
    SALT_EMAIL,
    SALT_IP,
    SALT_NAME,
    SALT_PASSWORD,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_WAREHOUSE,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_TRANSFORM_ROLE,
    SNOWFLAKE_TRANSFORM_SCHEMA,
    SNOWFLAKE_TRANSFORM_WAREHOUSE,
    SNOWFLAKE_USER,
    MCD_DEFAULT_API_ID,
    MCD_DEFAULT_API_TOKEN,
    SNOWFLAKE_STATIC_DATABASE,
]

DBT_MODULE_NAME = "gitlab_dotcom_backfill_chunked"

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    # TODO: this needs to start at the minimum commit_date of the data
    "start_date": datetime(2021, 7, 1),
}

# Create the DAG
dag = DAG(
    "dbt_merge_request_diff_commits_backfill",
    default_args=default_args,
    schedule_interval='30 16 * * 0',#
    concurrency=2,
)


dbt_models_cmd = f"""
        {dbt_install_deps_nosha_cmd} &&
        dbt run --profiles-dir profile --target {target} --models tag:{DBT_MODULE_NAME} ; ret=$?;
        montecarlo import dbt-run --manifest target/manifest.json --run-results target/run_results.json --project-name gitlab-analysis;
        python ../../orchestration/upload_dbt_file_to_snowflake.py results; exit $ret
        """

dbt_models_task_name = f"{DBT_MODULE_NAME}-dbt-models"
dbt_models_task = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id=dbt_models_task_name,
    name=dbt_models_task_name,
    secrets=dbt_secrets,
    env_vars=pod_env_vars,
    arguments=[dbt_models_cmd],
    dag=dag,
)


'''
dbt_test_task_name = f"{DBT_MODULE_NAME}-dbt-tests"
model_test_cmd = f"""
    {dbt_install_deps_nosha_cmd} &&
    dbt test --profiles-dir profile --target prod --models tag:{DBT_MODULE_NAME}+ {run_command_test_exclude} ; ret=$?;
    montecarlo import dbt-run --manifest target/manifest.json --run-results target/run_results.json --project-name gitlab-analysis;
    python ../../orchestration/upload_dbt_file_to_snowflake.py test; exit $ret
"""
dbt_test_task = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id=dbt_test_task_name,
    name=dbt_test_task_name,
    secrets=dbt_secrets,
    env_vars=gitlab_pod_env_vars,
    arguments=[model_test_cmd],
    affinity=get_affinity("production"),
    tolerations=get_toleration("production"),
    dag=dag,
)
'''

dbt_models_task
# dbt_models_task >> dbt_test_task
