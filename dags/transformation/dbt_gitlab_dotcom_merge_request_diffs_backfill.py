"""
Split backfill of a postgres table into 100 chunks.

From those 100 chunks, based on the max id of the table
evenly distribute the ids to each chunk
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

from gitlabdata.orchestration_utils import query_executor, snowflake_engine_factory


def get_max_id_target_table(pk, target_table):
    snowflake_engine = snowflake_engine_factory(env, "LOADER", "tap_postgres")
    query = f"select max({pk}) from {target_table};"
    max_id = query_executor(snowflake_engine, query)[0]
    return max_id


def generate_intervals(chunks: int, max_id: int):
    """
    Generates list of intervals, something like:
    [ (1, 10), (11, 20), (etc, etc) ]
    """
    if chunks > max_id:
        raise ValueError("not enough ids to chunk... aborting")

    intervals = []
    interval_size = max_id // chunks
    remaining = max_id % chunks
    start = 1

    for _ in range(chunks):
        end = start + interval_size - 1
        if remaining > 0:
            end += 1
            remaining -= 1
        pair = (start, end)
        intervals.append(pair)
        start = end + 1
    return intervals


def get_interval(intervals, chunk):
    interval = intervals[chunk]
    start = interval[0]
    end = interval[1]
    return start, end


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

DBT_MODULE_NAME = "gitlab_dotcom_merge_request_diffs_backfill"
CHUNKS = 10 # TODO


# Default arguments for the DAG
default_args = {
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "start_date": datetime(2023, 8, 28),
}

# Create the DAG
dag = DAG(
    f"dbt_{DBT_MODULE_NAME}",
    default_args=default_args,
    schedule_interval=None,  #
    max_active_runs=2,
    concurrency=2,
    catchup=True,
)


max_id = 3002
# TODO max_id = get_max_id_target_table()
intervals = generate_intervals(CHUNKS, max_id)

for chunk in range(CHUNKS):
    start, end = get_interval(intervals, chunk)
    dbt_vars = f'{{"backfill_start_id": {start}, "backfill_end_id": {end}}}'
    dbt_models_cmd = f"""
            {dbt_install_deps_nosha_cmd} &&
            dbt run --profiles-dir profile --target {target} --models workspaces.workspace_engineering.merge_request_diffs.* --vars '{dbt_vars}'; ret=$?;

            montecarlo import dbt-run --manifest target/manifest.json --run-results target/run_results.json --project-name gitlab-analysis;
            python ../../orchestration/upload_dbt_file_to_snowflake.py results; exit $ret
            """

    dbt_models_task_name = f"dbt-{DBT_MODULE_NAME}-{chunk:03d}"
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
