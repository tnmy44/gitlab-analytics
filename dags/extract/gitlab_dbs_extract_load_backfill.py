""" Gitlab.Com Extract and load DAG"""
import os
import yaml
from tokenize import String
from datetime import datetime, timedelta
from typing import Union

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

from airflow_utils import (
    DATA_IMAGE,
    clone_repo_cmd,
    gitlab_defaults,
    slack_failed_task,
    gitlab_pod_env_vars,
)

from kubernetes_helpers import get_affinity, get_toleration
from kube_secrets import (
    CUSTOMERS_DB_HOST,
    CUSTOMERS_DB_NAME,
    CUSTOMERS_DB_PASS,
    CUSTOMERS_DB_USER,
    GCP_PROJECT,
    GCP_REGION,
    GCP_SERVICE_CREDS,
    GITLAB_BACKFILL_BUCKET,
    GITLAB_COM_DB_HOST,
    GITLAB_COM_DB_NAME,
    GITLAB_COM_DB_PASS,
    GITLAB_COM_DB_USER,
    GITLAB_COM_PG_PORT,
    GITLAB_COM_SCD_PG_PORT,
    GITLAB_COM_CI_DB_NAME,
    GITLAB_COM_CI_DB_HOST,
    GITLAB_COM_CI_DB_PASS,
    GITLAB_COM_CI_DB_PORT,
    GITLAB_COM_CI_DB_USER,
    GITLAB_OPS_DB_USER,
    GITLAB_OPS_DB_PASS,
    GITLAB_OPS_DB_HOST,
    GITLAB_OPS_DB_NAME,
    PG_PORT,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
    GITLAB_METADATA_DB_NAME,
    GITLAB_METADATA_DB_HOST,
    GITLAB_METADATA_DB_PASS,
    GITLAB_METADATA_PG_PORT,
    GITLAB_METADATA_DB_USER,
    GITLAB_METADATA_SCHEMA,
)

# Load the env vars into a dict and set env vars
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
standard_secrets = [
    GCP_SERVICE_CREDS,
    PG_PORT,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_WAREHOUSE,
    SNOWFLAKE_LOAD_ROLE,
]


# Dictionary containing the configuration values for the various Postgres DBs
config_dict = {
    "el_gitlab_com": {
        "cloudsql_instance_name": None,
        "dag_name": "el_gitlab_com_backfil",
        "env_vars": {"HOURS": "96"},
        "extract_schedule_interval": "30 2,14 */1 * *",
        "incremental_backfill_interval": "30 2,14 * * *",
        "delete_interval": "30 2 * * 0",
        "secrets": [
            GITLAB_COM_DB_USER,
            GITLAB_COM_DB_PASS,
            GITLAB_COM_DB_HOST,
            GITLAB_COM_DB_NAME,
            GITLAB_COM_PG_PORT,
            GITLAB_BACKFILL_BUCKET,
            GITLAB_METADATA_DB_NAME,
            GITLAB_METADATA_DB_HOST,
            GITLAB_METADATA_DB_PASS,
            GITLAB_METADATA_PG_PORT,
            GITLAB_METADATA_DB_USER,
            GITLAB_METADATA_SCHEMA,
        ],
        "start_date": datetime(2023, 8, 2),
        "task_name": "gitlab-com",
        "description": "This DAG does Incremental extract & load  of gitlab.com database(Postgres) to snowflake",
        "description_incremental": "This DAG does backfill of incremental table extract & load of gitlab.com database(Postgres) to snowflake",
        "description_deletes": "This DAG loads the PK only to check for deletes within gitlab.com database(Postgres)",
    }
}


def get_task_pool(task_name) -> str:
    """Return airflow pool name"""
    return f"{task_name}-pool"


def is_incremental(raw_query):
    """Determine if the extraction is incremental or full extract i.e. SCD"""
    return "{EXECUTION_DATE}" in raw_query or "{BEGIN_TIMESTAMP}" in raw_query


def use_cloudsql_proxy(dag_name, operation, instance_name):
    """Use cloudsql proxy for connecting to ops Database"""
    return f"""
        {clone_repo_cmd} &&
        cd analytics/orchestration &&
        python ci_helpers.py use_proxy --instance_name {instance_name} --command " \
            python ../extract/saas_postgres_pipeline_backfill/postgres_pipeline/main.py tap  \
            ../extract/saas_postgres_pipeline_backfill/manifests_decomposed/{dag_name}_db_manifest.yaml {operation}
        "
    """


def get_last_loaded(dag_name: str) -> Union[None, str]:
    """Pull from xcom value  last loaded timestamp for the table"""
    if dag_name == "el_gitlab_ops":
        return None

    return "{{{{ task_instance.xcom_pull('{}', include_prior_dates=True)['max_data_available'] }}}}".format(
        task_identifier + "-pgp-extract"
    )


def generate_cmd(dag_name, operation, cloudsql_instance_name):
    """Generate the command"""
    if cloudsql_instance_name is None:
        return f"""
            {clone_repo_cmd} &&
            cd analytics/extract/saas_postgres_pipeline_backfill/postgres_pipeline/ &&
            python main.py tap ../manifests_decomposed/{dag_name}_db_manifest.yaml {operation}
        """

    return use_cloudsql_proxy(dag_name, operation, cloudsql_instance_name)


def extract_manifest(manifest_file_path):
    """Extract postgres pipeline manifest file"""
    with open(manifest_file_path, "r") as file:
        manifest_dict = yaml.load(file, Loader=yaml.FullLoader)
    return manifest_dict


def extract_table_list_from_manifest(manifest_contents):
    """Extract table from the manifest file for which extraction needs to be done"""
    return manifest_contents["tables"].keys()


# Sync DAG
incremental_backfill_dag_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=3),
    "dagrun_timeout": timedelta(hours=10),
    "trigger_rule": "all_success",
}

scd_dag_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "dagrun_timeout": timedelta(hours=10),
    "trigger_rule": "all_success",
}

# Extract DAG
extract_dag_args = {
    "catchup": True,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(hours=8),
    "sla_miss_callback": slack_failed_task,
    "dagrun_timeout": timedelta(hours=6),
    "trigger_rule": "all_success",
}
# Loop through each config_dict and generate a DAG
for source_name, config in config_dict.items():
    if "scd" not in source_name:
        extract_dag_args["start_date"] = config["start_date"]
        incremental_backfill_dag_args["start_date"] = config["start_date"]

        incremental_backfill_dag = DAG(
            f"{config['dag_name']}_db_incremental_backfillv2",
            default_args=incremental_backfill_dag_args,
            schedule_interval=config["incremental_backfill_interval"],
            concurrency=1,
            description=config["description_incremental"],
        )

        with incremental_backfill_dag:
            file_path = f"analytics/extract/saas_postgres_pipeline_backfill/manifests_decomposed/{config['dag_name']}_db_manifest.yaml"
            manifest = extract_manifest(file_path)
            table_list = extract_table_list_from_manifest(manifest)
            if config["dag_name"] == "el_gitlab_com_new":
                table_list = [
                    "merge_request_diff_commits",
                ]
            for table in table_list:
                if is_incremental(manifest["tables"][table]["import_query"]):
                    TASK_TYPE = "backfill"

                    task_identifier = (
                        f"el-{config['task_name']}-{table.replace('_','-')}-{TASK_TYPE}"
                    )

                    sync_cmd = generate_cmd(
                        config["dag_name"],
                        f"--load_type backfill --load_only_table {table}",
                        config["cloudsql_instance_name"],
                    )
                    sync_extract = KubernetesPodOperator(
                        **gitlab_defaults,
                        image=DATA_IMAGE,
                        task_id=task_identifier,
                        name=task_identifier,
                        pool=get_task_pool(config["task_name"]),
                        secrets=standard_secrets + config["secrets"],
                        env_vars={
                            **gitlab_pod_env_vars,
                            **config["env_vars"],
                            "TASK_INSTANCE": "{{ task_instance_key_str }}",
                        },
                        affinity=get_affinity(False),
                        tolerations=get_toleration(False),
                        arguments=[sync_cmd],
                        # do_xcom_push=True, # TODO: do we need this still?
                    )

        globals()[
            f"{config['dag_name']}_db_incremental_backfill"
        ] = incremental_backfill_dag
