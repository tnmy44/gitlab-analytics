"""
This process is used to deprovision users in Snowflake.
Users are deprovisioned if they are no longer in `roles.yml`.

Unlike the rest of the processes in `snowflake_provisioning_automation/`,
this process is not accessible CI job.
This is because there's no immediate rush to delete users,
and also because deleting users is more sensitive, it will be an
automatic job.

The entrypoint of this job will be from an Airflow task.
Ideally, this is run after the snowflake_permissions DAG finishes
provisioning new users.
"""

import os
import logging
import time
from typing import Tuple
from gitlabdata.orchestration_utils import query_executor, snowflake_engine_factory

from args_update_roles_yaml import parse_arguments
from utils_update_roles import (
    ROLES_KEY,
    USERS_KEY,
    get_roles_from_yaml,
)
from roles_struct import RolesStruct


def configure_logging():
    """configure logger"""
    logging.basicConfig(
        level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
    )


def process_args() -> Tuple[list, list, str, str, str]:
    """returns command line args passed in by user"""
    args = parse_arguments()
    return (
        args.usernames_to_remove,
        args.usernames_to_add,
        args.databases_template,
        args.roles_template,
        args.users_template,
        args.test_run,
    )


def get_existing_users_roles_yaml(roles_struct):
    users_roles_yaml = roles_struct.get_existing_value_keys(USERS_KEY)
    return users_roles_yaml


def get_existing_roles_roles_yaml(roles_struct):
    roles_roles_yaml = roles_struct.get_existing_value_keys(ROLES_KEY)
    return roles_roles_yaml


def get_existing_users_snowflake(snowflake_engine):
    query = """
    SELECT name
    FROM SNOWFLAKE.ACCOUNT_USAGE.ROLES
    WHERE role_type = 'ROLE'
    """
    users = query_executor(snowflake_engine, query)
    return users


def get_existing_roles_snowflake():
    pass

def compare_users(users_roles_yaml, users_snowflake):
    missing_users = [user for user in users_snowflake if user not in users_roles_yaml]
    return missing_users



def main():
    """entrypoint function"""
    configure_logging()
    # is_test_run = process_args()
    # logging.info(f"clean_snowflake_users_roles: {is_test_run}")
    time.sleep(5)  # give user a chance to abort

    roles_data = get_roles_from_yaml()
    roles_struct = RolesStruct(roles_data)

    users_roles_yaml = get_existing_users_roles_yaml(roles_struct)
    print(f"\nusers_roles_yaml: {users_roles_yaml}")

    roles_roles_yaml = get_existing_users_roles_yaml(roles_struct)
    print(f"\nroles_roles_yaml: {roles_roles_yaml}")

    config_dict = os.environ.copy()
    snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")

    users_snowflake = get_existing_users_snowflake(snowflake_engine)

    missing_users = compare_users(users_roles_yaml, users_snowflake)

    # TODO, figure out how to drop the missing users. Ideally, we use the sql template, maybe we can move one of the sql templates over.
    # We can test all of this using a DAG, `snowflake_cleanup`

if __name__ == "__main__":
    main()
