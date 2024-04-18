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
import sys
import logging
import time
from typing import Tuple

# from gitlabdata.orchestration_utils import query_executor, snowflake_engine_factory
from snowflake_connection import SnowflakeConnection


# needed to import update_roles_yaml/
abs_path = os.path.dirname(os.path.realpath(__file__))
roles_yaml_path = os.path.join(
    abs_path[: abs_path.find("/provision_users")], "update_roles_yaml/"
)
sys.path.insert(1, roles_yaml_path)
from args_update_roles_yaml import parse_arguments
from utils_update_roles import (
    USERS_KEY,
    get_roles_from_url,
)
from roles_struct import RolesStruct

config_dict = os.environ.copy()


def configure_logging():
    """configure logger"""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )


def process_args() -> Tuple[list, list, str, str, str]:
    """returns command line args passed in by user"""
    args = parse_arguments()
    return (
        args.usernames_to_remove,
        args.test_run,
    )


def get_users_from_roles_yaml():
    roles_data = get_roles_from_url()
    roles_struct = RolesStruct(roles_data)
    users_roles_yaml = roles_struct.get_existing_value_keys(USERS_KEY)
    print(f"\nusers_roles_yaml: {users_roles_yaml}")
    return users_roles_yaml


def get_users_snowflake(is_test_run=False):
    # return ["juwong", 'some_user_to_remove']
    sysadmin_connection = SnowflakeConnection(config_dict, "SYSADMIN", is_test_run)
    query = """
    SELECT lower(name) as name
    FROM SNOWFLAKE.ACCOUNT_USAGE.USERS
    WHERE true
    and has_password = false
    and deleted_on is NULL
    and created_on < DATEADD(WEEK, -1, CURRENT_TIMESTAMP)
    ORDER BY name;
    """
    # returns [(user1, ), (user2, )]
    users_tuple_list = sysadmin_connection.run_sql_statement(query)
    users_snowflake = [users_tuple[0] for users_tuple in users_tuple_list]
    return users_snowflake


def compare_users(users_roles_yaml, users_snowflake):
    missing_users = [user for user in users_snowflake if user not in users_roles_yaml]
    return missing_users


def main():
    """entrypoint function"""
    configure_logging()
    # is_test_run = process_args()
    # logging.info(f"clean_snowflake_users_roles: {is_test_run}")
    # time.sleep(5)  # give user a chance to abort

    users_roles_yaml = get_users_from_roles_yaml()
    users_snowflake = get_users_snowflake()

    missing_users = compare_users(users_roles_yaml, users_snowflake)
    print(f"\nmissing_users: {missing_users}")

    # TODO, figure out how to drop the missing users. Ideally, we use the sql template, maybe we can move one of the sql templates over.
    # We can test all of this using a DAG, `snowflake_cleanup`


if __name__ == "__main__":
    main()
