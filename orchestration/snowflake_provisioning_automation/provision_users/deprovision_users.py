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
from sqlalchemy.engine.base import Engine

from snowflake_connection import SnowflakeConnection
from provision_users import _provision

# need to import update_roles_yaml module
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
        args.users_to_remove,
        args.test_run,
    )


def get_users_from_roles_yaml():
    roles_data = get_roles_from_url()
    roles_struct = RolesStruct(roles_data)
    users_roles_yaml = roles_struct.get_existing_value_keys(USERS_KEY)
    print(f"\nusers_roles_yaml: {users_roles_yaml}")
    return users_roles_yaml


def flatten_list_of_tuples(tuple_list):
    return [tuple[0] for tuple in tuple_list]


def get_users_snowflake(sysadmin_connection):
    # return ["juwong", 'some_user_to_remove']
    query = """
    SELECT lower(name) as name
    FROM SNOWFLAKE.ACCOUNT_USAGE.USERS
    WHERE true
    and has_password = false
    and deleted_on is NULL
    and created_on < DATEADD(WEEK, -1, CURRENT_TIMESTAMP)
    ORDER BY name;
    """
    # users_tuple_list example: [(user1, ), (user2, )]
    users_tuple_list = sysadmin_connection.run_sql_statement(query)
    users_snowflake = flatten_list_of_tuples(users_tuple_list)
    return users_snowflake


def compare_users(users_roles_yaml, users_snowflake):
    missing_users = [user for user in users_snowflake if user not in users_roles_yaml]
    return missing_users


def get_users_to_remove(sysadmin_connection):
    users_roles_yaml = get_users_from_roles_yaml(sysadmin_connection)
    users_snowflake = get_users_snowflake()

    missing_users = compare_users(users_roles_yaml, users_snowflake)
    print(f"\nmissing_users: {missing_users}")
    return missing_users


def deprovision_users(connection: Engine, usernames: list):
    """
    Deprovision users in Snowflake
    Currently unused, will do Snowflake deprovision in separate process
    """
    template_filename = "deprovision_user.sql"
    logging.info("#### Deprovisioning users ####")
    _provision(connection, template_filename, usernames)


def main():
    """entrypoint function"""
    configure_logging()

    users_to_remove_arg, is_test_run = process_args()
    logging.info(f"is_test_run: {is_test_run}")
    time.sleep(5)  # give user a chance to abort

    sysadmin_connection = SnowflakeConnection(config_dict, "SYSADMIN", is_test_run)

    if not users_to_remove_arg:
        users_to_remove = get_users_to_remove(sysadmin_connection)
    else:
        users_to_remove = users_to_remove_arg
    logging.info(f'users_to_remove: {users_to_remove}')

    # deprovision_users(sysadmin_connection, users_to_remove)

    # TODO, figure out how to drop the missing users. Ideally, we use the sql template, maybe we can move one of the sql templates over.
    # We can test all of this using a DAG, `snowflake_cleanup`


if __name__ == "__main__":
    main()
