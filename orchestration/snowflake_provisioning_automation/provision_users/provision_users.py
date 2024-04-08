"""
In Snowflake, create the user/roles/grants needed.

There are 3 main things that need to be done:
    - for users being added:
        - 1) create the user and role
        - 2) optionally, create their development databases
    - for users being removed
        - 3) remove the user from Snowflake

Each of these actions is done via reading in a templated sql script,
rendering the script with the specified user,
and running the sql script via sqlalchemy connection.
"""

import os
import logging

import time
from typing import Tuple
from sqlalchemy.engine.base import Engine
from jinja2 import Template

from args_provision_users import parse_arguments
from snowflake_connection import SnowflakeConnection
from convert_sql_templates import get_template, process_template


def configure_logging():
    """configure logger"""
    logging.basicConfig(
        level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
    )


def process_args() -> Tuple[list, list, bool, bool]:
    """returns command line args passed in by user"""
    args = parse_arguments()
    return (
        args.usernames_to_remove,
        args.usernames_to_add,
        args.dev_db,
        args.test_run,
    )


def _get_snowflake_connection(role: str, is_test_run: bool):
    """helper method to return snowflake_connection for particular role"""
    config_dict = os.environ.copy()
    return SnowflakeConnection(config_dict, role, is_test_run)


def get_securityadmin_connection(is_test_run: bool):
    """return securityadmin snowflake connection"""
    role = "SECURITYADMIN"
    return _get_snowflake_connection(role, is_test_run)


def get_sysadmin_connection(is_test_run: bool):
    """return sysadmin snowflake connection"""
    role = "SYSADMIN"
    return _get_snowflake_connection(role, is_test_run)


def _provision(connection: Engine, sql_template: Template, usernames: list):
    """
    All provision types (new users, new databases, deprovision users)
    run this base method.

    The template is rendered into a series of string sql statements
    for each passed in user.
    Each sql statement is then run in Snowflake.
    """
    for username in usernames:
        logging.info(f"username: {username}")
        sql_statements = process_template(sql_template, username)
        connection.run_sql_statements(sql_statements)


def provision_users(connection: Engine, usernames: list):
    """provision user in Snowflake"""
    template_filename = "provision_user.sql"
    sql_template = get_template(template_filename)
    logging.info("#### Provisioning users ####")
    _provision(connection, sql_template, usernames)


def provision_databases(connection: Engine, usernames: list):
    """provision personal databases in Snowflake"""
    template_filename = "provision_database.sql"
    sql_template = get_template(template_filename)
    logging.info("#### Provisioning user databases ####")
    _provision(connection, sql_template, usernames)


def deprovision_users(connection: Engine, usernames: list):
    """deprovision users in Snowflake"""
    template_filename = "deprovision_user.sql"
    sql_template = get_template(template_filename)
    logging.info("#### Deprovisioning users ####")
    _provision(connection, sql_template, usernames)


def provision_all():
    """
    Performs the following actions:
        - provision users
        - provision databases
        - deprovision users
    """
    usernames_to_remove, usernames_to_add, is_dev_db, is_test_run = process_args()

    logging.info(f"provision users Snowflake, is_test_run: {is_test_run}\n")
    time.sleep(5)  # give user a chance to abort
    logging.info(f"usernames_to_add: {usernames_to_add}")
    logging.info(f"usernames_to_remove: {usernames_to_remove}\n")

    securityadmin_connection = get_securityadmin_connection(is_test_run)
    sysadmin_connection = get_sysadmin_connection(is_test_run)

    provision_users(securityadmin_connection, usernames_to_add)
    if is_dev_db:
        provision_databases(sysadmin_connection, usernames_to_add)
    deprovision_users(securityadmin_connection, usernames_to_remove)


def main():
    """entrypoint"""
    configure_logging()
    provision_all()


if __name__ == "__main__":
    main()
