"""
This module args.py parses the command line arguments provided by the user.
"""

import os
import sys
import argparse

# needed to import shared utils module
abs_path = os.path.dirname(os.path.realpath(__file__))
parent_path = abs_path[: abs_path.find("/provision_users")]
sys.path.insert(1, parent_path)
from utils_snowflake_provisioning import (
    get_user_changes,
)


def get_users_added() -> list:
    """returns the usernames ADDED to the snowflake_users.yml file"""
    return get_user_changes()[0]


def parse_arguments() -> argparse.Namespace:
    """
    The user can pass in the following arguemnts:
        --usernames-to-add
        --test-run
        --dev-db

    All arguments are optional- if no args passed in,
    then default arguments for `roles` and `users` are used.
    """
    parser = argparse.ArgumentParser(description="Provision users in Snowflake options")
    parser.add_argument(
        "-ua",
        "--users-to-add",
        nargs="+",
        type=str,
        default=get_users_added(),
        help="usernames to ADD to the roles.yml file",
    )
    # by default, only print snowflake queries, don't run in Snowflake
    parser.add_argument(
        "-t",
        "--test-run",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="If test, only print the sql statements, rather than running them in Snowflake",
    )
    # if arg not selected, will not run with dev-db
    parser.add_argument(
        "-db",
        "--dev-db",
        action=argparse.BooleanOptionalAction,
        help="If selected, create development databases for each user",
    )

    return parser.parse_args()
