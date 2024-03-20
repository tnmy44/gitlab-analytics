"""
utils module for both `provision_users/` and `update_roles_yaml/`

Child modules access this utils file by running
abs_path = os.path.dirname(os.path.realpath(__file__))
parent_path = abs_path[: abs_path.find("/update_roles_yaml")]
sys.path.insert(1, parent_path)

The `git diff` command could have been alternatively placed within
the CI job, but `get_username_changes()` logic is a little bit tricky
to do in bash.
Furthermore, by doing it in python, the entrypoint can be either CI job/python script
"""

import os
import subprocess
from typing import Tuple, List


abs_path = os.path.realpath(__file__)
YAML_PATH = abs_path[: abs_path.find("/orchestration")] + "/permissions/snowflake/"


def run_git_diff_command(file_path: str, base_branch: str = "master") -> str:
    """Run git diff command and capture the output"""

    git_diff_command = f"git diff {base_branch}  -- {file_path}"

    # If git diff has output, then grep for modified lines
    # https://stackoverflow.com/a/26622262
    grep_diff_command = f"""
    if [ -n "$({git_diff_command})" ]; then \
        {git_diff_command} \
    | grep '^[+-]' | grep -Ev '^(--- a/|\\+\\+\\+ b/|--- /dev/null)'
    else
        echo ''
    fi
    """

    diff_output = subprocess.check_output(grep_diff_command, shell=True, text=True)
    return diff_output


def get_username_changes() -> Tuple[List[str], List[str]]:
    """
    Based on git diff to the `snowflake_usernames.yml` file,
    returns user additions and removals
    """
    # Get the directory of the Python script
    usernames_file_name = "snowflake_usernames.yml"
    usernames_file_path = os.path.join(YAML_PATH, usernames_file_name)

    # Run the Git diff command
    base_branch = "origin/master"
    output = run_git_diff_command(usernames_file_path, base_branch)

    usernames_added = []
    usernames_removed = []

    for change in output.split("\n"):
        try:
            username = change[3:]
        except IndexError:
            continue

        # check that username isn't a blank line
        if change.startswith("+") and username:
            usernames_added.append(username)
        elif change.startswith("-") and username:
            usernames_removed.append(username)

    return usernames_added, usernames_removed
