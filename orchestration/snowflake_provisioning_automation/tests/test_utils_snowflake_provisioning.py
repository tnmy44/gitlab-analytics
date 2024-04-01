"""
Test utils file, most notably
that the correct usernames are returned based off the git diff.
"""

from unittest.mock import patch
from utils_snowflake_provisioning import (
    YAML_PATH,
    get_user_changes,
    get_snowflake_username,
)


def test_imports():
    """test that YAML_PATH variable was imported correctly"""
    assert YAML_PATH is not None


@patch("utils_snowflake_provisioning.run_git_diff_command")
def test_get_user_changes(mock_run_git_diff_command):
    """
    Test that given a diff it returns the correct results.
    Specifically lines with a '+' belong to usernames_added
    and lines with a '-' belong to usernames_removed

    Note that modified blank lines are ignored
    """
    added_user = "added_user"
    removed_user = "removed_user"
    test_diff = f"+\n+--{added_user}\n---{removed_user}"
    mock_run_git_diff_command.return_value = test_diff
    emails_added, usernames_added, emails_removed, usernames_removed = (
        get_user_changes()
    )

    assert emails_added[0] == f"{added_user}@gitlab.com"
    assert usernames_added[0] == added_user
    assert emails_removed[0] == f"{removed_user}@gitlab.com"
    assert usernames_removed[0] == removed_user


def test_get_snowflake_username():
    cleaned_username = "jdoe"

    # regular user
    user = "jdoe"
    username = get_snowflake_username(user)
    assert username == cleaned_username

    # external user
    user = "jdoe-ext"
    username = get_snowflake_username(user)
    assert username == cleaned_username

    # user with non \w char
    user = "j-doe"
    username = get_snowflake_username(user)
    assert username == cleaned_username

    # user with non \w char, part 2
    user = "j.doe"
    username = get_snowflake_username(user)
    assert username == cleaned_username

    # user with non \w char, part 2
    user = "jdoe@gitlab.com"
    username = get_snowflake_username(user)
    assert username == cleaned_username

    # user with non \w char, part 2
    user = "j.doe-ext@gitlab.com"
    username = get_snowflake_username(user)
    assert username == cleaned_username
