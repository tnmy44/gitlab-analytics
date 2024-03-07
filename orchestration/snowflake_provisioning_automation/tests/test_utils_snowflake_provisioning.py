from unittest.mock import patch
from utils_snowflake_provisioning import YAML_PATH, get_username_changes


def test_imports():
    """test that YAML_PATH variable was imported correctly"""
    assert YAML_PATH is not None


@patch("utils_snowflake_provisioning.run_git_diff_command")
def test_get_username_changes(mock_run_git_diff_command):
    """
    Test that given a diff it returns the correct results.
    Specifically lines with a '+' belong to usernames_added
    and lines with a '-' belong to usernames_removed

    Note that modified blank lines are ignored
    """
    test_diff = "+\n+--added_user\n---removed_user"
    mock_run_git_diff_command.return_value = test_diff
    usernames_added, usernames_removed = get_username_changes()

    assert usernames_added[0] == "added_user"
    assert usernames_removed[0] == "removed_user"
