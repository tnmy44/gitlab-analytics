"""
Main test unit for GitLab_data_yaml pipeline
"""
from unittest.mock import patch

import pytest

from extract.gitlab_data_yaml.upload import (
    get_base_url,
    get_json_file_name,
    get_private_token,
    manifest_reader,
    run_subprocess,
)


@pytest.fixture(name="mock_env")
def mock_env_fixture():
    """
    Return environment variables
    """
    return {
        "PRIVATE_TOKEN": "dummy_token123",
    }


@pytest.fixture(name="manifest_file")
def manifest_file_fixture():
    """
    Get manifest file
    """
    from logging import info
    import os

    info(f"XXXXXXXXX: {os.getcwd()}")
    print(f"XXXXXXXXX: {os.getcwd()}")
    return manifest_reader(file_path="../gitlab_data_yaml/file_specification.yml")


@pytest.mark.parametrize(
    "attribute, expected_value",
    [
        ("", "ymltemp"),
        ("test.yml", "test"),
        ("test", "test"),
    ],
)
def test_get_json_file_name(attribute, expected_value):
    """
    Test get_json_file_name
    """
    assert get_json_file_name(attribute) == expected_value


@patch("upload.subprocess.run")
def test_run_subprocess(mock_run):
    """
    Test run_subprocess
    """
    run_subprocess("test command", "test_file")
    mock_run.assert_called_once_with("test command", shell=True, check=True)


@pytest.mark.parametrize(
    "url_specification, table_name, expected_value",
    [
        ({"test": "string_url"}, "", "string_url"),
        (
            {"test": {"table_1": "table_1_url", "table_2": "table_2_url"}},
            "table_1",
            "table_1_url",
        ),
        (
            {"test": {"table_1": "table_1_url", "table_2": "table_2_url"}},
            "table_2",
            "table_2_url",
        ),
    ],
)
def test_get_base_url(url_specification, table_name, expected_value):
    """
    Test get_base_url
    """
    assert (
        get_base_url(url_specification=url_specification["test"], table_name=table_name)
        == expected_value
    )


@pytest.mark.parametrize(
    "attribute, expected_value",
    [
        ("PRIVATE_TOKEN", "dummy_token123"),
        ("NON_EXISTENT", None),
    ],
)
def test_get_private_token(mock_env, attribute, expected_value):
    """
    Test get_private_token
    """
    with patch.dict("upload.env", mock_env):
        assert get_private_token(attribute) == expected_value


def test_manifest_reader_non_exist():
    """
    Test manifest file reader for non existing file
    """
    with pytest.raises(FileNotFoundError):
        _ = manifest_reader(file_path="NON_EXISTING_FILE.yml")


def test_manifest_reader(manifest_file):
    """
    Test manifest file reader
    """

    assert isinstance(manifest_file, dict)
    for _, spec in manifest_file.items():
        assert isinstance(spec, dict)
