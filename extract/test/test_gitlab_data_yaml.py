"""
Main test unit for GitLab_data_yaml pipeline
"""
import base64
import json
from unittest.mock import patch

import pytest
import yaml
from extract.gitlab_data_yaml.upload import (
    decode_file,
    get_base_url,
    get_json_file_name,
    get_private_token,
    manifest_reader,
    run_subprocess,
    save_to_file,
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

    return manifest_reader(file_path="extract/gitlab_data_yaml/file_specification.yml")


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


@pytest.mark.parametrize(
    "attribute, expected_value",
    [
        (
            {
                "content": base64.b64encode(
                    yaml.dump({"key": "value"}).encode()
                ).decode()
            },
            {"key": "value"},
        ),
        ({"content": ""}, None),
        (None, None),
    ],
)
def test_decode(attribute, expected_value):
    """
    Test decode_file function
    """

    result = decode_file(attribute)
    assert result == expected_value


def test_save_to_file(tmp_path):
    """
    Test the save_to_file function.

    This test ensures that:
    1. The function creates a file with the correct name
    2. The file contains the expected JSON content
    """
    file = "test_file"
    request = {"key": "value"}
    expected_file = tmp_path / f"{file}.json"

    save_to_file(str(tmp_path / file), request)

    assert expected_file.exists()
    with open(file=expected_file, mode="r", encoding="utf8") as f:
        content = json.load(f)
    assert content == request


def test_decode_file():
    """
    Test the decode_file function.

    This test ensures that:
    1. The function correctly decodes base64 encoded content
    2. The decoded content is properly parsed as YAML
    """
    yaml_content = "key: value\nlist:\n  - item1\n  - item2"
    encoded_content = base64.b64encode(yaml_content.encode()).decode()
    response = {"content": encoded_content}

    result = decode_file(response)

    expected_result = {"key": "value", "list": ["item1", "item2"]}
    assert result == expected_result
