import pytest
import json
import yaml
from unittest.mock import patch, MagicMock
from extract.gitlab_data_yaml.upload import (
    upload_to_snowflake,
    request_download_decode_upload,
    get_json_file_name,
    run_subprocess,
    curl_and_upload
)

# Mock for snowflake_stage_load_copy_remove function
@pytest.fixture
def mock_snowflake_stage_load():
    with patch('upload.snowflake_stage_load_copy_remove') as mock:
        yield mock

# Test upload_to_snowflake function
def test_upload_to_snowflake(mock_snowflake_stage_load):
    upload_to_snowflake("test_file.json", "test_table")
    mock_snowflake_stage_load.assert_called_once_with(
        file="test_file.json",
        stage="gitlab_data_yaml.gitlab_data_yaml_load",
        table_path="gitlab_data_yaml.test_table",
        engine=pytest.approx(None)  # Since snowflake_engine is not accessible in this context
    )

# Test get_json_file_name function
@pytest.mark.parametrize("input_file,expected", [
    ("", "ymltemp"),
    ("test.yml", "test"),
    ("test", "test")
])
def test_get_json_file_name(input_file, expected):
    assert get_json_file_name(input_file) == expected

# Test run_subprocess function
@patch('upload.subprocess.run')
def test_run_subprocess_success(mock_run):
    mock_run.return_value.check_returncode.return_value = None
    run_subprocess("echo 'test'", "test_file")
    mock_run.assert_called_once_with("echo 'test'", shell=True, check=True)

@patch('upload.subprocess.run')
def test_run_subprocess_failure(mock_run):
    mock_run.side_effect = IOError("Command failed")
    with pytest.raises(IOError):
        run_subprocess("invalid_command", "test_file")

# Test request_download_decode_upload function
@patch('upload.requests.request')
@patch('upload.upload_to_snowflake')
def test_request_download_decode_upload(mock_upload, mock_request):
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "content": "dGVzdDogdmFsdWU="  # Base64 encoded "test: value"
    }
    mock_request.return_value = mock_response

    request_download_decode_upload(
        "test_table", "test_file", "http://test.com/", "test_token", ".yml"
    )

    mock_request.assert_called_once_with(
        "GET",
        "http://test.com/test_file.yml",
        headers={"Private-Token": "test_token"},
        timeout=10
    )
    mock_upload.assert_called_once_with(file_for_upload="test_file.json", table="test_table")

# Test curl_and_upload function
@patch('upload.run_subprocess')
@patch('upload.upload_to_snowflake')
def test_curl_and_upload(mock_upload, mock_run_subprocess):
    curl_and_upload("test_table", "test_file", "http://test.com/", "test_token")

    expected_command = "curl --header \"PRIVATE-TOKEN: test_token\" 'http://test.com/test_file%2Eyml/raw?ref=main' | yaml2json -o test_file.json"
    mock_run_subprocess.assert_called_once_with(command=expected_command, file="test_file")
    mock_upload.assert_called_once_with(file_for_upload="test_file.json", table="test_table")