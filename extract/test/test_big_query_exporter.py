import pytest
from unittest.mock import mock_open, patch
from extract.bigquery.src.bigquery_export import (
    get_export,
    set_bucket_path,
    get_partition,
    get_billing_data_query,
)


@pytest.fixture
def mock_file():
    data = 'stream: {project: test_project, credentials: test_creds, exports: [{name: test_export, export_query: "SELECT * FROM table"}]}'
    return mock_open(read_data=data)


def test_get_export(mock_file, monkeypatch):

    with patch("builtins.open", mock_file):
        project, creds, export = get_export("test_export", "config.yaml")
        assert project == "test_project"
        assert creds == "test_creds"
        assert export == {"name": "test_export", "export_query": "SELECT * FROM table"}

    with patch("builtins.open", mock_file):
        project, creds, export = get_export(None, "config.yaml")
        assert export == {"name": "test_export", "export_query": "SELECT * FROM table"}


def test_set_bucket_path():

    export = {"bucket_path": "gs://test/path"}
    set_bucket_path("master", export)
    assert export["bucket_path"] == "gs://test/path"

    export = {"bucket_path": "gs://test/path"}
    set_bucket_path("test_branch", export)
    assert export["bucket_path"] == "gs://test/BRANCH_TEST_test_branch/path"


@pytest.mark.parametrize(
    "partition_date_part, expected_partition",
    [(None, "2023-04-01"), ("d", "2023-04-01"), ("m", "2023-04")],
)
def test_get_partition(partition_date_part, expected_partition, monkeypatch):

    monkeypatch.setenv("EXPORT_DATE", "2023-04-01")
    export = {
        "bucket_path": "gs://test/path",
        "export_query": 'SELECT * FROM table WHERE date = "{EXPORT_DATE}"',
        "partition_date_part": "d",
    }
    if partition_date_part:
        export["partition_date_part"] = partition_date_part
    partition = get_partition(export)
    assert partition == expected_partition


@pytest.mark.parametrize(
    "branch, expected_path",
    [
        ("master", "gs://test/path/2023-04-01/*.parquet"),
        ("test_branch", "gs://test/BRANCH_TEST_test_branch/path/2023-04-01/*.parquet"),
    ],
)
def test_get_billing_data_query(branch, expected_path, monkeypatch):
    monkeypatch.setenv("EXPORT_DATE", "2023-04-01")
    monkeypatch.setenv("GIT_BRANCH", branch)
    export = {
        "bucket_path": "gs://test/path",
        "export_query": 'SELECT * FROM table WHERE date = "{EXPORT_DATE}"',
        "partition_date_part": "d",
    }
    query = get_billing_data_query(export)
    expected = f"EXPORT DATA OPTIONS(  uri='{expected_path}',  format='PARQUET',  overwrite=true  ) ASSELECT * FROM table WHERE date = '2023-04-01'"
    assert query == expected
