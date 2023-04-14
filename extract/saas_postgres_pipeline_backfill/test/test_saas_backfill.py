"""
Unit testing various 'pgp backfill' methods
"""
import os
import re
import sys
from unittest.mock import Mock, MagicMock, patch

abs_path = os.path.dirname(os.path.realpath(__file__))
abs_path = (
    abs_path[: abs_path.find("extract")]
    + "extract/saas_postgres_pipeline_backfill/postgres_pipeline/"
)
sys.path.append(abs_path)

from postgres_utils import (
    has_new_columns,
    get_latest_parquet_file,
    update_import_query_for_delete_export,
    schema_addition_check,
)


def test_has_new_columns():
    """Test that new col is source is ascertained correctly"""
    # same columns between source/gcs, no new source cols
    source_columns = ["a", "b"]
    gcs_columns = ["a", "b"]
    res = has_new_columns(source_columns, gcs_columns)
    assert res is False

    # gcs has more cols, no new source cols
    source_columns = ["a", "b"]
    gcs_columns = ["a", "b", "c"]
    res = has_new_columns(source_columns, gcs_columns)
    assert res is False

    # source has more cols, yes new source cols
    source_columns = ["a", "b", "c"]
    gcs_columns = ["a", "b"]
    res = has_new_columns(source_columns, gcs_columns)
    assert res


@patch("postgres_utils.get_gcs_columns")
@patch("postgres_utils.get_source_columns")
def test_schema_addition_check(mock_get_source_columns, mock_get_gcs_columns):
    """
    Test that the program correctly handles the following:
        - column additions
        - column deletions
        - no changes
    """
    raw_query = MagicMock()
    source_engine = MagicMock()
    source_table = MagicMock()

    # same columns between source/gcs, no new source cols
    mock_get_source_columns.return_value = ["a", "b"]
    mock_get_gcs_columns.return_value = ["a", "b"]
    result = schema_addition_check(raw_query, source_engine, source_table)
    expected_result = False
    assert result == expected_result

    # gcs has more cols, no new source cols
    mock_get_source_columns.return_value = ["a", "b"]
    mock_get_gcs_columns.return_value = ["a", "b", "c"]
    result = schema_addition_check(raw_query, source_engine, source_table)
    expected_result = False
    assert result == expected_result

    # source has more cols
    mock_get_source_columns.return_value = ["a", "b", "c"]
    mock_get_gcs_columns.return_value = ["a", "b"]
    result = schema_addition_check(raw_query, source_engine, source_table)
    expected_result = True
    assert result == expected_result


@patch("postgres_utils.get_gcs_bucket")
def test_get_latest_parquet_file(get_gcs_bucket_mock):
    """Test that the latest parquet file is correctly chosen"""
    # Create a mock bucket object
    bucket = MagicMock()
    # get_gcs_bucket = MagicMock()
    get_gcs_bucket_mock.return_value = bucket
    blob1 = MagicMock()
    blob1.name = "source_table/initial_load_start_2021-01-01.parquet.gzip"
    blob2 = MagicMock()
    blob2.name = "source_table/initial_load_start_2021-01-02.parquet.gzip"
    bucket.list_blobs.return_value = [blob1, blob2]

    # Call the function with the mock bucket object
    latest_parquet_file = get_latest_parquet_file("source_table")

    # Assert that the correct file name is returned
    assert (
        latest_parquet_file == "source_table/initial_load_start_2021-01-02.parquet.gzip"
    )

    # Test when the bucket doesn't have any files
    bucket.list_blobs.return_value = []

    # Call the function with the mock bucket object
    latest_parquet_file = get_latest_parquet_file("source_table")

    # Assert that the correct file name is returned
    assert latest_parquet_file is None


def test_update_import_query_for_delete_export():
    """
    For deletes, test that the import query is updated to reflect
    only the pk within the select
    """

    def clean_res(res):
        res = res.replace("\n", " ")
        res = re.sub(" +", " ", res)
        return res

    # Query 1
    import_query = """SELECT id
    , created_at
    , updated_at
    , relative_position
    , start_event_identifier
    , end_event_identifier
    , group_id
    , start_event_label_id
    , end_event_label_id
    , hidden
    , custom
    , name
    , group_value_stream_id
    FROM analytics_cycle_analytics_group_stages
    WHERE updated_at BETWEEN '{BEGIN_TIMESTAMP}'::timestamp
      AND '{END_TIMESTAMP}'::timestamp"""
    primary_key = "id"
    composite_key = None

    # Call the function with the test data
    updated_query = update_import_query_for_delete_export(
        import_query, primary_key, composite_key
    )

    # Assert that the function returns the expected result
    expected_query = "SELECT id FROM  analytics_cycle_analytics_group_stages\nWHERE updated_at BETWEEN '{BEGIN_TIMESTAMP}'::timestamp\n  AND '{END_TIMESTAMP}'::timestamp"

    expected_query_cleaned = clean_res(expected_query)
    updated_query_cleaned = clean_res(updated_query)
    assert expected_query_cleaned == updated_query_cleaned

    # Query 2
    import_query = """SELECT id
    , CAST(sha AS VARCHAR) AS sha
    , issue_id
    , created_at
    , author_id
    FROM design_management_versions"""

    primary_key = "id"
    composite_key = "sha, issue_id"

    # Call the function with the test data
    updated_query = update_import_query_for_delete_export(
        import_query, primary_key, composite_key
    )

    # Assert that the function returns the expected result
    expected_query = "SELECT sha, issue_id FROM  design_management_versions"

    expected_query_cleaned = clean_res(expected_query)
    updated_query_cleaned = clean_res(updated_query)
    assert expected_query_cleaned == updated_query_cleaned
