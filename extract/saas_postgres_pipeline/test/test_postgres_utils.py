import os
import pytest
from unittest.mock import MagicMock, patch
from sqlalchemy.engine.base import Engine
import pandas as pd
from datetime import datetime

from postgres_utils import (
    BACKFILL_METADATA_TABLE,
    INCREMENTAL_METADATA_TABLE,
    get_prefix_template,
    get_initial_load_prefix,
    get_upload_file_name,
    seed_and_upload_snowflake,
    INCREMENTAL_LOAD_TYPE_BY_ID,
    chunk_and_upload_metadata,
    check_and_handle_schema_removal,
    get_min_or_max_id,
)


class TestPostgresUtils:
    def setup(self):
        pass

    def test_get_prefix_template(self):
        """
        Check that the prefix matches expected prefix
        """
        staging_or_processed = "staging"
        load_by_id_export_type = "backfill"
        table = "alerts"
        initial_load_prefix = datetime(2023, 1, 1).strftime("%Y-%m-%d")

        actual = get_prefix_template().format(
            staging_or_processed=staging_or_processed,
            load_by_id_export_type=load_by_id_export_type,
            table=table,
            initial_load_prefix=initial_load_prefix,
        )
        expected = f"{staging_or_processed}/{load_by_id_export_type}/{table}/{initial_load_prefix}"
        assert actual == expected

    def test_get_initial_load_prefix(self):
        """
        Check that actual value matches expected value
        and that the prefix is all-lowercase
        """
        initial_load_start_date = datetime.now()
        expected_initial_load_prefix = f"initial_load_start_{initial_load_start_date.isoformat(timespec='milliseconds')}".lower()
        actual_initial_load_prefix = get_initial_load_prefix(initial_load_start_date)
        assert expected_initial_load_prefix == actual_initial_load_prefix
        assert all([not char.isupper() for char in actual_initial_load_prefix])

    def test_get_upload_file_name(self):
        load_by_id_export_type = "backfill"
        table = "alerts"
        initial_load_start_date = datetime.now()
        upload_date = datetime.now()

        actual = get_upload_file_name(
            load_by_id_export_type, table, initial_load_start_date, upload_date
        )

        initial_load_start_date_iso = initial_load_start_date.isoformat(
            timespec="milliseconds"
        )
        upload_date_iso = upload_date.isoformat(timespec="milliseconds")
        expected = f"staging/{load_by_id_export_type}/{table}/initial_load_start_{initial_load_start_date_iso}/{upload_date_iso}_{table}.parquet.gzip".lower()

        upload_date_iso = upload_date.isoformat(timespec="milliseconds")
        assert actual == expected

    def test_seed_and_upload_snowflake(self):
        """Test that non-temp tables are aborted"""
        database_kwargs = {"target_table": "alerts"}
        with pytest.raises(ValueError):
            seed_and_upload_snowflake(None, None, database_kwargs, None, None, None)

    @patch("postgres_utils.write_metadata")
    @patch("postgres_utils.upload_gcs_to_snowflake")
    @patch("postgres_utils.snowflake_engine_factory")
    @patch("postgres_utils.upload_to_gcs")
    @patch("postgres_utils.read_sql_tmpfile")
    def test_chunk_and_upload_metadata1(
        self,
        mock_read_sql_tmpfile,
        mock_upload_to_gcs,
        mock_snowflake_engine_factory,
        mock_upload_gcs_to_snowflake,
        mock_write_metadata,
    ):
        """
        Test that when we have not reached the max_source_id,
        that the files aren't uploaded to Snowflake
        """
        iter_csv = pd.read_csv(
            f"{os.path.dirname(os.path.realpath(__file__))}/test_iter_csv.csv",
            chunksize=5,
        )
        mock_read_sql_tmpfile.return_value = iter_csv
        some_engine = MagicMock(spec=Engine)
        mock_snowflake_engine_factory.return_value = some_engine

        query = "select 1;"
        primary_key = "ID"
        max_source_id = 25
        initial_load_start_date = datetime.now()
        database_kwargs = {
            "source_database": "some_db",
            "source_table": "alerts",
            "source_engine": some_engine,
            "metadata_engine": some_engine,
            "metadata_table": INCREMENTAL_METADATA_TABLE,
        }
        load_by_id_export_type = INCREMENTAL_LOAD_TYPE_BY_ID

        returned_initial_load_start_date = chunk_and_upload_metadata(
            query,
            primary_key,
            max_source_id,
            initial_load_start_date,
            database_kwargs,
            load_by_id_export_type,
        )

        mock_upload_gcs_to_snowflake.assert_not_called()
        assert returned_initial_load_start_date == initial_load_start_date

    @patch("postgres_utils.write_metadata")
    @patch("postgres_utils.upload_gcs_to_snowflake")
    @patch("postgres_utils.snowflake_engine_factory")
    @patch("postgres_utils.upload_to_gcs")
    @patch("postgres_utils.read_sql_tmpfile")
    def test_chunk_and_upload_metadata2(
        self,
        mock_read_sql_tmpfile,
        mock_upload_to_gcs,
        mock_snowflake_engine_factory,
        mock_upload_gcs_to_snowflake,
        mock_write_metadata,
    ):
        """
        Test INCREMENTAL_LOAD_TYPE_BY_ID

        Test that when we have reached the max_source_id,
        that the files ARE uploaded to Snowflake

        Should reach max_source_id after 2 loops and then terminate.
        Therefore, test that upload_to_gcs() and write_metadata()
        are called twice.

        Test that upload_gcs_to_snowflake is called only once

        """
        iter_csv = pd.read_csv(
            f"{os.path.dirname(os.path.realpath(__file__))}/test_iter_csv.csv",
            chunksize=5,
        )
        mock_read_sql_tmpfile.return_value = iter_csv
        some_engine = MagicMock(spec=Engine)
        mock_snowflake_engine_factory.return_value = some_engine

        query = "select 1;"
        primary_key = "ID"
        max_source_id = 10
        initial_load_start_date = datetime.now()
        database_kwargs = {
            "source_database": "some_db",
            "source_table": "alerts",
            "source_engine": some_engine,
            "metadata_engine": some_engine,
            "metadata_table": INCREMENTAL_METADATA_TABLE,
        }
        load_by_id_export_type = INCREMENTAL_LOAD_TYPE_BY_ID

        returned_initial_load_start_date = chunk_and_upload_metadata(
            query,
            primary_key,
            max_source_id,
            initial_load_start_date,
            database_kwargs,
            load_by_id_export_type,
        )

        assert mock_upload_to_gcs.call_count == 2
        assert mock_write_metadata.call_count == 2
        mock_upload_gcs_to_snowflake.assert_called_once_with(
            some_engine,
            database_kwargs,
            load_by_id_export_type,
            initial_load_start_date,
        )

        assert returned_initial_load_start_date == initial_load_start_date

    @patch("postgres_utils.write_metadata")
    @patch("postgres_utils.seed_and_upload_snowflake")
    @patch("postgres_utils.snowflake_engine_factory")
    @patch("postgres_utils.upload_to_gcs")
    @patch("postgres_utils.read_sql_tmpfile")
    def test_chunk_and_upload_metadata3(
        self,
        mock_read_sql_tmpfile,
        mock_upload_to_gcs,
        mock_snowflake_engine_factory,
        mock_seed_and_upload_snowflake,
        mock_write_metadata,
    ):
        """
        Test LOAD_TYPE_BY_ID=BACKFILL

        Similiar to abovve test2, but make sure we call `seed_and_upload_snowflake`
        instead of `upload_gcs_to_snowflake`

        """
        iter_csv = pd.read_csv(
            f"{os.path.dirname(os.path.realpath(__file__))}/test_iter_csv.csv",
            chunksize=5,
        )
        mock_read_sql_tmpfile.return_value = iter_csv
        some_engine = MagicMock(spec=Engine)
        mock_snowflake_engine_factory.return_value = some_engine

        query = "select 1;"
        primary_key = "ID"
        max_source_id = 10
        initial_load_start_date = datetime.now()
        database_kwargs = {
            "source_database": "some_db",
            "source_table": "alerts",
            "source_engine": some_engine,
            "metadata_engine": some_engine,
            "metadata_table": INCREMENTAL_METADATA_TABLE,
        }
        load_by_id_export_type = "backfill"

        returned_initial_load_start_date = chunk_and_upload_metadata(
            query,
            primary_key,
            max_source_id,
            initial_load_start_date,
            database_kwargs,
            load_by_id_export_type,
        )

        assert mock_upload_to_gcs.call_count == 2
        assert mock_write_metadata.call_count == 2
        mock_seed_and_upload_snowflake.assert_called_once()
        assert returned_initial_load_start_date == initial_load_start_date

    @patch("postgres_utils.drop_column_on_schema_removal")
    @patch("postgres_utils.get_source_and_target_columns")
    def test_check_and_handle_schema_removal_on_no_change(
        self, mock_get_source_and_target_columns, mock_drop_column_on_schema_removal
    ):
        raw_query = "select 1;"
        some_engine = MagicMock(spec=Engine)
        source_engine = some_engine
        target_engine = some_engine
        target_table = "alerts"
        source_columns = ["hello", "world"]
        target_columns = ["hello", "world"]

        mock_get_source_and_target_columns.return_value = source_columns, target_columns

        check_and_handle_schema_removal(
            raw_query, source_engine, target_engine, target_table
        )
        mock_drop_column_on_schema_removal.assert_not_called()

    @patch("postgres_utils.drop_column_on_schema_removal")
    @patch("postgres_utils.get_source_and_target_columns")
    def test_check_and_handle_schema_removal_on_schema_addition(
        self, mock_get_source_and_target_columns, mock_drop_column_on_schema_removal
    ):
        raw_query = "select 1;"
        some_engine = MagicMock(spec=Engine)
        source_engine = some_engine
        target_engine = some_engine
        target_table = "alerts"
        source_columns = ["hello", "world", "manifest_addition"]
        target_columns = ["hello", "world"]

        mock_get_source_and_target_columns.return_value = source_columns, target_columns

        check_and_handle_schema_removal(
            raw_query, source_engine, target_engine, target_table
        )
        mock_drop_column_on_schema_removal.assert_not_called()

    @patch("postgres_utils.drop_column_on_schema_removal")
    @patch("postgres_utils.get_source_and_target_columns")
    def test_check_and_handle_schema_removal_on_schema_removal(
        self, mock_get_source_and_target_columns, mock_drop_column_on_schema_removal
    ):
        raw_query = "select 1;"
        some_engine = MagicMock(spec=Engine)
        source_engine = some_engine
        target_engine = some_engine
        target_table = "alerts"
        source_columns = ["hello"]
        target_columns = ["hello", "world"]

        mock_get_source_and_target_columns.return_value = source_columns, target_columns

        check_and_handle_schema_removal(
            raw_query, source_engine, target_engine, target_table
        )
        mock_drop_column_on_schema_removal.assert_called_once()

    @patch("postgres_utils.query_results_generator")
    def test_get_min_or_max_id(self, mock_query_results_generator):
        """
        This is a tough function to test because the main logic involves running the min()/max()
        against the sql engine
        """
        primary_key = "ID"
        engine = MagicMock(spec=Engine)
        table = "alerts"

        min_or_max = "min"
        dataframe = pd.DataFrame({"ID": [1]})
        mock_query_results_generator.return_value = dataframe
        min_id = get_min_or_max_id(primary_key, engine, table, min_or_max)
        assert min_id == 1

        min_or_max = "max"
        dataframe = pd.DataFrame({"ID": [20]})
        mock_query_results_generator.return_value = dataframe
        max_id = get_min_or_max_id(primary_key, engine, table, min_or_max)
        assert max_id == 20
