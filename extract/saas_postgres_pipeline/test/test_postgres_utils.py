import os
import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
from datetime import datetime

from postgres_utils import (
    BACKFILL_METADATA_TABLE,
    INCREMENTAL_METADATA_TABLE,
    get_prefix_template,
    get_initial_load_prefix,
    get_upload_file_name,
    seed_and_upload_snowflake,
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

    @patch("postgres_utils.read_sql_tmpfile")
    def test_chunk_and_upload_metadata(self, mock_read_sql_tmpfile):
        iter_csv = pd.read_csv(f"{os.path.dirname(os.path.realpath(__file__))}/test_iter_csv.csv", chunksize=5)
        mock_read_sql_tmpfile.return_value = iter_csv
