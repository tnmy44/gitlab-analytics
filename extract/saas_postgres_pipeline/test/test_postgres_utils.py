from datetime import datetime

from postgres_utils import (
    BACKFILL_METADATA_TABLE,
    INCREMENTAL_METADATA_TABLE,
    get_prefix_template,
    get_initial_load_prefix,
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
        assert all([char.islower() for char in actual_initial_load_prefix])
    def test_get_upload_file_name():
        #TODO

