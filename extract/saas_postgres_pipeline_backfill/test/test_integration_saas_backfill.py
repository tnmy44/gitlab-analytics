import os
import pytest
import re
import sys
from unittest.mock import Mock, MagicMock, patch

abs_path = os.path.dirname(os.path.realpath(__file__))
abs_path = (
    abs_path[: abs_path.find("extract")]
    + "extract/saas_postgres_pipeline_backfill/postgres_pipeline/"
)
sys.path.append(abs_path)

from utils import (
    has_new_columns,
    get_latest_parquet_file,
    update_import_query_for_delete_export,
    # get_engines,
    postgres_engine_factory,
    manifest_reader
)

class TestBackfillIntegration():
    def setup(self):
        manifest_file_path = 'extract/saas_postgres_pipeline_backfill/manifests_decomposed/el_gitlab_com_new_db_manifest.yaml'

        manifest_dict = manifest_reader(manifest_file_path)
        env = os.environ.copy()
        metadata_engine = postgres_engine_factory(
            manifest_dict["connection_info"]['postgres_metadata_connection'], env
        )
        metadata_schema = 'saas_db_metadata'
        metadata_table = 'backfill_metadata'
        test_table_full_path = f'{metadata_schema}.test_{metadata_table}'

        delete_query = f""" drop table if exists {test_table_full_path}"""

        create_table_query = f"""
        create table {test_table_full_path}
        (like {metadata_schema}.{metadata_table});
        """

        # TODO: remove once Ved has approved other MR
        alter_query = f"""alter table {test_table_full_path} rename column is_backfill_completed to is_export_completed;"""

        with metadata_engine.connect() as connection:
            connection.execute(delete_query)
            connection.execute(create_table_query)
            connection.execute(alter_query)


    def test_if_new_table_backfill(self):
        # Arrange
        # Code to create a new table and populate it with data for testing.

        # Act
        # Code to backfill the new table.

        # Assert
        # Code to verify that the backfill was successful.
        pass

    '''
    def test_if_new_column_in_source_backfill(self):
        # Arrange
        # Code to add a new column to the source and populate it with data for testing.

        # Act
        # Code to backfill the table.

        # Assert
        # Code to verify that the backfill was successful.

    def test_if_removed_column_in_source_dont_backfill(self):
        # Arrange
        # Code to remove a column from the source and populate it with data for testing.

        # Act
        # Code to attempt to backfill the table.

        # Assert
        # Code to verify that the backfill was not attempted.

    def test_if_in_middle_of_backfill_less_than_24hr_since_last_write(self):
        # Arrange
        # Code to simulate being in the middle of a backfill with less than 24 hours since the last write.

        # Act
        # Code to resume the backfill.

        # Assert
        # Code to verify that the backfill was successful and resumed from the correct point.

    def test_if_in_middle_of_backfill_more_than_24hr_since_last_write(self):
        # Arrange
        # Code to simulate being in the middle of a backfill with more than 24 hours since the last write.

        # Act
        # Code to restart the backfill.

        # Assert
        # Code to verify that the backfill was successful and started over from the beginning.

    def test_dont_backfill_if_conditions_not_met(self):
        # Arrange
        # Code to simulate a scenario where the backfill conditions are not met.

        # Act
        # Code to attempt to backfill the table.

        # Assert
        # Code to verify that the backfill was not attempted.

    def test_row_counts_match_for_ci_triggers_table(self):
        # Arrange
        # Code to prepare test data and environment.

        # Act
        # Code to backfill the table and count the rows in the resulting Parquet file.

        # Assert
        # Code to verify that the row counts in the Parquet file and source match.
        '''

if __name__ == '__main__':
    unittest.main()
