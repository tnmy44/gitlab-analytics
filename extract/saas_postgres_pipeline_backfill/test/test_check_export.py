"""
Performs integration testing on the backfill metadata database, using
a test table.

Mostly tests the 'check_is_backfill_needed()' function as it has many conditions

Note that all GCS / Gitlab DB components still need to be mocked

1. If new table, backfill
    - implemented in `test_if_new_table_backfill`
1. Delete from metadata table, should be treated as 'new table', files from prev test should be deleted since they aren't 'processed'. 3 Conditions
    1. `remove_unprocessed_files` is called when new table
    1. `remove_unprocessed_files` is called when new schema
    1. `remove_unprocessed_files` is NOT called when 'in mid backfill'
3. If *new* column in source, backfill
    -  tested within test_saas_backfill.py: `test_schema_addition_check()`
4. If *removed* column in source, DONT backfill
    -  also tested within test_saas_backfill.py: `test_schema_addition_check()`
5. `resume_export()`
    1. Less than 24 HR since the last write: start where we left off
    2. More than 24 HR: start backfill over
    2. Don't run if not in middle of backfill
6. Don't backfill if above conditions aren't met

"""
import os
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from postgres_pipeline_table import PostgresPipelineTable
from postgres_utils import (
    METADATA_SCHEMA,
    is_new_table,
    is_resume_export,
    manifest_reader,
    postgres_engine_factory,
)
from sqlalchemy.engine.base import Engine


class TestCheckBackfill:
    """
    The main class for testing
    """
    def setup(self):
        """
        - Create test metdata table
        - Create a mock PostgresPipelineTable object
        """
        manifest_file_path = "extract/saas_postgres_pipeline_backfill/manifests_decomposed/el_gitlab_com_new_db_manifest.yaml"

        manifest_dict = manifest_reader(manifest_file_path)
        env = os.environ.copy()
        self.metadata_engine = postgres_engine_factory(
            manifest_dict["connection_info"]["postgres_metadata_connection"], env
        )
        metadata_table = "backfill_metadata"
        self.test_metadata_table = f"test_{metadata_table}"
        self.test_metadata_table_full_path = (
            f"{METADATA_SCHEMA}.{self.test_metadata_table}"
        )

        drop_query = f""" drop table if exists {self.test_metadata_table_full_path}"""

        create_table_query = f"""
        create table {self.test_metadata_table_full_path}
        (like {METADATA_SCHEMA}.{metadata_table});
        """

        with self.metadata_engine.connect() as connection:
            connection.execute(drop_query)
            connection.execute(create_table_query)

        # Create a mock PostgresPipelineTable object
        table_config = {
            "import_query": "SELECT * FROM some_table;",
            "import_db": "some_database",
            "export_table": "some_table",
            "export_table_primary_key": "id",
        }
        self.pipeline_table = PostgresPipelineTable(table_config)

    def teardown(self):
        """
        Teardown function
        """
        drop_query = f""" drop table if exists {self.test_metadata_table_full_path}"""

        with self.metadata_engine.connect() as connection:
            connection.execute(drop_query)

    def test_is_new_table(self):
        """
        When the metadata database is empty, ascertain that when
        backfilling 'some_table', it's considered a new table.

        After inserting the table into metadata, ascertain that
        it's no longer considered a new table
        """

        source_table = "some_table"
        # Test when table is missing in metadata
        result = is_new_table(
            self.metadata_engine, self.test_metadata_table, source_table
        )
        assert result is True

        # Insert test record
        database_name = "some_db"
        initial_load_start_date = datetime(2023, 1, 1)
        upload_date = datetime(2023, 1, 1)
        upload_file_name = "some_file"
        last_extracted_id = 10
        max_id = 20
        is_export_completed = True
        chunk_row_count = 3

        insert_query = f"""
        INSERT INTO {self.test_metadata_table_full_path}
        VALUES (
            '{database_name}',
            '{source_table}',
            '{initial_load_start_date}',
            '{upload_date}',
            '{upload_file_name}',
            {last_extracted_id},
            {max_id},
            {is_export_completed},
            {chunk_row_count});
        """

        # Test when table is inserted into metadata
        with self.metadata_engine.connect() as connection:
            connection.execute(insert_query)

        result = is_new_table(
            self.metadata_engine, self.test_metadata_table, source_table
        )
        assert result is False

    # @patch("postgres_pipeline_table.is_new_table")
    # @patch("postgres_pipeline_table.remove_unprocessed_files_from_gcs")
    # @patch("postgres_pipeline_table.is_resume_export")
    # def test_check_is_backfill_needed_new_table(
    #     self, mock_is_resume_export, mock_remove_unprocessed_files, mock_is_new_table
    # ):
    #     """
    #     Test that when is_new_table() is True, that
    #     remove_unprocessed_files_from_gcs() is called
    #     """
    #
    #     # Create a mock source_engine and metadata_engine objects
    #     source_engine = MagicMock(spec=Engine)
    #     metadata_engine = MagicMock(spec=Engine)
    #
    #     mock_is_resume_export.return_value = False, 1, None
    #     mock_is_new_table.return_value = True
    #     # Call the function being tested
    #     (
    #         is_backfill_needed,
    #         start_pk,
    #         initial_load_start_date,
    #     ) = self.pipeline_table.check_is_backfill_needed(
    #         source_engine, metadata_engine, self.test_metadata_table
    #     )
    #
    #     # Assert that remove_unprocessed_files_from_gcs was called with the correct arguments
    #     mock_remove_unprocessed_files.assert_called_once_with(
    #         self.test_metadata_table, self.pipeline_table.source_table_name
    #     )
    #     assert initial_load_start_date is None
    #     assert start_pk == 1
    #     assert is_backfill_needed is True

    # @patch("postgres_pipeline_table.schema_addition_check")
    # @patch("postgres_pipeline_table.is_new_table")
    # @patch("postgres_pipeline_table.remove_unprocessed_files_from_gcs")
    # @patch("postgres_pipeline_table.is_resume_export")
    # def test_check_is_backfill_needed_schema_change(
    #     self,
    #     mock_is_resume_export,
    #     mock_remove_unprocessed_files,
    #     mock_is_new_table,
    #     mock_schema_addition_check,
    # ):
    #     """
    #     Test that when there is a schema addition, that
    #     remove_unprocessed_files_from_gcs() is called
    #     """
    #
    #     # Create a mock source_engine and metadata_engine objects
    #     source_engine = MagicMock(spec=Engine)
    #     metadata_engine = MagicMock(spec=Engine)
    #
    #     mock_is_resume_export.return_value = False, 1, None
    #
    #     mock_is_new_table.return_value = False
    #     mock_schema_addition_check.return_value = True
    #
    #     # Call the function being tested
    #     (
    #         is_backfill_needed,
    #         start_pk,
    #         initial_load_start_date,
    #     ) = self.pipeline_table.check_is_backfill_needed(
    #         source_engine, metadata_engine, self.test_metadata_table
    #     )
    #
    #     # Assert that remove_unprocessed_files_from_gcs was called with the correct arguments
    #     mock_remove_unprocessed_files.assert_called_once_with(
    #         self.test_metadata_table, self.pipeline_table.source_table_name
    #     )
    #     assert initial_load_start_date is None
    #     assert start_pk == 1
    #     assert is_backfill_needed is True

    @patch("postgres_pipeline_table.schema_addition_check")
    @patch("postgres_pipeline_table.is_new_table")
    @patch("postgres_pipeline_table.remove_unprocessed_files_from_gcs")
    def test_is_resume_export_completed(
        self,
        mock_remove_unprocessed_files,
        mock_is_new_table,
        mock_schema_addition_check,
    ):
        """
        Check that no backfill is needed when
        the latest is_export_completed is True

        Assumes that `test_if_new_table_backfill()` was already run
        """

        # Create a mock source_engine and metadata_engine objects
        source_engine = MagicMock(spec=Engine)

        mock_is_new_table.return_value = False
        mock_schema_addition_check.return_value = False

        # Call the function being tested
        (
            is_backfill_needed,
            start_pk,
            initial_load_start_date,
        ) = self.pipeline_table.check_is_backfill_needed(
            source_engine, self.metadata_engine, self.test_metadata_table
        )

        # Verify results
        assert is_backfill_needed is False
        assert start_pk == 1
        assert initial_load_start_date is None
        mock_remove_unprocessed_files.assert_not_called()

    # @patch("postgres_pipeline_table.schema_addition_check")
    # @patch("postgres_pipeline_table.is_new_table")
    # @patch("postgres_pipeline_table.remove_unprocessed_files_from_gcs")
    # def test_is_resume_export_past_24hr(
    #     self,
    #     mock_remove_unprocessed_files,
    #     mock_is_new_table,
    #     mock_schema_addition_check,
    # ):
    #     """
    #     Insert a more recent record where is_export_completed = False
    #     But more than 24 hours has elapsed
    #
    #     Should backfill, but need to start from beginning
    #     """
    #
    #     # Arrange metadata table
    #     source_table = "some_table"
    #     database_name = "some_db"
    #     initial_load_start_date = datetime(2023, 1, 2)
    #     upload_date = datetime(2023, 1, 2)
    #     upload_file_name = "some_file"
    #     last_extracted_id = 10
    #     max_id = 20
    #     is_export_completed = False
    #     chunk_row_count = 3
    #
    #     insert_query = f"""
    #     INSERT INTO {self.test_metadata_table_full_path}
    #     VALUES (
    #         '{database_name}',
    #         '{source_table}',
    #         '{initial_load_start_date}',
    #         '{upload_date}',
    #         '{upload_file_name}',
    #         {last_extracted_id},
    #         {max_id},
    #         {is_export_completed},
    #         {chunk_row_count});
    #     """
    #     # Test when table is inserted into metadata
    #     with self.metadata_engine.connect() as connection:
    #         connection.execute(insert_query)
    #
    #     # Create a mock source_engine and metadata_engine objects
    #     source_engine = MagicMock(spec=Engine)
    #
    #     mock_is_new_table.return_value = False
    #     mock_schema_addition_check.return_value = False
    #
    #     # Call the function being tested
    #     (
    #         is_backfill_needed,
    #         start_pk,
    #         initial_load_start_date,
    #     ) = self.pipeline_table.check_is_backfill_needed(
    #         source_engine, self.metadata_engine, self.test_metadata_table
    #     )
    #
    #     # Verify results
    #     assert is_backfill_needed is True
    #     assert start_pk == 1
    #     assert initial_load_start_date is None
    #     mock_remove_unprocessed_files.assert_called_once_with(
    #         self.test_metadata_table, self.pipeline_table.source_table_name
    #     )

    @patch("postgres_pipeline_table.schema_addition_check")
    @patch("postgres_pipeline_table.is_new_table")
    @patch("postgres_pipeline_table.remove_unprocessed_files_from_gcs")
    def test_is_resume_export_within_24hr(
        self,
        mock_remove_unprocessed_files,
        mock_is_new_table,
        mock_schema_addition_check,
    ):
        """
        Insert a more recent record where is_export_completed = False
        But more than 24 hours has elapsed


        Should backfill, but need to start from beginning
        """

        # Arrange metadata table
        source_table = "some_table"
        database_name = "some_db"
        initial_load_start_date = datetime(2023, 2, 1)
        upload_date_less_than_24hr = datetime.utcnow() - timedelta(hours=23, minutes=40)
        upload_date = upload_date_less_than_24hr
        upload_file_name = "some_file"
        last_extracted_id = 10
        max_id = 20
        is_export_completed = False
        chunk_row_count = 3

        insert_query = f"""
        INSERT INTO {self.test_metadata_table_full_path}
        VALUES (
            '{database_name}',
            '{source_table}',
            '{initial_load_start_date}',
            '{upload_date}',
            '{upload_file_name}',
            {last_extracted_id},
            {max_id},
            {is_export_completed},
            {chunk_row_count});
        """
        # Test when table is inserted into metadata
        with self.metadata_engine.connect() as connection:
            connection.execute(insert_query)

        # Create a mock source_engine and metadata_engine objects
        source_engine = MagicMock(spec=Engine)

        mock_is_new_table.return_value = False
        mock_schema_addition_check.return_value = False

        # Call the function being tested
        (
            is_backfill_needed,
            start_pk,
            returned_initial_load_start_date,
        ) = self.pipeline_table.check_is_backfill_needed(
            source_engine, self.metadata_engine, self.test_metadata_table
        )

        # Verify results
        assert is_backfill_needed is True
        assert start_pk == last_extracted_id + 1
        assert returned_initial_load_start_date == initial_load_start_date
        mock_remove_unprocessed_files.assert_not_called()

    @patch("postgres_pipeline_table.schema_addition_check")
    @patch("postgres_pipeline_table.remove_unprocessed_files_from_gcs")
    def test_no_backfill_needed(
        self,
        mock_remove_unprocessed_files,
        mock_schema_addition_check,
    ):
        """
        Test that no backfill is needed when the following conditions are true:
        - not a new table
        - No new schema addition
        - resume_export is False
        """
        # Update metdata table
        # Insert record indicating export_completed=True
        upload_date_less_than_24hr = datetime.utcnow() - timedelta(hours=23, minutes=40)

        source_table = "some_table"
        database_name = "some_db"
        initial_load_start_date = datetime(2023, 2, 1)
        upload_date = upload_date_less_than_24hr
        upload_file_name = "some_file"
        last_extracted_id = 10
        max_id = 20
        is_export_completed = True
        chunk_row_count = 3

        insert_query = f"""
        INSERT INTO {self.test_metadata_table_full_path}
        VALUES (
            '{database_name}',
            '{source_table}',
            '{initial_load_start_date}',
            '{upload_date}',
            '{upload_file_name}',
            {last_extracted_id},
            {max_id},
            {is_export_completed},
            {chunk_row_count});
        """
        with self.metadata_engine.connect() as connection:
            connection.execute(insert_query)

        # have to mock schema addition check because can't connect to Postgres DB
        mock_schema_addition_check.return_value = False

        # Check if backfill needed - main code
        source_engine = MagicMock(spec=Engine)
        is_backfill_needed, _, _ = self.pipeline_table.check_is_backfill_needed(
            source_engine, self.metadata_engine, self.test_metadata_table
        )

        # Verify results
        mock_remove_unprocessed_files.assert_not_called()
        assert is_backfill_needed is False


class TestCheckDelete:
    def setup(self):
        """
        - Create test metdata table
        - Create a mock PostgresPipelineTable object
        """
        manifest_file_path = "extract/saas_postgres_pipeline_backfill/manifests_decomposed/el_gitlab_com_new_db_manifest.yaml"

        manifest_dict = manifest_reader(manifest_file_path)
        env = os.environ.copy()
        self.metadata_engine = postgres_engine_factory(
            manifest_dict["connection_info"]["postgres_metadata_connection"], env
        )
        metadata_table = "delete_metadata"
        self.test_metadata_table = f"test_{metadata_table}"
        self.test_metadata_table_full_path = (
            f"{METADATA_SCHEMA}.{self.test_metadata_table}"
        )

        drop_query = f""" drop table if exists {self.test_metadata_table_full_path}"""

        create_table_query = f"""
        create table {self.test_metadata_table_full_path}
        (like {METADATA_SCHEMA}.{metadata_table});
        """

        with self.metadata_engine.connect() as connection:
            connection.execute(drop_query)
            connection.execute(create_table_query)

        # Create a mock PostgresPipelineTable object
        table_config = {
            "import_query": "SELECT * FROM some_table;",
            "import_db": "some_database",
            "export_table": "some_table",
            "export_table_primary_key": "id",
        }
        self.pipeline_table = PostgresPipelineTable(table_config)

    def teardown(self):
        """
        teardown function
        """
        drop_query = f""" drop table if exists {self.test_metadata_table_full_path}"""

        with self.metadata_engine.connect() as connection:
            connection.execute(drop_query)

    # @patch("postgres_pipeline_table.remove_unprocessed_files_from_gcs")
    # def test_check_delete_normal(self, mock_remove_unprocessed_files):
    #     """
    #     Check that regular deletes
    #     are processed correctly
    #
    #     Test the following conditions:
    #         1. start_pk = 1
    #         2. initial_load_start_date = None
    #         3. remove_unprocessed_files_from_gcs() is called
    #
    #     In addition, check that is_resume_export() returns correct values:
    #         1. Not resuming export
    #     """
    #     start_pk, initial_load_start_date = self.pipeline_table.check_delete(
    #         self.metadata_engine, self.test_metadata_table
    #     )
    #     # Tests
    #     mock_remove_unprocessed_files.assert_called_once_with(
    #         self.test_metadata_table, self.pipeline_table.source_table_name
    #     )
    #     assert start_pk == 1
    #     assert initial_load_start_date is None
    #
    #     # Test is_resume_export as well
    #     (
    #         is_resume_export_needed,
    #         resume_pk,
    #         resume_initial_load_start_date,
    #     ) = is_resume_export(
    #         self.metadata_engine,
    #         self.test_metadata_table,
    #         self.pipeline_table.source_table_name,
    #     )
    #     assert is_resume_export_needed is False
    #     assert resume_pk == 1
    #     assert resume_initial_load_start_date is None

    # @patch("postgres_pipeline_table.remove_unprocessed_files_from_gcs")
    # def test_check_delete_past_24hr(self, mock_remove_unprocessed_files):
    #     """
    #     Check that midway deletes past 24 hr
    #     are processed correctly
    #
    #     Test the following conditions:
    #         1. start_pk = 1
    #         2. initial_load_start_date = None
    #         3. remove_unprocessed_files_from_gcs() is called
    #
    #     In addition, check that is_resume_export() returns correct values:
    #         1. resuming export, but starting over since past 24hr
    #     """
    #
    #     # Insert test record
    #     source_table = "some_table"
    #     database_name = "some_db"
    #     initial_load_start_date = datetime(2023, 1, 1)
    #     upload_date = datetime(2023, 1, 1)
    #     upload_file_name = "some_file"
    #     last_extracted_id = 10
    #     max_id = 20
    #     is_export_completed = False
    #     chunk_row_count = 3
    #
    #     insert_query = f"""
    #     INSERT INTO {self.test_metadata_table_full_path}
    #     VALUES (
    #         '{database_name}',
    #         '{source_table}',
    #         '{initial_load_start_date}',
    #         '{upload_date}',
    #         '{upload_file_name}',
    #         {last_extracted_id},
    #         {max_id},
    #         {is_export_completed},
    #         {chunk_row_count});
    #     """
    #
    #     # Test when table is inserted into metadata
    #     with self.metadata_engine.connect() as connection:
    #         connection.execute(insert_query)
    #
    #     start_pk, initial_load_start_date = self.pipeline_table.check_delete(
    #         self.metadata_engine, self.test_metadata_table
    #     )
    #     # Tests
    #     mock_remove_unprocessed_files.assert_called_once_with(
    #         self.test_metadata_table, self.pipeline_table.source_table_name
    #     )
    #     assert start_pk == 1
    #     assert initial_load_start_date is None
    #
    #     # Test is_resume_export as well
    #     (
    #         is_resume_export_needed,
    #         resume_pk,
    #         resume_initial_load_start_date,
    #     ) = is_resume_export(
    #         self.metadata_engine,
    #         self.test_metadata_table,
    #         self.pipeline_table.source_table_name,
    #     )
    #     assert is_resume_export_needed is True
    #     assert resume_pk == 1
    #     assert resume_initial_load_start_date is None

    @patch("postgres_pipeline_table.remove_unprocessed_files_from_gcs")
    def test_check_delete_within_24hr(self, mock_remove_unprocessed_files):
        """
        Check that midway deletes within 24 hr
        are processed correctly

        Test the following conditions:
            1. start_pk = 1
            2. initial_load_start_date = None
            3. remove_unprocessed_files_from_gcs() is called

        In addition, check that is_resume_export() returns correct values:
            1. resuming export from last pk / initial_load_start_date
        """

        # Insert test record
        source_table = "some_table"
        database_name = "some_db"
        initial_load_start_date = datetime(2023, 2, 1)
        upload_date_less_than_24hr = datetime.utcnow() - timedelta(hours=23, minutes=40)
        upload_date = upload_date_less_than_24hr
        upload_file_name = "some_file"
        last_extracted_id = 10
        max_id = 20
        is_export_completed = False
        chunk_row_count = 3

        insert_query = f"""
        INSERT INTO {self.test_metadata_table_full_path}
        VALUES (
            '{database_name}',
            '{source_table}',
            '{initial_load_start_date}',
            '{upload_date}',
            '{upload_file_name}',
            {last_extracted_id},
            {max_id},
            {is_export_completed},
            {chunk_row_count});
        """

        # Test when table is inserted into metadata
        with self.metadata_engine.connect() as connection:
            connection.execute(insert_query)

        (
            start_pk,
            returned_initial_load_start_date,
        ) = self.pipeline_table.check_delete(
            self.metadata_engine, self.test_metadata_table
        )
        # Tests
        mock_remove_unprocessed_files.assert_not_called()
        assert start_pk == last_extracted_id + 1
        assert returned_initial_load_start_date == initial_load_start_date

        # Test is_resume_export as well
        (
            is_resume_export_needed,
            resume_pk,
            resume_initial_load_start_date,
        ) = is_resume_export(
            self.metadata_engine,
            self.test_metadata_table,
            self.pipeline_table.source_table_name,
        )
        assert is_resume_export_needed is True
        assert resume_pk == last_extracted_id + 1
        assert resume_initial_load_start_date == initial_load_start_date
