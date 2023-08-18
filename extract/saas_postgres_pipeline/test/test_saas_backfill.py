"""
Performs integration testing on the backfill metadata database, using
a test table.

Mostly tests the 'check_backfill_metadata()' function as it has many conditions

Note that all GCS / Gitlab DB components still need to be mocked

1. If new table, backfill
    - implemented in `test_if_new_table_backfill`
1. Delete from metadata table, should be treated as 'new table', files from prev test should be deleted since they aren't 'processed'. 3 Conditions
    1. `remove_files_from_gcs` is called when new table
    1. `remove_files_from_gcs` is called when new schema
    1. `remove_files_from_gcs` is NOT called when 'in mid backfill'
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
from sqlalchemy.engine.base import Engine


from postgres_pipeline_table import PostgresPipelineTable
from postgres_utils import (
    BACKFILL_METADATA_TABLE,
    METADATA_SCHEMA,
    check_is_new_table,
    check_is_new_table_or_schema_addition,
    postgres_engine_factory,
    manifest_reader,
    is_resume_export,
)


def insert_into_metadata_db(metadata_engine, full_table_path, metadata):
    insert_query = f"""
    INSERT INTO {full_table_path}
    VALUES (
        '{metadata["database_name"]}',
        '{metadata["source_table"]}',
        '{metadata["initial_load_start_date"]}',
        '{metadata["upload_date"]}',
        '{metadata["upload_file_name"]}',
        {metadata["last_extracted_id"]},
        {metadata["max_id"]},
        {metadata["is_export_completed"]},
        {metadata["chunk_row_count"]});
    """
    # Test when table is inserted into metadata
    with metadata_engine.connect() as connection:
        connection.execute(insert_query)


class TestCheckBackfill:
    def setup(self):
        """
        - Create test metdata table
        - Create a mock PostgresPipelineTable object
        """
        # manifest_file_path = "extract/saas_postgres_pipeline/manifests_decomposed/saas_el_gitlab_com_db_manifest.yaml "
        manifest_file_path = "extract/saas_postgres_pipeline/manifests_decomposed/saas_el_gitlab_com_db_manifest.yaml"

        manifest_dict = manifest_reader(manifest_file_path)
        env = os.environ.copy()

        # Create a mock source/self.target_engine
        self.source_engine = MagicMock(spec=Engine)
        self.target_engine = MagicMock(spec=Engine)

        self.metadata_engine = postgres_engine_factory(
            manifest_dict["connection_info"]["postgres_metadata_connection"], env
        )
        self.test_metadata_table = f"test_{BACKFILL_METADATA_TABLE}"
        self.test_metadata_table_full_path = (
            f"{METADATA_SCHEMA}.{self.test_metadata_table}"
        )

        drop_query = f""" drop table if exists {self.test_metadata_table_full_path}"""

        create_table_query = f"""
        create table {self.test_metadata_table_full_path}
        (like {METADATA_SCHEMA}.{BACKFILL_METADATA_TABLE});
        """

        with self.metadata_engine.connect() as connection:
            connection.execute(drop_query)
            print(f"\ncreate_table_query: {create_table_query}")
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
        drop_query = f""" drop table if exists {self.test_metadata_table_full_path}"""

        with self.metadata_engine.connect() as connection:
            connection.execute(drop_query)

    def test_check_is_new_table(self):
        """
        When the metadata database is empty, ascertain that when
        backfilling 'some_table', it's considered a new table.

        After inserting the table into metadata, ascertain that
        it's no longer considered a new table
        """

        # Test that metadata table is not a new table
        self.test_metadata_table_full_path
        result = check_is_new_table(
            self.metadata_engine, self.test_metadata_table, schema=METADATA_SCHEMA
        )
        assert result is False

        # Test that non-existant table is a new table
        table_name = "some_missing_table"
        result = check_is_new_table(
            self.metadata_engine, table_name, schema=METADATA_SCHEMA
        )
        assert result is True

    @patch("postgres_utils.get_source_and_target_columns")
    @patch("postgres_utils.check_is_new_table")
    def test_check_is_new_table_or_schema_addition(
        self, mock_check_is_new_table, mock_get_source_and_target_columns
    ):
        raw_query = "some_query"
        target_table = "some_table"
        mock_check_is_new_table.return_value = False

        # Check that new source_columns returns True
        source_columns = ["col1", "col2"]
        target_columns = ["col1"]
        mock_get_source_and_target_columns.return_value = source_columns, target_columns

        res = check_is_new_table_or_schema_addition(
            raw_query, self.source_engine, self.target_engine, target_table
        )
        assert res is True

        # Check that same columns returns False
        source_columns = ["col1", "col2"]
        target_columns = ["col1", "col2"]
        mock_get_source_and_target_columns.return_value = source_columns, target_columns

        res = check_is_new_table_or_schema_addition(
            raw_query, self.source_engine, self.target_engine, target_table
        )
        assert res is False

        # Check that 'dropped col in source' returns False
        source_columns = ["col1"]
        target_columns = ["col1", "col2"]
        mock_get_source_and_target_columns.return_value = source_columns, target_columns

        res = check_is_new_table_or_schema_addition(
            raw_query, self.source_engine, self.target_engine, target_table
        )
        assert res is False

    @patch("postgres_pipeline_table.check_is_new_table_or_schema_addition")
    @patch("postgres_pipeline_table.remove_files_from_gcs")
    @patch("postgres_pipeline_table.is_resume_export")
    def test_check_backfill_metadata_on_schema_addition(
        self,
        mock_is_resume_export,
        mock_remove_files_from_gcs,
        mock_check_is_new_table_or_schema_addition,
    ):
        """
        Test that when check_is_new_table_or_schema_addition() is True,
        remove_files_from_gcs() is called
        """

        # Create a mock self.source_engine and metadata_engine objects
        metadata_engine = MagicMock(spec=Engine)
        export_type = "backfill"

        mock_is_resume_export.return_value = False, None, 1
        mock_check_is_new_table_or_schema_addition.return_value = True
        # Call the function being tested
        (
            is_backfill_needed,
            initial_load_start_date,
            start_pk,
        ) = self.pipeline_table.check_backfill_metadata(
            self.source_engine,
            self.target_engine,
            metadata_engine,
            self.test_metadata_table,
            export_type,
        )

        # Assert that remove_files_from_gcs was called with the correct arguments
        mock_remove_files_from_gcs.assert_called_once_with(
            export_type, self.pipeline_table.source_table_name
        )
        assert initial_load_start_date is None
        assert start_pk == 1
        assert is_backfill_needed is True

    @patch("postgres_pipeline_table.check_is_new_table_or_schema_addition")
    @patch("postgres_pipeline_table.remove_files_from_gcs")
    def test_is_resume_export_completed(
        self,
        mock_remove_files_from_gcs,
        mock_check_is_new_table_or_schema_addition,
    ):
        """
        Check that no backfill is needed when
        the latest is_export_completed is True

        Assumes that `test_if_new_table_backfill()` was already run
        """

        # Create a mock self.source_engine and metadata_engine objects
        export_type = "backfill"

        mock_check_is_new_table_or_schema_addition.return_value = False

        metadata = {
            "source_table": "some_table",
            "database_name": "some_db",
            "initial_load_start_date": datetime(2023, 1, 2),
            "upload_date": datetime(2023, 1, 2),
            "upload_file_name": "some_file",
            "last_extracted_id": 10,
            "max_id": 20,
            "is_export_completed": True,
            "chunk_row_count": 3,
        }

        insert_into_metadata_db(
            self.metadata_engine, self.test_metadata_table_full_path, metadata
        )

        # Call the function being tested
        (
            is_backfill_needed,
            initial_load_start_date,
            start_pk,
        ) = self.pipeline_table.check_backfill_metadata(
            self.source_engine,
            self.target_engine,
            self.metadata_engine,
            self.test_metadata_table,
            export_type,
        )

        # Verify results
        assert is_backfill_needed is False
        assert start_pk == 1
        assert initial_load_start_date is None
        mock_remove_files_from_gcs.assert_not_called()

    @patch("postgres_pipeline_table.check_is_new_table_or_schema_addition")
    @patch("postgres_pipeline_table.remove_files_from_gcs")
    def test_is_resume_export_past_24hr(
        self,
        mock_remove_files_from_gcs,
        mock_check_is_new_table_or_schema_addition,
    ):
        """
        Insert a more recent record where is_export_completed = False
        But more than 24 hours has elapsed

        Should backfill, but need to start from beginning
        """

        export_type = "backfill"
        metadata = {
            "source_table": "some_table",
            "database_name": "some_db",
            "initial_load_start_date": datetime(2023, 1, 2),
            "upload_date": datetime(2023, 1, 2),
            "upload_file_name": "some_file",
            "last_extracted_id": 10,
            "max_id": 20,
            "is_export_completed": False,
            "chunk_row_count": 3,
        }

        insert_into_metadata_db(
            self.metadata_engine, self.test_metadata_table_full_path, metadata
        )

        # Create a mock self.source_engine and metadata_engine objects
        mock_check_is_new_table_or_schema_addition.return_value = False

        # Call the function being tested
        (
            is_backfill_needed,
            initial_load_start_date,
            start_pk,
        ) = self.pipeline_table.check_backfill_metadata(
            self.source_engine,
            self.target_engine,
            self.metadata_engine,
            self.test_metadata_table,
            export_type,
        )

        # Verify results
        assert is_backfill_needed is True
        assert initial_load_start_date is None
        assert start_pk == 1
        mock_remove_files_from_gcs.assert_called_once_with(
            export_type, self.pipeline_table.source_table_name
        )

    @patch("postgres_pipeline_table.check_is_new_table_or_schema_addition")
    @patch("postgres_pipeline_table.remove_files_from_gcs")
    def test_is_resume_export_within_24hr(
        self,
        mock_remove_files_from_gcs,
        mock_check_is_new_table_or_schema_addition,
    ):
        """
        Insert a more recent record where is_export_completed = False
        But more than 24 hours has elapsed


        Should backfill, but need to start from beginning
        """

        export_type = "backfill"
        last_extracted_id = 10
        initial_load_start_date = datetime(2023, 2, 1)
        # Arrange metadata table
        metadata = {
            "source_table": "some_table",
            "database_name": "some_db",
            "initial_load_start_date": initial_load_start_date,
            "upload_date_less_than_24hr": datetime.utcnow()
            - timedelta(hours=23, minutes=40),
            "upload_date": datetime.utcnow() - timedelta(hours=23, minutes=40),
            "upload_file_name": "some_file",
            "last_extracted_id": last_extracted_id,
            "max_id": 20,
            "is_export_completed": False,
            "chunk_row_count": 3,
        }

        insert_into_metadata_db(
            self.metadata_engine, self.test_metadata_table_full_path, metadata
        )

        mock_check_is_new_table_or_schema_addition.return_value = False

        # Call the function being tested
        (
            is_backfill_needed,
            returned_initial_load_start_date,
            start_pk,
        ) = self.pipeline_table.check_backfill_metadata(
            self.source_engine,
            self.target_engine,
            self.metadata_engine,
            self.test_metadata_table,
            export_type,
        )

        # Verify results
        assert is_backfill_needed is True
        assert start_pk == last_extracted_id + 1
        assert returned_initial_load_start_date == initial_load_start_date
        mock_remove_files_from_gcs.assert_not_called()

    @patch("postgres_pipeline_table.check_is_new_table_or_schema_addition")
    @patch("postgres_pipeline_table.remove_files_from_gcs")
    def test_no_backfill_needed(
        self,
        mock_remove_files_from_gcs,
        mock_check_is_new_table_or_schema_addition,
    ):
        """
        Test that no backfill is needed when the following conditions are true:
        - not a new table
        - No new schema addition
        - resume_export is False (since is_export_completed=True)
        """
        export_type = "backfill"
        # Update metdata table
        upload_date_less_than_24hr = datetime.utcnow() - timedelta(hours=23, minutes=40)

        metadata = {
            "source_table": "some_table",
            "database_name": "some_db",
            "initial_load_start_date": datetime(2023, 2, 1),
            "upload_date_less_than_24hr": upload_date_less_than_24hr,
            "upload_date": upload_date_less_than_24hr,
            "upload_file_name": "some_file",
            "last_extracted_id": 10,
            "max_id": 20,
            "is_export_completed": True,
            "chunk_row_count": 3,
        }

        insert_into_metadata_db(
            self.metadata_engine, self.test_metadata_table_full_path, metadata
        )
        # have to mock schema addition check because can't connect to Postgres DB
        mock_check_is_new_table_or_schema_addition.return_value = False

        # Check if backfill needed - main code
        is_backfill_needed, _, _ = self.pipeline_table.check_backfill_metadata(
            self.source_engine,
            self.target_engine,
            self.metadata_engine,
            self.test_metadata_table,
            export_type,
        )

        # Verify results
        mock_remove_files_from_gcs.assert_not_called()
        assert is_backfill_needed is False
