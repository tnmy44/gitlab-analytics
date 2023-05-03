import logging

from datetime import datetime
from typing import Dict, Any

from sqlalchemy.engine.base import Engine

import load_functions
from postgres_utils import (
    is_new_table,
    schema_addition_check,
    is_resume_export,
    update_import_query_for_delete_export,
    remove_unprocessed_files_from_gcs,
    BACKFILL_METADATA_TABLE,
    DELETE_METADATA_TABLE,
)


class PostgresPipelineTable:
    def __init__(self, table_config: Dict[str, str]):
        self.query = table_config["import_query"]
        self.import_db = table_config["import_db"]
        self.source_table_name = table_config["export_table"]
        self.source_table_primary_key = table_config["export_table_primary_key"]
        self.source_table_composite_key = table_config.get("export_table_composite_key")
        if "additional_filtering" in table_config:
            self.additional_filtering = table_config["additional_filtering"]
        self.advanced_metadata = ("advanced_metadata" in table_config) and table_config[
            "advanced_metadata"
        ] == "true"
        self.target_table_name = "{import_db}_{export_table}".format(
            **table_config
        ).upper()
        self.table_dict = table_config
        self.target_table_name_td_sf = table_config["export_table"]

    def is_scd(self) -> bool:
        return not self.is_incremental()

    def do_scd(
        self, source_engine: Engine, target_engine: Engine, use_temp_table: bool
    ) -> bool:
        if not self.is_scd():
            return True
        target_table = (
            self.get_temp_target_table_name()
            if use_temp_table
            else self.get_target_table_name()
        )
        return load_functions.load_scd(
            source_engine,
            target_engine,
            self.source_table_name,
            self.table_dict,
            target_table,
            is_append_only=self.table_dict.get("append_only", False),
        )

    def is_incremental(self) -> bool:
        return "{EXECUTION_DATE}" in self.query or "{BEGIN_TIMESTAMP}" in self.query

    def do_incremental_backfill(
        self,
        source_engine: Engine,
        target_engine: Engine,
        metadata_engine: Engine,
    ) -> bool:
        (
            is_backfill_needed,
            start_pk,
            initial_load_start_date,
        ) = self.check_is_backfill_needed(
            source_engine, metadata_engine, BACKFILL_METADATA_TABLE
        )

        backfill_chunksize = 5_000_000
        logging.info(f"\nstart_pk: {start_pk}")
        logging.info(f"\ninitial_load_start_date: {initial_load_start_date}")
        logging.info(f"\nis_backfill_needed: {is_backfill_needed}")

        if not self.is_incremental() or not is_backfill_needed:
            logging.info("table does not need incremental backfill")
            return False

        target_table = self.get_target_table_name()
        return load_functions.load_ids(
            self.table_dict,
            source_engine,
            self.import_db,
            self.source_table_name,
            target_engine,
            target_table,
            metadata_engine,
            BACKFILL_METADATA_TABLE,
            start_pk,
            initial_load_start_date,
            backfill_chunksize,
        )

    def check_new_table(
        self, source_engine: Engine, target_engine: Engine, schema_changed: bool
    ) -> bool:
        if not schema_changed:
            logging.info(
                f"Table {self.get_target_table_name()} already exists and won't be tested."
            )
            return False
        target_table = self.get_temp_target_table_name()
        return load_functions.check_new_tables(
            source_engine,
            target_engine,
            self.source_table_name,
            self.table_dict,
            target_table,
        )

    def do_incremental_delete_export(
        self,
        source_engine: Engine,
        target_engine: Engine,
        metadata_engine: Engine,
    ) -> bool:
        """ Incrementally load delete data which is the PK of the table """
        delete_chunksize = 50_000_000
        self.table_dict["import_query"] = update_import_query_for_delete_export(
            self.query, self.source_table_primary_key, self.source_table_composite_key
        )

        start_pk, initial_load_start_date = self.check_delete(
            metadata_engine, DELETE_METADATA_TABLE
        )
        target_table = self.get_target_table_name()

        logging.info("Starting delete export...")
        return load_functions.load_ids(
            self.table_dict,
            source_engine,
            self.import_db,
            self.source_table_name,
            target_engine,
            target_table,
            metadata_engine,
            DELETE_METADATA_TABLE,
            start_pk,
            initial_load_start_date,
            delete_chunksize,
        )

    def do_load(
        self,
        load_type: str,
        source_engine: Engine,
        target_engine: Engine,
        metadata_engine: Engine,
    ) -> bool:
        load_types = {
            "backfill": self.do_incremental_backfill,
            "deletes": self.do_incremental_delete_export,
            # "test": self.check_new_table,
            # "scd": self.do_scd,
        }
        return load_types[load_type](
            source_engine,
            target_engine,
            metadata_engine,
        )

    def check_is_backfill_needed(
        self,
        source_engine: Engine,
        metadata_engine: Engine,
        backfill_metadata_table: str,
    ):
        """
        There are 3 criteria that determine if a backfill is necessary:
            1. New table
            1. In the middle of a backfill
            1. New columns in source

        Will check in the above order. Must check if in middle of backfill first
        because if in mid-backfill that includes new column(s), we want to
        re-start where we left off, rather than from the beginning.


        If in the middle of a valid backfill, need to return the metadata
        associated with it so that backfill can start in correct spot.

        Furthermore, if backfill needed, but NOT in middle of backfill,
        delete any unprocessed backfill files
        """
        initial_load_start_date = None
        start_pk = 1

        if is_new_table(
            metadata_engine, backfill_metadata_table, self.source_table_name
        ):
            logging.info(
                f"Backfill needed- processing new table: {self.source_table_name}."
            )
            is_backfill_needed = True

        else:
            # check if mid-backfill
            is_backfill_needed, start_pk, initial_load_start_date = is_resume_export(
                metadata_engine, backfill_metadata_table, self.source_table_name
            )

            # if not in mid-backfill, check if there was a schema addition
            if is_backfill_needed is False and schema_addition_check(
                self.query,
                source_engine,
                self.source_table_name,
            ):
                logging.info(
                    f"Backfill needed- schema has changed for table: {self.source_table_name}."
                )
                is_backfill_needed = True

        # remove unprocessed files if backfill needed but not in middle of backfill
        if is_backfill_needed and initial_load_start_date is None:
            remove_unprocessed_files_from_gcs(
                backfill_metadata_table, self.source_table_name
            )

        return is_backfill_needed, start_pk, initial_load_start_date

    def check_delete(self, metadata_engine: Engine, delete_metadata_table: str):
        """
        Check if in hanging-delete state,
        if so start there, else start from beginning
        """
        start_pk, initial_load_start_date = 1, None

        (
            is_resume_export_needed,
            resume_pk,
            resume_initial_load_start_date,
        ) = is_resume_export(
            metadata_engine, delete_metadata_table, self.source_table_name
        )
        # need to resume previous export and less than 24 hr has elapsed
        if is_resume_export_needed and resume_initial_load_start_date is not None:
            start_pk = resume_pk
            initial_load_start_date = resume_initial_load_start_date
        else:
            remove_unprocessed_files_from_gcs(
                delete_metadata_table, self.source_table_name
            )
        return start_pk, initial_load_start_date

    def get_target_table_name(self):
        return self.target_table_name

    def get_temp_target_table_name(self):
        return self.get_target_table_name() + "_TEMP"
