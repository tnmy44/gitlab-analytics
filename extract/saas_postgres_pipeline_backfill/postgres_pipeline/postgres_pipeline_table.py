import logging

from datetime import datetime
from typing import Dict, Any

from sqlalchemy.engine.base import Engine

import load_functions
from utils import (
    is_new_table,
    schema_addition_check,
    is_resume_export,
    update_import_query_for_delete_export,
    delete_from_gcs,
)


class PostgresPipelineTable:
    def __init__(self, table_config: Dict[str, str]):
        self.query = table_config["import_query"]
        self.import_db = table_config["import_db"]
        self.source_table_name = table_config["export_table"]
        self.source_table_primary_key = table_config["export_table_primary_key"]
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
        ) = self.check_is_backfill_needed(source_engine, metadata_engine)

        logging.info(f"\nstart_pk: {start_pk}")
        logging.info(f"\ninitial_load_start_date: {initial_load_start_date}")
        logging.info(f"\nis_backfill_needed: {is_backfill_needed}")

        if not self.is_incremental() or not is_backfill_needed:
            logging.info("table does not need incremental backfill")
            return False

        metadata_table = "backfill_metadata"
        target_table = self.get_target_table_name()
        return load_functions.sync_incremental_ids(
            source_engine,
            target_engine,
            self.import_db,
            self.source_table_name,
            self.table_dict,
            target_table,
            metadata_engine,
            metadata_table,
            start_pk,
            initial_load_start_date,
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
        start_pk, initial_load_start_date = 0, None
        metadata_table = "delete_export_metadata"

        (
            is_resume_export_needed,
            resume_pk,
            resume_initial_load_start_date,
        ) = is_resume_export(metadata_engine, metadata_table, self.source_table_name)
        if is_resume_export_needed:
            start_pk = resume_pk
            initial_load_start_date = resume_initial_load_start_date

        self.table_dict["import_query"] = update_import_query_for_delete_export(
            self.query, self.source_table_primary_key
        )

        target_table = self.get_target_table_name()
        return load_functions.sync_incremental_ids(
            source_engine,
            target_engine,
            self.import_db,
            self.source_table_name,
            self.table_dict,
            target_table,
            metadata_engine,
            metadata_table,
            start_pk,
            initial_load_start_date,
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
            "delete_export": self.do_incremental_delete_export,
            # "test": self.check_new_table,
            # "scd": self.do_scd,
        }
        return load_types[load_type](
            source_engine,
            target_engine,
            metadata_engine,
        )

    def check_is_backfill_needed(self, source_engine: Engine, metadata_engine: Engine):
        """
        There are 3 criteria that determine if a backfill is necessary:
            1. New table
            2. New columns in source
            3. In the middle of a backfill

        Will check in the above order.
        If in the middle of a valid backfill, need to return the metadata
        associated with it so that backfill can start in correct spot.

        Furthermore, if backfill needed, but NOT in middle of backfill,
        delete any unprocessed backfill files
        """
        initial_load_start_date = None
        start_pk = 1
        is_backfill_needed = True
        metadata_table = "backfill_metadata"

        if is_new_table(metadata_engine, metadata_table, self.source_table_name):
            logging.info(
                f"Backfill needed- processing new table: {self.source_table_name}."
            )
            delete_from_gcs(self.source_table_name)
            return is_backfill_needed, start_pk, initial_load_start_date

        if schema_addition_check(
            self.query,
            source_engine,
            self.source_table_name,
            self.source_table_primary_key,
        ):
            logging.info(
                f"Backfill needed- schema has changed for table: {self.source_table_name}."
            )
            delete_from_gcs(self.source_table_name)
            return is_backfill_needed, start_pk, initial_load_start_date

        return is_resume_export(metadata_engine, metadata_table, self.source_table_name)

    def get_target_table_name(self):
        return self.target_table_name

    def get_temp_target_table_name(self):
        return self.get_target_table_name() + "_TEMP"
