import logging

from datetime import datetime
from typing import Dict, Any

from sqlalchemy.engine.base import Engine

import load_functions
from utils import is_new_table, schema_addition_check, is_resume_backfill


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

    def needs_incremental_backfill(
        self, source_engine: Engine, target_engine: Engine
    ) -> bool:
        """
        Temporary logic for now
        """
        return self.is_incremental()

    def do_incremental_backfill(
        self,
        source_engine: Engine,
        target_engine: Engine,
        metadata_engine: Engine,
        is_backfill_needed: bool,
        start_pk,
        initial_load_start_date,
    ) -> bool:
        if not self.is_incremental() or not is_backfill_needed:
            logging.info("table does not need incremental backfill")
            return False
        target_table = self.get_target_table_name()
        return load_functions.sync_incremental_ids(
            source_engine,
            target_engine,
            self.source_table_name,
            self.table_dict,
            target_table,
            metadata_engine,
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

    def do_load(
        self,
        load_type: str,
        source_engine: Engine,
        target_engine: Engine,
        metadata_engine: Engine,
        is_backfill_needed: bool,
        start_pk: int,
        initial_load_start_date: datetime,
    ) -> bool:
        load_types = {
            "scd": self.do_scd,
            "backfill": self.do_incremental_backfill,
            "test": self.check_new_table,
        }
        return load_types[load_type](
            source_engine,
            target_engine,
            metadata_engine,
            is_backfill_needed,
            start_pk,
            initial_load_start_date,
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
        """
        initial_load_start_date = None
        start_pk = 1
        is_backfill_needed = True

        if is_new_table(metadata_engine, self.source_table_name):
            logging.info(
                f"Backfill needed- processing new table: {self.source_table_name}."
            )
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
            return is_backfill_needed, start_pk, initial_load_start_date

        return is_resume_backfill(metadata_engine, self.source_table_name)

    def get_target_table_name(self):
        return self.target_table_name
