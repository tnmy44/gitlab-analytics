import logging

from datetime import datetime, timedelta
from typing import Dict, Any

from sqlalchemy.engine.base import Engine

import load_functions
from utils import is_new_table, schema_addition_check, query_backfill_status


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
        self, source_engine: Engine, target_engine: Engine, schema_changed: bool
    ) -> bool:
        if not self.is_incremental() or not schema_changed:
            logging.info("table does not need incremental backfill")
            return False
        target_table = self.get_target_table_name()
        return load_functions.sync_incremental_ids(
            source_engine,
            target_engine,
            self.source_table_name,
            self.table_dict,
            target_table,
            initial_load_start_date
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
        schema_changed: bool,
    ) -> bool:
        load_types = {
            "scd": self.do_scd,
            "backfill": self.do_incremental_backfill,
            "test": self.check_new_table,
        }
        return load_types[load_type](source_engine, target_engine, schema_changed)

    def check_if_backfill_needed(self, source_engine: Engine, metadata_engine: Engine):
        load_start_date = datetime.now()
        start_pk = 1

        if is_new_table(metadata_engine, self.source_table_name):
            logging.info(f"New table: {self.source_table_name}.")
            return (True, start_pk, load_start_date)

        if schema_addition_check(
            self.query,
            source_engine,
            self.source_table_name,
            self.source_table_primary_key,
        ):
            logging.info(f"Schema has changed for table: {self.target_table_name}.")
            return (True, start_pk, load_start_date)

        return self.is_resume_backfill(metadata_engine)

    def is_resume_backfill(self, metadata_engine: Engine):

        is_backfill_complete, load_start_date, last_extracted_id, last_write_date = query_backfill_status()

        if not is_backfill_complete:
            time_difference = datetime.now() - last_write_date
            if time_difference < timedelta(hours=24):
                return (True, last_extracted_id, load_start_date)
        return (False, None, None)

    def get_target_table_name(self):
        return self.target_table_name

    # REMOVE
    '''
    def get_temp_target_table_name(self):
        return self.get_target_table_name() + "_TEMP"
    '''
