import logging

from datetime import datetime
from typing import Dict, Any

from sqlalchemy.engine.base import Engine

import load_functions
from utils import check_is_new_table, schema_addition_check


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

    def query_backfill_metadata():
        #TODO
        latest_backfill_status = 'in progress'
        latest_backfill_updated_at = datetime.now()
        return latest_backfill_status, latest_backfill_updated_at

    def no_gcs_schema_change():
        pass

    def check_if_backfill_needed(self, source_engine: Engine, metadata_engine: Engine):
        is_new_table = check_is_new_table(metadata_engine, self.source_table_name)

        #TODO later:
        '''
        latest_backfill_status, latest_backfill_updated_at = query_backfill_metadata()

        if latest_backfill_status == 'in progress':

            # last backfill file was less than 24 hr ago
            if latest_backfill_updated_at < '24hr':

                # no schema change since backfill started
                if no_gcs_schema_change():
                    return 'Start from last file'
            return False

        # backfill not already in progres
        else:
            return self.check_if_schema_changed()
        '''


    def check_if_schema_changed(
        self, source_engine: Engine, target_engine: Engine
    ) -> bool:
        schema_changed = schema_addition_check(
            self.query,
            source_engine,
            self.source_table_name,
            self.source_table_primary_key,
            target_engine,
            self.target_table_name,
        )
        if schema_changed:
            logging.info(f"Schema has changed for table: {self.target_table_name}.")
        return schema_changed

    def get_target_table_name(self):
        return self.target_table_name

    # REMOVE
    '''
    def get_temp_target_table_name(self):
        return self.get_target_table_name() + "_TEMP"
    '''
