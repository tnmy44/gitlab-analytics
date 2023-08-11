import logging
from typing import Dict, Any

from sqlalchemy.engine.base import Engine

import load_functions
from postgres_utils import (
    BACKFILL_METADATA_TABLE,
    check_is_new_table_or_schema_addition,
    check_and_handle_schema_removal,
    is_resume_export,
    remove_files_from_gcs,
    swap_temp_table,
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
        self, source_engine: Engine, target_engine: Engine, is_schema_addition: bool
    ) -> bool:
        if not self.is_scd():
            return True
        target_table = (
            self.get_temp_target_table_name()
            if is_schema_addition
            else self.get_target_table_name()
        )
        loaded = load_functions.load_scd(
            source_engine,
            target_engine,
            self.source_table_name,
            self.table_dict,
            target_table,
            is_append_only=self.table_dict.get("append_only", False),
        )

        return loaded

    def is_incremental(self) -> bool:
        return "{EXECUTION_DATE}" in self.query or "{BEGIN_TIMESTAMP}" in self.query

    def do_incremental(
        self, source_engine: Engine, target_engine: Engine, is_schema_addition: bool
    ) -> bool:
        if (is_schema_addition) or (not self.is_incremental()):
            return False
        target_table = self.get_target_table_name()
        return load_functions.load_incremental(
            source_engine,
            target_engine,
            self.source_table_name,
            self.table_dict,
            target_table,
        )

    def do_trusted_data_pgp(
        self, source_engine: Engine, target_engine: Engine, is_schema_addition: bool
    ) -> bool:
        """
        The function is used for trusted data extract and load.
        It is responsible for setting up the target table and then call trusted_data_pgp load function.
        """
        target_table = self.target_table_name_td_sf
        return load_functions.trusted_data_pgp(
            source_engine,
            target_engine,
            self.source_table_name,
            self.table_dict,
            target_table,
        )

    def do_incremental_backfill(
        self, source_engine: Engine, target_engine: Engine, metadata_engine: Engine
    ) -> bool:
        (
            is_backfill_needed,
            start_pk,
            initial_load_start_date,
        ) = self.check_backfill_metadata(
            source_engine,
            target_engine,
            metadata_engine,
            BACKFILL_METADATA_TABLE,
            "backfill",
        )

        if not self.is_incremental() or not is_backfill_needed:
            logging.info("table does not need incremental backfill")
            return False

        # dipose current engine, create new Snowflake engine before load
        target_engine.dispose()
        database_kwargs = {
            "metadata_engine": metadata_engine,
            "metadata_table": BACKFILL_METADATA_TABLE,
            "source_engine": source_engine,
            "source_table": self.source_table_name,
            "source_database": self.import_db,
            "target_table": self.get_temp_target_table_name(),
            "real_target_table": self.get_target_table_name(),
        }
        loaded = load_functions.load_ids(
            database_kwargs,
            self.table_dict,
            initial_load_start_date,
            start_pk,
            "backfill",
        )
        return loaded

    def check_new_table(
        self, source_engine: Engine, target_engine: Engine, is_schema_addition: bool
    ) -> bool:
        if not is_schema_addition:
            logging.info(
                f"Table {self.get_target_table_name()} already exists and won't be tested."
            )
            return False
        target_table = self.get_temp_target_table_name()
        loaded = load_functions.check_new_tables(
            source_engine,
            target_engine,
            self.source_table_name,
            self.table_dict,
            target_table,
        )
        return loaded

    def do_load(
        self,
        load_type: str,
        source_engine: Engine,
        target_engine: Engine,
        metadata_engine: Engine,
    ) -> bool:
        load_types = {
            "incremental": self.do_incremental,
            "scd": self.do_scd,
            "backfill": self.do_incremental_backfill,
            "test": self.check_new_table,
            "trusted_data": self.do_trusted_data_pgp,
        }

        if load_type == "backfill":
            return load_types[load_type](source_engine, target_engine, metadata_engine)

        else:
            is_schema_addition = self.check_is_new_table_or_schema_addition(
                source_engine, target_engine
            )
            if not is_schema_addition:
                self.check_and_handle_schema_removal(source_engine, target_engine)
            loaded = load_types[load_type](
                source_engine, target_engine, is_schema_addition
            )

            # If temp table, swap it
            self.swap_temp_table_on_schema_change(
                is_schema_addition, loaded, target_engine
            )
            return loaded

    def check_is_new_table_or_schema_addition(
        self, source_engine: Engine, target_engine: Engine
    ) -> bool:
        is_schema_addition = check_is_new_table_or_schema_addition(
            self.query,
            source_engine,
            target_engine,
            self.target_table_name,
        )
        if is_schema_addition:
            logging.info(f"New table or schema addition: {self.target_table_name}.")
        return is_schema_addition

    def check_and_handle_schema_removal(
        self, source_engine: Engine, target_engine: Engine
    ) -> bool:
        check_and_handle_schema_removal(
            self.query,
            source_engine,
            target_engine,
            self.target_table_name,
        )

    def get_target_table_name(self):
        return self.target_table_name

    def get_temp_target_table_name(self):
        return self.get_target_table_name() + "_TEMP"

    def swap_temp_table_on_schema_change(
        self, is_schema_addition: bool, loaded: bool, engine: Engine
    ):
        if is_schema_addition and loaded:
            swap_temp_table(
                engine, self.get_target_table_name(), self.get_temp_target_table_name()
            )

    def check_backfill_metadata(
        self,
        source_engine: Engine,
        target_engine: Engine,
        metadata_engine: Engine,
        backfill_metadata_table: str,
        export_type: str,
    ):
        """
        There are 3 criteria that determine if a backfill is necessary:
            1. In the middle of a backfill
            2. New table | New columns in source

        Will check in the above order. Must check if in middle of backfill first
        because if in mid-backfill that includes new column(s), we want to
        re-start where we left off, rather than from the beginning.


        If in the middle of a valid backfill, need to return the metadata
        associated with it so that backfill can start in correct spot.

        Furthermore, if backfill needed, but NOT in middle of backfill,
        delete any unprocessed backfill files
        """

        # check if mid-backfill first, must always check before schema_change
        # if not mid-backfill, returns start_pk=1, initial_load_start_date=None
        is_backfill_needed, start_pk, initial_load_start_date = is_resume_export(
            metadata_engine, backfill_metadata_table, self.source_table_name
        )

        if not is_backfill_needed:
            is_backfill_needed = self.check_is_new_table_or_schema_addition(
                source_engine, target_engine
            )

        # remove unprocessed files if backfill needed but not in middle of backfill
        if is_backfill_needed and initial_load_start_date is None:
            remove_files_from_gcs(export_type, self.source_table_name)

        return is_backfill_needed, start_pk, initial_load_start_date
