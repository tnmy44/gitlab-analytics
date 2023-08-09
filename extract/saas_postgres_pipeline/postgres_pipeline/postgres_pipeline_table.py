import logging
from typing import Dict, Any

from sqlalchemy.engine.base import Engine

import load_functions
from postgres_utils import (
    check_if_schema_changed,
    is_resume_export,
    remove_files_from_gcs,
    BACKFILL_METADATA_TABLE,
)

from gitlabdata.orchestration_utils import snowflake_engine_factory, query_executor


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

    def do_scd(self, source_engine: Engine, target_engine: Engine) -> bool:
        schema_changed = self.check_if_schema_changed(source_engine, target_engine)
        if not self.is_scd():
            return True
        target_table = (
            self.get_temp_target_table_name()
            if schema_changed
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

        self.swap_temp_table_on_schema_change(loaded, schema_changed, target_engine)
        return loaded

    def is_incremental(self) -> bool:
        return "{EXECUTION_DATE}" in self.query or "{BEGIN_TIMESTAMP}" in self.query

    def do_incremental(self, source_engine: Engine, target_engine: Engine) -> bool:
        if self.check_if_schema_changed(source_engine, target_engine):
            return False
        if not self.is_incremental():
            return False
        target_table = self.get_target_table_name()
        return load_functions.load_incremental(
            source_engine,
            target_engine,
            self.source_table_name,
            self.table_dict,
            target_table,
        )

    def do_trusted_data_pgp(self, source_engine: Engine, target_engine: Engine) -> bool:
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

        database_kwargs = {
            "chunksize": 5_000_000,
            "metadata_engine": metadata_engine,
            "metadata_table": BACKFILL_METADATA_TABLE,
            "source_engine": source_engine,
            "source_table": self.source_table_name,
            "source_database": self.import_db,
            "target_engine": target_engine,
            "target_table": self.get_temp_target_table_name(),
        }
        loaded = load_functions.load_ids(
            database_kwargs,
            self.table_dict,
            initial_load_start_date,
            start_pk,
            "backfill",
        )
        self.swap_temp_table_on_schema_change(loaded, is_backfill_needed, target_engine)
        return loaded

    def check_new_table(
        self, source_engine: Engine, target_engine: Engine, schema_changed: bool
    ) -> bool:
        schema_changed = self.check_if_schema_changed(source_engine, target_engine)
        if not schema_changed:
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
        self.swap_temp_table_on_schema_change(loaded, schema_changed, target_engine)
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
        if load_type == 'backfill':  # for backfills
            return load_types[load_type](source_engine, target_engine, metadata_engine)
        else:
            return load_types[load_type](source_engine, target_engine)

    def check_if_schema_changed(
        self, source_engine: Engine, target_engine: Engine
    ) -> bool:
        schema_changed = check_if_schema_changed(
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

    def get_temp_target_table_name(self):
        return self.get_target_table_name() + "_TEMP"

    def _swap_temp_table(
        self, engine: Engine, real_table: str, temp_table: str
    ) -> None:
        """
        Drop the real table and rename the temp table to take the place of the
        real table.
        """

        if engine.has_table(real_table):
            logging.info(
                f"Swapping the temp table: {temp_table} with the real table: {real_table}"
            )
            swap_query = f"ALTER TABLE IF EXISTS tap_postgres.{temp_table} SWAP WITH tap_postgres.{real_table}"
            query_executor(engine, swap_query)
        else:
            logging.info(f"Renaming the temp table: {temp_table} to {real_table}")
            rename_query = f"ALTER TABLE IF EXISTS tap_postgres.{temp_table} RENAME TO tap_postgres.{real_table}"
            query_executor(engine, rename_query)

        drop_query = f"DROP TABLE IF EXISTS tap_postgres.{temp_table}"
        query_executor(engine, drop_query)

    def swap_temp_table_on_schema_change(
        self, schema_changed: bool, loaded: bool, engine: Engine
    ):
        if schema_changed and loaded:
            self._swap_temp_table(
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
            1. New table
            1. New columns in source

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
            is_backfill_needed = self.check_if_schema_changed(
                source_engine, target_engine
            )

        # remove unprocessed files if backfill needed but not in middle of backfill
        if is_backfill_needed and initial_load_start_date is None:
            remove_files_from_gcs(export_type, self.source_table_name)

        return is_backfill_needed, start_pk, initial_load_start_date
