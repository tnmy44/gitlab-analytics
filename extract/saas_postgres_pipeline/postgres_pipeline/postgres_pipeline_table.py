import logging
from typing import Dict, Any

from sqlalchemy.engine.base import Engine

import load_functions
from postgres_utils import (
    BACKFILL_METADATA_TABLE,
    INCREMENTAL_METADATA_TABLE,
    check_is_new_table_or_schema_addition,
    check_and_handle_schema_removal,
    is_resume_export,
    remove_files_from_gcs,
    swap_temp_table,
    get_min_or_max_id,
    INCREMENTAL_LOAD_TYPE_BY_ID,
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
        # options: load_by_id | load_by_date
        self.incremental_type = table_config.get("incremental_type", "load_by_date")

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
        return (
            "{EXECUTION_DATE}" in self.query or "{BEGIN_TIMESTAMP}" in self.query
        ) or (self.incremental_type)

    def do_incremental(
        self,
        source_engine: Engine,
        target_engine: Engine,
        metadata_engine: Engine,
        is_schema_addition: bool,
    ) -> bool:
        if (is_schema_addition) or (not self.is_incremental()):
            logging.info("Aborting... because schema_change OR non_incremental_load")
            return False
        # load by incrementally by id rather than by date
        if self.incremental_type == INCREMENTAL_LOAD_TYPE_BY_ID:
            return self.do_incremental_load_by_id(
                source_engine, target_engine, metadata_engine
            )

        # default, load incrementally by date
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

    def __do_load_by_id(
        self,
        source_engine: Engine,
        target_engine: Engine,
        metadata_engine: Engine,
        target_table: str,
        metadata_table: str,
        export_type: str,
        initial_load_start_date,
        start_pk,
    ) -> bool:
        # dipose current engine, create new Snowflake engine before load
        target_engine.dispose()
        database_kwargs = {
            "metadata_engine": metadata_engine,
            "metadata_table": metadata_table,
            "source_engine": source_engine,
            "source_table": self.source_table_name,
            "source_database": self.import_db,
            # is either temp or actual table
            "target_table": target_table,
            "real_target_table": self.get_target_table_name(),
        }
        loaded = load_functions.load_ids(
            database_kwargs,
            self.table_dict,
            initial_load_start_date,
            start_pk,
            export_type,
        )
        return loaded

    def do_incremental_backfill(
        self, source_engine: Engine, target_engine: Engine, metadata_engine: Engine
    ) -> bool:
        export_type = "backfill"
        (
            is_backfill_needed,
            initial_load_start_date,
            start_pk,
        ) = self.check_backfill_metadata(
            source_engine,
            target_engine,
            metadata_engine,
            BACKFILL_METADATA_TABLE,
            export_type,
        )

        if not self.is_incremental() or not is_backfill_needed:
            logging.info("table does not need incremental backfill")
            return False

        target_table = self.get_temp_target_table_name()
        return self.__do_load_by_id(
            source_engine,
            target_engine,
            metadata_engine,
            target_table,
            BACKFILL_METADATA_TABLE,
            export_type,
            initial_load_start_date,
            start_pk,
        )

    def do_incremental_load_by_id(
        self, source_engine: Engine, target_engine: Engine, metadata_engine: Engine
    ) -> bool:
        export_type = self.incremental_type
        initial_load_start_date, start_pk = self.check_incremental_load_by_id_metadata(
            target_engine, metadata_engine, INCREMENTAL_METADATA_TABLE
        )
        target_table = self.get_target_table_name()
        return self.__do_load_by_id(
            source_engine,
            target_engine,
            metadata_engine,
            target_table,
            INCREMENTAL_METADATA_TABLE,
            export_type,
            initial_load_start_date,
            start_pk,
        )

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

        # Non-backfill section
        is_schema_addition = self.check_is_new_table_or_schema_addition(
            source_engine, target_engine
        )
        if not is_schema_addition:
            self.check_and_handle_schema_removal(source_engine, target_engine)

        if load_type == "incremental":
            loaded = load_types[load_type](
                source_engine, target_engine, metadata_engine, is_schema_addition
            )
        # remaining load_types
        else:
            loaded = load_types[load_type](
                source_engine, target_engine, is_schema_addition
            )

        # If temp table, swap it, for scd schema change
        self.swap_temp_table_on_schema_change(is_schema_addition, loaded, target_engine)
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
        metadata_table: str,
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
        is_backfill_needed, initial_load_start_date, start_pk = is_resume_export(
            metadata_engine, metadata_table, self.source_table_name
        )

        if not is_backfill_needed:
            is_backfill_needed = self.check_is_new_table_or_schema_addition(
                source_engine, target_engine
            )

        # remove unprocessed files if backfill needed but not in middle of backfill
        if is_backfill_needed and initial_load_start_date is None:
            remove_files_from_gcs(export_type, self.source_table_name)

        return is_backfill_needed, initial_load_start_date, start_pk

    def check_incremental_load_by_id_metadata(
        self, target_engine: Engine, metadata_engine: Engine, metadata_table: str
    ):
        """
        Similiar to `check_backfill_metadata()`.

        Check metadata  to see if extract should start
        from previous load (i.e due to network failure),

        Or start a new extract starting from
        the max PK in the target Snowflake table.
        """
        target_start_pk = (
            get_min_or_max_id(self.source_table_primary_key, target_engine, "max") + 1
        )

        (
            is_resume_export_needed,
            prev_initial_load_start_date,
            metadata_start_pk,
        ) = is_resume_export(metadata_engine, metadata_table, self.source_table_name)
        # use metadata_start_pk and continue export
        if is_resume_export_needed and metadata_start_pk > target_start_pk:
            initial_load_start_date, start_pk = (
                prev_initial_load_start_date,
                metadata_start_pk,
            )
        # use latest Snowflake pk
        else:
            initial_load_start_date, start_pk = None, target_start_pk

        return initial_load_start_date, start_pk
