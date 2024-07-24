"""
Util unit for data classification
"""

import json
import os
import sys
from json import JSONDecodeError
from logging import error, info

import pandas as pd
import yaml
from gitlabdata.orchestration_utils import dataframe_uploader, snowflake_engine_factory


class DataClassification:
    """
    Main class for data classification for:
    - PII
    - MNPI
    """

    def __init__(
        self, tagging_type: str, mnpi_raw_file: str, incremental_load_days: int
    ):
        """
        Define parameters
        """

        self.encoding = "utf8"
        self.schema_name = "data_classification"
        self.table_name = "sensitive_objects_classification"
        self.processing_role = "SYSADMIN"
        self.loader_engine = None
        self.connected = False
        self.mnpi_file_name = "mnpi_table_list.csv"
        self.specification_file = "../../extract/data_classification/specification.yml"
        self.tagging_type = tagging_type
        self.mnpi_raw_file = mnpi_raw_file
        self.config_vars = os.environ.copy()
        self.incremental_load_days = incremental_load_days
        # self.branch = self.config_vars["GIT_BRANCH"]
        info(f"...GIT_BRANCH: {self.config_vars}")
        self.raw = ""
        self.prep = ""
        self.prod = ""

    def __connect(self):
        """
        Connect to engine factory, return connection object
        """
        if not self.connected:
            self.loader_engine = snowflake_engine_factory(
                self.config_vars, self.processing_role
            )
            self.connected = True
            return self.loader_engine.connect()

    def __dispose(self) -> None:
        """
        Dispose from engine factory
        """
        if self.connected:
            self.connected = False
            self.loader_engine.dispose()

    def upload_to_snowflake(self) -> None:
        """
        Upload dataframe to Snowflake
        """
        try:
            dataframe_uploader(
                dataframe=self.identify_mnpi_data,
                engine=self.loader_engine,
                table_name=self.table_name,
                schema=self.schema_name,
            )
        except Exception as e:
            error(f"Error uploading to Snowflake: {e.__class__.__name__} - {e}")
            sys.exit(1)

    @staticmethod
    def quoted(input_str: str) -> str:
        """
        Returns input string with single quote
        """
        return "'" + input_str + "'"

    def load_mnpi_list(self) -> list:
        """
        Load MNPI list generated via dbt command
        """

        if not os.path.exists(self.mnpi_raw_file):
            error(f"File {self.mnpi_raw_file} is note generated, stopping processing")
            sys.exit(1)

        with open(self.mnpi_raw_file, mode="r", encoding=self.encoding) as file:
            res = []
            for line in file:
                try:
                    res.append(json.loads(line.rstrip()))
                except JSONDecodeError as e:
                    info(f"Wrong json format, discard and continue (expected): {e}")
                except Exception as e:
                    error(f"Exception: {e.__class__.__name__} - {e}")
                    sys.exit(1)
            return res

    def transform_mnpi_list(self, mnpi_list: list) -> list:
        """
        Transform MNPI list to uppercase in a proper format
        """

        def extract_full_path(x: dict) -> list:
            return [
                x.get("config").get("database").upper(),
                x.get("config").get("schema").upper(),
                x.get("alias").upper(),
            ]

        return [extract_full_path(x) for x in mnpi_list]

    def get_pii_scope(self, section: str, scope_type: str) -> str:
        """
        Get the WHERE clause for the PII data
        """
        res = ""
        scope = self.scope.get("data_classification").get(section).get(scope_type)
        databases = scope.get("databases")
        schemas = scope.get("schemas")
        tables = scope.get("tables")
        exclude_statement = "NOT" if scope_type == "exclude" else ""

        if databases:
            res = f" (table_catalog {exclude_statement} IN ({', '.join(self.quoted(x) for x in databases)}))"

        if schemas:
            res += " AND"

            if exclude_statement:
                res += f" {exclude_statement}"

            for i, schema in enumerate(schemas, start=1):
                schema_list = schema.split(".")

                res += (
                    f" (table_catalog = {self.quoted(schema_list[0])} AND table_schema"
                )
                if "*" in schema_list:
                    res += f" ILIKE {self.quoted('%')})"
                else:
                    res += f" = {self.quoted(schema_list[1])})"

                if i < len(schemas):
                    res += " OR"
        if tables:
            res += " AND"
            if exclude_statement:
                res += f" {exclude_statement}"
            for i, table in enumerate(tables, start=1):
                table_list = table.split(".")

                if "*" in table_list:
                    res += f" (table_catalog = {self.quoted(table_list[0])} AND table_schema"
                    if table_list.count("*") == 1:
                        res += f" = {self.quoted(table_list[1])} AND table_name ILIKE {self.quoted('%')})"
                    if table_list.count("*") == 2:
                        res += f" ILIKE {self.quoted('%')} AND table_name ILIKE {self.quoted('%')})"
                else:
                    res += f" (table_catalog = {self.quoted(table_list[0])} AND table_schema = {self.quoted(table_list[1])} and table_name = {self.quoted(table_list[2])})"

                if i < len(tables):
                    res += " OR"

        return f"AND ({res})"

    @property
    def pii_query(self) -> str:
        """
        Property method for the query generated to define the scope
        for tagging PII data
        """
        section = "PII"
        insert_statement = (
            f"INSERT INTO {self.schema_name}.{self.table_name}(classification_type, created, last_altered,last_ddl, database_name, schema_name, table_name, table_type, _uploaded_at) "
            f"WITH base AS ("
            f"SELECT {self.quoted(section)} AS classification_type, created,last_altered, last_ddl, table_catalog, table_schema, table_name, REPLACE(table_type,'BASE TABLE','TABLE') AS table_type, DATE_PART(epoch_second, CURRENT_TIMESTAMP()) "
            f"  FROM raw.information_schema.tables "
            f" WHERE table_schema != 'INFORMATION_SCHEMA' "
            f" UNION "
            f"SELECT {self.quoted(section)} AS classification_type, created,last_altered, last_ddl, table_catalog, table_schema, table_name, REPLACE(table_type,'BASE TABLE','TABLE') AS table_type, DATE_PART(epoch_second, CURRENT_TIMESTAMP()) "
            f"  FROM prep.information_schema.tables "
            f" WHERE table_schema != 'INFORMATION_SCHEMA' "
            f" UNION "
            f"SELECT {self.quoted(section)} AS classification_type, created,last_altered, last_ddl, table_catalog, table_schema, table_name, REPLACE(table_type,'BASE TABLE','TABLE') AS table_type, DATE_PART(epoch_second, CURRENT_TIMESTAMP()) "
            f"  FROM prod.information_schema.tables"
            f" WHERE table_schema != 'INFORMATION_SCHEMA') "
            f"SELECT *"
            f"  FROM base"
            f" WHERE 1=1 "
        )

        where_clause_include = self.get_pii_scope(section=section, scope_type="include")
        where_clause_exclude = self.get_pii_scope(section=section, scope_type="exclude")

        res = f"{insert_statement}{where_clause_include}{where_clause_exclude}"
        return res

    @property
    def mnpi_metadata_update_query(self) -> str:
        """
        Generate update statement for the MNPI metadata
        """
        return (
            f"MERGE INTO {self.schema_name}.{self.table_name} USING ( "
            f"WITH database_list AS (SELECT DISTINCT database_name FROM  {self.schema_name}.{self.table_name}) "
            "SELECT 'MNPI' AS classification_type, "
            "       created, "
            "       last_altered, "
            "       last_ddl, "
            "       table_catalog, "
            "       table_schema, "
            "       table_name, "
            "       REPLACE(table_type,'BASE TABLE','TABLE') AS table_type "
            "  FROM raw.information_schema.tables "
            " WHERE table_schema != 'INFORMATION_SCHEMA' "
            "   AND table_catalog IN (SELECT database_name FROM database_list) "
            " UNION "
            "SELECT 'MNPI' AS classification_type, "
            "       created,last_altered, "
            "       last_ddl, "
            "       table_catalog, "
            "       table_schema, "
            "       table_name, "
            "       REPLACE(table_type,'BASE TABLE','TABLE') AS table_type "
            "  FROM prep.information_schema.tables "
            " WHERE table_schema != 'INFORMATION_SCHEMA' "
            "   AND table_catalog IN (SELECT database_name FROM database_list) "
            " UNION "
            "SELECT 'MNPI' AS classification_type, "
            "       created, "
            "       last_altered, "
            "       last_ddl, "
            "       table_catalog, "
            "       table_schema, "
            "       table_name, "
            "       REPLACE(table_type,'BASE TABLE','TABLE') AS table_type "
            "  FROM prod.information_schema.tables "
            " WHERE table_schema != 'INFORMATION_SCHEMA' "
            "   AND table_catalog IN (SELECT database_name FROM database_list)) AS full_table_list "
            f" ON full_table_list.classification_type                  = {self.table_name}.classification_type "
            f"AND full_table_list.table_catalog                        = {self.table_name}.database_name "
            f"AND full_table_list.table_schema                         = {self.table_name}.schema_name "
            f"AND full_table_list.table_name                           = {self.table_name}.table_name "
            f"AND {self.table_name}.classification_type = 'MNPI' "
            "WHEN MATCHED THEN "
            "UPDATE "
            f"   SET {self.table_name}.created      = full_table_list.created, "
            f"       {self.table_name}.last_altered = full_table_list.last_altered, "
            f"       {self.table_name}.last_ddl     = full_table_list.last_ddl, "
            f"       {self.table_name}.table_type   = full_table_list.table_type"
        )

    @property
    def delete_data_query(self) -> str:
        """
        Query to delete data from table
        """
        return f"DELETE FROM {self.schema_name}.{self.table_name}"

    def classify_query(
        self, date_from: str, unset: str = "FALSE", tagging_type: str = "INCREMENTAL"
    ) -> str:
        """
        Query to call procedure with parameters for classification
        """
        return (
            f"CALL {self.schema_name}.execute_data_classification("
            f"p_type => {self.quoted(tagging_type)}, "
            f"p_date_from=>{self.quoted(date_from)} , "
            f"p_unset=> {self.quoted(unset)})"
        )

    def get_mnpi_scope(self, section: str, scope_type: str, row: list) -> bool:
        """
        Define what is included and what is excluded for MNPI data
        DATABASE, SCHEMA and TABLE level
        """
        scope = self.scope.get("data_classification").get(section).get(scope_type)

        databases = scope.get("databases")
        schemas = scope.get("schemas")
        tables = scope.get("tables")

        database_name = str(row[0]).upper()
        schema_name = str(row[1]).upper()
        table_name = str(row[2]).upper()

        if database_name in databases:
            if (
                f"{database_name}.{schema_name}" in schemas
                or f"{database_name}.*" in schemas
            ):
                full_name = f"{database_name}.{schema_name}.{table_name}"

                if (
                    full_name in tables
                    or f"{database_name}.{schema_name}.*" in tables
                    or f"{database_name}.*.*" in tables
                ):

                    return True

        return False

    def filter_data(self, mnpi_data: list, section: str = "MNPI") -> list:
        """
        filtering data based on the configuration
        """
        res = []

        for row in mnpi_data:

            include = self.get_mnpi_scope(
                section=section, scope_type="include", row=row
            )
            exclude = self.get_mnpi_scope(
                section=section, scope_type="exclude", row=row
            )

            if include and not exclude:
                null_value = None
                row = [
                    section,
                    null_value,
                    null_value,
                    null_value,
                    row[0],
                    row[1],
                    row[2],
                    null_value,
                ]
                res.append(row)
        return res

    @property
    def identify_mnpi_data(self) -> pd.DataFrame:
        """
        Entry point to identify MNPI data
        """
        mnpi_list = self.load_mnpi_list()
        mnpi_data = self.transform_mnpi_list(mnpi_list=mnpi_list)
        mnpi_data_filtered = self.filter_data(mnpi_data=mnpi_data, section="MNPI")

        columns = [
            "classification_type",
            "created",
            "last_altered",
            "last_ddl",
            "database_name",
            "schema_name",
            "table_name",
            "table_type",
        ]

        return pd.DataFrame(data=mnpi_data_filtered, columns=columns)

    @property
    def scope(self):
        """
        Open and load specification file with the definition what to include and what to exclude
        on DATABASE, SCHEMA and TABLE level
        """
        with open(
            file=self.specification_file, mode="r", encoding=self.encoding
        ) as file:
            manifest_dict = yaml.load(file, Loader=yaml.FullLoader)

        return manifest_dict

    def classify(
        self, date_from: str, unset: str = "FALSE", tagging_type: str = "INCREMENTAL"
    ):
        """
        Routine to classify all data
        using stored procedure
        """
        info("START classify.")
        query = self.classify_query(
            date_from=date_from, unset=unset, tagging_type=tagging_type
        )
        info(f"....Call stored procedure: {query}")
        # self.__execute_query(query=query)
        info("END classify.")

    def clear_tags(self):
        """
        Routine to clear all tags
        """
        self.clear_pii_tags()
        self.clear_mnpi_tags()

    def __execute_query(self, query: str):
        """
        Execute SQL query
        """
        try:
            connection = self.__connect()
            connection.execute(statement=query)
        except Exception as e:
            error(f".... ERROR with executing query:  {e.__class__.__name__} - {e}")
            error(f".... QUERY: {query}")
        finally:
            self.__dispose()

    def upload_pii_data(self):
        """
        Upload PII data
        """
        info(".... START upload_pii_data.")
        self.__execute_query(query=self.pii_query)
        info(".... END upload_pii_data.")

    def delete_data(self):
        """
        Delete data from the table
        """
        info(".... START deleting data.")
        self.__execute_query(query=self.delete_data_query)
        info(".... END deleting data.")

    def update_mnpi_metadata(self):
        """
        Update timestamp data for MNPI
        as initially we do not have it
        """
        info(".... START update MNPI metadata.")
        self.__execute_query(query=self.mnpi_metadata_update_query)
        info(".... END update MNPI metadata.")

    def upload_mnpi_data(self):
        """
        Upload MNPI data
        """
        info(".... START upload_mnpi_data.")
        self.upload_to_snowflake()
        self.update_mnpi_metadata()
        info(".... END upload_mnpi_data.")

    def extract(self):
        """
        Routine to extract objects needed for tagging
        """
        info("START extract.")
        self.delete_data()
        self.upload_pii_data()
        self.upload_mnpi_data()
        info("END extract.")
