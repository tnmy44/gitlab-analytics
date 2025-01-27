#!/usr/bin/env python3
import json
import argparse
from os import environ as env
from typing import Dict, List

from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError
from loguru import logger
from gitlabdata.orchestration_utils import query_executor
from gitlabdata.orchestration_utils import data_science_engine_factory

from simple_dependency_resolver.simple_dependency_resolver import DependencyResolver


class DbtModelClone:
    """"""

    def __init__(self, config_vars: Dict):
        self.environment = config_vars["ENVIRONMENT"].upper()
        if self.environment == "CI":
            self.engine = create_engine(
                URL(
                    user=config_vars["SNOWFLAKE_USER"],
                    password=config_vars["SNOWFLAKE_PASSWORD"],
                    account=config_vars["SNOWFLAKE_ACCOUNT"],
                    role=config_vars["SNOWFLAKE_SYSADMIN_ROLE"],
                    warehouse=config_vars["SNOWFLAKE_LOAD_WAREHOUSE"],
                )
            )

            # Snowflake database name should be in CAPS
            # see https://gitlab.com/meltano/analytics/issues/491
            self.branch_name = config_vars["GIT_BRANCH"].upper()

        elif self.environment == "LOCAL_BRANCH":
            self.engine = data_science_engine_factory()
            # Snowflake database name should be in CAPS
            # see https://gitlab.com/meltano/analytics/issues/491
            self.branch_name = config_vars["GIT_BRANCH"].upper()

        elif self.environment == "LOCAL_USER":
            self.engine = data_science_engine_factory()

            self.branch_name = self.engine.url.database.replace("/", "").upper()

        self.prep_database = f"{self.branch_name}_PREP"
        self.prod_database = f"{self.branch_name}_PROD"
        self.raw_database = f"{self.branch_name}_RAW"

    def create_schema(self, schema_name: str):
        """

        :param schema_name:
        :return:
        """
        query = f"""CREATE SCHEMA IF NOT EXISTS {schema_name};"""
        query_executor(self.engine, query)

        grants_query = f"""GRANT ALL ON SCHEMA {schema_name} TO TRANSFORMER;"""
        query_executor(self.engine, grants_query)

        grants_query = f"""GRANT ALL ON SCHEMA {schema_name} TO GITLAB_CI"""
        query_executor(self.engine, grants_query)

        return True

    def clean_dbt_input(self, model_input: List[str]) -> List[Dict]:
        """

        :param model_input:
        :return:
        """
        joined = " ".join(model_input)

        delimeter = '{"database":'

        input_list = [delimeter + x for x in joined.split(delimeter) if x]

        list_of_dicts = []
        for i in input_list:
            loaded_dict = json.loads(i)
            actual_dependencies = [
                n for n in loaded_dict.get("depends_on").get("nodes") if "seed" not in n
            ]
            loaded_dict["actual_dependencies"] = actual_dependencies
            list_of_dicts.append(loaded_dict)

        sorted_output = []

        for i in list_of_dicts:
            sorted_output.append(
                {"id": i.get("unique_id"), "dependencies": i.get("actual_dependencies")}
            )

        dep_resolver = DependencyResolver()
        resolved_dependencies = dep_resolver.simple_resolution(sorted_output)
        sorted_list = []

        for resolved_dependency in resolved_dependencies:
            for i in list_of_dicts:
                if i.get("unique_id") == resolved_dependency.name:
                    sorted_list.append(i)

        return sorted_list

    def grant_table_view_rights(self, object_type: str, object_name: str) -> None:
        """
            Right type can be table or view
        :param object_type:
        :param object_name:
        :return:
        """
        if self.environment in ("LOCAL_USER", "LOCAL_BRANCH"):
            grants_query = f"""
                        GRANT ALL ON {object_type.upper()} {object_name.upper()} TO TRANSFORMER 
                        """
        else:
            grants_query = f"""
                GRANT OWNERSHIP ON {object_type.upper()} {object_name.upper()} TO TRANSFORMER REVOKE CURRENT GRANTS
                """
        query_executor(self.engine, grants_query)

        grants_query = (
            f"""GRANT ALL ON {object_type.upper()} {object_name.upper()} TO GITLAB_CI"""
        )
        query_executor(self.engine, grants_query)

    def clean_view_dll(self, table_name: str, dll_input: str) -> str:
        """
        Essentially, this code is finding and replacing the DB name in only the first line for recreating
        views. This is because we have a database & schema named PREP, which creates a special case in the
        rest of the views they are replaced completely.

        :param table_name:
        :param dll_input:
        :return:
        """
        split_file = dll_input.splitlines()

        first_line = split_file[0]

        new_first_line = f"{first_line[:dll_input.find('view')]}view {table_name} ("
        replaced_file = [
            f.replace('"PREP".', f'"{self.prep_database}".').replace(
                '"PROD".', f'"{self.prod_database}".'
            )
            for f in split_file
        ]
        joined_lines = "\n".join(replaced_file[1:])

        output_query = new_first_line + "\n" + joined_lines

        return output_query

    def clone_dbt_models(self, model_input: List[str]):
        """

        :param model_input:
        :return:
        """

        sorted_list = self.clean_dbt_input(model_input)

        # Total number of models to process
        total_models = len(sorted_list)
        current_model = 0  # Initializing the current model counter

        for i in sorted_list:
            current_model += 1
            database_name = i.get("database").upper()
            schema_name = i.get("schema").upper()
            table_name = i.get("name").upper()
            alias = i.get("alias").upper()

            if "PROD" in database_name:
                database_name = "PROD"

            if "PREP" in database_name:
                database_name = "PREP"

            if alias:
                table_name = alias.upper()

            full_name = f""""{database_name}"."{schema_name}"."{table_name}" """.strip()

            output_table_name = f""""{self.branch_name}_{full_name[1:]}"""
            output_schema_name = output_table_name.replace(f'."{table_name}"', "")

            logger.info(
                f"Processing {output_table_name}: {current_model}/{total_models} (current/total)"
            )

            query = f"""
                SELECT
                    TABLE_TYPE,
                    IS_TRANSIENT
                FROM {database_name}.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = UPPER('{schema_name}')
                AND TABLE_NAME = UPPER('{table_name}')
            """
            res = query_executor(self.engine, query)
            try:
                table_or_view = res[0][0]
            except IndexError:
                logger.warning(
                    f"Table/view {output_table_name} does not exist in PROD yet and must be created with "
                    f"regular dbt"
                )
                continue

            self.create_schema(output_schema_name)

            if table_or_view == "VIEW":
                query = (
                    f"""SELECT GET_DDL('VIEW', '{full_name.replace('"', '')}', TRUE)"""
                )
                res = query_executor(self.engine, query)

                base_dll = res[0][0]

                output_query = self.clean_view_dll(output_table_name, base_dll)
                try:
                    query_executor(self.engine, output_query)
                    self.grant_table_view_rights("view", output_table_name)
                    logger.info(f"{output_table_name} successfully created. ")
                except ProgrammingError as error:
                    logger.warning(f"Problem processing {output_table_name}")
                    logger.warning(str(error))

                continue

            transient_table = res[0][1]

            try:
                clone_statement = f"CREATE OR REPLACE {'TRANSIENT' if transient_table == 'YES' else ''} TABLE {output_table_name} CLONE {full_name} COPY GRANTS;"

                query_executor(self.engine, clone_statement)
                self.grant_table_view_rights("table", output_table_name)
                logger.info(f"{output_table_name} successfully created. ")
            except ProgrammingError as error:
                logger.warning(f"Problem processing {output_table_name}")
                logger.warning(str(error))
                continue


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("INPUT", nargs="+")
    args = parser.parse_args()

    cloner = DbtModelClone(env.copy())
    cloner.clone_dbt_models(args.INPUT)
