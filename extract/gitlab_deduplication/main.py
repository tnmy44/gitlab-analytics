""" This the transformation code for deduplication of data of postgres"""
import logging
from os import environ as env
from datetime import datetime
from fire import Fire
from yaml import safe_load, YAMLError
from typing import Dict
from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    query_executor,
)


def build_table_name(
    table_name: str, table_prefix: str = None, table_suffix: str = None
) -> str:
    """The function is responsible for adding prefix and suffix"""
    if table_prefix is None and table_suffix is None:
        return table_name
    elif table_prefix is None and table_suffix is not None:
        return table_name + table_suffix
    elif table_prefix is not None and table_suffix is None:
        return table_prefix + table_name
    else:
        return f"{table_prefix}{table_name}{table_suffix}"


def create_table_name(manifest_dict: Dict, table_name: str) -> tuple:
    """Prepare the backup table name and original table name as the table name passed in manifest i postgres table name not the snowflake table name"""
    table_suffix = "_" + datetime.now().strftime("%Y%m%d")
    table_prefix = manifest_dict["generic_info"]["table_prefix"]

    if table_prefix:
        bkp_table_name = build_table_name(table_name, table_prefix, table_suffix)
        original_table_name = build_table_name(table_name, table_prefix)
    else:
        bkp_table_name = build_table_name(table_name, table_suffix)
        original_table_name = build_table_name(table_name)
    return bkp_table_name, original_table_name


def create_backup_table(
    manifest_dict: dict,
    table_name: str,
) -> bool:
    """This function create clone of the table in the backup schema , if the backup has been taken today it self it will abort here."""
    raw_database = manifest_dict["raw_database"]
    backup_schema_name = manifest_dict["generic_info"]["backup_schema"]
    raw_schema = manifest_dict["generic_info"]["raw_schema"]
    bkp_table_name, original_table_name = create_table_name(manifest_dict, table_name)
    create_backup_table = f"CREATE TABLE {raw_database}.{backup_schema_name}.{bkp_table_name} CLONE {raw_database}.{raw_schema}.{original_table_name};"
    logging.info(f"create_backup_table")
    query_executor(snowflake_engine, create_backup_table)
    return True


def create_temp_table_ddl(manifest_dict: Dict, table_name: str):
    raw_database = manifest_dict["raw_database"]
    raw_schema = manifest_dict["generic_info"]["raw_schema"]
    bkp_table_name, original_table_name = create_table_name(manifest_dict, table_name)
    backup_schema = manifest_dict["generic_info"]["backup_schema"]
    build_ddl_statement = f"""
                             SELECT CONCAT('CREATE TRANSIENT TABLE {raw_database}.{raw_schema}.{original_table_name}_temp\n AS (' ,'SELECT  \n', 
                                    LISTAGG(
                                            CASE 
                                            WHEN LOWER(column_name) = '_uploaded_at' THEN 'MIN( _uploaded_at) AS _uploaded_at'
                                            WHEN LOWER(column_name) = '_task_instance' THEN 'MAX(_task_instance)  AS _task_instance'
                                            ELSE column_name 
                                            END ,',\n') WITHIN GROUP (ORDER BY ordinal_position ASC) 
                                            , ' \n FROM {raw_database}.{backup_schema}.{bkp_table_name}' ,
                             '\n GROUP BY  \n', 
                                    (SELECT LISTAGG(column_name,',\n') WITHIN GROUP (ORDER BY ordinal_position ASC) FROM {raw_database}.information_schema.columns 
                                    WHERE LOWER(table_name) = ('{original_table_name}')   
                                    AND LOWER(column_name) NOT IN ('_uploaded_at','_task_instance')),
                             ' \r\n UNION \n',
                                'SELECT  \n', 
                                    LISTAGG(
                                            CASE 
                                            WHEN LOWER(column_name) = '_uploaded_at' THEN 'MAX( _uploaded_at) AS _uploaded_at'
                                            WHEN LOWER(column_name) = '_task_instance' THEN 'MAX(_task_instance)  AS _task_instance'
                                            ELSE column_name 
                                            END ,',\n') WITHIN GROUP (ORDER BY ordinal_position ASC),
                             '\n FROM {raw_database}.{backup_schema}.{bkp_table_name}' ,
                             '\n GROUP BY  ', 
                                   (SELECT LISTAGG(column_name,',\n') WITHIN GROUP (ORDER BY ordinal_position ASC) FROM {raw_database}.information_schema.columns 
                                    WHERE LOWER(table_name) = ('{original_table_name}')   
                                    AND LOWER(column_name) NOT IN ('_uploaded_at','_task_instance')),');')
                                    from {raw_database}.information_schema.columns 
                                    where LOWER(table_name) = '{original_table_name}';"""

    # Create the temporary table definition
    table_definition_to_create_table = query_executor(
        snowflake_engine, build_ddl_statement
    )
    logging.info(table_definition_to_create_table)
    logging.info(type(table_definition_to_create_table))
    return table_definition_to_create_table[0]


def create_temp_table(manifest_dict: Dict, table_name: str):
    """This function create the table in the schema"""
    table_definition = create_temp_table_ddl(manifest_dict, table_name)
    query_executor(snowflake_engine, table_definition)
    return True


def swap_and_drop_temp_table(manifest_dict: Dict, table_name: str):
    raw_database = manifest_dict["raw_database"]
    raw_schema = manifest_dict["generic_info"]["raw_schema"]
    bkp_table_name, original_table_name = create_table_name(manifest_dict, table_name)
    # Swap the table name with main table.
    temp_table_name = f"{original_table_name}_temp"
    swap_query = f"ALTER TABLE {raw_database}.{raw_schema}.{temp_table_name} SWAP WITH {raw_database}.{raw_schema}.{original_table_name};"
    # Drop the temp table in tap_postgres schema
    drop_query = f"DROP TABLE {raw_database}.{raw_schema}.{temp_table_name};"
    if query_executor(snowflake_engine, swap_query):
        query_executor(snowflake_engine, drop_query)


def get_yaml_file(path: str):
    """
    Get all the report name for which tasks for loading
    needs to be created
    """

    with open(file=path, mode="r", encoding="utf-8") as file:
        try:
            loaded_file = safe_load(stream=file)
            return loaded_file
        except YAMLError as exc:
            logging.error(f"Issue with the yaml file: {exc}")
            return None


def main(file_name: str, table_name: str) -> None:
    """
    Read table name FROM manifest file and decide if the table exist in the database. Check if the advance metadata column `_task_instance`
    is present in the table.
    Check for backup schema is present in snowflake.
    Check for old backup table in snowflake and if present check if the creation date is older than 15 days. If yes drop backup table if not create a new backup table.
    Once the backup is complete, create  table with deduplicate records.
    Swap the table with original table.
    Drop the swapped temp table.
    """
    # Process the manifest
    logging.info(f"Proceeding with table {table_name} for deduplication")
    manifest_dict = get_yaml_file(path=file_name)
    # Update database name to the manifest for in case of MR branch.
    manifest_dict.update({"raw_database": env.copy()["SNOWFLAKE_LOAD_DATABASE"]})
    print(manifest_dict)
    # Create backup table
    create_clone_table = create_backup_table(manifest_dict, table_name)
    # validate if backup created successfully then create the temp table.
    if create_clone_table:
        table_definition = create_temp_table(manifest_dict, table_name)
    if table_definition:
        swap_and_drop_temp_table(manifest_dict, table_name)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("snowflake.connector.cursor").disabled = True
    logging.getLogger("snowflake.connector.connection").disabled = True
    config_dict = env.copy()
    snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")
    Fire({"deduplication": main})
