""" Various util functions used by kantata.py """

import re
from datetime import datetime
from logging import info
from os import environ
from zoneinfo import ZoneInfo

from gitlabdata.orchestration_utils import (
    query_executor,
    snowflake_engine_factory,
    snowflake_stage_load_copy_remove,
)
from pandas import api as pd_api, DataFrame, read_sql
from sqlalchemy import Column, MetaData, Table, func
from sqlalchemy.engine.base import Engine
from sqlalchemy.schema import CreateTable, DropTable
from sqlalchemy.types import Boolean, DateTime, Float, Integer, String

from args import parse_arguments

SCHEMA_NAME = "kantata"
STAGE_NAME = "kantata_stage"
config_dict = environ.copy()
HEADERS = {"Authorization": f"Bearer {config_dict.get('KANTATA_OAUTH_TOKEN', '')}"}


def process_args() -> list:
    """returns command line args passed in by user"""
    args = parse_arguments()
    if args.reports:
        return args.reports
    raise ValueError(
        "Please provide the report names using the --reports flag, e.g., --reports report1 report2"
    )


def convert_timezone(
    input_datetime_str: str, from_tz: str = "US/Pacific", to_tz: str = "UTC"
) -> str:
    """
    An example of input_datetime_str: '2024-07-08T09:00:48-07:00'
    Kantata response is in PST timezone, need to convert to UTC
    """

    # Parse the string using datetime
    try:
        dt = datetime.fromisoformat(input_datetime_str)
    except ValueError:
        raise
    dt_from_tz = dt.replace(tzinfo=ZoneInfo(from_tz))
    dt_to_tz = dt_from_tz.astimezone(ZoneInfo(to_tz))
    return dt_to_tz.isoformat()


def clean_string(string_input: str) -> str:
    """
    Clean any string by only keep a-Z, 0-9
    Used for the following:
    - Convert Kantata Report name to Snowflake table name

    Cleaning steps:
    - Replace '-' with '_'
    - Replace whitespace with '_'
    - Remove all non-letter/number characters except '_'
    - Ensure the name doesn't start or end with '_'
    """
    patterns = {
        r"[^a-zA-Z0-9_]": "_",
        r"_+": "_",
    }

    cleaned_string = string_input.lower()
    for find, replace in patterns.items():
        cleaned_string = re.sub(find, replace, cleaned_string)

    cleaned_string = cleaned_string.strip("_")
    return cleaned_string


def add_csv_file_extension(prefix: str) -> str:
    """Add file extension"""
    return f"{prefix}.csv.gz"


def map_dtypes(dtype):
    """Function to map pandas dtypes to SQLAlchemy types"""
    if pd_api.types.is_integer_dtype(dtype):
        return Integer
    if pd_api.types.is_float_dtype(dtype):
        return Float
    if pd_api.types.is_bool_dtype(dtype):
        return Boolean
    if pd_api.types.is_datetime64_any_dtype(dtype):
        return DateTime
    return String


def have_columns_changed(
    snowflake_engine: Engine, df: DataFrame, snowflake_table_name: str
) -> bool:
    """
    Check if schema has changed between API report and Snowflake table
    """
    api_columns = list(df.columns) + ["uploaded_at"]
    source_query = f"select * from {snowflake_table_name} limit 1;"
    snowflake_columns = read_sql(
        sql=source_query,
        con=snowflake_engine,
    ).columns
    is_column_change = sorted(api_columns) != sorted(snowflake_columns)
    if is_column_change:
        info("Column(s) have changed")
    return is_column_change


def seed_kantata_table(
    snowflake_engine: Engine, df: DataFrame, snowflake_table_name: str
):
    """
    Create an empty Snowflake table based on the dtypes of the pandas df
    """
    info(
        f"Either table does not exist, or schema has changed... \
        Creating table: {snowflake_table_name}"
    )
    snowflake_types = [
        Column(col, map_dtypes(dtype)) for col, dtype in df.dtypes.items()
    ]
    snowflake_types.append(
        Column("uploaded_at", DateTime, server_default=func.current_timestamp())
    )  # Add timestamp column with default value
    table = Table(snowflake_table_name, MetaData(), *snowflake_types)

    # Drop table if it already exists (in the case of schema change)
    if snowflake_engine.has_table(snowflake_table_name):
        query_executor(snowflake_engine, DropTable(table))
    query_executor(snowflake_engine, CreateTable(table))


def get_snowflake_columns_str(columns):
    """Format the list of columns to a single string for COPY INTO"""
    columns = [f'"{column}"' for column in columns]
    return f"({', '.join(columns)})"


def upload_kantanta_to_snowflake(
    df: DataFrame, snowflake_table_name: str, upload_file_name: str
):
    """
    Check if Snowflake table exists; if not, seed the table.
    Then run snowflake_stage_load_copy_remove() on the csv file to upload
    """

    snowflake_engine = snowflake_engine_factory(
        config_dict, "LOADER", schema=SCHEMA_NAME
    )
    if not snowflake_engine.has_table(snowflake_table_name) or have_columns_changed(
        snowflake_engine, df, snowflake_table_name
    ):
        seed_kantata_table(snowflake_engine, df, snowflake_table_name)

    snowflake_columns_str = get_snowflake_columns_str(df.columns)
    file_format_options = (
        """FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 COMPRESSION = 'GZIP'"""
    )
    info(f"Running COPY INTO  snowflake_table_name: {snowflake_table_name}")
    snowflake_stage_load_copy_remove(
        upload_file_name,
        f"{SCHEMA_NAME}.{STAGE_NAME}",
        f"{SCHEMA_NAME}.{snowflake_table_name}",
        snowflake_engine,
        type="CSV",
        file_format_options=file_format_options,
        col_names=snowflake_columns_str,
    )
