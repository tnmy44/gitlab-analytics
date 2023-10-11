import logging
import os
import sys
import tempfile
from datetime import datetime, timedelta
from typing import Dict, List, Generator, Any, Tuple, Optional
import yaml

from gitlabdata.orchestration_utils import (
    dataframe_enricher,
    snowflake_engine_factory,
    query_executor,
)
import pandas as pd
import sqlalchemy
from google.cloud import storage
from google.cloud.storage.bucket import Bucket
from google.oauth2 import service_account
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Boolean,
    Date,
    Float,
    DateTime,
    Table,
)
from sqlalchemy.engine.base import Engine
from sqlalchemy.schema import CreateTable, DropTable


METADATA_SCHEMA = os.environ.get("GITLAB_METADATA_SCHEMA")
BUCKET_NAME = os.environ.get("GITLAB_BACKFILL_BUCKET")
BACKFILL_METADATA_TABLE = "backfill_metadata"
INCREMENTAL_METADATA_TABLE = "incremental_metadata"
DELETE_METADATA_TABLE = "delete_metadata"
BACKFILL_EXTRACT_CHUNKSIZE = 15_000_000
CSV_CHUNKSIZE_BACKFILL = 5_000_000
CSV_CHUNKSIZE_REGULAR = 1_000_000
INCREMENTAL_LOAD_TYPE_BY_ID = "load_by_id"


def get_gcs_scoped_credentials():
    """Get scoped credential"""
    # create the credentials object
    keyfile = yaml.load(os.environ["GCP_SERVICE_CREDS"], Loader=yaml.FullLoader)
    credentials = service_account.Credentials.from_service_account_info(keyfile)

    scope = ["https://www.googleapis.com/auth/cloud-platform"]
    scoped_credentials = credentials.with_scopes(scope)
    return scoped_credentials


def get_gcs_bucket() -> Bucket:
    """Do the auth and return a usable gcs bucket object."""
    scoped_credentials = get_gcs_scoped_credentials()
    storage_client = storage.Client(credentials=scoped_credentials)
    return storage_client.get_bucket(BUCKET_NAME)


def upload_to_gcs(
    advanced_metadata: bool, upload_df: pd.DataFrame, upload_file_name: str
) -> bool:
    """
    Write a dataframe to local storage and then upload it to a GCS bucket.
    """
    bucket = get_gcs_bucket()

    # Write out the parquet and upload it
    enriched_df = dataframe_enricher(advanced_metadata, upload_df)
    os.makedirs(
        os.path.dirname(upload_file_name), exist_ok=True
    )  # need to create director(ies) prior to to_parquet()
    enriched_df.to_parquet(
        upload_file_name,
        compression="gzip",
        index=False,
    )
    logging.info(f"GCS save location: {upload_file_name}")
    blob = bucket.blob(upload_file_name)
    blob.upload_from_filename(upload_file_name)

    return True


def get_internal_identifier_keys(identifiers: list) -> list:
    """
    Get a list of current internal GitLab project or namespace keys from dbt seed files
    """
    dbt_seed_data_path = "https://gitlab.com/gitlab-data/analytics/-/raw/master/transform/snowflake-dbt/data"

    internal_identifiers = {
        "project_id": [
            "projects_part_of_product_ops.csv",
            "projects_part_of_product.csv",
        ],
        "project_path": [
            "projects_part_of_product_ops.csv",
            "projects_part_of_product.csv",
        ],
        "namespace_id": ["internal_gitlab_namespaces.csv"],
        "namespace_path": ["internal_gitlab_namespaces.csv"],
    }

    internal_identifier_keys = []

    for identifier in identifiers:
        seed_files = internal_identifiers[identifier]

        for seed_file in seed_files:
            file_location = f"{dbt_seed_data_path}/{seed_file}"
            df = pd.read_csv(file_location)
            internal_identifier_keys.extend(list(df[identifier]))

    return internal_identifier_keys


def trigger_snowflake_upload(
    engine: Engine, table: str, upload_file_name: str, purge: bool = False
) -> None:
    """Trigger Snowflake to upload a tsv file from GCS."""
    logging.info("Loading from GCS into SnowFlake")

    purge_opt = "purge = true" if purge else ""

    upload_query = f"""
        copy into {table}
        from 'gcs://{BUCKET_NAME}'
        storage_integration = gcs_integration
        pattern = '{upload_file_name}'
        {purge_opt}
        file_format = (type = parquet)
        match_by_column_name = case_insensitive;
    """
    logging.info(f"\nupload_query: {upload_query}")
    results = query_executor(engine, upload_query)
    total_rows = 0

    for result in results:
        if result[1] == "LOADED":
            total_rows += result[2]

    log_result = f"Loaded {total_rows} rows from {len(results)} files"
    logging.info(log_result)


def postgres_engine_factory(
    connection_dict: Dict[str, str], env: Dict[str, str]
) -> Engine:
    """
    Create a postgres engine to be used by pandas.
    """

    # Set the Vars
    user = env[connection_dict["user"]]
    password = env[connection_dict["pass"]]
    host = env[connection_dict["host"]]
    database = env[connection_dict["database"]]
    port = env[connection_dict["port"]]

    # Inject the values to create the engine
    engine = create_engine(
        f"postgresql://{user}:{password}@{host}:{port}/{database}",
        connect_args={"sslcompression": 0, "options": "-c statement_timeout=9000000"},
    )
    logging.info(engine)
    return engine


def manifest_reader(file_path: str) -> Dict[str, Dict]:
    """
    Read a yaml manifest file into a dictionary and return it.
    """

    with open(file_path, "r") as file:
        manifest_dict = yaml.load(file, Loader=yaml.FullLoader)

    return manifest_dict


def query_results(query: str, engine: Engine) -> pd.DataFrame:
    """
    Use pandas to run a sql query and load it into a dataframe.
    Yield it back in chunks for scalability.
    """

    try:
        query_df = pd.read_sql(sql=query, con=engine)
    except Exception as e:
        logging.exception(e)
        sys.exit(1)
    return query_df


def transform_dataframe_column(column_name: str, pg_type: str) -> List[Column]:
    if pg_type == "timestamp with time zone":
        return Column(column_name, DateTime)
    elif pg_type in ("integer", "smallint", "numeric", "bigint"):
        return Column(column_name, Integer)
    elif pg_type == "date":
        return Column(column_name, Date)
    elif pg_type == "boolean":
        return Column(column_name, Boolean)
    elif pg_type in ("float", "double precision"):
        return Column(column_name, Float)
    else:
        return Column(column_name, String)


def get_postgres_types(table_name: str, source_engine: Engine) -> Dict[str, str]:
    query = f"""
      SELECT column_name, data_type
      FROM information_schema.columns
      WHERE table_name = '{table_name}'
    """
    query_results = query_executor(source_engine, query)
    type_dict = {}
    for result in query_results:
        type_dict[result[0]] = result[1]
    return type_dict


def transform_source_types_to_snowflake_types(
    df: pd.DataFrame, source_table_name: str, source_engine: Engine
) -> List[Column]:
    pg_types = get_postgres_types(source_table_name, source_engine)

    # defaulting to string for any renamed columns or results of functions -- can be cast downstream in dbt source model
    table_columns = [
        transform_dataframe_column(column, pg_types.get(column, "string"))
        for column in df
    ]
    return table_columns


def seed_table(
    advanced_metadata: bool,
    snowflake_types: List[Column],
    target_table_name: str,
    target_engine: Engine,
) -> None:
    """
    Sets the proper data types and column names.
    """
    logging.info(f"Creating table {target_table_name}")
    snowflake_types.append(Column("_uploaded_at", Float))
    if advanced_metadata:
        snowflake_types.append(Column("_task_instance", String))
    table = Table(target_table_name, sqlalchemy.MetaData(), *snowflake_types)
    if target_engine.has_table(target_table_name):
        query_executor(target_engine, DropTable(table))
    query_executor(target_engine, CreateTable(table))
    logging.info(f"{target_table_name} created")


def chunk_and_upload(
    query: str,
    source_engine: Engine,
    target_engine: Engine,
    target_table: str,
    source_table: str,
    advanced_metadata: bool = False,
    backfill: bool = False,  # this is needed for scd load
) -> None:
    """
    Call the functions that upload the dataframes as TSVs in GCS and then trigger Snowflake
    to load those new files.

    Each chunk is uploaded to GCS with a suffix of which chunk number it is.
    All of the chunks are uploaded by using a regex that gets all of the files.
    """

    rows_uploaded = 0
    prefix = f"staging/regular/{target_table}/{target_table}_chunk".lower()
    extension = ".parquet.gzip"

    with tempfile.TemporaryFile() as tmpfile:
        iter_csv = read_sql_tmpfile(
            query, source_engine, tmpfile, CSV_CHUNKSIZE_REGULAR
        )

        for idx, chunk_df in enumerate(iter_csv):
            if backfill:
                schema_types = transform_source_types_to_snowflake_types(
                    chunk_df, source_table, source_engine
                )
                seed_table(advanced_metadata, schema_types, target_table, target_engine)
                backfill = False

            row_count = chunk_df.shape[0]
            rows_uploaded += row_count

            upload_file_name = f"{prefix}{str(idx)}{extension}"
            if row_count > 0:
                upload_to_gcs(advanced_metadata, chunk_df, upload_file_name)
                logging.info(f"Uploaded {row_count} to GCS in {upload_file_name}")

    if rows_uploaded > 0:
        trigger_snowflake_upload(
            target_engine,
            target_table,
            f"{prefix}.*{extension}$",
            purge=True,
        )
        logging.info(f"Uploaded {rows_uploaded} total rows to table {target_table}.")

    target_engine.dispose()
    source_engine.dispose()


def write_metadata(
    metadata_engine: Engine,
    metadata_table: Engine,
    database_name: str,
    table_name: str,
    initial_load_start_date: datetime,
    upload_date: datetime,
    upload_file_name: str,
    last_extracted_id: int,
    max_id: int,
    is_export_completed: bool,
    chunk_row_count: int,
) -> None:
    """Write status of backfill to postgres"""

    insert_query = f"""
        INSERT INTO {METADATA_SCHEMA}.{metadata_table} (
            database_name,
            table_name,
            initial_load_start_date,
            upload_date,
            upload_file_name,
            last_extracted_id,
            max_id,
            is_export_completed,
            chunk_row_count
        )
        VALUES (
            '{database_name}',
            '{table_name}',
            '{initial_load_start_date}',
            '{upload_date}',
            '{upload_file_name}',
            {last_extracted_id},
            {max_id},
            {is_export_completed},
            {chunk_row_count}
        );
    """
    with metadata_engine.connect() as connection:
        connection.execute(insert_query)

    logging.info(f"Wrote to {metadata_table} table for: {upload_file_name}")


def get_prefix(
    staging_or_processed, load_by_id_export_type, table, initial_load_prefix
) -> str:
    """
    Returns something like this:
    staging/backfill_data/alert_management_http_integrations/initial_load_start_2023-04-07t16:50:28.132
    """
    return (
        f"{staging_or_processed}/{load_by_id_export_type}/{table}/{initial_load_prefix}"
    ).lower()


def get_initial_load_prefix(initial_load_start_date):
    initial_load_prefix = f"initial_load_start_{initial_load_start_date.isoformat(timespec='milliseconds')}".lower()
    return initial_load_prefix


def get_upload_file_name(
    load_by_id_export_type: str,
    table: str,
    initial_load_start_date: datetime,
    upload_date: datetime,
    version: str = None,
    filetype: str = "parquet",
    compression: str = "gzip",
    filename_template: str = "{timestamp}_{table}{version}.{filetype}.{compression}",
) -> str:
    """Generate a unique and descriptive filename for uploading data to cloud storage.

    Args:
        table (str): The name of the table.
        initial_load_start_date (datetime): When load started
        version (str, optional): The version of the data. Defaults to None.
        filetype (str, optional): The file format. Defaults to 'parquet'.
        compression (str, optional): The compression method. Defaults to 'gzip'.
            Defaults to get_prefix()'s template
        filename_template (str, optional): The filename template.
            Defaults to '{timestamp}_{table}_{version}.{filetype}.{compression}'.

    Returns:
        str: The upload name with the folder structure and filename.
    """
    # Format folder structure
    initial_load_prefix = get_initial_load_prefix(initial_load_start_date)
    prefix = get_prefix(
        staging_or_processed="staging",
        load_by_id_export_type=load_by_id_export_type,
        table=table,
        initial_load_prefix=initial_load_prefix,
    )

    # Format filename
    timestamp = upload_date.isoformat(timespec="milliseconds")
    if version is None:
        version = ""
    else:
        version = f"_{version}"
    filename = filename_template.format(
        timestamp=timestamp,
        table=table,
        version=version,
        filetype=filetype,
        compression=compression,
    )

    # Combine folder structure and filename
    return os.path.join(prefix, filename).lower()


def upload_initial_load_prefix_to_snowflake(
    target_engine,
    database_kwargs,
    load_by_id_export_type,
    initial_load_start_date,
    purge: bool = True,
):
    """
    From GCS bucket, upload all files from a
    initial_load_start_date prefix -> Snowflake
    """
    prefix = get_prefix(
        staging_or_processed="staging",
        load_by_id_export_type=load_by_id_export_type,
        table=database_kwargs["real_target_table"],
        initial_load_prefix=get_initial_load_prefix(initial_load_start_date),
    )
    logging.info(
        f"Beginning COPY INTO from GCS to Snowflake table '{database_kwargs['target_table']}'"
    )
    # don't purge files, will do after swap
    trigger_snowflake_upload(
        target_engine,
        database_kwargs["target_table"],
        f"{prefix}/.*.parquet.gzip$",
        purge,
    )
    logging.info(
        f"Finished COPY INTO from GCS to Snowflake table '{database_kwargs['target_table']}'"
    )


def seed_and_upload_snowflake(
    target_engine,
    chunk_df,
    database_kwargs,
    load_by_id_export_type,
    advanced_metadata,
    initial_load_start_date,
):
    """
    Seed (create a new table in Snowflake with correct schema)
    and then upload the data from GCS -> Snowflake
    """
    if "temp" not in database_kwargs["target_table"].lower():
        raise ValueError(
            f"Target table {database_kwargs['target_table']} is NOT a TEMP table, aborting upload to Snowflake"
        )

    schema_types = transform_source_types_to_snowflake_types(
        chunk_df,
        database_kwargs["source_table"],
        database_kwargs["source_engine"],
    )
    seed_table(
        advanced_metadata,
        schema_types,
        database_kwargs["target_table"],
        target_engine,
    )

    upload_initial_load_prefix_to_snowflake(
        target_engine, database_kwargs, load_by_id_export_type, initial_load_start_date
    )

    # We do the swap here because snowflake engine instantiated here
    swap_temp_table(
        target_engine,
        database_kwargs["real_target_table"],
        database_kwargs["target_table"],
    )

    logging.info(
        f"Finished swapping tables to Snowflake table '{database_kwargs['real_target_table']}'"
    )


def upload_to_snowflake_after_extraction(
    chunk_df,
    database_kwargs,
    load_by_id_export_type,
    initial_load_start_date,
    advanced_metadata,
):
    # need to re-instantiate to avoid client session time-out
    target_engine = snowflake_engine_factory(
        os.environ.copy(), role="LOADER", schema="tap_postgres"
    )

    if load_by_id_export_type == INCREMENTAL_LOAD_TYPE_BY_ID:
        # upload directly to snowflake if incremental
        upload_initial_load_prefix_to_snowflake(
            target_engine,
            database_kwargs,
            load_by_id_export_type,
            initial_load_start_date,
        )
    else:
        # else need to create 'temp' table first
        seed_and_upload_snowflake(
            target_engine,
            chunk_df,
            database_kwargs,
            load_by_id_export_type,
            advanced_metadata,
            initial_load_start_date,
        )
    database_kwargs["source_engine"].dispose()
    target_engine.dispose()


def chunk_and_upload_metadata(
    query: str,
    primary_key: str,
    max_source_id: int,
    initial_load_start_date: datetime,
    database_kwargs: Dict[Any, Any],
    load_by_id_export_type: str,
    advanced_metadata: bool = False,
) -> datetime:
    """
    Similiar to chunk_and_upload(), with the following differences:
        - After each upload to GCS, write to metadata table
        - COPY to Snowflake after all files have been uploaded to GCS
    """
    rows_uploaded = 0

    with tempfile.TemporaryFile() as tmpfile:
        iter_csv = read_sql_tmpfile(
            query,
            database_kwargs["source_engine"],
            tmpfile,
            CSV_CHUNKSIZE_BACKFILL,
        )

        for chunk_df in iter_csv:
            row_count = chunk_df.shape[0]
            rows_uploaded += row_count
            last_extracted_id = chunk_df[primary_key].max()
            logging.info(
                f"\nlast_extracted_id for current Postgres chunk: {last_extracted_id}"
            )

            upload_date = datetime.now()
            if initial_load_start_date is None:
                initial_load_start_date = upload_date

            upload_file_name = get_upload_file_name(
                load_by_id_export_type,
                database_kwargs["real_target_table"],
                initial_load_start_date,
                upload_date,
            )

            if row_count > 0:
                upload_to_gcs(advanced_metadata, chunk_df, upload_file_name)
                logging.info(f"Uploaded {row_count} rows to GCS in {upload_file_name}")
                is_export_completed = last_extracted_id >= max_source_id

                # upload to Snowflake before writing metadata=complete for safety
                if is_export_completed:
                    upload_to_snowflake_after_extraction(
                        chunk_df,
                        database_kwargs,
                        load_by_id_export_type,
                        initial_load_start_date,
                        advanced_metadata,
                    )

                write_metadata(
                    database_kwargs["metadata_engine"],
                    database_kwargs["metadata_table"],
                    database_kwargs["source_database"],
                    database_kwargs["real_target_table"],
                    initial_load_start_date,
                    upload_date,
                    upload_file_name,
                    last_extracted_id,
                    max_source_id,
                    is_export_completed,
                    row_count,
                )
                # for loop should auto-terminate, but to be safe, avoid table overwrite
                if is_export_completed:
                    break

    # need to return in case it was first set here
    return initial_load_start_date


def read_sql_tmpfile(
    query: str, db_engine: Engine, tmp_file: Any, chunksize
) -> pd.DataFrame:
    """
    Uses postGres commands to copy data out of the DB and return a DF iterator
    """
    copy_sql = f"COPY ({query}) TO STDOUT WITH CSV HEADER"
    logging.info(f" running COPY ({query}) TO STDOUT WITH CSV HEADER")
    conn = db_engine.raw_connection()
    cur = conn.cursor()
    cur.copy_expert(copy_sql, tmp_file)
    tmp_file.seek(0)
    logging.info("Reading csv")
    df = pd.read_csv(tmp_file, chunksize=chunksize, parse_dates=True, low_memory=False)
    logging.info("CSV read")
    return df


def range_generator(
    start: int, stop: int, step: int = 750_000
) -> Generator[Tuple[int, ...], None, None]:
    """
    Yields a list that contains the starting and ending number for a given window.
    """
    while True:
        if start > stop:
            logging.info("No more id pairs to extract. Stopping")
            break
        yield tuple([start, start + step])
        start += step


def get_source_and_target_columns(
    raw_query, source_engine, target_engine, target_table
):
    """
    Using the respective query engines, retrieve source and target cols.
    Used to check if the schema has changed
    """
    # Get the columns from the current query
    query_stem = raw_query.lower().split("where")[0]
    source_query = "{0} limit 1"
    source_columns = pd.read_sql(
        sql=source_query.format(query_stem),
        con=source_engine,
    ).columns

    # Get the columns from the target_table
    target_query = "select * from {0} limit 1"
    target_columns = (
        pd.read_sql(sql=target_query.format(target_table), con=target_engine)
        .drop(axis=1, columns=["_uploaded_at", "_task_instance"], errors="ignore")
        .columns
    )
    return source_columns, target_columns


def check_is_new_table(engine: Engine, table: Engine, schema=None) -> bool:
    return not engine.has_table(table, schema=schema)


def check_is_new_table_or_schema_addition(
    raw_query: str,
    source_engine: Engine,
    target_engine: Engine,
    target_table: str,
) -> bool:
    """
    Query the source table with the manifest query to get the columns, then check
    what columns currently exist in the DW. Return a bool depending on whether
    there has been a change or not.

    If the table does not exist this function will also return True.
    """

    is_new_table = check_is_new_table(target_engine, target_table)
    if is_new_table:
        return True

    source_columns, target_columns = get_source_and_target_columns(
        raw_query, source_engine, target_engine, target_table
    )
    return not all(source_column in target_columns for source_column in source_columns)


def drop_column_on_schema_removal(engine, table, columns_to_drop):
    """Execute the drop function"""
    columns_to_drop_str = ", ".join(columns_to_drop)
    alter_query = f"ALTER TABLE {table} DROP COLUMN {columns_to_drop_str};"
    query_executor(engine, alter_query)
    logging.info(f"Column(s) {columns_to_drop} were successfully dropped in Snowflake")


def check_and_handle_schema_removal(
    raw_query: str, source_engine: Engine, target_engine: Engine, target_table: str
):
    """
    When manifest file has a column removed, drop the column from Snowflake
    rather than re-backfilling the entire table
    """

    source_columns, target_columns = get_source_and_target_columns(
        raw_query, source_engine, target_engine, target_table
    )

    if not target_engine.has_table(target_table):
        return
    columns_to_drop = list(set(target_columns) - set(source_columns))

    if columns_to_drop:
        logging.info(
            "Manifest column(s) removed, dropping in Snowflake table as well..."
        )
        drop_column_on_schema_removal(target_engine, target_table, columns_to_drop)


def id_query_generator(
    primary_key: str,
    raw_query: str,
    min_source_id: int,
    max_source_id: int,
    id_range: int,
) -> Generator[str, Any, None]:
    """
    Yields a new query containing incrementing min/max id's based on the chunk size.
    """

    # Generate the range pairs based on the max source id and the
    # greatest of either the min_source_id or the max_target_id
    for id_pair in range_generator(min_source_id, max_source_id, step=id_range):
        id_range_query = (
            "".join(raw_query.lower().split("where")[0])
            + f" WHERE {primary_key} BETWEEN {id_pair[0]} AND {id_pair[1]}"
        )
        logging.info(f"ID Range: {id_pair}")
        yield id_range_query


def get_engines(connection_dict: Dict[Any, Any]) -> Tuple[Engine, Engine, Engine]:
    """
    Generates Snowflake and Postgres engines from env vars and returns them.
    """

    logging.info("Creating database engines...")
    env = os.environ.copy()
    postgres_engine = postgres_engine_factory(
        connection_dict["postgres_source_connection"], env
    )

    snowflake_engine = snowflake_engine_factory(
        env, role="LOADER", schema="tap_postgres"
    )

    if connection_dict.get("postgres_metadata_connection"):
        metadata_engine = postgres_engine_factory(
            connection_dict["postgres_metadata_connection"], env
        )
    else:
        metadata_engine = None
    return postgres_engine, snowflake_engine, metadata_engine


def query_backfill_status(
    metadata_engine: Engine, metadata_table: str, target_table: str
) -> List[Tuple[Any, Any, Any, Any]]:
    """
    Query the most recent record in the table to get the state of the backfill
    """

    query = (
        "SELECT is_export_completed, initial_load_start_date, last_extracted_id, upload_date "
        f"FROM {METADATA_SCHEMA}.{metadata_table} "
        "WHERE upload_date = ("
        "  SELECT MAX(upload_date)"
        f" FROM {METADATA_SCHEMA}.{metadata_table}"
        f" WHERE table_name = '{target_table}');"
    )
    logging.info(f"\nquery export status: {query}")
    results = query_executor(metadata_engine, query)
    return results


def is_resume_export(
    metadata_engine: Engine, metadata_table: str, target_table: str
) -> Tuple[bool, Optional[Any], int]:
    """
    Determine if export should be resumed, for either 'backfill or 'delete'

    First query the metadata database to see if there's a backfill in progress
    If the backfill is in progress, check when the last file was written
    If last file was written within 24 hours, continue from last_extracted_id
    """
    # initialize variables
    is_resume_export_needed = False
    start_pk = 1
    initial_load_start_date = None

    results = query_backfill_status(metadata_engine, metadata_table, target_table)

    # if backfill metadata exists for table
    if results:
        (
            is_export_completed,
            prev_initial_load_start_date,
            last_extracted_id,
            last_upload_date,
        ) = results[0]
        time_since_last_upload = datetime.now() - last_upload_date

        if not is_export_completed:
            is_resume_export_needed = True
            # if more than 24 HR since last upload, start backfill over,
            if time_since_last_upload > timedelta(hours=24):
                logging.info(
                    f"In middle of export for {target_table}, but more than 24 HR has elapsed since last upload: {time_since_last_upload}. Discarding this export."
                )

            # else proceed with last extracted_id
            else:
                start_pk = last_extracted_id + 1
                initial_load_start_date = prev_initial_load_start_date

    return is_resume_export_needed, initial_load_start_date, start_pk


def remove_files_from_gcs(load_by_id_export_type: str, target_table: str):
    """
    Prior to a fresh backfill/delete, remove all previously
    backfilled files that haven't been processed downstream
    """
    bucket = get_gcs_bucket()

    prefix = get_prefix(
        staging_or_processed="staging",
        load_by_id_export_type=load_by_id_export_type,
        table=target_table,
        initial_load_prefix="initial_load_start_",
    )

    blobs = bucket.list_blobs(prefix=prefix)

    for i, blob in enumerate(blobs):
        if i == 0:
            logging.info(
                f"In preparation of export, removing unprocessed files with prefix: {prefix}"
            )
        blob.delete()


def get_min_or_max_id(
    primary_key: str,
    engine: Engine,
    table: str,
    min_or_max: str,
    additional_filtering: str = "",
) -> int:
    """
    Retrieve the minimum or maximum value of the specified primary key column in the specified table.

    Parameters:
    primary_key (str): The name of the primary key column.
    engine (Engine): The database engine to use for the query.
    table (str): The name of the table to query.
    min_or_max (str): Either "min" or "max" to indicate whether to retrieve the minimum or maximum ID.

    Returns:
    int: The minimum or maximum ID value.
    """
    id_query = f"SELECT COALESCE({min_or_max}({primary_key}), 0) as {primary_key} FROM {table} WHERE true {additional_filtering};"
    try:
        id_results = query_results(id_query, engine)
        id_value = id_results[primary_key].tolist()[0]
    except sqlalchemy.exc.ProgrammingError as e:
        logging.exception(e)
        raise
    logging.info(f"{min_or_max} ID: {id_value}")

    if id_value is None:
        logging.info(f"No data found when querying {min_or_max}(id) -- exiting")
        sys.exit(0)
    return id_value


def swap_temp_table(engine: Engine, real_table: str, temp_table: str) -> None:
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