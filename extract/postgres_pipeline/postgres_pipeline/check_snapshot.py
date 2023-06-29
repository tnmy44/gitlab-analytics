import logging
import sys
import os
from time import time
from typing import Dict
import sqlalchemy
from sqlalchemy.engine.base import Engine
from sqlalchemy import (
    create_engine
)
from gitlabdata.orchestration_utils import (
    query_executor
)

def load_incremental(
    source_engine: Engine
):

    last_replication_check_query = "select pg_last_xact_replay_timestamp();"
    replication_timestamp_query = (
        "select last_replica_time from public.last_replication_timestamp"
    )
    pg_replication_timestamp = query_executor(
        source_engine, last_replication_check_query
    )[0][0]
    replication_timestamp_value = query_executor(
        source_engine, replication_timestamp_query
    )[0][0]
    logging.info(
        f"Timestamp value from pg_last_xact_replay_timestamp:{pg_replication_timestamp}"
    )
    logging.info(
        f"Timestamp value from replication_timestamp_value:{replication_timestamp_value}"
    )
    

def postgres_engine_factory(
    connection_dict: Dict[str, str], env: Dict[str, str]
) -> Engine:
    """
    Create a postgres engine to be used by pandas.
    """

    logging.info("Creating database engines...")
    env = os.environ.copy()
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
    load_incremental(engine)

    return engine