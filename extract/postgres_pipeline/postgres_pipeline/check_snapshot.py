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
import fire

def check_snapshot_replica(
    source_engine: Engine
):
    current_date_check_query = "SELECT CURRENT_DATE;"
    pg_date_timestamp = query_executor(
        source_engine, current_date_check_query
    )[0][0]
    if current_date_check_query:
        logging.info(
            f"Timestamp value from Postgres:{pg_date_timestamp}"
        )

def check_snapshot_ci() -> None:
    config_dict = os.environ.copy()
    database = config_dict.get("GITLAB_COM_CI_DB_NAME")
    host = config_dict.get("GITLAB_COM_CI_DB_HOST")
    password = config_dict.get("GITLAB_COM_CI_DB_PASS")
    port = config_dict.get("GITLAB_COM_CI_DB_PORT")
    user = config_dict.get("GITLAB_COM_CI_DB_USER")
    logging.info("Creating database engines...")
    engine = create_engine(
        f"postgresql://{user}:{password}@{host}:{port}/{database}",
        connect_args={"sslcompression": 0, "options": "-c statement_timeout=9000000"},
    )
    logging.info(engine)
    check_snapshot_replica(engine)
    logging.info("Complete")

def check_snapshot_main_db_incremental() -> None:
    config_dict = os.environ.copy()
    database = config_dict.get("GITLAB_COM_DB_NAME")
    host = config_dict.get("GITLAB_COM_DB_HOST")
    password = config_dict.get("GITLAB_COM_DB_PASS")
    port = config_dict.get("GITLAB_COM_PG_PORT")
    user = config_dict.get("GITLAB_COM_DB_USER")
    logging.info("Creating database engines...")
    engine = create_engine(
        f"postgresql://{user}:{password}@{host}:{port}/{database}",
        connect_args={"sslcompression": 0, "options": "-c statement_timeout=9000000"},
    )
    logging.info(engine)
    check_snapshot_replica(engine)
    logging.info("Complete")

def check_snapshot_gitlab_dotcom_scd() -> None:
    config_dict = os.environ.copy()
    database = config_dict.get("GITLAB_COM_DB_NAME")
    host = config_dict.get("GITLAB_COM_DB_HOST")
    password = config_dict.get("GITLAB_COM_DB_PASS")
    port = config_dict.get("GITLAB_COM_SCD_PG_PORT")
    user = config_dict.get("GITLAB_COM_DB_USER")
    engine = create_engine(
        f"postgresql://{user}:{password}@{host}:{port}/{database}",
        connect_args={"sslcompression": 0, "options": "-c statement_timeout=9000000"},
    )
    logging.info(engine)
    check_snapshot_replica(engine)
    logging.info("Complete")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    fire.Fire()