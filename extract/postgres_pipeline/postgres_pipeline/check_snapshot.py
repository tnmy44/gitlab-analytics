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
from fire import Fire

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


# def check_snapshot(
#     connection_dict: Dict[str, str], env: Dict[str, str]
# ) -> Engine:
#     """
#     Create a postgres engine to be used by pandas.
#     """

#     logging.info("Creating database engines...")
#     env = os.environ.copy()
#     # Set the Vars
#     user = env[connection_dict["user"]]
#     password = env[connection_dict["pass"]]
#     host = env[connection_dict["host"]]
#     database = env[connection_dict["database"]]
#     port = env[connection_dict["port"]]

#     # Inject the values to create the engine
#     engine = create_engine(
#         f"postgresql://{user}:{password}@{host}:{port}/{database}",
#         connect_args={"sslcompression": 0, "options": "-c statement_timeout=9000000"},
#     )
#     logging.info(engine)
#     check_snapshot_replica(engine)

#     return engine


def main():
    config_dict = os.environ.copy()
    dag_db_name = config_dict.get("dag_name")
    print(dag_db_name)
    logging.info(dag_db_name)
    if 'el_gitlab_com_ci' in dag_db_name :
        database = config_dict.get("GITLAB_COM_CI_DB_NAME")
        host = config_dict.get("GITLAB_COM_CI_DB_HOST")
        password = config_dict.get("GITLAB_COM_CI_DB_PASS")
        port = config_dict.get("GITLAB_COM_CI_DB_PORT")
        user = config_dict.get("GITLAB_COM_CI_DB_USER")
    elif 'el_customers_scd_db' in dag_db_name :
        database = config_dict.get("GITLAB_COM_CI_DB_NAME")
        host = config_dict.get("GITLAB_COM_CI_DB_HOST")
        password = config_dict.get("GITLAB_COM_CI_DB_PASS")
        port = config_dict.get("GITLAB_COM_CI_DB_PORT")
        user = config_dict.get("GITLAB_COM_CI_DB_USER")
    elif 'el_gitlab_ops' in dag_db_name :
        database = config_dict.get("GITLAB_COM_CI_DB_NAME")
        host = config_dict.get("GITLAB_COM_CI_DB_HOST")
        password = config_dict.get("GITLAB_COM_CI_DB_PASS")
        port = config_dict.get("GITLAB_COM_CI_DB_PORT")
        user = config_dict.get("GITLAB_COM_CI_DB_USER")
    else :
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

if __name__ == "__main__":
    main()