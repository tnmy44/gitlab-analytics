import json
import logging
import sys
from os import environ as env
from typing import List, Dict

from fire import Fire
from gitlabdata.orchestration_utils import snowflake_engine_factory
from sqlalchemy.engine import Engine
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError


def get_records_with_extended_feedback(table, key, column, tstamp_column):
    """
    retrieves snowplow events with Duo extended feedback populated
    """

    query = f"""
    SELECT {key}, {column}
    FROM {table}
    WHERE {tstamp_column} <= dateadd(days, -60, current_timestamp) 
    AND se_label ='response_feedback'
    AND contexts like '%"extendedFeedback":%'
    """

    try:
        config_dict = env.copy()
        engine = snowflake_engine_factory(config_dict, "SYSADMIN")
        logging.info("Getting snowplow events with extended feedback")
        logging.info(f"running query: {query}")
        connection = engine.connect()
        duo_feedback_events = connection.execute(query).fetchall()

        for key_value, column_value in duo_feedback_events:
            logging.info(f"{key}: {key_value}, {column}: {column_value}")

            column_value_json = json.loads(column_value)
            
            column_value_json['data'][0]['data']['extra']['extendedFeedback'] = "***DATA REDACTED***"

            column_value_json_escaped = column_value_json.replace("'","''")

            logging.info(f"update {table} set {column} = '{column_value_json_escaped}' where {key} ='{key_value}' ")

    except:
        logging.info("Failed to get snowplow events")
        raise

    finally:
        connection.close()
        engine.dispose()

    return duo_feedback_events


def redact_extended_feedback(table, key, column, tstamp_column):
    
    records = get_records_with_extended_feedback(table, key, column, tstamp_column)


if __name__ == "__main__":
    logging.basicConfig(level=20)
    Fire(redact_extended_feedback)
    logging.info("completed")
