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


def get_records_with_extended_feedback(table, schema, database):
    """
    retrieves snowplow events with Duo extended feedback populated
    """
    fully_qualified_table = f'"{database}".{schema}.{table}'
    query = f"""
    SELECT event_id, contexts
    FROM {fully_qualified_table}
    WHERE collector_tstamp <= dateadd(days, -60, current_timestamp) 
    AND se_label ='response_feedback'
    AND contexts like '%"extendedFeedback":%'
    AND contexts not like '%***DATA REDACTED***%'
    """

    try:
        config_dict = env.copy()
        engine = snowflake_engine_factory(config_dict, "SYSADMIN")
        logging.info("Getting snowplow events with extended feedback")
        logging.info(f"query: {query}")
        connection = engine.connect()
        duo_feedback_events = connection.execute(query).fetchall()
        record_count = len(duo_feedback_events)
        logging.info(f"found {record_count} records")

        for key_value, column_value in duo_feedback_events:
            column_value_json = json.loads(column_value)

            column_value_json["data"][0]["data"]["extra"][
                "extendedFeedback"
            ] = "***DATA REDACTED***"

            new_column_value = json.dumps(column_value_json)
            logging.info(f"redacting from event: event_id = {key_value}")
            update_cmd = f"update {fully_qualified_table} set contexts = $${new_column_value}$$ where event_id ='{key_value}'"
            update_results = connection.execute(update_cmd).fetchall()

    except:
        logging.info("Failed to get snowplow events")
        raise
    finally:
        connection.close()
        engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(level=20)
    Fire(get_records_with_extended_feedback)
    logging.info("completed")
