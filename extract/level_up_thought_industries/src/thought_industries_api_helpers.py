"""
Helpers to the main level_up module
"""
import os
import datetime
import time
import json

from typing import Any, Dict, Optional
from logging import info, error

import requests
import dateutil.parser

from gitlabdata.orchestration_utils import (
    snowflake_stage_load_copy_remove,
    snowflake_engine_factory,
)

config_dict = os.environ.copy()

from sqlalchemy.engine.base import Engine
from sqlalchemy import create_engine


# TODO remove this function, need to currently use it to test
def snowflake_stage_load_copy_remove(
    file: str,
    stage: str,
    table_path: str,
    engine: Engine,
    type: str = "json",
    on_error: str = "skip_file",
    file_format_options: str = "",
) -> None:
    """
    Upload file to stage, copy to table, remove file from stage on Snowflake
    """

    import logging
    import os
    import sys
    from pathlib import Path
    from time import time

    from snowflake.sqlalchemy import URL as snowflake_URL

    put_query = f"put 'file://{file}' @{stage} auto_compress=true;"

    if type == "json":
        copy_query = f"""copy into {table_path} (jsontext)
                         from @{stage}
                         file_format=(type='{type}'),
                         on_error='{on_error}';"""

        remove_query = f"remove @{stage} pattern='.*.{type}.gz'"
    else:
        file_name = os.path.basename(file)
        file_pattern = f".*{file_name}.gz"
        copy_query = f"""copy into {table_path}
                         from @{stage}
                         file_format=(type='{type}' {file_format_options}),
                         on_error='{on_error}'
                         pattern='{file_pattern}';
                        """

        remove_query = f"remove @{stage} pattern='{file_pattern}'"

    logging.basicConfig(stream=sys.stdout, level=20)

    try:
        connection = engine.connect()

        logging.info(f"Clearing {type} files from stage.")
        remove = connection.execute(remove_query)
        logging.info(f"Query successfully run")

        logging.info("Writing to Snowflake.")
        results = connection.execute(put_query)
        logging.info(f"Query successfully run")
    except Exception as e:
        raise (str(e))
    finally:
        connection.close()
        engine.dispose()

    try:
        connection = engine.connect()

        logging.info(f"Copying to Table {table_path}.")
        copy_results = connection.execute(copy_query)
        logging.info(f"Query successfully run")

        logging.info(f"Removing {file} from stage.")
        remove = connection.execute(remove_query)
        logging.info(f"Query successfully run")
    except Exception as e:
        raise (str(e))
    finally:
        connection.close()
        engine.dispose()


def upload_payload_to_snowflake(
    payload: Dict[Any, Any],
    schema_name: str,
    stage_name: str,
    table_name: str,
    json_dump_filename: str = 'to_upload.json'
):
    """
    Upload payload to Snowflake using snowflake_stage_load_copy_remove()
    """
    loader_engine = snowflake_engine_factory(config_dict, "LOADER")
    with open(json_dump_filename, "w+", encoding="utf8") as upload_file:
        json.dump(payload, upload_file)

    snowflake_stage_load_copy_remove(
        json_dump_filename,
        f"{schema_name}.{stage_name}",
        f"{schema_name}.{table_name}",
        loader_engine,
    )
    loader_engine.dispose()


def iso8601_to_epoch_ts_ms(iso8601_timestamp: str) -> int:
    """
    Converts a string representation of a timestamp in the ISO-8601 format
    to an epoch timestamp in **milliseconds**.

    Args:
    timestamp (str): The string representation of a timestamp in the ISO-8601 format,
    i.e "2023-02-10T16:44:45.084Z"

    Returns:
    int: Epoch timestamp, i.e number of seconds elapsed since 1/1/1970
    """
    date_time = dateutil.parser.isoparse(iso8601_timestamp)
    # 1/1/1970
    dt_epoch_beginning = datetime.datetime.utcfromtimestamp(0).replace(
        tzinfo=datetime.timezone.utc
    )

    delta = date_time - dt_epoch_beginning
    epoch_ts = delta.total_seconds()
    epoch_ts_ms = int(epoch_ts * 1000)
    return epoch_ts_ms


def epoch_ts_ms_to_datetime_str(epoch_ts_ms: int) -> str:
    """
    convert from epoch ts in milliseconds, i.e 1675904400000
    to datetime str, i.e '2023-02-09 01:00:00'
    """
    date_time = datetime.datetime.utcfromtimestamp(epoch_ts_ms / 1000)
    return date_time.strftime('%Y-%m-%d %H:%M:%S')


def make_request(
    request_type: str,
    url: str,
    headers: Optional[Dict[Any, Any]] = None,
    params: Optional[Dict[Any, Any]] = None,
    json_body: Optional[Dict[Any, Any]] = None,
    timeout: int = 60,
    current_retry_count: int = 0,
    max_retry_count: int = 3,
) -> requests.models.Response:
    """Generic function that handles making GET and POST requests"""
    if current_retry_count >= max_retry_count:
        raise Exception(f"Too many retries when calling the {url}")
    try:
        if request_type == "GET":
            response = requests.get(
                url, headers=headers, params=params, timeout=timeout
            )
        elif request_type == "POST":
            response = requests.post(
                url, headers=headers, json=json_body, timeout=timeout
            )
        else:
            raise ValueError("Invalid request type")

        response.raise_for_status()
        return response
    except requests.exceptions.RequestException:
        if response.status_code == 429:
            retry_after = int(response.headers["Retry-After"])
            info(f'\nToo many requests... Sleeping for {retry_after} seconds')
            time.sleep(retry_after)
            current_retry_count += 1
            # Make the request again
            return make_request(
                request_type=request_type,
                url=url,
                headers=headers,
                params=params,
                json_body=json_body,
                timeout=timeout,
                current_retry_count=current_retry_count,
                max_retry_count=max_retry_count,
            )
        error(f"request exception for url {url}, see below")
        raise


def is_invalid_ms_timestamp(epoch_start_ms, epoch_end_ms):
    """
    Checks if timestamp in milliseconds > 9/9/2001
    More info here: https://stackoverflow.com/a/23982005
    """
    if len(str(epoch_start_ms)) != 13 or (len(str(epoch_end_ms)) != 13):
        return True
    return False
