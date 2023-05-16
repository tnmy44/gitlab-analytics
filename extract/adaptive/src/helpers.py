import os
import time
from logging import info, error
import requests
from gitlabdata.orchestration_utils import (
    dataframe_uploader,
    snowflake_engine_factory,
)

config_dict = os.environ.copy()


def make_request(
    request_type: str,
    url: str,
    current_retry_count: int = 0,
    max_retry_count: int = 3,
    **kwargs,
) -> requests.models.Response:
    """Generic function that handles making GET and POST requests"""

    additional_backoff = 20
    if current_retry_count >= max_retry_count:
        raise requests.exceptions.HTTPError(
            f"Manually raising 429 Client Error: Too many retries when calling the {url}."
        )
    try:
        if request_type == "GET":
            response = requests.get(url, **kwargs)
        elif request_type == "POST":
            response = requests.post(url, **kwargs)
        else:
            raise ValueError("Invalid request type")

        response.raise_for_status()
        return response
    except requests.exceptions.RequestException:
        if response.status_code == 429:
            # if no retry-after exists, wait default time
            retry_after = int(response.headers.get("Retry-After", additional_backoff))
            info(f"\nToo many requests... Sleeping for {retry_after} seconds")
            # add some buffer to sleep
            time.sleep(retry_after + (additional_backoff * current_retry_count))
            # Make the request again
            return make_request(
                request_type=request_type,
                url=url,
                current_retry_count=current_retry_count + 1,
                max_retry_count=max_retry_count,
                **kwargs,
            )
        error(f"request exception for url {url}, see below")
        raise


def dataframe_uploader_adaptive(dataframe, table):
    loader_engine = snowflake_engine_factory(config_dict, "LOADER")
    schema = 'adaptive_custom'
    dataframe_uploader(dataframe, loader_engine, table, schema)
