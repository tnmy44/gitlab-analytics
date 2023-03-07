"""
Run each class (which requests from its respective endpoint) from the command line
"""
import os
import logging
from typing import Type

import fire
import thought_industries_api


def cls_factory(class_name_to_run: str) -> Type:
    """Returns instantiated class obj based on class string name"""
    cls_obj = getattr(thought_industries_api, class_name_to_run)
    return cls_obj()


def main(class_name_to_run: str):
    """dynamically create class, then call its fetch_and_upload_data()"""
    # fail if env variables are missing
    epoch_start_ms = int(os.environ["EPOCH_START_STR"]) * 1000
    epoch_end_ms = int(os.environ["EPOCH_END_STR"]) * 1000
    class_to_run = cls_factory(class_name_to_run)
    class_to_run.fetch_and_upload_data(epoch_start_ms, epoch_end_ms)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("snowflake.connector.cursor").disabled = True
    logging.getLogger("snowflake.connector.connection").disabled = True
    fire.Fire(main)
    logging.info("Complete.")
