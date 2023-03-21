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


def calculate_epoch():
    """
    Retrieve the epoch start/end dates environ variables.
    If the start_date is before 12/31/2021, set the start_date to 2010.

    This is to capture all data in one call prior to 12/31/2021 as
    current Airflow isn't happy with backfilling daily tasks
    too far back.
    """
    # add 1 ms so that there's no overlap between runs
    epoch_start_ms = (int(os.environ["EPOCH_START_STR"]) * 1000) + 1
    if epoch_start_ms < 1640991600000:  # 12/31/2021
        epoch_start_ms = 1293755421000  # 12/31/2010

    epoch_end_ms = int(os.environ["EPOCH_END_STR"]) * 1000
    return epoch_start_ms, epoch_end_ms


def main(class_name_to_run: str):
    """dynamically create class, then call its fetch_and_upload_data()"""
    # add 1 ms to avoid overlap
    class_to_run = cls_factory(class_name_to_run)
    epoch_start_ms, epoch_end_ms = calculate_epoch()
    print(f"\nepoch_start_ms: {epoch_start_ms}")
    print(f"\nepoch_end_ms : {epoch_end_ms }")
    class_to_run.fetch_and_upload_data(epoch_start_ms, epoch_end_ms)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("snowflake.connector.cursor").disabled = True
    logging.getLogger("snowflake.connector.connection").disabled = True
    fire.Fire(main)
    logging.info("Complete.")
