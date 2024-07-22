"""
    Main entry point for the data classification:
    - PII
    - MNPI
"""
import sys
from datetime import datetime, timedelta
from logging import basicConfig, info

from data_classification_utils import DataClassification
from fire import Fire


def run_extract(
    operation: str,
    date_from: str,
    unset: str = "FALSE",
    tagging_type: str = "INCREMENTAL",
):
    """
    Run process
    """
    data_classification = DataClassification(
        tagging_type=tagging_type, mnpi_raw_file="safe_models.json"
    )
    if not date_from:
        curr_date = datetime.now() - timedelta(
            days=data_classification.INCREMENTAL_LOAD_DAYS
        )
        date_from = curr_date.strftime("%Y-%m-%d 00:00:00")

    if operation == "EXTRACT":
        data_classification.extract()
    if operation == "CLASSIFY":
        info(f"DATE_FROM: {date_from}")
        info(f"unset: {unset}")
        info(f"tagging_type: {tagging_type}")
        data_classification.classify(
            date_from=date_from, unset=unset, tagging_type=tagging_type
        )


if __name__ == "__main__":
    basicConfig(stream=sys.stdout, level=20)
    info("START data classification.")
    Fire(run_extract)
    info("END with data classification.")
