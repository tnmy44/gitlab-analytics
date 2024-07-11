"""
    Main entry point for the data classification:
    - PII
    - MNPI
"""
import sys
from logging import basicConfig, info

from data_classification_utils import DataClassification
from fire import Fire


def run():
    """
    Run process
    """
    data_classification = DataClassification(
        tagging_type="full", mnpi_raw_file="safe_models.json"
    )
    data_classification.identify()
    data_classification.upload()


if __name__ == "__main__":
    basicConfig(stream=sys.stdout, level=20)
    info("START data classification.")
    Fire(run())
    info("END with data classification.")
