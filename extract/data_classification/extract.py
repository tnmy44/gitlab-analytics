"""
    Main entry point for the data classification:
    - PII
    - MNPI
"""
from fire import Fire
from data_classification_utils import DataClassification
from logging import basicConfig, info
import sys
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
