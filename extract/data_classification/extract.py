"""
    Main entry point for the data classification:
    - PII
    - MNPI
"""
import sys
from logging import basicConfig, info

from data_classification_utils import DataClassification
from fire import Fire


def run_extract(operation: str, date_from:str,unset:str, tagging_type: str = "INCREMENTAL"):
    """
    Run process
    """
    data_classification = DataClassification(
        tagging_type=tagging_type,
        mnpi_raw_file="safe_models.json"
    )
    if operation == "EXTRACT":
        data_classification.extract()
    if operation == "CLASSIFY":
        if unset =='True':
            unset = True
        else:
            unset = False
        data_classification.classify(date_from=date_from, unset=unset,tagging_type=tagging_type)

if __name__ == "__main__":
    basicConfig(stream=sys.stdout, level=20)
    info("START data classification.")
    Fire(run_extract())
    info("END with data classification.")
