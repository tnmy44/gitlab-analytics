"""
    Main entry point for the data classification:
    - PII
    - MNPI
"""

from data_classification_utils import DataClassification


def run():
    """
    Run process
    """
    data_classification = DataClassification(
        tagging_type="full", mnpi_raw_file="safe_models.json"
    )
    data_classification.identify()

    print(data_classification.identify_mnpi_data)


if __name__ == "__main__":
    run()
