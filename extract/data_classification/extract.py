"""
    TODO: rbacovic
"""
from data_classification_utils import DataClassification

def run():
    """
    Run process
    """
    data_classification = DataClassification(tagging_type="full", mnpi_raw_file="safe_models.json")
    data_classification.identify_mnpi_data()
    # print(data_classification.scope)

if __name__ == '__main__':
    run()
