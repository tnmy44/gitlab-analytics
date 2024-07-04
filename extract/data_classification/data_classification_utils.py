"""
    TODO: rbacovic
"""

import json
import yaml


class DataClassification:
    def __init__(self, tagging_type: str, mnpi_raw_file: str):
        self.encoding = "utf8"
        self.mnpi_file_name = {
            "MNPI": "mnpi_table_list.csv",
            "PII": "pii_table_list.csv",
        }
        self.specification_file = "specification.yml"
        self.tagging_type = tagging_type
        self.mnpi_raw_file = mnpi_raw_file
        self.scope = self.get_scope()

    def load_mnpi_list(self) -> list:
        with open(self.mnpi_raw_file, mode="r", encoding=self.encoding) as file:
            return [json.loads(line.rstrip()) for line in file]

    def transform_mnpi_list(self, mnpi_list: list) -> list:

        extract_full_path = (
            lambda x: f"{x.get('config').get('database')}."
            f"{x.get('config').get('schema')}."
            f"{x.get('name')}".upper()
        )
        return [extract_full_path(x) for x in mnpi_list]

    # TODO: rbacovic identify PII data
    def identify_pii_data(self):
        pass
        # load
        # prepare
        # save

    def save_to_file(self, data: list):
        with open(
            self.mnpi_file_name.get("MNPI"),
            mode="w",
            encoding=self.encoding,
        ) as file:
            for row in sorted(data):
                file.write(f"{row},\n")

    # TODO: rbacovic identify MNPI data
    def identify_mnpi_data(self):
        mnpi_list = self.load_mnpi_list()
        mnpi_data = self.transform_mnpi_list(mnpi_list=mnpi_list)
        self.save_to_file(data=mnpi_data)

    def identify(self):
        self.identify_pii_data()
        self.identify_mnpi_data()

    # TODO: rbacovic define the scope for PII/MNPI data (include/exclude)
    def get_scope(self):
        with open(
            file=self.specification_file, mode="r", encoding=self.encoding
        ) as file:
            manifest_dict = yaml.load(file, Loader=yaml.FullLoader)

        return manifest_dict

    # TODO: rbacovic Tagging PII data
    # TODO: rbacovic Tagging PII data - full
    # TODO: rbacovic Tagging PII data - incremental
    def tag_pii_data(self):
        pass  # TODO: rbacovic add type

    # TODO: rbacovic Tagging MNPI data
    # TODO: rbacovic Tagging MNPI data - full
    # TODO: rbacovic Tagging MNPI data - incremental
    def tag_mnpi_data(self):
        pass  # TODO: rbacovic add type

    def tag(self):
        self.tag_pii_data()
        self.tag_mnpi_data()

    # TODO: rbacovic Clear PII tags
    def clear_pii_tags(self):
        pass

    # TODO: rbacovic Clear MNPI tags
    def clear_mnpi_tags(self):
        pass

    def clear_tags(self):
        self.clear_pii_tags()
        self.clear_mnpi_tags()
