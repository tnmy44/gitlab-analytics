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
        self.scope = self.get_scope_file()

    def load_mnpi_list(self) -> list:
        with open(self.mnpi_raw_file, mode="r", encoding=self.encoding) as file:
            return [json.loads(line.rstrip()) for line in file]

    def transform_mnpi_list(self, mnpi_list: list) -> list:

        def extract_full_path(x: str) -> list:
            return [
                x.get("config").get("database").upper(),
                x.get("config").get("schema").upper(),
                x.get("name").upper(),
            ]

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

    def get_scope(self, section: str, type: str, row: list) -> bool:

        scope = self.scope.get("data_classification").get(section).get(type)

        databases = scope.get("databases")
        schemas = scope.get("schemas")
        tables = scope.get("tables")

        # handling include part
        database_name = str(row[0]).upper()
        schema_name = str(row[1]).upper()
        table_name = str(row[2]).upper()

        if database_name in databases:
            if (
                f"{database_name}.{schema_name}" in schemas
                or f"{database_name}.*" in schemas
            ):
                full_name = f"{database_name}.{schema_name}.{table_name}"

                if (
                    full_name in tables
                    or f"{database_name}.{schema_name}.*" in tables
                    or f"{database_name}.*.*" in tables
                ):

                    return True

        return False

    def filter_data(self, mnpi_data: list, section: str = "MNPI"):
        res = []

        #
        for row in mnpi_data:
            include, exclude = False, False

            include = self.get_scope(section=section, type="include", row=row)
            exclude = self.get_scope(section=section, type="exclude", row=row)

            if include and not exclude:
                res.append(f"{row[0]}.{row[1]}.{row[2]}")
        return res

    # TODO: rbacovic identify MNPI data
    def identify_mnpi_data(self):
        mnpi_list = self.load_mnpi_list()
        mnpi_data = self.transform_mnpi_list(mnpi_list=mnpi_list)
        mnpi_data_filtered = self.filter_data(mnpi_data=mnpi_data, section="MNPI")
        self.save_to_file(data=mnpi_data_filtered)

    def identify(self):
        self.identify_pii_data()
        self.identify_mnpi_data()

    # TODO: rbacovic define the scope for PII/MNPI data (include/exclude)
    def get_scope_file(self):
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
