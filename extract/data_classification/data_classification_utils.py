"""
    TODO: rbacovic
"""

import json

import yaml


class DataClassification:
    def __init__(self, tagging_type: str, mnpi_raw_file: str):
        self.encoding = "utf8"
        self.database_name = "rbacovic_prep"
        self.schema_name = "benchmark_pii"
        self.mnpi_file_name = {
            "MNPI": "mnpi_table_list.csv",
            "PII": "pii_table_list.csv",
        }
        self.specification_file = "specification.yml"
        self.tagging_type = tagging_type
        self.mnpi_raw_file = mnpi_raw_file
        self.scope = self.get_scope_file()

    @staticmethod
    def quoted(input_str: str) -> str:
        return "'" + input_str + "'"
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

    def get_pii_scope(self, section: str, scope_type: str) -> str:

        scope = self.scope.get("data_classification").get(section).get(scope_type)
        res = ""
        databases = scope.get("databases")
        schemas = scope.get("schemas")
        tables = scope.get("tables")



        if databases:
             res = f" AND (table_catalog IN ({', '.join(self.quoted(x) for x in databases)}))"


        if schemas:
            res += " AND"
            for i, schema in enumerate(schemas, start=1):
                schema_list = schema.split(".")

                if "*" in schema_list:
                    res += f" (table_catalog = {self.quoted(schema_list[0])} AND table_schema ILIKE {self.quoted('%')})"
                else:
                    res += f" (table_catalog = {self.quoted(schema_list[0])} AND table_schema = {self.quoted(schema_list[1])})"

                if i < len(schemas):
                    res += " OR"
        if tables:
            res += " AND"
            for i, table in enumerate(tables, start=1):
                table_list = table.split(".")

                if "*" in table_list:
                    res += f" (table_catalog = {self.quoted(table_list[0])} AND"
                    if table_list.count("*") == 1:
                        res += (
                            f" table_schema = {self.quoted(table_list[1])} and table_name ILIKE {self.quoted('%')})"
                        )
                    if table_list.count("*") == 2:
                        res += f" table_schema ILIKE {self.quoted('%')} and table_name ILIKE {self.quoted('%')})"
                else:
                    res += f" (table_catalog = {self.quoted(table_list[0])} AND table_schema = {self.quoted(table_list[1])} and table_name = {self.quoted(table_list[2])})"

                if i < len(tables):
                    res += " OR"

        return res

    # TODO: rbacovic identify PII data
    def identify_pii_data(self):
        scope_type = "PII"
        insert_statement = (
            f"INSERT INTO {self.database_name}.{self.schema_name}.sensitive_objects_classification (classification_type, created, last_altered,last_ddl, database_name, schema_name, table_name, table_type) "
            f"WITH base AS ("
            f"SELECT {self.quoted(scope_type)} AS classification_type, created,last_altered, last_ddl, table_catalog, table_schema, table_name, table_type "
            f"  FROM raw.information_schema.tables "
            f" WHERE table_schema != 'INFORMATION_SCHEMA' "
            f" UNION "
            f"SELECT {self.quoted(scope_type)} AS classification_type, created,last_altered, last_ddl, table_catalog, table_schema, table_name, table_type "
            f"  FROM prep.information_schema.tables "
            f" WHERE table_schema != 'INFORMATION_SCHEMA' "
            f" UNION "
            f"SELECT {self.quoted(scope_type)} AS classification_type, created,last_altered, last_ddl, table_catalog, table_schema, table_name, table_type "
            f"  FROM prod.information_schema.tables"
            f" WHERE table_schema != 'INFORMATION_SCHEMA') "
            f"SELECT *"
            f"  FROM base"
            f" WHERE 1=1 "
        )

        where_clause = self.get_pii_scope(section=scope_type, scope_type="include")
        print(insert_statement+where_clause)

    def save_to_file(self, data: list):
        with open(
            self.mnpi_file_name.get("MNPI"),
            mode="w",
            encoding=self.encoding,
        ) as file:
            for row in sorted(data):
                file.write(f"{row},\n")

    def get_scope(self, section: str, scope_type: str, row: list) -> bool:

        scope = self.scope.get("data_classification").get(section).get(scope_type)

        databases = scope.get("databases")
        schemas = scope.get("schemas")
        tables = scope.get("tables")

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
