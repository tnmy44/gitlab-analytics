class DataClassification:
    def __init__(self, tagging_type: str):
        self.tagging_type = tagging_type

    # TODO: rbacovic identify PII data
    def identify_pii_data(self):
        pass

    # TODO: rbacovic identify MNPI data
    def identify_mnpi_data(self):
        pass

    def identify(self):
        self.identify_pii_data()
        self.identify_mnpi_data()

    # TODO: rbacovic define the scope for PII/MNPI data (include/exclude)
    def scope(file_name:str = "specification.yml"):
        pass

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
