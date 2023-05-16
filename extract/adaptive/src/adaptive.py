import os
import pandas as pd
import xmltodict

from logging import info, error
from datetime import datetime
from io import StringIO

from helpers import make_request, dataframe_uploader_adaptive


config_dict = os.environ.copy()


class Adaptive:
    def __init__(self):
        pass

    def _base_xml(self, method, additional_body):
        username = config_dict["ADAPTIVE_USERNAME"]
        password = config_dict["ADAPTIVE_PASSWORD"]
        caller_name = "Data"

        xml_string = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            + f'<call method="{method}" callerName="{caller_name}">'
            + f'<credentials login="{username}" password="{password}"/>'
            + additional_body
            + "</call>"
        )

        return xml_string

    def _export(self, method, additional_body):
        url = "https://api.adaptiveinsights.com/api/v29"
        headers = {"Content-Type": "application/xml"}
        xml_string = self._base_xml(method, additional_body)

        response = make_request("POST", url, headers=headers, data=xml_string)
        parsed_dict = xmltodict.parse(response.text)
        export_output = parsed_dict["response"].get("output")
        return export_output

    def export_versions(self):
        method = "exportVersions"
        additional_body = ""
        versions_output = self._export(method, additional_body)
        versions = versions_output["versions"]
        return versions

    def export_data(self, version_name):
        method = "exportData"

        additional_body = (
            f'<version name="{version_name}" isDefault="false"/>'
            + '<format useInternalCodes="true" includeUnmappedItems="false"/>'
        )
        data = self._export(method, additional_body)
        return data

    def _iterate_versions_helper(self, root_version, valid_versions):
        # convert dict to [], i.e for "@name": "409A Versions",
        if type(root_version) == dict:
            root_version = [root_version]

        for version_d in root_version:
            if version_d["@type"] == "VERSION_FOLDER" and version_d.get("version"):
                self._iterate_versions_helper(version_d["version"], valid_versions)
            else:
                valid_versions.append(version_d["@name"])

    def iterate_versions(self, versions):
        root_version = versions["version"]
        valid_versions = []

        self._iterate_versions_helper(root_version, valid_versions)
        return valid_versions

    def exported_data_to_df(self, exported_data):
        exported_data.lstrip("![CDATA[").rstrip("]]")
        exported_data_io = StringIO(exported_data)
        df = pd.read_csv(exported_data_io)
        return df

    def process_versions(self):
        """
        For each version, export the data
        Then upload the data to Snowflake
        """
        versions = self.export_versions()
        valid_versions = self.iterate_versions(versions)
        for valid_version in valid_versions:
            exported_data = adaptive.export_data(valid_version)
            dataframe = self.exported_data_to_df(exported_data)
            dataframe_uploader_adaptive(dataframe, valid_version)


if __name__ == "__main__":
    adaptive = Adaptive()
    export_all = False

    if export_all:
        adaptive.process_versions()

    else:
        version = "FY24 Plan (Board)"  # legit yearly forecast
        version = "Live forecast snapshot 1A"  # test

        exported_data = adaptive.export_data(version)
        dataframe = adaptive.exported_data_to_df(exported_data)
        dataframe_uploader_adaptive(dataframe, version)
