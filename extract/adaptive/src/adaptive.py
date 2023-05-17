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

    def _iterate_versions(self, versions, version_reports):
        """
        Versions is a json structure of nested list of dictionaries
        that represents a folder structure of versions.

        This function loops through this nested data structure, and returns
        the name of each version.
        """
        if type(versions) == dict:
            versions = [versions]

        for version_d in versions:
            if version_d["@type"] == "VERSION_FOLDER" and version_d.get("version"):
                self._iterate_versions(
                    version_d["version"],
                    version_reports,
                )
            else:
                version_reports.append(version_d["@name"])

    def _filter_for_subfolder(self, versions, folder_criteria):
        """
        @versions: a nested list of dictionaries, representing a file struct
        @folder_criteria: which sub-folder to filter for

        Filters for just the sub-folder from versions
        returning a piece of the original nested json
        """
        if folder_criteria is None:
            return versions

        if type(versions) == dict:
            versions = [versions]

        for version_d in versions:
            if version_d["@type"] == "VERSION_FOLDER" and version_d.get("version"):
                foldername = version_d["@name"]
                versions = version_d["version"]
                if foldername == folder_criteria:
                    return versions
                res = self._filter_for_subfolder(versions, folder_criteria)
                if res: # only return if match was found
                    return res

    def get_valid_versions(self, versions, folder_criteria):
        """
        @versions: a nested list of dictionaries, representing a file struct
        @folder_criteria: which sub-folder to filter for

        First filters for the json that represents the sub-folder.
        From that filtered json, extract all the version names
        """

        root_versions = versions["version"]
        filtered_versions = self._filter_for_subfolder(root_versions, folder_criteria)

        version_reports = []
        self._iterate_versions(filtered_versions, version_reports)
        return version_reports

    def exported_data_to_df(self, exported_data):
        exported_data.lstrip("![CDATA[").rstrip("]]")
        exported_data_io = StringIO(exported_data)
        df = pd.read_csv(exported_data_io)
        return df

    def process_versions(self, folder_criteria):
        """
        For each version, export the data
        Then upload the data to Snowflake
        """
        versions = self.export_versions()
        valid_versions = self.get_valid_versions(versions, folder_criteria)
        print(f'\nvalid_versions: {valid_versions}')
        return valid_versions
        for valid_version in valid_versions:
            print(f'\nprocessing version: {valid_version}')
            exported_data = adaptive.export_data(valid_version)
            dataframe = self.exported_data_to_df(exported_data)
            dataframe_uploader_adaptive(dataframe, valid_version)
            print(f'\nfinished processing: {valid_version}')


if __name__ == "__main__":
    adaptive = Adaptive()
    export_all = True

    if export_all:
        folder_criteria = "FY24 Versions"
        adaptive.process_versions(folder_criteria)

    else:
        version = "FY24 Plan (Board)"  # legit yearly forecast
        version = "Live forecast snapshot 1A"  # test

        exported_data = adaptive.export_data(version)
        dataframe = adaptive.exported_data_to_df(exported_data)
        dataframe_uploader_adaptive(dataframe, version)
