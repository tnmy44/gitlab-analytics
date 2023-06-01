import sys
import os
import requests
import pandas as pd
import xmltodict

from logging import info, basicConfig, getLogger
from io import StringIO
from typing import Any, Dict, Union, List, Optional

from helpers import (
    make_request,
    upload_exported_data,
    read_processed_versions_table,
    upload_processed_version,
    wide_to_long,
)


config_dict = os.environ.copy()


class Adaptive:
    def __init__(self):
        pass

    def _base_xml(self, method: str, additional_body: str) -> str:
        """Every endpoint needs to be called with this base xml argument"""
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

    def handle_response(self, response: requests.models.Response) -> Union[dict, str]:
        """API returns a xml string, check the xml string for success"""
        parsed_dict = xmltodict.parse(response.text)
        success = parsed_dict["response"]["@success"]
        info(f"\nIs successful response: {success}")

        if success == "false":
            error_message = parsed_dict["response"]["messages"]["message"]["#text"]
            info(f"\nAborting... error_message: {error_message}")
            sys.exit()

        else:
            export_output = parsed_dict["response"]["output"]
        return export_output

    def _export(self, method: str, additional_body: str) -> Union[dict, str]:
        """Generic function to export data from any Adaptive endpoint"""
        url = "https://api.adaptiveinsights.com/api/v29"
        headers = {"Content-Type": "application/xml"}
        xml_string = self._base_xml(method, additional_body)

        response = make_request("POST", url, headers=headers, data=xml_string)

        export_output = self.handle_response(response)
        return export_output

    def export_versions(self) -> Dict[Any, Any]:
        """Call exportVersions endpoint to get list of versions"""
        method = "exportVersions"
        additional_body = ""
        info("Calling export_version endpoint...")
        versions_output = self._export(method, additional_body)
        if isinstance(versions_output, dict):
            versions = versions_output["versions"]
        return versions

    def export_data(self, version_name: str) -> str:
        """Call exportData endpoint to the data associated with a version"""
        method = "exportData"

        additional_body = (
            f'<version name="{version_name}" isDefault="false"/>'
            + '<format useInternalCodes="true" includeUnmappedItems="false"/>'
        )
        info("Calling export_data endpoint...")
        data = self._export(method, additional_body)
        return str(data)  # coercing str -> str to remove mypy error

    def _iterate_versions(self, versions: List[Any], version_reports: List[Any]):
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

    def _filter_for_subfolder(
        self, versions: Union[List[Any], Dict[Any, Any]], folder_criteria: str
    ) -> Optional[List[Any]]:
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
                inner_versions = version_d["version"]
                if foldername == folder_criteria:
                    return inner_versions
                res = self._filter_for_subfolder(inner_versions, folder_criteria)
                if res:  # only return if match was found
                    return res
        return None

    def get_valid_versions(
        self, versions: Dict[Any, Any], folder_criteria: str
    ) -> List[Any]:
        """
        @versions: a nested list of dictionaries, representing a file struct
        @folder_criteria: which sub-folder to filter for

        First filters for the json that represents the sub-folder.
        From that filtered json, extract all the version names
        """

        root_versions = versions["version"]
        filtered_versions = self._filter_for_subfolder(root_versions, folder_criteria)
        if filtered_versions is None:  # no versions match folder_criteria
            return []

        version_reports: List[Any] = []
        self._iterate_versions(filtered_versions, version_reports)
        return version_reports

    def exported_data_to_df(self, exported_data: str) -> pd.DataFrame:
        """
        exported_data is a string that partially contains csv delimited data
        Take the csv delimited data and convert to pandas dataframe
        """
        exported_data.lstrip("![CDATA[").rstrip("]]")
        exported_data_io = StringIO(exported_data)
        dataframe = pd.read_csv(exported_data_io)
        info(f"\ndataframe.head(): {dataframe.head()}")
        return dataframe

    def is_version_already_processed(self, version: str) -> bool:
        """
        Check if the version is already processed by checking the processed table
        """
        dataframe = read_processed_versions_table()
        if version in dataframe["version"]:
            return True
        return False

    def process_versions(self, folder_criteria: str):
        """
        For each version, export the data
        Then upload the data to Snowflake
        Lastly, add verison to processed table so it's not processed again
        """
        versions = self.export_versions()
        valid_versions = self.get_valid_versions(versions, folder_criteria)
        info(f"\nversions to process (if not already processed):\n {valid_versions}")
        for valid_version in valid_versions:
            if self.is_version_already_processed(valid_version):
                continue
            info(f"\nprocessing version: {valid_version}")
            exported_data = self.export_data(valid_version)
            dataframe = self.exported_data_to_df(exported_data)
            long_dataframe = wide_to_long(dataframe)
            upload_exported_data(long_dataframe, valid_version)
            upload_processed_version(valid_version)
            info(f"\nfinished processing: {valid_version}")


def main(export_all=True):
    """
    Main function to run the export.
    Either export one version, or export all unprocessed versions
    """
    adaptive = Adaptive()

    # export all versions in a folder (including subfolders)
    if export_all:
        folder_criteria = "FY24 Versions"
        folder_criteria = "409A Versions"  # 1 version
        folder_criteria = "FY22 PLAN Versions"  # 4 versions
        adaptive.process_versions(folder_criteria)

    # export a specific version
    else:
        version = "FY24 Plan (Board)"  # legit yearly forecast
        version = "Live forecast snapshot 1A"  # test

        exported_data = adaptive.export_data(version)
        dataframe = adaptive.exported_data_to_df(exported_data)
        long_dataframe = wide_to_long(dataframe)
        upload_exported_data(long_dataframe, version)


if __name__ == "__main__":
    basicConfig(stream=sys.stdout, level=20)
    getLogger("snowflake.connector.cursor").disabled = True
    main()
    info("Complete.")
