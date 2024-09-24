"""
Source code to perform extraction of YAML files from:
1. Gitlab handbook
2. internal handbook
3. compensation calculator
4. cloud connector
"""

import base64
import json
import subprocess
import sys
import traceback
from logging import basicConfig, error, info
from os import environ as env

import requests
import yaml
from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    snowflake_stage_load_copy_remove,
)

config_dict = env.copy()
basicConfig(stream=sys.stdout, level=20)


def upload_to_snowflake(file_for_upload: str, table: str) -> None:
    """
    Upload json file to Snowflake
    """
    info(f"....Start uploading to Snowflake, file: {file_for_upload}")
    snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")

    snowflake_stage_load_copy_remove(
        file=file_for_upload,
        stage="gitlab_data_yaml.gitlab_data_yaml_load",
        table_path=f"gitlab_data_yaml.{table}",
        engine=snowflake_engine,
    )
    info(f"....End uploading to Snowflake, file: {file_for_upload}")


def decode_file(response):
    """
    Decode file and get the content from response
    """
    if response is None:
        return None

    file_content = response.get("content")
    message_bytes = base64.b64decode(file_content)

    return yaml.load(message_bytes, Loader=yaml.Loader)


def save_to_file(file: str, request: dict) -> None:
    """
    Save json to file for uploading
    """

    with open(f"{file}.json", "w", encoding="UTF-8") as file_name_json:
        json.dump(request, file_name_json, indent=4)


def stream_processing(
    table_to_upload: str, file_name: str, base_url: str, private_token=None
):
    """
    This function is designed to stream the API content by using Python request library.
    Also, it will be responsible for decoding and generating json file output and upload
    it to external stage of snowflake. Once the file gets loaded it will be deleted from external stage.
    This function can be extended but for now this used for the decoding the encoded content
    """
    info(f"Downloading {file_name} to {file_name}.json file.")

    # Check if there is private token issued for the URL
    if private_token:
        request_url = f"{base_url}{file_name}"
        response = requests.request(
            "GET", request_url, headers={"Private-Token": private_token}, timeout=10
        )

    api_response_json = response.json()
    record_count = len(api_response_json)

    if record_count > 1:
        output_json_request = decode_file(response=api_response_json)
        save_to_file(file=f"{file_name}.json", request=output_json_request)
        upload_to_snowflake(file_for_upload=f"{file_name}.json", table=table_to_upload)
    else:
        error(
            f"The file for {file_name} is either empty or the location has changed, please investigate."
        )


def get_json_file_name(input_file: str) -> str:
    """
    Return json file name
    based on input file
    """
    res = ""
    if input_file == "":
        res = "ymltemp"
    elif ".yml" in input_file:
        res = input_file.split(".yml")[0]
    else:
        res = input_file

    return res


def run_subprocess(command: str, file: str) -> None:
    """
    Run subprocess in a separate function

    """
    try:
        process_check = subprocess.run(command, shell=True, check=True)
        process_check.check_returncode()
    except IOError:
        traceback.print_exc()
        error(
            f"The file for {file} is either empty or the location has changed investigate"
        )


def batch_processing(
    table_to_upload: str, file_name: str, base_url: str, private_token=None
):
    """
    The function uses curl to download the file and convert the YAML to JSON.
    Then upload the JSON file to external stage and then load it snowflake.
    Post load the files are removed from the external stage
    """
    json_file_name = get_json_file_name(input_file=file_name)

    info(f"... Start downloading {file_name} to {json_file_name}.json file.")

    if private_token:
        header = f'--header "PRIVATE-TOKEN: {private_token}"'
        command = f"curl {header} '{base_url}{file_name}%2Eyml/raw?ref=main' | yaml2json -o {json_file_name}.json"
    else:
        command = f"curl {base_url}{file_name} | yaml2json -o {json_file_name}.json"

    run_subprocess(command=command, file=file_name)
    info(f"... End downloading {file_name} to {json_file_name}.json file.")

    upload_to_snowflake(file_for_upload=f"{json_file_name}.json", table=table_to_upload)


def manifest_reader(file_path: str):
    """
    Read a yaml manifest file into a dictionary and return it.
    """

    with open(file=file_path, mode="r", encoding="utf8") as file:
        manifest_dict = yaml.load(file, Loader=yaml.FullLoader)

    return manifest_dict


def get_base_url(url_specification, table_name: str) -> str:
    """
    Return base url
    """
    if isinstance(url_specification, dict):
        return url_specification[table_name]
    return url_specification


def get_private_token(token_name: str):
    """
    Return private token, if exists.
    Otherwise, return None
    """
    if token_name:
        return env.get(token_name)
    return None


def process_file(specification: dict, table_to_upload: str, file_name: str) -> None:
    """
    Assign properties and process file
    """
    info(f"... Start processing {file_name} to Snowflake stage.")

    streaming = specification.get("streaming", False)
    base_url = get_base_url(
        url_specification=specification["URL"], table_name=table_to_upload
    )
    private_token = get_private_token(
        token_name=specification.get("private_token", None)
    )

    if streaming:
        stream_processing(
            table_to_upload=table_to_upload,
            file_name=file_name,
            base_url=base_url,
            private_token=private_token,
        )
    else:
        batch_processing(
            table_to_upload=table_to_upload,
            file_name=file_name,
            base_url=base_url,
            private_token=private_token,
        )
    info(f"... End processing {file_name} to Snowflake stage.")


def run(file_path: str = "gitlab_data_yaml/file_specification.yml") -> None:
    """
    Procedure to process files from the manifest file.
    """
    manifest = manifest_reader(file_path=file_path)

    for file_group, specification in manifest.items():
        info(f"Start processing {file_group} to Snowflake stage.")

        for table_to_upload, file_name in specification["files"].items():
            process_file(
                specification=specification,
                table_to_upload=table_to_upload,
                file_name=file_name,
            )

        info(f"End processing {file_group} to Snowflake stage.")


if __name__ == "__main__":
    run()
