"""
Extracts data from GCP bucket, refactors ticket_audits and uploads it snowflake.
"""
import sys
import os
import logging
import pandas as pd
import fire
from logging import info, error, basicConfig, getLogger
import io
from google.cloud import storage
from os import environ as env
from yaml import load, FullLoader
from google.oauth2 import service_account

from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    dataframe_uploader,
)

config_dict = os.environ.copy()


def read_file_from_gcp_bucket():
    """
    Read file from GCP bucket for ticket_audits
    """
    ZENDESK_SENSITIVE_SERVICE_ACCOUNT_CREDENTIALS = config_dict.get(
        "ZENDESK_SENSITIVE_SERVICE_ACCOUNT_CREDENTIALS"
    )
    storage_client = storage.Client.from_service_account_json(
        ZENDESK_SENSITIVE_SERVICE_ACCOUNT_CREDENTIALS
    )
    bucket_name = "meltano_data_ops"
    BUCKET = storage_client.get_bucket(bucket_name)

    # load all.jsonl files in bucket one by one
    for blob in BUCKET.list_blobs(
        prefix="meltano/tap_zendesk__sensitive/ticket_audits/"
    ):
        info(f"Reading file {blob.name}")
        if blob.name.endswith(".jsonl"):
            # download this .jsonl blob and store it in pandas dataframe
            df = pd.read_json(io.BytesIO(blob.download_as_string()), lines=True)
            refactor_ticket_audits(df)
        else:
            error(f"No file found!")
            sys.exit(1)


def refactor_ticket_audits(df):
    """
    This function will refactor the ticket audits table where it flattens the events object and extracts field_name,type,value,id out of it
    """
    output_list = []
    info(f"Transforming file...")
    for ind in df.index:
        # _sdc_batched_at = df['_sdc_batched_at'][ind]
        # _sdc_deleted_at = df['_sdc_deleted_at'][ind]
        # _sdc_extracted_at = df['_sdc_extracted_at'][ind]
        via = df["via"][ind]
        id = df["events"][ind]
        created_at = df["created_at"][ind]
        author_id = df["author_id"][ind]
        ticket_id = df["ticket_id"][ind]
        events = df["events"][ind]
        # iterate through all keys in events object
        for key in events:
            if "field_name" in key:
                if key["field_name"] in ("sla_policy", "priority", "is_public"):
                    if key["field_name"] is None:
                        field_name = "null"
                    else:
                        field_name = key["field_name"]
                    if key["type"] is None:
                        type = "null"
                    else:
                        type = key["type"]
                    if key["value"] is None:
                        value = "null"
                    else:
                        value = key["value"]
                    if key["id"] is None:
                        id = "null"
                    else:
                        id = key["id"]
                    row_list = [
                        author_id,
                        created_at,
                        field_name,
                        type,
                        value,
                        id,
                        ticket_id,
                        via,
                    ]
                    output_list.append(row_list)

    # add output_list to output_df
    output_df = pd.DataFrame(
        output_list,
        columns=[
            "author_id",
            "created_at",
            "field_name",
            "type",
            "value",
            "id",
            "ticket_id",
            "via",
        ],
    )
    upload_to_snowflake(output_df)


def upload_to_snowflake(output_df):
    """
    This function will upload the dataframe to snowflake
    """
    loader_engine = snowflake_engine_factory(config_dict, "LOADER")
    dataframe_uploader(
        output_df,
        loader_engine,
        table_name="ticket_audits_test",
        schema="tap_zendesk",
        if_exists="append",
        add_uploaded_at=True,
    )
    print(f"\nUploaded 'ticket_audits' to Snowflake")


def main():
    read_file_from_gcp_bucket()


if __name__ == "__main__":
    basicConfig(stream=sys.stdout, level=20)
    getLogger("snowflake.connector.cursor").disabled = True
    fire.Fire(main)
    info("Complete.")
