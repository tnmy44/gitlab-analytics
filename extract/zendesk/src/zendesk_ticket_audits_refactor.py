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
import json

from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    dataframe_uploader,
)

config_dict = os.environ.copy()


def refactor_ticket_audits_read_gcp():
    """
    Read file from GCP bucket for ticket_audits
    """
    ZENDESK_SENSITIVE_SERVICE_ACCOUNT_CREDENTIALS = config_dict.get(
        "ZENDESK_SENSITIVE_SERVICE_ACCOUNT_CREDENTIALS"
    )
    bucket_name = "meltano_data_ops"
    scope = ["https://www.googleapis.com/auth/cloud-platform"]
    keyfile = json.loads(ZENDESK_SENSITIVE_SERVICE_ACCOUNT_CREDENTIALS, strict=False)
    credentials = service_account.Credentials.from_service_account_info(keyfile)
    scoped_credentials = credentials.with_scopes(scope)
    storage_client = storage.Client(credentials=scoped_credentials)
    BUCKET = storage_client.get_bucket(bucket_name)

    df = pd.DataFrame()

    # load all.jsonl files in bucket one by one
    for blob in BUCKET.list_blobs(
        prefix="meltano/tap_zendesk__sensitive/ticket_audits/"
    ):
        if blob.name.endswith(".jsonl"):
            # download this .jsonl blob and store it in pandas dataframe
            info(f"Reading the file {blob.name}")
            try:
                chunks = pd.read_json(
                    io.BytesIO(blob.download_as_string()), lines=True, chunksize=20000
                )
                count = 1
                for chunk in chunks:
                    info(f"Uploading to dataframe, batch:{count}")
                    df = pd.concat([df, chunk])
                    count = count + 1
                refactor_ticket_audits(df)
            except:
                error(f"Error reading {blob.name}")
            # blob.delete() # delete the file after successfull upload to the table, commentiong it for now for testing purposes
        else:
            error(f"No file found!")
            sys.exit(1)


def refactor_ticket_audits(df: pd.DataFrame):
    """
    This function will refactor the ticket audits table where it flattens the events object and extracts field_name,type,value,id out of it
    """
    output_list = []
    info(f"Transforming file...")
    for ind in df.index:
        via = df["via"][ind]
        id = df["id"][ind]
        created_at = df["created_at"][ind]
        author_id = df["author_id"][ind]
        ticket_id = df["ticket_id"][ind]
        events = df["events"][ind]
        EVENTS_OUT = []
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
                        field_id = "null"
                    else:
                        field_id = key["id"]
                    EVENTS_DICT_REC = {
                        "id": field_id,
                        "value": value,
                        "type": type,
                        "field_name": field_name,
                    }
                    EVENTS_OUT.append(EVENTS_DICT_REC)
        row_list = [
            author_id,
            created_at,
            EVENTS_OUT,
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
            "events",
            "id",
            "ticket_id",
            "via",
        ],
    )
    info(f"Transformation complete, uploading records to snowflake...")
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
    info(f"\nUploaded 'ticket_audits_test' to Snowflake")


def main():
    refactor_ticket_audits_read_gcp()


if __name__ == "__main__":
    basicConfig(stream=sys.stdout, level=20)
    getLogger("snowflake.connector.cursor").disabled = True
    fire.Fire(main)
    info("Complete.")
