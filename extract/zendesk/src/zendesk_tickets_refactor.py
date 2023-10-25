"""
Extracts data from GCP bucket, refactors tickets and uploads it snowflake.
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


def refactor_tickets_read_gcp():
    """
    Read file from GCP bucket for tickets
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

    df_tickets = pd.DataFrame()

    # load all.jsonl files in bucket one by one
    for blob in BUCKET.list_blobs(prefix="meltano/tap_zendesk__sensitive/tickets/"):
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
                    df_tickets = pd.concat([df_tickets, chunk])
                    count = count + 1
                refactor_tickets(df_tickets)
                # blob.delete() # delete the file after successfull upload to the table, commentiong it for now for testing purposes
            except:
                error(f"Error reading {blob.name}")
            # blob.delete() # delete the file after successfull upload to the table, commentiong it for now for testing purposes
        else:
            error(f"No file found!")
            sys.exit(1)


def refactor_tickets(df_tickets: pd.DataFrame):
    """
    This function will refactor the tickets table
    """
    df_ticket_fields_extracted = pd.DataFrame()

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
    for blob in BUCKET.list_blobs(
        prefix="meltano/tap_zendesk__sensitive/ticket_fields/"
    ):
        info(f"Reading the file {blob.name}")
        if blob.name.endswith(".jsonl"):
            # open a csv file and put contents of it in dataframe
            df_ticket_fields_extracted = pd.read_json(
                io.BytesIO(blob.download_as_string()), lines=True
            )

    output_list_ticket_field = []

    for ind in df_ticket_fields_extracted.index:
        CUSTOM_FIELD_OPTIONS = df_ticket_fields_extracted["custom_field_options"][ind]
        # print(CUSTOM_FIELD_OPTIONS)
        id = df_ticket_fields_extracted["id"][ind]
        if (
            type(CUSTOM_FIELD_OPTIONS) is not float
        ):  # When the data is null CUSTOM_FIELD_OPTIONS is a float
            for key in CUSTOM_FIELD_OPTIONS:
                # print(key['value'])
                if id == 360020421853:
                    ticket_field_value = key["value"]
                    output_list_ticket_field.append(ticket_field_value)
        else:
            continue

    # convert dataframe column names to upper caps

    df_tickets.columns = map(str.upper, df_tickets.columns)

    output_list = []
    # print(df_tickets)
    info(f"Tranformation in progress...")
    for ind in df_tickets.index:
        ALLOW_ATTACHMENTS = df_tickets["ALLOW_ATTACHMENTS"][ind]
        ALLOW_CHANNELBACK = df_tickets["ALLOW_CHANNELBACK"][ind]
        ASSIGNEE_ID = df_tickets["ASSIGNEE_ID"][ind]
        BRAND_ID = df_tickets["BRAND_ID"][ind]
        COLLABORATOR_IDS = df_tickets["COLLABORATOR_IDS"][ind]
        CREATED_AT = df_tickets["CREATED_AT"][ind]
        CUSTOM_FIELDS = df_tickets["CUSTOM_FIELDS"][ind]
        # CUSTOM_FIELDS_DICT = json.loads(CUSTOM_FIELDS)
        DESCRIPTION = df_tickets["DESCRIPTION"][ind]
        DUE_AT = df_tickets["DUE_AT"][ind]
        EMAIL_CC_IDS = df_tickets["EMAIL_CC_IDS"][ind]
        EXTERNAL_ID = df_tickets["EXTERNAL_ID"][ind]
        FOLLOWER_IDS = df_tickets["FOLLOWER_IDS"][ind]
        FOLLOWUP_IDS = df_tickets["FOLLOWUP_IDS"][ind]
        FORUM_TOPIC_ID = df_tickets["FORUM_TOPIC_ID"][ind]
        GENERATED_TIMESTAMP = df_tickets["GENERATED_TIMESTAMP"][ind]
        GROUP_ID = df_tickets["GROUP_ID"][ind]
        HAS_INCIDENTS = df_tickets["HAS_INCIDENTS"][ind]
        ID = df_tickets["ID"][ind]
        IS_PUBLIC = df_tickets["IS_PUBLIC"][ind]
        ORGANIZATION_ID = df_tickets["ORGANIZATION_ID"][ind]
        PRIORITY = df_tickets["PRIORITY"][ind]
        PROBLEM_ID = df_tickets["PROBLEM_ID"][ind]
        RECIPIENT = df_tickets["RECIPIENT"][ind]
        REQUESTER_ID = df_tickets["REQUESTER_ID"][ind]
        SATISFACTION_RATING = df_tickets["SATISFACTION_RATING"][ind]
        SHARING_AGREEMENT_IDS = df_tickets["SHARING_AGREEMENT_IDS"][ind]
        STATUS = df_tickets["STATUS"][ind]
        SUBJECT = df_tickets["SUBJECT"][ind]
        SUBMITTER_ID = df_tickets["SUBMITTER_ID"][ind]
        TAGS = df_tickets["TAGS"][ind]
        TICKET_FORM_ID = df_tickets["TICKET_FORM_ID"][ind]
        TYPE = df_tickets["TYPE"][ind]
        UPDATED_AT = df_tickets["UPDATED_AT"][ind]
        URL = df_tickets["URL"][ind]
        VIA = df_tickets["VIA"][ind]
        CUSTOM_FIELDS_OUT = []
        for key in CUSTOM_FIELDS:
            if key["id"] is None and key["value"] is None:
                ticket_custom_field_id = "null"
                ticket_custom_field_value = "null"
            else:
                # iterate through each field in output_list_ticket_field
                for item in output_list_ticket_field:
                    # print(item)
                    if item == key["value"]:
                        ticket_custom_field_value = key["value"]
                        ticket_custom_field_id = key["id"]
                        CUSTOM_FIELDS_DICT_REC = {
                            "id": ticket_custom_field_id,
                            "value": ticket_custom_field_value,
                        }
                        CUSTOM_FIELDS_OUT.append(CUSTOM_FIELDS_DICT_REC)

        # append all the columns along with ticket_custom_field_id, ticket_custom_field_value in output list
        row_list = [
            ALLOW_ATTACHMENTS,
            ALLOW_CHANNELBACK,
            ASSIGNEE_ID,
            BRAND_ID,
            COLLABORATOR_IDS,
            CREATED_AT,
            CUSTOM_FIELDS_OUT,
            DESCRIPTION,
            DUE_AT,
            EMAIL_CC_IDS,
            EXTERNAL_ID,
            FOLLOWER_IDS,
            FOLLOWUP_IDS,
            FORUM_TOPIC_ID,
            GENERATED_TIMESTAMP,
            GROUP_ID,
            HAS_INCIDENTS,
            ID,
            IS_PUBLIC,
            ORGANIZATION_ID,
            PRIORITY,
            PROBLEM_ID,
            RECIPIENT,
            REQUESTER_ID,
            SATISFACTION_RATING,
            SHARING_AGREEMENT_IDS,
            STATUS,
            SUBJECT,
            SUBMITTER_ID,
            TAGS,
            TICKET_FORM_ID,
            TYPE,
            UPDATED_AT,
            URL,
            VIA,
        ]
        # replace the value of description with null in row_list
        row_list[7] = "null"
        # replace the value of subject with null in row_list
        row_list[27] = "null"
        output_list.append(row_list)

    output_df = pd.DataFrame(
        output_list,
        columns=[
            "ALLOW_ATTACHMENTS",
            "ALLOW_CHANNELBACK",
            "ASSIGNEE_ID",
            "BRAND_ID",
            "COLLABORATOR_IDS",
            "CREATED_AT",
            "CUSTOM_FIELDS",
            "DESCRIPTION",
            "DUE_AT",
            "EMAIL_CC_IDS",
            "EXTERNAL_ID",
            "FOLLOWER_IDS",
            "FOLLOWUP_IDS",
            "FORUM_TOPIC_ID",
            "GENERATED_TIMESTAMP",
            "GROUP_ID",
            "HAS_INCIDENTS",
            "ID",
            "IS_PUBLIC",
            "ORGANIZATION_ID",
            "PRIORITY",
            "PROBLEM_ID",
            "RECIPIENT",
            "REQUESTER_ID",
            "SATISFACTION_RATING",
            "SHARING_AGREEMENT_IDS",
            "STATUS",
            "SUBJECT",
            "SUBMITTER_ID",
            "TAGS",
            "TICKET_FORM_ID",
            "TYPE",
            "UPDATED_AT",
            "URL",
            "VIA",
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
        table_name="tickets",
        schema="tap_zendesk",
        if_exists="append",
        add_uploaded_at=True,
    )
    info(f"\nUploaded 'tickets_test' to Snowflake")


def main():
    refactor_tickets_read_gcp()


if __name__ == "__main__":
    basicConfig(stream=sys.stdout, level=20)
    getLogger("snowflake.connector.cursor").disabled = True
    fire.Fire(main)
    info("Complete.")
