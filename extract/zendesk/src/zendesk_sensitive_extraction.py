import fire
import sys
import os
from logging import info, error, basicConfig, getLogger
from zendesk_ticket_audits_refactor import refactor_ticket_audits_read_gcp
from zendesk_tickets_refactor import refactor_tickets_read_gcp
import json
from google.cloud import storage
from google.oauth2 import service_account

config_dict = os.environ.copy()

def get_bucket_details():
    """
    Validate GCP bucket for credentials
    """
    GCP_SERVICE_CREDS = config_dict.get("GCP_SERVICE_CREDS")
    ZENDESK_SENSITIVE_EXTRACTION_BUCKET_NAME = config_dict.get(
        "ZENDESK_SENSITIVE_EXTRACTION_BUCKET_NAME"
    )
    scope = ["https://www.googleapis.com/auth/cloud-platform"]
    keyfile = json.loads(GCP_SERVICE_CREDS, strict=False)
    credentials = service_account.Credentials.from_service_account_info(keyfile)
    scoped_credentials = credentials.with_scopes(scope)
    storage_client = storage.Client(credentials=scoped_credentials)
    BUCKET = storage_client.get_bucket(ZENDESK_SENSITIVE_EXTRACTION_BUCKET_NAME)

    return BUCKET

if __name__ == "__main__":
    basicConfig(stream=sys.stdout, level=20)
    getLogger("snowflake.connector.cursor").disabled = True
    fire.Fire(
        {
            "refactor_ticket_audits": refactor_ticket_audits_read_gcp,
            "refactor_tickets": refactor_tickets_read_gcp,
        }
    )
    info("Complete.")
