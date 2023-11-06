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
