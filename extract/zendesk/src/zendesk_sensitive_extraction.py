import fire
import sys
from logging import info, error, basicConfig, getLogger
from zendesk_ticket_audits_refactor import refactor_ticket_audits_read_gcp
from zendesk_tickets_refactor import refactor_tickets_read_gcp

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
