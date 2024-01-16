"""
 This is main code base to kick of the extraction for elasticsearch billing.
"""
import os
import sys
from logging import basicConfig, getLogger, info

import fire

from elasticsearch_billing_itemized_costs import (
    extract_load_billing_itemized_costs_full_load,
)
from elasticsearch_billing_costs_overview import (
    extract_load_billing_costs_overview_full_load,
)
from elasticsearch_billing_itemized_costs_by_deployment import (
    extract_load_billing_itemized_costs_by_deployment_full_load,
)

config_dict = os.environ.copy()

if __name__ == "__main__":
    basicConfig(stream=sys.stdout, level=20)
    getLogger("snowflake.connector.cursor").disabled = True
    fire.Fire(
        {
            "extract_load_billing_itemized_costs_by_deployment_full_load": extract_load_billing_itemized_costs_by_deployment_full_load,
            "extract_load_billing_itemized_costs_full_load": extract_load_billing_itemized_costs_full_load,
            "extract_load_billing_costs_overview_full_load": extract_load_billing_costs_overview_full_load,
        }
    )
    info("Complete.")
