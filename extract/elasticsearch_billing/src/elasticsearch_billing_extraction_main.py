"""
 This is main code base to kick of the extraction for elasticsearch billing.
"""
import os
import sys
from logging import basicConfig, getLogger, info

import fire

from extract.elasticsearch_billing.src.itemized_costs import (
    extract_load_billing_itemized_costs,
    get_itemized_costs_backfill,
)
from extract.elasticsearch_billing.src.costs_overview import (
    extract_load_billing_costs_overview,
    get_costs_overview_backfill,
)
from extract.elasticsearch_billing.src.itemized_costs_by_deployment import (
    extract_load_billing_itemized_costs_by_deployment,
    get_itemized_costs_by_deployments_backfill,
)

config_dict = os.environ.copy()

if __name__ == "__main__":
    basicConfig(stream=sys.stdout, level=20)
    getLogger("snowflake.connector.cursor").disabled = True
    fire.Fire(
        {
            "extract_load_billing_itemized_costs_by_deployment": extract_load_billing_itemized_costs_by_deployment,
            "extract_load_billing_itemized_costs": extract_load_billing_itemized_costs,
            "extract_load_billing_costs_overview": extract_load_billing_costs_overview,
            "extract_load_billing_itemized_costs_by_deployment_full_load": get_itemized_costs_by_deployments_backfill,
            "extract_load_billing_itemized_costs_full_load": get_itemized_costs_backfill,
            "extract_load_billing_costs_overview_full_load": get_costs_overview_backfill,
        }
    )
    info("Complete.")
