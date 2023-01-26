import argparse
import subprocess
import logging
import os
from datetime import datetime
from api import ZuoraRevProAPI

# Define the argument required to run the  extraction process
parser = argparse.ArgumentParser(
    description="This enable to run the Extraction process for one table at a time."
)
bucket_name=os.getenv('zuora_bucket')
api_dns_name=os.getenv('zuora_dns')
api_auth_code=os.getenv('authorization_code')

parser.add_argument(
    "-extract_date",
    action="store",
    dest="extract_date",
    required=False,
    help="Pass parameter download todays report data format `DD-MMM-YYYY` example `16-JAN-2023`",
)

results = parser.parse_args()

if __name__ == "__main__":
    log_file_name = (
        "zuora_report_extract_"
        + (datetime.now()).strftime("%d-%m-%Y-%H:%M:%S")
        + ".log"
    )
    logging.basicConfig(
        filename="logs/" + log_file_name,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        level=20,
    )
    logger = logging.getLogger(__name__)
    logger.info("Prepare the URL for data extraction and authentication")
    config_dict = {
        "headers": {
            "role": "APIRole",
            "clientname": "Default",
            "Authorization": api_auth_code,
        },
        "authenticate_url_zuora_revpro": (
            "https://" + api_dns_name + "/api/integration/v1/authenticate"
        ),
        "zuora_fetch_report_list_url": (
            "https://" + api_dns_name + "/api/integration/v1/reports/list?createddate="
        ),
        "zuora_download_report_url":(
            "https://" + api_dns_name + "/api/integration/v1/reports/download/"
        ), 
        "bucket_name": bucket_name
    }

    extract_date = results.extract_date
    
    # Initialise the API class
    zuora_revpro = ZuoraRevProAPI(config_dict)

    report_date = zuora_revpro.get_extract_date(extract_date)
    
    zuora_report_list_df= zuora_revpro.get_report_list(report_date)

    print(zuora_report_list_df)
    #Read yml file 
    zuora_report_list_to_download=zuora_revpro.get_requested_report()

    zuora_revpro.zuora_download_report(zuora_report_list_df,zuora_report_list_to_download)
    

    