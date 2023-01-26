# type: ignore
import logging
import sys
import requests
import json
import yaml
import pandas as pd
from typing import Dict, Any
from datetime import datetime, timedelta
from os import environ as env


class ZuoraRevProAPI:
    def __init__(self, config_dict: Dict[str, str]):
        self.headers = config_dict["headers"]
        self.authenticate_url_zuora_revpro = config_dict[
            "authenticate_url_zuora_revpro"
        ]
        self.zuora_fetch_data_url = config_dict["zuora_download_report_url"]
        self.bucket_name = config_dict["bucket_name"]
        self.zuora_fetch_report_list_url = config_dict["zuora_fetch_report_list_url"]        
        logging.basicConfig(
            filename="zuora_report_extract.log",
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            level=20,
        )
        self.logger = logging.getLogger(__name__)
        zuora_token=self.get_auth_token()
        self.request_headers = {
            "token": f"{zuora_token}",
        }

 
    def get_auth_token(self) -> str:
        """
        This function receives url and header information and generate authentication token in response.
        If we don't receive the response status code  200 then exit the program.
        """
        response = requests.post(
            self.authenticate_url_zuora_revpro, headers=self.headers
        )
        if response.status_code == 200:
            self.logger.info("Authentication token generated successfully")
            authToken = response.headers["Revpro-Token"]
            return authToken
        else:
            self.logger.error(
                f"HTTP {response.status_code} - {response.reason}, Message {response.text}"
            )
            sys.exit(1)

    
    def get_extract_date(self,extract_date: str = None) -> str:
        """
        This function provide the date for which we need to look for report.Reports can be downloaded only for 
        48 hours from the time it runs. 
        If todays  data has to be loaded then pass that as parameter else it will download data for current_date - 1.
        Zuora consume date in format `DD-MMM-YYYY` example `16-JAN-2023` 
        """

        if extract_date is None:
            yesterday=datetime.now() - timedelta(1)
            extract_date=datetime.strftime(yesterday, '%d-%b-%Y')
        else:
            extract_date=extract_date
        
        return extract_date

    def get_report_list_df(self,report_list):
        return pd.DataFrame(report_list)


    def get_report_list(self,extract_date: str) -> dict:

        get_report_list_url = f"{self.zuora_fetch_report_list_url}{extract_date}"
        report_list = requests.get(get_report_list_url, headers=self.request_headers)

        if report_list.status_code == 200:
            zuora_report_list = json.loads(report_list.text)
        else:
            self.logger.error(
                f"HTTP {report_list.status_code} - {report_list.reason}, Message {report_list.text}"
            )
            sys.exit(1)
        
        #print(zuora_report_list.get("Result"))
        return self.get_report_list_df(zuora_report_list.get("Result"))
    
    def get_requested_report(self,zuora_report_api_list: str = "zuora_report_api_list.yml"):
        with open(zuora_report_api_list) as file:
            report_list_to_download = yaml.load(file, Loader=yaml.FullLoader)
        
        return report_list_to_download

    def get_report_list_key(self,zuora_requested_report_list):
        """Extract list of report from the manifest file for which extraction needs to be done"""
        print(list(zuora_requested_report_list["report_list"].keys()))
        return list(zuora_requested_report_list["report_list"].keys())

    def get_report_list_dict(self,zuora_report_list_to_download,report):
        
        """Read manifest file for all required values"""
        report_list_dict={}
        report_list_dict["category"]=zuora_report_list_to_download["report_list"][report]["category"]
        report_list_dict["rep_name"]=zuora_report_list_to_download["report_list"][report]["rep_name"]
        report_list_dict["layout_name"]=zuora_report_list_to_download["report_list"][report]["layout_name"]
        report_list_dict["rep_desc"]=zuora_report_list_to_download["report_list"][report]["rep_desc"]
        return report_list_dict



    def zuora_download_report(self,zuora_report_list_df,zuora_report_list_to_download):
        requested_report_list=self.get_report_list_key(zuora_report_list_to_download)
        print(requested_report_list)
        #Iterate over each report which is required to be downloaded.
        for report in requested_report_list:
            print(report)
            report_list_dict = self.get_report_list_dict(zuora_report_list_to_download,report)
            file_name=zuora_report_list_df.loc[(zuora_report_list_df['category'] == report_list_dict.get('category')) 
                                    & (zuora_report_list_df['layout_name'] == report_list_dict.get('layout_name')) 
                                    & (zuora_report_list_df['rep_name'] == report_list_dict.get('rep_name'))
                                    & (zuora_report_list_df['rep_desc'] == report_list_dict.get('rep_desc'))
                                    ,['file_name']]
            list_of_file=[i for s in file_name.values for i in s]                        
            for files in list_of_file:

                

        



        

