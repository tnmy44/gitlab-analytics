"""
Extracts data from GCP bucket, refactors ticket_audits and uploads it snowflake.
"""
import sys
import os
import logging
import pandas as pd
import json
import fire
from logging import info, error, basicConfig, getLogger

from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    dataframe_uploader,
)

config_dict = os.environ.copy()

def read_file_from_gcp_bucket():
    pass

def refactor_ticket_audits():
    """
    This function will refactor the ticket audits table where it flattens the events object and extracts field_name,type,value,id out of it
    """
    df = pd.read_csv('csv file to read goes here')
    df= df.rename(columns=str.lower)

    #CREATE An empty dataframe
    output_df = pd.DataFrame()
    output_list=[]

    for ind in df.index:
        _sdc_batched_at = df['_sdc_batched_at'][ind]
        _sdc_deleted_at = df['_sdc_deleted_at'][ind]
        _sdc_extracted_at = df['_sdc_extracted_at'][ind]
        via = df['via'][ind]
        id = df['events'][ind]
        created_at = df['created_at'][ind]
        author_id = df['author_id'][ind]
        ticket_id = df['ticket_id'][ind]
        events = json.loads(df['events'][ind])
        #iterate through all keys in events object
        for key in events:
            if 'field_name' in key:
                if key['field_name'] in ('sla_policy','priority','is_public'):
                    field_name = key['field_name']
                    type = key['type']
                    value = key['value']
                    id = key['id']
                    row_list=[_sdc_batched_at,_sdc_deleted_at,_sdc_extracted_at,author_id,created_at,field_name,type,value,id,ticket_id,via]
                    output_list.append(row_list)

    # add output_list to output_df
    output_df = pd.DataFrame(output_list, columns =['_sdc_batched_at','_sdc_deleted_at','_sdc_extracted_at','author_id','created_at','field_name','type','value','id','ticket_id','via'])
    upload_to_snowflake(output_df)

def upload_to_snowflake(output_df):
    """
    This function will upload the dataframe to snowflake
    """
    loader_engine = snowflake_engine_factory(config_dict, "LOADER")
    dataframe_uploader(
            output_df,
            loader_engine,
            table_name="ticket_audits",
            schema="tap_zendesk",
            add_uploaded_at=False,
        )
    print(f"\nUploaded 'ticket_audits'")


def main():
    read_file_from_gcp_bucket()
    refactor_ticket_audits()

if __name__ == "__main__":
    basicConfig(stream=sys.stdout, level=20)
    getLogger("snowflake.connector.cursor").disabled = True
    fire.Fire(main)
    info("Complete.")