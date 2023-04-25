from os import environ as env
from api import ZuoraQueriesAPI
from typing import Dict
from fire import Fire
import logging
import yaml

def manifest_reader(file_path: str) -> Dict[str, Dict]:
    """
    Read a yaml manifest file into a dictionary and return it.
    """

    with open(file_path, "r") as file:
        manifest_dict = yaml.load(file, Loader=yaml.FullLoader)

    return manifest_dict


def filter_manifest(manifest_dict: Dict, load_only_table: str = None) -> Dict:
    # When load_only_table specified reduce manifest to keep only relevant table config
    if load_only_table and load_only_table in manifest_dict["tables"].keys():
        manifest_dict["tables"] = {
            load_only_table: manifest_dict["tables"][load_only_table]
        }

    return manifest_dict


def main(file_path: str, load_only_table: str = None) -> None:
    config_dict = env.copy()
    zq = ZuoraQueriesAPI(config_dict)

    manifest_dict = manifest_reader(file_path)
    # When load_only_table specified reduce manifest to keep only relevant table config
    manifest_dict = filter_manifest(manifest_dict, load_only_table)

    logging.info(manifest_dict)

    for table_spec in manifest_dict.get('tables'):
        logging.info(f"Processing {table_spec}")
        logging.info(manifest_dict.get(table_spec))
        job_id = zq.request_data_query_data(
                query_string=manifest_dict.get(table_spec).get("query")
        )
        logging.info(manifest_dict.get(table_spec).get("query"))
        df = zq.get_data_query_file(job_id)
        dataframe_uploader(
                df,
                zq.snowflake_engine,
                table_spec,
                schema="ZUORA_QUERY_API",
                if_exists="replace",
        )
        logging.info(f"Processed {table_spec}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("snowflake.connector.cursor").disabled = True
    logging.getLogger("snowflake.connector.connection").disabled = True
    Fire({"tap": main})