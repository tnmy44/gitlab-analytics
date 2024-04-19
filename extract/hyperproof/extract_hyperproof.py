import requests
from logging import error, info, basicConfig, getLogger, warning
import pandas as pd
from os import environ as env

from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    snowflake_stage_load_copy_remove,
)


class HyperproofAPIClient:
    def __init__(self, client_id: str, client_secret: str) -> None:
        """

        :param client_id:
        :param client_secret:
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None
        info(f"Client ID: {client_id}")
        info(f"Client Secret: {client_secret}")

    def authenticate(self):
        """

        :return:
        """
        auth_url = "https://accounts.hyperproof.app/oauth/token"
        payload = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        response = requests.post(auth_url, data=payload)
        if response.status_code == 200:
            self.access_token = response.json().get("access_token")
            info("Authentication successful")
        else:
            error(f"Failed to authenticate. Status code: {response.status_code}")
            info(f"Client ID: {client_id}")
            info(f"Client Secret: {client_secret}")

    def get_data_from_all_endpoints(self):
        """

        :return:
        """
        if not self.access_token:
            self.authenticate()

        endpoints = self.get_available_endpoints()
        all_data = {}
        for endpoint in endpoints:
            data = self.get_data(endpoint)
            all_data[endpoint] = data
        return all_data

    def get_available_endpoints(self):
        """
            Currently no available API call for this so have dummied this function to just return this list.
        :return:
        """
        return [
            "labels",
            "taskstatuses",
            "tasks",
            "users",
            "controls",
            "customapps",
            "roles",
            "risks",
            "programs",
        ]

    def get_data(self, endpoint: str):
        """

        :param endpoint:
        :return:
        """
        if not self.access_token:
            self.authenticate()

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }
        url = f"https://api.hyperproof.app/v1/{endpoint}"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            error(
                f"Failed to retrieve data from {endpoint}. Status code: {response.status_code}"
            )
            return None


if __name__ == "__main__":
    config_dict = env.copy()

    client_id = env["HYPERPROOF_CLIENT_ID"]
    client_secret = env["HYPERPROOF_CLIENT_SECRET"]
    info("ABC")
    info(f"Client ID: {client_id}")
    info(f"Client Secret: {client_secret}")
    client = HyperproofAPIClient(client_id, client_secret)
    # Get data from all available endpoints
    all_data = client.get_data_from_all_endpoints()

    snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")

    if all_data:
        info("Retrieved data from all endpoints:")
        for endpoint, data in all_data.items():
            if data and len(data) > 0:
                df = pd.DataFrame([data])
                df.to_json(f"{endpoint}.json", index=False)

                info(f"Uploading {endpoint}.json to Snowflake stage.")

                snowflake_stage_load_copy_remove(
                    f"{endpoint}.json",
                    "raw.hyperproof.hyperproof_extract",
                    f"raw.hyperproof.{endpoint}",
                    snowflake_engine,
                )
            else:
                warning(f"No data available for {endpoint}")
