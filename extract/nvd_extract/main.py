""" Extracts data from the NVD API stream """
import json
import sys
import time
import requests
import pandas as pd


def extract_cpes(node, cpe_array):
    """
    Extracts CPEs from the given node, there can be multiple CPE's in the node.
    returns a list of CPEs
    """
    if "cpeMatch" in node:
        for cpe_match in node["cpeMatch"]:
            cpe_array.append(cpe_match["criteria"])
    return cpe_array


def deduplicate_cpes(cpes):
    """
    Function to remove duplicates CPEs for a given CVE
    returns a list of CPEs
    """
    cpe_set = set()
    for cpe in cpes:
        if cpe not in cpe_set:
            cpe_set.add(cpe)
        else:
            cpes.remove(cpe)
    return cpes


def refacor_response(response_dict):
    """
    Function to refactor the response from API and get the release data, CVE and it's associated CPE's and store it in output.csv
    """
    with open("output.json", "a") as outfile:
        json.dump(response_dict, outfile, indent=4)
    cve_object = {}
    for item in response_dict["vulnerabilities"]:
        cve_object["id"] = item["cve"]["id"]
        cve_object["published_date"] = item["cve"]["published"]
        cpe_matches = []
        if "configurations" in item["cve"]:
            for configurations in item["cve"]["configurations"]:
                for cpe_node in configurations["nodes"]:
                    extract_cpes(cpe_node, cpe_matches)
            cpe_matches = deduplicate_cpes(cpe_matches)
            cve_object["cpe_matches"] = cpe_matches

            # print(cve_object)

            # Convert to a dataframe and store it in output.csv which will be uploaded to SF
            df = pd.DataFrame(cve_object)
            df.to_csv("output.csv", mode="a", index=False, header=False)


def extract_data():
    """
    Main Function to extract data from the NVD API stream
    """
    # set the variables required for an api request
    api_key = ""  # Add your API key here, to generate one, visit https://nvd.nist.gov/developers/request-an-api-key
    baseUrl = "https://services.nvd.nist.gov/rest/json/cves/2.0/"
    header = {"apiKey": f"{api_key}"}

    startIndex = 0  # For initial extraction, startIndex is 0
    resultsPerPage = 100  # Limiting the results per request to 100

    has_response = (
        True  # Flag variable to check startIndex does not index resultsPerPage
    )

    while has_response:
        url_parameters = f"?resultsPerPage={resultsPerPage}&startIndex={startIndex}"
        url = baseUrl + url_parameters
        try:
            results = requests.get(url, headers=header)
            response_dict = results.json()

            refacor_response(response_dict)

            totalResults = response_dict["totalResults"]

            if startIndex > totalResults:
                has_response = False
            else:
                print(f"startIndex : {startIndex}")
                print(f"totalResults : {totalResults}")
            startIndex = (
                startIndex + resultsPerPage
            )  # Placing it here to fetch the last iteration of records which may be less than resultsPerPage
            time.sleep(
                3
            )  # sleep for 3 seconds between each request to avoid rate limits.
        except requests.exceptions.RequestException:
            print(
                "Error"
            )  # Raises exception if any https errors are encountered from the request.
            sys.exit(1)


extract_data()
