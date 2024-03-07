"""
Responsible for fetching the direct tableau dependencies for a model, using the Monte Carlo API.
This acts as a base script for the tableau_direct_dependency_query CI job.
"""

import os
from logging import info
import requests

dwId = os.environ.get("CI_DATA_WAREHOUSE_ID")
x_mcd_id = os.environ.get("CI_MCD_TOKEN_ID")
x_mcd_token = os.environ.get("CI_MCD_TOKEN_SECRET")
URL = "https://api.getmontecarlo.com/graphql"


HEADERS = {
    "x-mcd-id": x_mcd_id,
    "x-mcd-token": x_mcd_token,
    "Content-Type": "application/json",
}


def get_response(payload):
    """
    Return response object from Monte Carlo API
    """
    # try to get response from Monte Carlo API

    response = requests.post(URL, headers=HEADERS, json=payload, timeout=180)
    if response.status_code != 200:
        raise Exception(
            f"Error fetching data from Monte Carlo API. Status code: {response.status_code}"
        )
    response_content = response.json()
    return response_content


def get_table_path_query(table_id):
    """
    Return table path based on table name
    i.e table_name='instance_redis_metrics'

    For this particular use case, it will return the table_path
    i.e, raw:saas_usage_ping.instance_redis_metrics
    """
    first = 1
    json = {
        "query": "query GetTables($dwId:UUID,$tableId:String,$first:Int) {getTables(dwId:$dwId,tableId:$tableId,first:$first) {edges{node{mcon,fullTableId}}}}",
        "variables": {"dwId": f"{dwId}", "tableId": f"{table_id}", "first": f"{first}"},
    }
    response_content = get_response(json)
    if len(response_content["data"]["getTables"]["edges"]) > 0:
        table_path = response_content["data"]["getTables"]["edges"][0]["node"][
            "fullTableId"
        ]
    else:
        table_path = None
    return table_path


def query_table(full_table_id):
    """
    Return table information based on full_table_path
    i.e full_table_path='raw:saas_usage_ping.instance_redis_metrics'

    For this particular use case, used to return the table mcon (monte carlo table id)
    """
    json = {
        "query": "query GetTable($dwId: UUID,$fullTableId: String){getTable(dwId:$dwId,fullTableId:$fullTableId){tableId,mcon}}",
        "variables": {"dwId": f"{dwId}", "fullTableId": f"{full_table_id}"},
    }

    response_content = get_response(json)
    table_mcon = response_content["data"]["getTable"]["mcon"]

    return table_mcon


def get_downstream_node_dependencies(table_mcon):
    """
    This will return all directly dependent downstream tableau nodes for a given model.
    """
    direction = "downstream"
    json = {
        "query": "query GetTableLineage($direction: String!, $mcon: String) {getTableLineage(direction:$direction,mcon:$mcon){connectedNodes{displayName,mcon,objectType}}}",
        "variables": {"direction": f"{direction}", "mcon": f"{table_mcon}"},
    }

    response_content = get_response(json)
    response_derived_tables_partial_lineage = response_content["data"][
        "getTableLineage"
    ]

    return response_derived_tables_partial_lineage


def check_response_for_tableau_dependencies(response_downstream_dependencies):
    """
    This will return all dependent downstream nodes for a given source table.
    """
    dependency_list = []
    for node in response_downstream_dependencies["connectedNodes"]:
        output_dict = {}
        object_type = [
            "tableau-published-datasource-live",
            "tableau-published-datasource-extract",
            "tableau-view",
        ]
        if (
            node["objectType"] in object_type
            and node["objectType"] != "periscope-chart"
        ):
            # append node to output_dict
            output_dict[
                node["displayName"]
            ] = f"https://getmontecarlo.com/assets/{node['mcon']} ({node['objectType']})"
            # append output_dict to dependency_list
            dependency_list.append(output_dict)

    return dependency_list


# Assumes git diff was run to output the sql files that changed
with open("diff.txt", "r", encoding="UTF-8") as f:
    lines = f.readlines()
    for line in lines:
        info(
            "Checking for downstream dependencies in Tableau for the model "
            + line.strip()
            + ""
        )
        full_table_path = get_table_path_query(line)
        print(line)
        # if no path is returned exit the script
        if full_table_path is None:
            print("No dependencies returned for model {}".format(line))
        else:
            source_table_mcon = query_table(full_table_path)
            response_downstream_node_dependencies = get_downstream_node_dependencies(
                source_table_mcon
            )
            output_list = check_response_for_tableau_dependencies(
                response_downstream_node_dependencies
            )

            # if length of output_list > 0 then show the list of downstream dependencies
            if len(output_list) > 0:
                # show each key value pair in output_list and append them in comparison.txt
                with open("comparison.txt", "a", encoding="UTF-8") as f:
                    write_string = f"\n\ndbt model: {line}\nFound {len(output_list)} downstream dependencies in Tableau for the model {line.strip()}\n"
                    f.write(write_string)
                    for item in output_list:
                        for key, value in item.items():
                            f.write(f"\n{key}: {value}")
