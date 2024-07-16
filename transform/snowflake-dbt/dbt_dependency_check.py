"""Module returning a csv with downstream dbt dependencies and data science exposures for a list of model provided by the user."""

import os
import pandas as pd
from dbt.cli.main import dbtRunner, dbtRunnerResult


def dbt_model_dependencies_list(model_name_list: list, resource_type: str) -> list:
    """Takes in a list of models, returns the model and its dependencies"""

    # create empty lists to hold models
    model_dependencies = []

    # initialize dbt invoke
    dbt = dbtRunner()

    # for each create CLI args as a list of strings
    for model in model_name_list:
        cli_args = ["list", "--select", f"{model}+", f"--resource-type={resource_type}"]

        # run the command for models
        res_models: dbtRunnerResult = dbt.invoke(cli_args)

        # get counts
        if res_models.result is not None:
            count_dependencies = len(res_models.result)
        else:
            count_dependencies = 0

        # append results in table for models
        model_dependencies.append(
            {
                "model_name": f"{model}",
                "dependencies": res_models.result,
                "count_dependencies": count_dependencies,
            }
        )

    return model_dependencies


# Find directory name
dirname = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

"""
Navigate to the file models_to_check.csv and add the dbt model names (ex. fct_behavior_structured_event).
The file should contain a list of model names to test with no header, with one model name per line.

example file list:
gitlab_dotcom_audit_events
dim_crm_opportunity
"""
fileToReadPath = f"{dirname}/snowflake-dbt/models_to_check.csv"

# Create empty set to append the cleaned model names to
model_names = set()

# open file and add models to empty list for testing
with open(fileToReadPath, "r", newline="", encoding="utf-8") as readFile:
    lines = readFile.readlines()
    for line in lines:
        model_names.add(line.strip().lower())

# generate results
# Note: model dependencies count includes the model requested + all downstream, so the minimum number of dependencies is 1
models_with_dependencies = dbt_model_dependencies_list(model_names, "model")
models_with_exposures = dbt_model_dependencies_list(model_names, "exposure")

# convert lists to dataframes in order to merge the results into a single output
models_with_dependencies_df = pd.DataFrame(models_with_dependencies)
models_with_exposures_df = pd.DataFrame(models_with_exposures)

# merge the two results on model name to get a single output
models_with_dependencies_and_exposures = pd.merge(
    models_with_dependencies_df, models_with_exposures_df, on="model_name"
)

# create csv of the output called models_with_dependencies.csv
with open("models_with_dependencies.csv", "w", encoding="utf-8") as csvfile:
    models_with_dependencies_and_exposures.to_csv(
        "models_with_dependencies.csv", index=False
    )
