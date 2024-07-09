"""Module returning a csv with downstream dbt dependencies and data science exposures for a list of model provided by the user."""

import csv
import pandas as pd
from dbt.cli.main import dbtRunner, dbtRunnerResult


def dbt_model_dependencies_list(model_name_list: list):
    """Takes in a list of models, returns the model and its dependencies"""

    # create empty lists to hold models
    model_dependencies = []

    # initialize dbt invoke
    dbt = dbtRunner()

    # for each create CLI args as a list of strings
    for model in model_name_list:
        cli_args_model = ["list", "--select", f"{model}+", "--resource-type=model"]

        # run the command for models
        res_models: dbtRunnerResult = dbt.invoke(cli_args_model)

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


def dbt_model_exposures_list(model_name_list: list):
    """Takes in a list of models, returns the model and its data science exposures"""

    # create empty lists to hold exposures
    model_exposures = []

    # initialize dbt invoke
    dbt = dbtRunner()

    # for each create CLI args as a list of strings
    for model in model_name_list:
        cli_args_exposure = [
            "list",
            "--select",
            f"{model}+",
            "--resource-type=exposure",
        ]

        # run the command for exposures
        res_exposures: dbtRunnerResult = dbt.invoke(cli_args_exposure)

        # get counts
        if res_exposures.result is not None:
            count_exposures = len(res_exposures.result)
        else:
            count_exposures = 0

        # append results in table for exposures
        model_exposures.append(
            {
                "model_name": f"{model}",
                "exposures": res_exposures.result,
                "count_exposures": count_exposures,
            }
        )

    return model_exposures


model_names = []

# responses should be of the form
# full/path/from/home/directory/to/file/my_read_and_write_files.csv
fileToReadPath = input("Provide the full path to your read file: ")

with open(fileToReadPath, "r", newline="", encoding="utf-8") as readFile:
    reader = csv.reader(
        readFile, skipinitialspace=True, delimiter=",", quoting=csv.QUOTE_NONE
    )
    for row in reader:
        model_names.append(row)

model_list = model_names[0]

# generate results
models_with_dependencies = dbt_model_dependencies_list(model_list)
models_with_exposures = dbt_model_exposures_list(model_list)

# convert lists to dataframes in order to merge
models_with_dependencies_df = pd.DataFrame(models_with_dependencies)
models_with_exposures_df = pd.DataFrame(models_with_exposures)

# merge
models_with_dependencies_and_exposures = pd.merge(
    models_with_dependencies_df, models_with_exposures_df, on="model_name"
)

# create csv
with open("models_with_dependencies.csv", "w", encoding="utf-8") as csvfile:
    models_with_dependencies_and_exposures.to_csv(
        "models_with_dependencies.csv", index=False
    )
