""" This file contains common operators/functions to be used across multiple DAGs """
import os
import urllib.parse
import pathlib
from datetime import date, timedelta
from typing import List, Dict




SSH_REPO = "git@gitlab.com:gitlab-data/analytics.git"
HTTP_REPO = "https://gitlab.com/gitlab-data/analytics.git"
DATA_IMAGE = "registry.gitlab.com/gitlab-data/data-image/data-image:v1.0.27"
DBT_IMAGE = "registry.gitlab.com/gitlab-data/dbt-image:v0.0.1"
PERMIFROST_IMAGE = "registry.gitlab.com/gitlab-data/permifrost:v0.13.1"
ANALYST_IMAGE = "registry.gitlab.com/gitlab-data/analyst-image:v0.0.2"

SALES_ANALYTICS_NOTEBOOKS_PATH = "/Users/nfiguera/repos/analytics/sales_analytics_notebooks"


def get_sales_analytics_notebooks(frequency: str) -> Dict:
    notebooks_paths = []
    fileNames = []
    notebooks_folders = []

    path = pathlib.Path(f"{SALES_ANALYTICS_NOTEBOOKS_PATH}/{frequency}/")
    
    for file in path.rglob("*.ipynb"):
       
        relative_path = file.relative_to(SALES_ANALYTICS_NOTEBOOKS_PATH)
        notebooks_paths.append(relative_path.as_posix())
        folder = relative_path.parent.as_posix()
        notebooks_folders.append(folder)
        expanded_name = folder.replace('/','_') + '_' +  relative_path.stem
        fileNames.append(expanded_name)
    return list(zip(notebooks_paths, notebooks_folders, fileNames))

print (get_sales_analytics_notebooks('daily'))