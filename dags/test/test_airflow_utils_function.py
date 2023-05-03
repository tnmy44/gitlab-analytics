import os
import urllib.parse
import pathlib
from datetime import date, timedelta
from typing import List, Dict

def get_sales_analytics_notebooks(frequency: str) -> Dict:
    notebooks = []
    fileNames = []

    #path = pathlib.Path(f"{SALES_ANALYTICS_NOTEBOOKS_PATH}/{frequency}/")
    path = pathlib.Path(f"/Users/nfiguera/repos/analytics/sales_analytics_notebooks/daily")

    for file in path.rglob("*.ipynb"):
       
        absolute_path = file.absolute()
        notebooks.append(absolute_path)
        fileNames.append(os.path.splitext(absolute_path)[0])
        print(absolute_path)
        print(os.path.splitext(absolute_path)[0])
    return dict(zip(notebooks, fileNames))

print(get_sales_analytics_notebooks(""))