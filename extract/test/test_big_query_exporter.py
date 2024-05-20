from datetime import datetime

import pytest
import os

from extract.bigquery.src.bigquery_export import get_billing_data_query

os.environ["GIT_BRANCH"] = "test-branch"
os.environ["EXPORT_DATE"] = "2023-10-10"

test_dict_d = {
    "name": "export-name",
    "table": "gcp-projext.data_set.table",
    "bucket_path": "gs://bucket/table",
    "partition_date_part": "d",
    "export_query": "SELECT *\n , current_timestamp() as gcs_export_time\nFROM `gcp-projext.data_set.table`\nWHERE date = '{EXPORT_DATE}'",
}


def test_query_compilation():
    returned_query = get_billing_data_query(test_dict_d)

    accepted_q = (
        "EXPORT DATA OPTIONS("
        "  uri='gs://bucket/BRANCH_TEST_test-branch/table/2023-10-10/*.parquet',"
        "  format='PARQUET',"
        "  overwrite=true"
        "  ) AS"
        "    SELECT *\n , current_timestamp() as gcs_export_time\nFROM `gcp-projext.data_set.table`\nWHERE date = '2023-10-10'"
    )

    assert returned_query == accepted_q
