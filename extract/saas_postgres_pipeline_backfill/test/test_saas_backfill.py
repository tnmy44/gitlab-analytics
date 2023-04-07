"""
Testing routine for manifest decomposition
"""
import os
import sys
from unittest.mock import Mock

abs_path = os.path.dirname(os.path.realpath(__file__))
abs_path = (
    abs_path[: abs_path.find("extract")]
    + "extract/saas_postgres_pipeline_backfill/postgres_pipeline/"
)
sys.path.append(abs_path)

from utils import has_new_columns


def test_has_new_columns():
    source_columns = ["a", "b"]
    gcs_columns = ["a", "b"]
    res = has_new_columns(source_columns, gcs_columns)
    assert res is False

    source_columns = ["a", "b"]
    gcs_columns = ["a", "b", "c"]
    res = has_new_columns(source_columns, gcs_columns)
    assert res is False

    source_columns = ["a", "b", "c"]
    gcs_columns = ["a", "b"]
    res = has_new_columns(source_columns, gcs_columns)
    assert res
