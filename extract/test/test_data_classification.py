import pytest
from unittest.mock import Mock, patch
from extract.data_classification.data_classification_utils import DataClassification
import pandas as pd


@pytest.fixture
def data_classification():
    return DataClassification("PII", "mnpi_raw_file.json")


def test_init(data_classification):
    assert data_classification.tagging_type == "PII"
    assert data_classification.mnpi_raw_file == "mnpi_raw_file.json"
    assert data_classification.database_name == "rbacovic_prep"
    assert data_classification.schema_name == "benchmark_pii"
    assert data_classification.table_name == "sensitive_objects_classification"


def test_quoted():
    assert DataClassification.quoted("test") == "'test'"


# def test_load_mnpi_list(data_classification):
#     with patch("builtins.open", mock_open(read_data='{"name": "test"}\n{"name": "test2"}')):
#         result = data_classification.load_mnpi_list()
#         assert result == [{"name": "test"}, {"name": "test2"}]
#
#
def test_transform_mnpi_list(data_classification):
    input_list = [
        {"config": {"database": "db", "schema": "sch"}, "name": "table"},
        {"config": {"database": "db2", "schema": "sch2"}, "name": "table2"}
    ]
    expected_output = [["DB", "SCH", "TABLE"], ["DB2", "SCH2", "TABLE2"]]
    assert data_classification.transform_mnpi_list(input_list) == expected_output


# @pytest.mark.parametrize("section,scope_type,expected", [
#     ("PII", "include", " AND ((table_catalog IN ('DB1', 'DB2')))"),
#     ("PII", "exclude", " AND (NOT (table_catalog IN ('DB3', 'DB4')))"),
# ])
# def test_get_pii_scope(data_classification, section, scope_type, expected):
#     data_classification.scope = {
#         "data_classification": {
#             "PII": {
#                 "include": {"databases": ["DB1", "DB2"]},
#                 "exclude": {"databases": ["DB3", "DB4"]}
#             }
#         }
#     }
#     assert data_classification.get_pii_scope(section, scope_type) == expected


# def test_pii_query(data_classification):
#     data_classification.get_pii_scope = Mock(return_value=" AND (condition)")
#     query = data_classification.pii_query
#     assert "INSERT INTO rbacovic_prep.benchmark_pii.sensitive_objects_classification" in query
#     assert "AND (condition)" in query
#
#
# @pytest.mark.parametrize("section,scope_type,row,expected", [
#     ("MNPI", "include", ["DB1", "SCH1", "TABLE1"], True),
#     ("MNPI", "exclude", ["DB2", "SCH2", "TABLE2"], False),
# ])
# def test_get_mnpi_scope(data_classification, section, scope_type, row, expected):
#     data_classification.scope = {
#         "data_classification": {
#             "MNPI": {
#                 "include": {
#                     "databases": ["DB1"],
#                     "schemas": ["DB1.SCH1"],
#                     "tables": ["DB1.SCH1.TABLE1"]
#                 },
#                 "exclude": {
#                     "databases": ["DB3"],
#                     "schemas": ["DB3.SCH3"],
#                     "tables": ["DB3.SCH3.TABLE3"]
#                 }
#             }
#         }
#     }
#     assert data_classification.get_mnpi_scope(section, scope_type, row) == expected
#
#
# def test_filter_data(data_classification):
#     data_classification.get_mnpi_scope = Mock(side_effect=[True, False])
#     input_data = [["DB1", "SCH1", "TABLE1"]]
#     expected_output = [["MNPI", " ", " ", " ", "DB1", "SCH1", "TABLE1", "TABLE"]]
#     assert data_classification.filter_data(input_data) == expected_output
#
#
# @patch("data_classification_utils.DataClassification.load_mnpi_list")
# @patch("data_classification_utils.DataClassification.transform_mnpi_list")
# @patch("data_classification_utils.DataClassification.filter_data")
# def test_identify_mnpi_data(mock_filter, mock_transform, mock_load, data_classification):
#     mock_load.return_value = [{"name": "test"}]
#     mock_transform.return_value = [["DB", "SCH", "TABLE"]]
#     mock_filter.return_value = [["MNPI", " ", " ", " ", "DB", "SCH", "TABLE", "TABLE"]]
#
#     result = data_classification.identify_mnpi_data
#     assert isinstance(result, pd.DataFrame)
#     assert list(result.columns) == ["classification_type", "created", "last_altered", "last_ddl", "database_name",
#                                     "schema_name", "table_name", "table_type"]
#
#
# def test_get_scope_file(data_classification):
#     with patch("builtins.open",
#                mock_open(read_data="data_classification:\n  PII:\n    include:\n      databases: [DB1]")):
#         result = data_classification.get_scope_file()
#         assert result == {"data_classification": {"PII": {"include": {"databases": ["DB1"]}}}}
#
