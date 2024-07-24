import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from extract.data_classification.data_classification_utils import DataClassification
from os import environ

environ["SNOWFLAKE_PREP_DATABASE"] = "PREP"
environ["SNOWFLAKE_PROD_DATABASE"] = "PROD"
environ["SNOWFLAKE_LOAD_DATABASE"] = "RAW"

@pytest.fixture
def data_classification():
    return DataClassification(tagging_type="test", mnpi_raw_file="test.json", incremental_load_days=7)


def test_quoted(data_classification):
    assert data_classification.quoted("test") == "'test'"


def test_double_quoted(data_classification):
    assert data_classification.double_quoted("test") == '"test"'

def test_transform_mnpi_list(data_classification):
    input_list = [{"config": {"database": "db", "schema": "schema"}, "alias": "table"}]
    result = data_classification.transform_mnpi_list(input_list)
    assert result == [["DB", "SCHEMA", "TABLE"]]

@patch.object(DataClassification, "scope", new_callable=MagicMock)
def test_get_mnpi_scope(mock_scope, data_classification):
    mock_scope.get.return_value = {
        "MNPI": {
            "include": {
                "databases": ["DB1"],
                "schemas": ["DB1.SCHEMA1"],
                "tables": ["DB1.SCHEMA1.TABLE1"]
            },
            "exclude": {
                "databases": [],
                "schemas": [],
                "tables": []
            }
        }
    }
    assert data_classification.get_mnpi_scope("MNPI", "include", ["DB1", "SCHEMA1", "TABLE1"]) == True
    assert data_classification.get_mnpi_scope("MNPI", "include", ["DB2", "SCHEMA1", "TABLE1"]) == False


def test_filter_data(data_classification):
    with patch.object(DataClassification, "get_mnpi_scope") as mock_get_mnpi_scope:
        mock_get_mnpi_scope.side_effect = [True, False]  # include True, exclude False
        input_data = [["DB1", "SCHEMA1", "TABLE1"]]
        result = data_classification.filter_data(input_data)
        assert result == [["MNPI", None, None, None, "DB1", "SCHEMA1", "TABLE1", None]]


@patch("pandas.DataFrame")
def test_identify_mnpi_data(mock_dataframe, data_classification):
    with patch.object(DataClassification, "load_mnpi_list") as mock_load:
        with patch.object(DataClassification, "transform_mnpi_list") as mock_transform:
            with patch.object(DataClassification, "filter_data") as mock_filter:
                mock_load.return_value = [{"config": {"database": "db", "schema": "schema"}, "alias": "table"}]
                mock_transform.return_value = [["DB", "SCHEMA", "TABLE"]]
                mock_filter.return_value = [["MNPI", None, None, None, "DB", "SCHEMA", "TABLE", None]]

                data_classification.identify_mnpi_data

                mock_dataframe.assert_called_once()
                _, kwargs = mock_dataframe.call_args
                assert kwargs["data"] == [["MNPI", None, None, None, "DB", "SCHEMA", "TABLE", None]]
                assert kwargs["columns"] == [
                    "classification_type", "created", "last_altered", "last_ddl",
                    "database_name", "schema_name", "table_name", "table_type"
                ]

def test_check_pii_query(data_classification):
    """
    Check PII query
    """
    query = data_classification.pii_query
    print(query)

# @patch("os.path.exists")
# @patch("builtins.open", new_callable=MagicMock)
# def test_load_mnpi_list(mock_open, mock_exists, data_classification):
#     mock_exists.return_value = True
#     mock_open.return_value.__enter__.return_value.readlines.return_value = [
#         '{"config": {"database": "db", "schema": "schema"}, "alias": "table"}\n'
#     ]
#     result = data_classification.load_mnpi_list()
#     assert result == [{"config": {"database": "db", "schema": "schema"}, "alias": "table"}]




#
# @patch.object(DataClassification, "scope", new_callable=MagicMock)
# def test_get_pii_scope(mock_scope, data_classification):
#     mock_scope.get.return_value = {
#         "PII": {
#             "include": {
#                 "databases": ["DB1", "DB2"],
#                 "schemas": ["DB1.SCHEMA1", "DB2.*"],
#                 "tables": ["DB1.SCHEMA1.TABLE1", "DB2.*.TABLE2"]
#             }
#         }
#     }
#     result = data_classification.get_pii_scope("PII", "include")
#     assert "table_catalog IN ('DB1', 'DB2')" in result
#     assert "table_schema = 'SCHEMA1'" in result
#     assert "table_schema ILIKE '%'" in result
#     assert "table_name = 'TABLE1'" in result
#     assert "table_name = 'TABLE2'" in result


# @patch.object(DataClassification, "scope", new_callable=MagicMock)
# def test_pii_query(mock_scope, data_classification):
#     mock_scope.get.return_value = {
#         "PII": {
#             "include": {"databases": ["DB1"]},
#             "exclude": {"schemas": ["DB1.EXCLUDE_SCHEMA"]}
#         }
#     }
#     query = data_classification.pii_query
#     assert "INSERT INTO data_classification.sensitive_objects_classification" in query
#     assert "WHERE 1=1" in query
#     assert "AND (table_catalog IN ('DB1'))" in query
#     assert "AND NOT (table_catalog = 'DB1' AND table_schema = 'EXCLUDE_SCHEMA')" in query


