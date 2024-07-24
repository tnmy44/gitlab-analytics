"""
Testing unit for data classification
"""
from os import environ
from unittest.mock import MagicMock, patch

import pytest

from extract.data_classification.data_classification_utils import DataClassification


@pytest.fixture
def data_classification():
    """
    Create env variables and initialize
    DataClassification object
    """
    environ["SNOWFLAKE_PREP_DATABASE"] = "PREP"
    environ["SNOWFLAKE_PROD_DATABASE"] = "PROD"
    environ["SNOWFLAKE_LOAD_DATABASE"] = "RAW"
    return DataClassification(
        tagging_type="FULL", mnpi_raw_file="test.json", incremental_load_days=7
    )


def test_initialization(data_classification):
    """
    Test class creation attributes
    """
    assert data_classification.encoding == "utf8"
    assert data_classification.schema_name == "data_classification"
    assert data_classification.table_name == "sensitive_objects_classification"
    assert data_classification.processing_role == "SYSADMIN"
    assert data_classification.loader_engine is None
    assert data_classification.connected is False
    assert (
        data_classification.specification_file
        == "../../extract/data_classification/specification.yml"
    )
    assert data_classification.tagging_type == "FULL"
    assert data_classification.mnpi_raw_file == "test.json"
    assert data_classification.incremental_load_days == 7
    assert data_classification.raw == "RAW"
    assert data_classification.prep == "PREP"
    assert data_classification.prod == "PROD"


def test_quoted(data_classification):
    """
    Test single quoted
    """
    assert data_classification.quoted("test") == "'test'"


def test_double_quoted(data_classification):
    """
    Test double quoted
    """
    assert data_classification.double_quoted("test") == '"test"'


def test_transform_mnpi_list(data_classification):
    """
    Test transform_mnpi_list
    """
    input_list = [{"config": {"database": "db", "schema": "schema"}, "alias": "table"}]
    result = data_classification.transform_mnpi_list(input_list)
    assert result == [["DB", "SCHEMA", "TABLE"]]


@patch.object(DataClassification, "scope", new_callable=MagicMock)
def test_get_mnpi_scope(mock_scope, data_classification):
    """
    Test get_mnpi_scope
    """
    mock_scope.get.return_value = {
        "MNPI": {
            "include": {
                "databases": ["DB1"],
                "schemas": ["DB1.SCHEMA1"],
                "tables": ["DB1.SCHEMA1.TABLE1"],
            },
            "exclude": {"databases": [], "schemas": [], "tables": []},
        }
    }
    assert (
        data_classification.get_mnpi_scope(
            "MNPI", "include", ["DB1", "SCHEMA1", "TABLE1"]
        )
        is True
    )
    assert (
        data_classification.get_mnpi_scope(
            "MNPI", "include", ["DB2", "SCHEMA1", "TABLE1"]
        )
        is False
    )


def test_filter_data(data_classification):
    """
    Test filter_data
    """
    with patch.object(DataClassification, "get_mnpi_scope") as mock_get_mnpi_scope:
        mock_get_mnpi_scope.side_effect = [True, False]  # include True, exclude False
        input_data = [["DB1", "SCHEMA1", "TABLE1"]]
        result = data_classification.filter_data(input_data)
        assert result == [["MNPI", None, None, None, "DB1", "SCHEMA1", "TABLE1", None]]


@patch("pandas.DataFrame")
def test_identify_mnpi_data(mock_dataframe, data_classification):
    """
    Test identify_mnpi_data
    """
    with patch.object(DataClassification, "load_mnpi_list") as mock_load:
        with patch.object(DataClassification, "transform_mnpi_list") as mock_transform:
            with patch.object(DataClassification, "filter_data") as mock_filter:
                mock_load.return_value = [
                    {"config": {"database": "db", "schema": "schema"}, "alias": "table"}
                ]
                mock_transform.return_value = [["DB", "SCHEMA", "TABLE"]]
                mock_filter.return_value = [
                    ["MNPI", None, None, None, "DB", "SCHEMA", "TABLE", None]
                ]

                data_classification.identify_mnpi_data

                mock_dataframe.assert_called_once()
                _, kwargs = mock_dataframe.call_args
                assert kwargs["data"] == [
                    ["MNPI", None, None, None, "DB", "SCHEMA", "TABLE", None]
                ]
                assert kwargs["columns"] == [
                    "classification_type",
                    "created",
                    "last_altered",
                    "last_ddl",
                    "database_name",
                    "schema_name",
                    "table_name",
                    "table_type",
                ]


@pytest.mark.parametrize(
    "expected_value",
    [
        "('DB1', 'DB2')",
        "table_schema = 'SCHEMA1'",
        "table_schema ILIKE '%'",
        "table_name = 'TABLE1'",
        "table_name = 'TABLE2'",
    ],
)
@patch.object(DataClassification, "scope", new_callable=MagicMock)
def test_get_pii_scope(mock_scope, data_classification, expected_value):
    """
    Test get_pii_scope
    """
    mock_scope.get.return_value = {
        "PII": {
            "include": {
                "databases": ["DB1", "DB2"],
                "schemas": ["DB1.SCHEMA1", "DB2.*"],
                "tables": ["DB1.SCHEMA1.TABLE1", "DB2.TT.TABLE2"],
            }
        }
    }
    result = data_classification.get_pii_scope("PII", "include")
    assert expected_value in result


@pytest.mark.parametrize(
    "expected_value",
    [
        "INSERT INTO data_classification.sensitive_objects_classification",
        "WHERE 1=1",
        "('DB1'))",
        "AND NOT (table_catalog = 'DB1' AND table_schema = 'EXCLUDE_SCHEMA')",
    ],
)
@patch.object(DataClassification, "scope", new_callable=MagicMock)
def test_pii_query(mock_scope, data_classification, expected_value):
    """
    Test pii_query
    """
    mock_scope.get.return_value = {
        "PII": {
            "include": {"databases": ["DB1"]},
            "exclude": {"schemas": ["DB1.EXCLUDE_SCHEMA"]},
        }
    }
    query = data_classification.pii_query
    assert expected_value in query
