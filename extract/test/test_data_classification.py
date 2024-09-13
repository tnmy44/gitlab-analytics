"""
Testing unit for data classification
"""
from os import environ
from unittest.mock import MagicMock, patch

import pytest
from extract.data_classification.data_classification import DataClassification


@pytest.fixture(name="data_classification")
def fixture_data_classification():
    """
    Create env variables and initialize
    DataClassification object
    """
    environ["SNOWFLAKE_PREP_DATABASE"] = "PREP"
    environ["SNOWFLAKE_PROD_DATABASE"] = "PROD"
    environ["SNOWFLAKE_LOAD_DATABASE"] = "RAW"
    return DataClassification(tagging_type="FULL", incremental_load_days=90)


@pytest.mark.parametrize(
    "attribute, expected_value",
    [
        ("schema_name", "data_classification"),
        ("table_name", "sensitive_objects_classification"),
        ("specification_file", "../../extract/data_classification/specification.yml"),
        ("tagging_type", "FULL"),
        ("mnpi_raw_file", "mnpi_models.json"),
        ("incremental_load_days", 90),
        ("raw", "RAW"),
        ("prep", "PREP"),
        ("prod", "PROD"),
    ],
)
def test_initialization(data_classification, attribute, expected_value):
    """
    Test class creation attributes
    """
    assert getattr(data_classification, attribute) == expected_value


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
        data_classification.get_mnpi_scope("include", ["DB1", "SCHEMA1", "TABLE1"])
        is True
    )
    assert (
        data_classification.get_mnpi_scope("include", ["DB2", "SCHEMA1", "TABLE1"])
        is False
    )


def test_filter_data(data_classification):
    """
    Test filter_mnpi_data
    """
    with patch.object(DataClassification, "get_mnpi_scope") as mock_get_mnpi_scope:
        mock_get_mnpi_scope.side_effect = [True, False]  # include True, exclude False
        input_data = [["DB1", "SCHEMA1", "TABLE1"]]
        result = data_classification.filter_mnpi_data(mnpi_data=input_data)
        assert result == [["MNPI", None, None, None, "DB1", "SCHEMA1", "TABLE1", None]]


@patch("pandas.DataFrame")
def test_identify_mnpi_data(mock_dataframe, data_classification):
    """
    Test identify_mnpi_data
    """
    with patch.object(DataClassification, "load_mnpi_list") as mock_load:
        with patch.object(DataClassification, "transform_mnpi_list") as mock_transform:
            with patch.object(DataClassification, "filter_mnpi_data") as mock_filter:
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
def test_get_pii_scope_include(mock_scope, data_classification, expected_value):
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
    result = data_classification.get_pii_scope(scope_type="include")
    assert expected_value in result


@pytest.mark.parametrize(
    "expected_value",
    [
        "table_catalog NOT IN ('DB2')",
        "NOT (table_catalog = 'DB2' AND table_schema = 'TT' and table_name = 'TABLE2')",
        "NOT (table_catalog = 'DB2' AND table_schema ILIKE '%')",
        "NOT (table_catalog = 'DB2' AND table_schema = 'TT' and table_name = 'TABLE2')",
    ],
)
@patch.object(DataClassification, "scope", new_callable=MagicMock)
def test_get_pii_scope_exclude(mock_scope, data_classification, expected_value):
    """
    Test get_pii_scope
    """
    mock_scope.get.return_value = {
        "PII": {
            "include": {
                "databases": ["DB1", "DB2"],
                "schemas": ["DB1.SCHEMA1", "DB2.*"],
                "tables": ["DB1.SCHEMA1.TABLE1", "DB2.TT.TABLE2"],
            },
            "exclude": {
                "databases": ["DB2"],
                "schemas": ["DB2.*"],
                "tables": ["DB2.TT.TABLE2"],
            },
        }
    }
    result = data_classification.get_pii_scope(scope_type="exclude")
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


@pytest.mark.parametrize(
    "expected_value",
    [
        "MERGE INTO",
        "data_classification.sensitive_objects_classification",
        "MNPI",
        "RAW",
        "PREP",
        "PROD",
        "SELECT",
        "INFORMATION_SCHEMA",
        "REPLACE(table_type,'BASE TABLE','TABLE')",
    ],
)
def test_mnpi_metadata_update_query(data_classification, expected_value):
    """
    Test test_mnpi_metadata_update_query
    """
    query = data_classification.mnpi_metadata_update_query
    assert expected_value in query


@pytest.mark.parametrize(
    "expected_value",
    [
        "CALL",
        "FALSE",
        "execute_data_classification",
        "data_classification",
        "p_type",
        "p_date_from",
        "p_unset",
    ],
)
def test_classify_query(data_classification, expected_value):
    """
    Test for calling stored procedure
    """
    actual = data_classification.classify_mnpi_data(
        date_from="2024-01-01", unset="FALSE", tagging_type="FULL"
    )
    assert expected_value in actual


@pytest.mark.parametrize(
    "input_value, expected_value",
    [
        ("RAW", "RAW"),
        ("PREP", "PREP"),
        ("PROD", "PROD"),
    ],
)
def test_get_mnpi_select_part_query(data_classification, input_value, expected_value):
    """
    Test get_mnpi_select_part_query
    """
    actual = data_classification._get_mnpi_select_part_query(input_value)
    assert expected_value in actual


@pytest.mark.parametrize(
    "input_value, expected_value",
    [
        ("RAW", "RAW"),
        ("PREP", "PREP"),
        ("PROD", "PROD"),
    ],
)
def test_get_pii_select_part_query(data_classification, input_value, expected_value):
    """
    Test get_pii_select_part_query
    """
    actual = data_classification._get_pii_select_part_query(input_value)
    assert expected_value in actual


@pytest.mark.parametrize(
    "exclude_statement, databases, expected",
    [
        ("", ["RAW", "PREP"], " (table_catalog  IN ('RAW', 'PREP'))"),
        ("NOT", ["PROD"], " (table_catalog NOT IN ('PROD'))"),
    ],
)
def test_get_database_where_clause(
    data_classification, exclude_statement, databases, expected
):
    """
    Test the _get_database_where_clause method of DataClassification class.

    This test verifies that the method correctly generates the WHERE clause
    for database filtering in SQL queries.
    """
    result = data_classification._get_database_where_clause(
        exclude_statement=exclude_statement, databases=databases
    )
    assert result == expected


@pytest.mark.parametrize(
    "exclude_statement, schemas, expected",
    [
        (
            "",
            ["RAW.*", "PREP.SCHEMA_A"],
            " AND (table_catalog = 'RAW' AND table_schema ILIKE '%') OR (table_catalog = 'PREP' AND table_schema = 'SCHEMA_A')",
        ),
        (
            "NOT",
            ["PROD.SCHEMA_B"],
            " AND NOT (table_catalog = 'PROD' AND table_schema = 'SCHEMA_B')",
        ),
    ],
)
def test_get_schema_where_clause(
    data_classification, exclude_statement, schemas, expected
):
    """
    Test the _get_schema_where_clause method of DataClassification class.

    This test ensures that the method correctly generates
    """
    result = data_classification._get_schema_where_clause(
        exclude_statement=exclude_statement, schemas=schemas
    )
    assert result == expected


@pytest.mark.parametrize(
    "exclude_statement, tables, expected",
    [
        (
            "",
            ["RAW.*.*", "PREP.SCHEMA_A.*", "PROD.SCHEMA_B.TABLE_C"],
            " AND (table_catalog = 'RAW' AND table_schema ILIKE '%' AND table_name ILIKE '%') OR (table_catalog = 'PREP' AND table_schema = 'SCHEMA_A' AND table_name ILIKE '%') OR (table_catalog = 'PROD' AND table_schema = 'SCHEMA_B' and table_name = 'TABLE_C')",
        ),
        (
            "NOT",
            ["RAW.SCHEMA_X.TABLE_Y", "PREP.*.*"],
            " AND NOT (table_catalog = 'RAW' AND table_schema = 'SCHEMA_X' and table_name = 'TABLE_Y') OR (table_catalog = 'PREP' AND table_schema ILIKE '%' AND table_name ILIKE '%')",
        ),
    ],
)
def test_get_table_where_clause(
    data_classification, exclude_statement, tables, expected
):
    """
    Test the _get_table_where_clause method of DataClassification class.

    This test ensures that the method correctly generates the WHERE clause
    for table filtering in SQL queries, handling various patterns including
    wildcards and specific table names.

    Parameters:
    - exclude_statement: String indicating whether to exclude ("NOT") or include ("") the tables.
    - tables: List of table patterns to be included or excluded.
    - expected: The expected SQL WHERE clause string.

    The test verifies that the method correctly handles:
    1. Full wildcard patterns (e.g., "RAW.*.*")
    2. Partial wildcard patterns (e.g., "PREP.SCHEMA_A.*")
    3. Specific table names (e.g., "PROD.SCHEMA_B.TABLE_C")
    4. Combinations of different patterns
    5. Inclusion and exclusion logic
    """
    result = data_classification._get_table_where_clause(
        exclude_statement=exclude_statement, tables=tables
    )
    assert result == expected


def test_brackets_mnpi_metadata_update_query(data_classification):
    """
    Test test_mnpi_metadata_update_query
    """
    query = data_classification.mnpi_metadata_update_query
    assert query.count("(") == query.count(")") == 8


def test_get_pii_classify_schema_query(data_classification):
    """
    Test get_pii_classify_schema_query
    """
    database = "TEST_DB"
    schema = "TEST_SCHEMA"
    expected_query = "CALL SYSTEM$CLASSIFY_SCHEMA('TEST_DB.TEST_SCHEMA', {'sample_count': 100, 'auto_tag': true})"
    assert (
        data_classification.get_pii_classify_schema_query(database, schema)
        == expected_query
    )


@pytest.mark.parametrize(
    "database, schema, expected_query",
    [
        (
            "PROD",
            "SALES",
            "CALL SYSTEM$CLASSIFY_SCHEMA('PROD.SALES', {'sample_count': 100, 'auto_tag': true})",
        ),
        (
            "DEV",
            "USERS",
            "CALL SYSTEM$CLASSIFY_SCHEMA('DEV.USERS', {'sample_count': 100, 'auto_tag': true})",
        ),
        (
            "STAGING",
            "ORDERS",
            "CALL SYSTEM$CLASSIFY_SCHEMA('STAGING.ORDERS', {'sample_count': 100, 'auto_tag': true})",
        ),
    ],
)
def test_get_pii_classify_schema_query_with_different_inputs(
    data_classification, database, schema, expected_query
):
    """
    Test get_pii_classify_schema_query_with_different_inputs
    """
    assert (
        data_classification.get_pii_classify_schema_query(database, schema)
        == expected_query
    )


def test_get_pii_classify_schema_query_with_special_characters(data_classification):
    """
    Test get_pii_classify_schema_query_with_special_characters
    """
    database = "TEST_DB-1"
    schema = "TEST_SCHEMA_2"
    expected_query = "CALL SYSTEM$CLASSIFY_SCHEMA('TEST_DB-1.TEST_SCHEMA_2', {'sample_count': 100, 'auto_tag': true})"
    assert (
        data_classification.get_pii_classify_schema_query(database, schema)
        == expected_query
    )


def test_get_pii_classify_schema_query_with_lowercase_inputs(data_classification):
    """
    Test get_pii_classify_schema_query_with_lowercase_inputs
    """
    database = "test_db"
    schema = "test_schema"
    expected_query = "CALL SYSTEM$CLASSIFY_SCHEMA('test_db.test_schema', {'sample_count': 100, 'auto_tag': true})"
    assert (
        data_classification.get_pii_classify_schema_query(database, schema)
        == expected_query
    )
