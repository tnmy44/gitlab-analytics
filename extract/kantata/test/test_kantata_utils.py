""" Test kantata_utils.py """

from unittest.mock import MagicMock, patch

import pandas as pd
from kantata_utils import (
    add_csv_file_extension,
    clean_string,
    convert_pst_to_utc_str,
    has_schema_changed,
    map_dtypes,
)
from pytest import raises
from sqlalchemy.types import Boolean, DateTime, Float, Integer, String


def test_convert_pst_to_utc_str():
    """
    Test1: make sure regular date is correctly converted
    Test2: Daylight savings time check
    Test2: check that correct error is thrown on invalid dt_pst_str
    """
    # Test1
    dt_pst_str = "2022-07-01T09:00:48"
    res = convert_pst_to_utc_str(dt_pst_str)
    assert res == "2022-07-01T16:00:48+00:00"

    # Test2
    dt_pst_str = "2022-01-01T08:00:48"
    res = convert_pst_to_utc_str(dt_pst_str)
    assert res == "2022-01-01T16:00:48+00:00"

    # Test3
    dt_pst_str = ""
    with raises(
        ValueError,
        match="Invalid isoformat string",
    ):
        convert_pst_to_utc_str(dt_pst_str)


def test_cleaned_string():
    """
    Test1: test a real Kantata report
    Test2: Make sure special characters are removed
    Test3: Make sure extra spaces are removed
    """
    # Test1
    str_to_clean = "Verify - Time Entry - Financial"
    cleaned_str = clean_string(str_to_clean)
    assert cleaned_str == "verify_time_entry_financial"

    # Test2
    str_to_clean = "~some_&report)"
    cleaned_str = clean_string(str_to_clean)
    assert cleaned_str == "some_report"

    # Test3
    str_to_clean = "_some__other      report "
    cleaned_str = clean_string(str_to_clean)
    assert cleaned_str == "some_other_report"


def test_add_csv_file_extension():
    """Test file extension was added corrrectly"""
    prefix = "some_file"
    filename = add_csv_file_extension(prefix)
    assert filename == f"{prefix}.csv.gz"


def test_map_dtypes():
    """
    Test that column has the expected mapped sql_type
    i.e 'full_name' is a String
    """
    data = {
        "full_name": ["John Doe", "Jane Smith", "Alice Johnson", "Bob Brown"],
        "age": [28, 34, 45, 23],
        "salary": [75000.00, 85000.50, 95000.75, 65000.25],
        "is_usa": [True, False, True, False],
        "move_date": pd.to_datetime(
            ["2021-07-01", "2020-05-15", "2019-12-23", "2022-01-10"]
        ),
    }

    # Create DataFrame
    df = pd.DataFrame(data)
    for col, dtype in df.dtypes.items():
        sql_dtype = map_dtypes(dtype)
        if col == "full_name":
            assert sql_dtype == String
        if col == "age":
            assert sql_dtype == Integer
        if col == "salary":
            assert sql_dtype == Float
        if col == "is_usa":
            assert sql_dtype == Boolean
        if col == "move_date":
            assert sql_dtype == DateTime


@patch("kantata_utils.read_sql")
def test_has_schema_changed(mock_snowflake_read_sql):
    """
    Test1: The API and snowflake table has matching columns
    Test2: The API and snowflake table do NOT have matching columns
    """
    snowflake_engine = MagicMock()
    snowflake_table_name = "some_table"
    # Test1
    data = {
        "first_name": ["John", "Jane"],
        "last_name": ["Doe", "Smith"],
        "age": [28, 34],
    }
    mock_snowflake_read_sql.return_value = pd.DataFrame(data)
    mock_snowflake_read_sql.return_value["uploaded_at"] = ""

    df_api = pd.DataFrame(data)
    assert has_schema_changed(snowflake_engine, df_api, snowflake_table_name) is False

    # Test2
    data2 = {
        "first_name": ["John", "Jane"],
        "last_name": ["Doe", "Smith"],
    }
    df_api2 = pd.DataFrame(data2)
    assert has_schema_changed(snowflake_engine, df_api2, snowflake_table_name) is True
