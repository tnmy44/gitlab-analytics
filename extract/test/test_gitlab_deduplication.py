"""
Test gitlab_deduplication
"""

import pytest

from extract.gitlab_deduplication.main import (
    build_table_name,
    create_swap_table_ddl,
    create_drop_table_ddl,
)


@pytest.mark.parametrize(
    "table_prefix, table_name, table_suffix, expected",
    [
        (None, None, None, ""),
        ("", "", "", ""),
        (None, "TABLE", None, "TABLE"),
        (None, "TABLE", "_SUFFIX", "TABLE_SUFFIX"),
        ("PREFIX_", None, "_SUFFIX", "PREFIX__SUFFIX"),
        ("PREFIX_", "TABLE", "_SUFFIX", "PREFIX_TABLE_SUFFIX"),
    ],
)
def test_build_table_name(table_prefix, table_name, table_suffix, expected):
    """
    Test build_table_name
    """

    actual = build_table_name(
        table_prefix=table_prefix, table_name=table_name, table_suffix=table_suffix
    )

    assert actual == expected


@pytest.mark.parametrize(
    "raw_database, raw_schema, temp_table_name, original_table_name, expected",
    [
        (
            None,
            None,
            None,
            None,
            "ALTER TABLE None.None.None SWAP WITH None.None.None;",
        ),
        ("", "", "", "", "ALTER TABLE .. SWAP WITH ..;"),
        (
            "RAW",
            "Test_schema",
            "test_table_temp",
            "test_table",
            "ALTER TABLE RAW.Test_schema.test_table_temp SWAP WITH RAW.Test_schema.test_table;",
        ),
    ],
)
def test_create_swap_table_ddl(
    raw_database, raw_schema, temp_table_name, original_table_name, expected
):
    """
    Test swap table ddl
    """
    actual = create_swap_table_ddl(
        raw_database=raw_database,
        raw_schema=raw_schema,
        temp_table_name=temp_table_name,
        original_table_name=original_table_name,
    )

    assert actual == expected


@pytest.mark.parametrize(
    "raw_database, raw_schema, temp_table_name, expected",
    [
        (None, None, None, "DROP TABLE None.None.None;"),
        ("", "", "", "DROP TABLE ..;"),
        (
            "RAW",
            "TAP_POSTGRES",
            "gitlab_db_columns_name",
            "DROP TABLE RAW.TAP_POSTGRES.gitlab_db_columns_name;",
        ),
    ],
)
def test_create_drop_table_ddl(raw_database, raw_schema, temp_table_name, expected):
    """
    Test Drop table DDL
    """
    actual = create_drop_table_ddl(
        raw_database=raw_database,
        raw_schema=raw_schema,
        temp_table_name=temp_table_name,
    )
    assert actual == expected
