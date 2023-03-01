"""
Test gitlab_deduplication
"""

import pytest

from extract.gitlab_deduplication.main import build_table_name, dummy_test


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
    "actual, expected",
    [
        ('radovan','123_radovan'),
        ('ved','123_ved'),
        ('',''),

    ],
)
def test_dummy_test(actual, expected):
    actual = dummy_test('RADOVAN')

    assert actual == 'radovan_RADOVAN'