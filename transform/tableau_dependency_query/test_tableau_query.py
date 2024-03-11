import pytest
from unittest.mock import patch
from tableau_query import query_table, get_table_path_query


def test_get_table_path_query():
    table_name = "tickets"
    expected_path = "raw:tap_zendesk.tickets"

    # Mock get_response to return fake response
    def mock_get_response(json):
        return {
            "data": {"getTables": {"edges": [{"node": {"fullTableId": expected_path}}]}}
        }

    # Patch get_response with mock
    with patch("get_response", side_effect=mock_get_response):
        actual_path = get_table_path_query(table_name)

    assert actual_path == expected_path


def test_get_table_path_query_no_path():
    table_name = "missing_table"

    # Mock get_response to return empty response
    def mock_get_response(json):
        return {"data": {"getTables": {"edges": []}}}

    # Patch get_response with mock
    with patch("get_response", side_effect=mock_get_response):
        with pytest.raises(ValueError):
            get_table_path_query(table_name)


def test_query_table():
    full_table_path = "raw:tap_zendesk.tickets"
    expected_mcon = "MCON++07a010bd-4365-442a-9bec-c06ac57c28dc++557c5de5-160a-4026-99fb-9ec2985c510b++table++raw:tap_zendesk.tickets"

    # Mock get_response to return fake response
    def mock_get_response(json):
        return {"data": {"getTable": {"tableId": "my_table", "mcon": expected_mcon}}}

    # Patch get_response with mock
    with patch("get_response", side_effect=mock_get_response):
        actual_mcon = query_table(full_table_path)

    assert actual_mcon == expected_mcon
