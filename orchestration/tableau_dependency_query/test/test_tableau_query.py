"""
This file contains tests for the tableau_query.py file.
"""

from unittest.mock import patch
import pytest
from orchestration.tableau_dependency_query.src.tableau_query import (
    query_table,
    get_table_path_query,
)


# test function validate status code 200
def test_status_code_200(mocker):
    """
    Test query function returns status code 200
    """
    mock_resp = mocker.Mock()
    mock_resp.status_code = 200
    mocker.patch("requests.get", return_value=mock_resp)

    response = query_table("raw:tap_zendesk.tickets")
    assert response.status_code == 200


def test_get_table_path_query():
    """
    Test get_table_path_query function
    """
    table_name = "tickets"
    expected_path = "raw:tap_zendesk.tickets"

    # Patch get_response with mock
    actual_path = get_table_path_query(table_name)

    assert actual_path == expected_path


def test_get_table_path_query_no_path():
    """
    Test get_table_path_query function when no path is returned
    """
    table_name = "missing_table"

    # Mock get_response to return empty response
    def mock_get_response():
        return {"data": {"getTables": {"edges": []}}}

    # Patch get_response with mock
    with patch("get_response", side_effect=mock_get_response):
        with pytest.raises(ValueError):
            get_table_path_query(table_name)


def test_query_table():
    """
    Test query_table function
    """
    full_table_path = "raw:tap_zendesk.tickets"
    expected_mcon = "MCON++07a010bd-4365-442a-9bec-c06ac57c28dc++557c5de5-160a-4026-99fb-9ec2985c510b++table++raw:tap_zendesk.tickets"

    # Mock get_response to return fake response
    def mock_get_response():
        return {"data": {"getTable": {"tableId": "my_table", "mcon": expected_mcon}}}

    # Patch get_response with mock
    with patch("get_response", side_effect=mock_get_response):
        actual_mcon = query_table(full_table_path)

    assert actual_mcon == expected_mcon
