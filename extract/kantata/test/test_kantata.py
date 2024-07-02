import os
from unittest.mock import patch

# set env prior to importing kantata.config_dict
os.environ["data_interval_start"] = "2024-01-01T08:00:00"
os.environ["data_interval_end"] = "2024-01-02T08:00:00"
from kantata import (
    retrieve_insight_report_external_identifier,
    retrieve_scheduled_insight_report,
    has_valid_latest_export,
    download_report_from_latest_export,
)


def test_retrieve_insight_report_external_identifier():
    """
    Test1: Test that the correct identifier is returned
    Test2: Test that None is returned when the report title doesn't exist
    """
    report_name_to_find = "some_report"

    # Test1
    insight_reports = [
        {"title": "some_report", "identifier": "some_identifier"},
        {"title": "some_other_report", "identifier": "some_other_identifier"},
    ]
    identifier = retrieve_insight_report_external_identifier(
        report_name_to_find, insight_reports
    )
    assert identifier == "some_identifier"

    # Test1
    insight_reports = [{}]
    identifier = retrieve_insight_report_external_identifier(
        report_name_to_find, insight_reports
    )
    assert identifier is None


def test_retrieve_scheduled_insight_report():
    """
    Test1: Test that the correct report dict is returned
    Test2: Test that None is returned when the identifier doesn't exist
    """
    report_d1 = {}
    report_d2 = {
        "external_report_object_identifier": "some_identifier",
        "title": "some_title",
    }
    scheduled_insight_reports = [report_d1, report_d2]

    # Test1
    report_external_identifier = "some_identifier"
    result_d = retrieve_scheduled_insight_report(
        report_external_identifier, scheduled_insight_reports
    )
    assert result_d == report_d2

    # Test2
    report_external_identifier = "some_missing_identifier"
    result_d = retrieve_scheduled_insight_report(
        report_external_identifier, scheduled_insight_reports
    )
    assert result_d is None


@patch("kantata.convert_pst_to_utc_str")
def test_has_valid_latest_export(mock_convert_pst_to_utc_str):
    """
    Test1: latest_result key doesn't exist
    Test2: latest_result['status'] == 'failure'
    Test3: latest_result['status'] == 'success' | created_at is outside the date_interval
    Test4: latest_result['status'] == 'success' | created_at == data_interval_end
    Test4: latest_result['status'] == 'success' | created_at is inside the date_interval
    """

    # Test1
    scheduled_insight_report = {
        "title": "Test1",
        "external_report_object_identifier": "some_identifier",
    }
    is_valid_latest_export1 = has_valid_latest_export(scheduled_insight_report)
    assert is_valid_latest_export1 is False

    # Test2
    scheduled_insight_report = {
        "title": "Test2",
        "latest_result": {"status": "failure"},
        "external_report_object_identifier": "some_identifier",
    }
    is_valid_latest_export2 = has_valid_latest_export(scheduled_insight_report)
    assert is_valid_latest_export2 is False

    # Test3
    scheduled_insight_report = {
        "title": "Test3",
        "latest_result": {"status": "success"},
        "external_report_object_identifier": "some_identifier",
        "created_at": "some_data_that_will_be_mocked",
    }

    mock_convert_pst_to_utc_str.return_value = "2024-01-01T07:59:59"

    is_valid_latest_export3 = has_valid_latest_export(scheduled_insight_report)
    assert is_valid_latest_export3 is False

    # Test4

    mock_convert_pst_to_utc_str.return_value = "2024-01-02T08:00:00"

    is_valid_latest_export4 = has_valid_latest_export(scheduled_insight_report)
    assert is_valid_latest_export4 is False

    # Test5
    scheduled_insight_report = {
        "title": "Test5",
        "latest_result": {"status": "success"},
        "external_report_object_identifier": "some_identifier",
        "created_at": "some_data_that_will_be_mocked",
    }
    mock_convert_pst_to_utc_str.return_value = "2024-01-01T08:00:00"

    is_valid_latest_export5 = has_valid_latest_export(scheduled_insight_report)
    assert is_valid_latest_export5 is True


@patch("kantata.make_request")
def test_download_report_from_latest_export(mock_make_request):
    """Test request was made correctly"""
    mock_response = mock_make_request.return_value
    mock_response.status_code = -1
    url = "some_url.com"
    latest_export = {"url": url}
    result = download_report_from_latest_export(latest_export)
    assert result.status_code == -1
    mock_make_request.assert_called_once_with("GET", url)
