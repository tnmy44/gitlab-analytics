"""
The main test routine for Automated Service Ping
"""

import os
import pytest
from datetime import datetime, timedelta

import requests

os.environ["LEVEL_UP_THOUGHT_INDUSTRIES_API_KEY"] = 'some_key'
from extract.level_up_thought_industries.src.thought_industries_api import CourseCompletions, Logins, Visits, CourseViews

from extract.level_up_thought_industries.src.thought_industries_api_helpers import (
    iso8601_to_epoch_ts_ms, epoch_ts_ms_to_datetime_str, make_request,
    is_invalid_ms_timestamp
)


@pytest.fixture(name="some_dict")
def get_metrics_definition_test_dict():
    """
    """
    return {}


def test_instantiation():
    try:
        obj = CourseCompletions()
    except Exception as e:
        assert False, f"Failed to instantiate {obj.get_name()}: {str(e)}"

    try:
        obj = Logins()
    except Exception as e:
        assert False, f"Failed to instantiate {obj.get_name()}: {str(e)}"

    try:
        obj = Visits()
    except Exception as e:
        assert False, f"Failed to instantiate {obj.get_name()}: {str(e)}"
    try:
        obj = CourseViews()
    except Exception as e:
        assert False, f"Failed to instantiate {obj.get_name()}: {str(e)}"


def test_iso8601_to_epoch_ts_ms():
    # Test that func returns correct value
    valid_iso8601 = "2023-02-10T16:44:45.084Z"
    res = iso8601_to_epoch_ts_ms(valid_iso8601)
    assert res == 1676047485084

    # Test that ValueError is thrown if iso is invalid
    invalid_iso8601 = "2023-13-10T25:44:45.084Z"
    error_str = 'month must'
    with pytest.raises(ValueError) as exc:
        res = iso8601_to_epoch_ts_ms(invalid_iso8601)
    assert error_str in str(exc.value)


def test_epoch_ts_ms_to_datetime_str():
    epoch_ts_ms = 1675904400000
    res = epoch_ts_ms_to_datetime_str(epoch_ts_ms)
    assert res == '2023-02-09 01:00:00'


def test_is_invalid_ms_timestamp():

    valid_epoch_start_ms = 1104541200000  # 2005-01-01
    valid_epoch_end_ms = 4075049156000  # 2099-02-17

    # soley invalid in the eyes of custom function
    invalid_epoch_start_ms = 915152400000  # 1999-01-01
    invalid_epoch_end_ms = 32503683600000  # 3000-01-01

    # valid start / valid end = valid
    res = is_invalid_ms_timestamp(valid_epoch_start_ms, valid_epoch_end_ms)
    assert not res

    # invalid start / valid end = invalid
    res = is_invalid_ms_timestamp(invalid_epoch_start_ms, valid_epoch_end_ms)
    assert res

    # valid start / invalid end = invalid
    res = is_invalid_ms_timestamp(valid_epoch_start_ms, invalid_epoch_end_ms)
    assert res

    # invalid start / valid end = invalid
    res = is_invalid_ms_timestamp(invalid_epoch_start_ms, invalid_epoch_end_ms)
    assert res


def test_make_request():
    """Test requests"""
    # Test that google request passes
    request_type = "GET"
    url = "https://www.google.com"
    resp = make_request(request_type, url)
    assert resp.status_code == 200

    # Test that an invalid request type throws an error
    error_str = "Invalid request type"
    with pytest.raises(ValueError) as exc:
        request_type = "nonexistent_request_type"
        make_request(request_type, url)
    assert error_str in str(exc.value)

    # Test HTTP error
    request_type = "GET"
    with pytest.raises(requests.exceptions.HTTPError) as exc:
        url = "https://www.google.com/invalid_url"
        make_request(request_type, url)
    error_str = "404 Client Error"
    assert error_str in str(exc.value)

    # Test 429 error
    request_type = "GET"
    with pytest.raises(requests.exceptions.HTTPError) as exc:
        url = "https://httpbin.org/status/429"
        make_request(request_type, url, max_retry_count=1)
    error_str = "429 Client Error"
    assert error_str in str(exc.value)
