""" The main test routine for Level Up """

import os
import unittest
from unittest.mock import patch, Mock, MagicMock
from sqlalchemy.engine.base import Engine

# can't import without key
os.environ["LEVEL_UP_THOUGHT_INDUSTRIES_API_KEY"] = "some_key"
from thought_industries_api import (
    CourseCompletions,
    Users,
)


class TestCursorEndpoint(unittest.TestCase):
    """Test suite for CursorEndpoint"""

    def setUp(self):
        """setup class"""
        self.user_endpoint = Users()

    def test_get_name(self):
        """test get_name()"""
        expected_result = "users"
        assert self.user_endpoint.get_name() == expected_result
        assert self.user_endpoint.name == expected_result

    def test_get_endpoint_url(self):
        """test get_endpoint_url()"""
        expected_result = "https://university.gitlab.com/incoming/v2/users"
        assert self.user_endpoint.get_endpoint_url() == expected_result
        assert self.user_endpoint.endpoint_url == expected_result

    def test_get_cursor_url(self):
        """test get_cursor_url()"""
        cursor = "someCursor"
        formatted_cursor = f"?cursor={cursor}"
        expected_result = f"{self.user_endpoint.endpoint_url}{formatted_cursor}"
        result = self.user_endpoint.get_cursor_url(cursor)
        assert result == expected_result

        cursor = ""
        result = self.user_endpoint.get_cursor_url(cursor)
        expected_result = self.user_endpoint.endpoint_url
        assert result == expected_result

    @patch("thought_industries_api.CursorEndpoint.read_cursor_state")
    @patch("thought_industries_api.make_request")
    def test_fetch_from_endpoint_termination_by_has_more(
        self, mock_make_request, mock_read_cursor_state
    ):
        """
        Test that `fetch_from_endpoint` terminates on first response once hasMore=False
        Response is mocked so that hasMore=False on first response
        """
        mock_read_cursor_state.return_value = "some_cursor"
        users_value = [{"name": "John Doe"}]

        mock_response = Mock()
        mock_response.json.return_value = {
            "users": users_value,
            "pageInfo": {"hasMore": False},  # Termination condition
        }
        mock_make_request.return_value = mock_response

        metadata_engine = MagicMock(spec=Engine)
        results, _, has_more = self.user_endpoint.fetch_from_endpoint(metadata_engine)

        self.assertEqual(results, users_value)
        self.assertEqual(has_more, False)
        mock_make_request.assert_called_once()  # Only one call due to termination

    @patch("thought_industries_api.CursorEndpoint.read_cursor_state")
    @patch("thought_industries_api.make_request")
    def test_fetch_from_endpoint_termination_by_record_threshold(
        self, mock_make_request, mock_read_cursor_state
    ):
        """
        Test that `fetch_from_endpoint` terminates after RECORD_THRESHOLD has been reached
        Response is mocked so that RECORD_THRESHOLD is reached by the 2nd response
        """
        mock_read_cursor_state.return_value = "some_cursor"
        # this multipler always ensures we'll reach the record threshold within 2 calls
        multiplier = int(self.user_endpoint.RECORD_THRESHOLD * 0.6)
        users_value = [{"name": "John Doe"}] * multiplier

        mock_response = Mock()
        mock_response.json.return_value = {
            "users": users_value,
            "pageInfo": {"hasMore": True, "cursor": "some_cursor"},
        }
        mock_make_request.return_value = mock_response

        # Setting the mock to return two responses sequentially
        mock_make_request.side_effect = [mock_response, mock_response]

        metadata_engine = MagicMock(spec=Engine)
        results, _, has_more = self.user_endpoint.fetch_from_endpoint(metadata_engine)

        self.assertEqual(results, users_value * 2)
        self.assertEqual(has_more, True)
        self.assertEqual(
            mock_make_request.call_count, 2
        )  # 2 calls to reach RECORD_THRESHOLD


class TestDateIntervalEndpoint(unittest.TestCase):
    """Test suite for CursorEndpoint"""

    def setUp(self):
        """setup class"""
        self.course_endpoint = CourseCompletions()

    def test_get_name(self):
        """test get_name()"""
        expected_result = "course_completions"
        assert self.course_endpoint.get_name() == expected_result
        assert self.course_endpoint.name == expected_result

    def test_get_endpoint_url(self):
        """test get_endpoint_url()"""
        expected_result = (
            "https://university.gitlab.com/incoming/v2/events/courseCompletion"
        )
        assert self.course_endpoint.get_endpoint_url() == expected_result
        assert self.course_endpoint.endpoint_url == expected_result
