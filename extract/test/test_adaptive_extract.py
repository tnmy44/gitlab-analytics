"""
Test clari extract
"""
import pytest
from datetime import datetime

from unittest.mock import Mock

from extract.adaptive.src.adaptive import (
    Adaptive,
)


def test_handle_response():
    """ Test _base_xml() """
    # test Q1
    adaptive = Adaptive()
    response = Mock()

    response.text = '''
    <response>
      <output>some_csv_data</output>
    </response>
    '''
    export_output = adaptive.handle_response(response)
    assert export_output == 'some_csv_data'

