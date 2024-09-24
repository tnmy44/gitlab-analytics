"""
Test DataClassificationUtils
"""
import pytest
from extract.data_classification.data_classification_utils import ClassificationUtils


@pytest.fixture(name="data_classification_utils")
def fixture_data_classification_utils():
    """
    Create env variables and initialize
    ClassificationUtils object
    """
    return ClassificationUtils()


@pytest.mark.parametrize(
    "attribute, expected_value",
    [
        ("encoding", "utf8"),
        ("processing_role", "SYSADMIN"),
        ("loader_engine", None),
        ("connected", False),
    ],
)
def test_initialization(data_classification_utils, attribute, expected_value):
    """
    Test class creation attributes
    """
    assert getattr(data_classification_utils, attribute) == expected_value


def test_quoted(data_classification_utils):
    """
    Test single quoted
    """
    assert data_classification_utils.quoted("test") == "'test'"


def test_double_quoted(data_classification_utils):
    """
    Test double-quoted
    """
    assert data_classification_utils.double_quoted("test") == '"test"'
