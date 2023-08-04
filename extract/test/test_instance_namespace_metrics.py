"""
The main test routine for instance_namespace_metrics
"""

from datetime import datetime, timedelta

import pytest

from extract.saas_usage_ping.instance_namespace_metrics import InstanceNamespaceMetrics


@pytest.fixture(name="namespace_ping")
def get_namespace_ping():
    """
    Return NamespacePing object
    """
    namespace_ping = InstanceNamespaceMetrics()
    namespace_ping.end_date = datetime.now()
    namespace_ping.start_date_28 = namespace_ping.end_date - timedelta(days=28)

    return namespace_ping


@pytest.fixture(name="namespace_file")
def get_usage_ping_namespace_file(namespace_ping):
    """
    Fixture for namespace file
    """

    return namespace_ping.get_meta_data_from_file(
        file_name="usage_ping_namespace_queries.json"
    )


def test_static_variables(namespace_ping):
    """
    Check static variables
    """
    assert namespace_ping.utils.NAMESPACE_FILE == "usage_ping_namespace_queries.json"


def test_namespace_ping_class(namespace_ping):
    """
    Check class creation
    """
    assert namespace_ping


@pytest.mark.parametrize(
    "test_value, expected_value",
    [
        ("active_user_count", False),
        (
            "usage_activity_by_stage_monthly.manage.groups_with_event_streaming_destinations",
            True,
        ),
        ("usage_activity_by_stage_monthly.manage.audit_event_destinations", True),
        ("counts.boards", False),
        ("usage_activity_by_stage_monthly.configure.instance_clusters_enabled", True),
        ("counts_monthly.deployments", True),
    ],
)
def test_get_backfill_filter(
    namespace_ping, namespace_file, test_value, expected_value
):
    """
    test backfill filter accuracy with
    lambda as a return statement
    """

    metrics_filter = namespace_ping.filter_instance_namespace_metrics([test_value])

    for namespace in namespace_file:
        if metrics_filter(namespace):
            assert namespace.get("time_window_query") == expected_value
            assert expected_value is True
            assert namespace.get("counter_name") == test_value


def test_json_file_consistency_time_window_query(namespace_file):
    """
    Test is dictionary is constructed properly in
    the file usage_ping_namespace_queries.json

    If time_window_query=True,
    counter_query should contain ["between_start_date","between_end_date"]
    """

    for metrics in namespace_file:
        counter_query = metrics.get("counter_query")
        time_window_query = bool(metrics.get("time_window_query", False))

        time_window_yes = (
            "between_start_date" in counter_query
            and "between_end_date" in counter_query
            and time_window_query is True
        )
        time_window_no = (
            "between_start_date" not in counter_query
            and "between_end_date" not in counter_query
            and time_window_query is False
        )

        assert time_window_yes or time_window_no


def test_namespace_file(namespace_file):
    """
    Test file loading
    """

    assert namespace_file


def test_namespace_file_error(namespace_ping):
    """
    Test file loading
    """
    with pytest.raises(FileNotFoundError):
        namespace_ping.get_meta_data_from_file(file_name="THIS_DOES_NOT_EXITS.json")


def test_json_file_consistency_level(namespace_file):
    """
    Test is dictionary is constructed properly in
    the file usage_ping_namespace_queries.json

    If level=namespace
    """

    for metrics in namespace_file:
        level = metrics.get("level")

        assert level == "namespace"


def test_replace_placeholders(namespace_ping):
    """
    Test string replace for query
    """
    sql = "SELECT 1 FROM TABLE WHERE created_at BETWEEN between_start_date AND between_end_date"

    actual = namespace_ping.replace_placeholders(sql=sql)

    assert "between_start_date" not in actual
    assert "between_end_date" not in actual

    assert datetime.strftime(namespace_ping.end_date, "%Y-%m-%d") in actual
    assert datetime.strftime(namespace_ping.start_date_28, "%Y-%m-%d") in actual


def test_validate_namespace_queries(namespace_file):
    """
    Test does namespace file have proper SQL
    % -> %%
    """

    for n in namespace_file:
        if "%" in n.get("counter_query", ""):
            assert "%%" in n.get("counter_query", "")


def test_prepare_insert_query(namespace_file, namespace_ping):
    """
    Test query replacement
    """
    insert_part = (
        "INSERT INTO "
        "gitlab_dotcom_namespace"
        "(id, namespace_ultimate_parent_id, counter_value, "
        "ping_name, level, query_ran, error, ping_date, _uploaded_at)"
    )
    for namespace_query in namespace_file:
        expected = namespace_ping.prepare_insert_query(
            query_dict=namespace_query.get("counter_query"),
            ping_name=namespace_query.get("counter_name"),
            ping_date=namespace_ping.end_date,
        )
        assert insert_part in expected
        assert "Success" in expected
        assert "namespace" in expected
        assert "FROM" in expected
