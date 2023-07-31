"""
Namespace module for support instance_namespace_ping pipeline
"""

class NamespacePing():
    def __init__(self):
        self.metrics_filter = []

    def set_metrics_filter(self, metrics: list):
        """
        setter for metrics filter
        """
        self.metrics_filter = metrics

    def get_metrics_filter(self) -> list:
        """
        getter for metrics filter
        """
        return self.metrics_filter

    def filter_instance_namespace_metrics(filter_list: list):
        """
        Filter instance_namespace_metrics for
        processing a namespace metrics load
        """

        return (
            lambda namespace: namespace.get("time_window_query")
                              and namespace.get("counter_name") in filter_list
        )

    def backfill(self):
        """
        Routine to back-filling
        data for instance_namespace_metrics ping
        """

        # pick up metrics from the parameter list
        # and only if time_window_query == False
        namespace_filter_list = self.get_metrics_filter()

        namespace_filter = get_backfill_filter(filter_list=namespace_filter_list)

        self.saas_namespace_ping(metrics_filter=namespace_filter)

