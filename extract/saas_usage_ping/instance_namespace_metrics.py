"""
Namespace module for support instance_namespace_ping pipeline

instance_namespace_metrics.py is responsible for uploading the following into Snowflake:
- usage ping namespace
"""
import datetime
import os
import sys
from logging import basicConfig, info

from fire import Fire
from utils import EngineFactory, Utils


class InstanceNamespaceMetrics:
    """
    Handling instance_namespace_metrics pipeline
    """

    def __init__(
        self, ping_date=None, namespace_metrics_filter=None, chunk_no=0, no_of_tasks=0
    ):
        if ping_date is not None:
            self.end_date = datetime.datetime.strptime(ping_date, "%Y-%m-%d").date()
        else:
            self.end_date = datetime.datetime.now().date()

        self.start_date_28 = self.end_date - datetime.timedelta(28)

        if namespace_metrics_filter is not None:
            self.metrics_backfill_filter = namespace_metrics_filter
        else:
            self.metrics_backfill_filter = []

        # chunk_no = 0 - instance_namespace_metrics backfilling (no chunks)
        # chunk_no > 0 - load instance_namespace_metrics in chunks
        self.chunk_no = chunk_no
        self.no_of_tasks = no_of_tasks

        self.engine_factory = EngineFactory()
        self.utils = Utils()

        self.SQL_INSERT_PART = (
            "INSERT INTO "
            "gitlab_dotcom_namespace"
            "(id, "
            "namespace_ultimate_parent_id, "
            "counter_value, "
            "ping_name, "
            "level, "
            "query_ran, "
            "error, "
            "ping_date, "
            "_uploaded_at) "
        )

    def get_meta_data_from_file(self, file_name: str) -> dict:
        """
        Load metadata from .json file from the file system
        param file_name: str
        return: dict
        """
        full_path = os.path.join(os.path.dirname(__file__), file_name)

        return self.utils.load_from_json_file(file_name=full_path)

    def get_metrics_filter(self) -> list:
        """
        getter for metrics filter
        """
        return self.metrics_backfill_filter

    @staticmethod
    def filter_instance_namespace_metrics(filter_list: list):
        """
        Filter instance_namespace_metrics for
        processing a namespace metrics load
        """

        return (
            lambda namespace: namespace.get("time_window_query")
            and namespace.get("counter_name") in filter_list
        )

    def replace_placeholders(self, sql: str) -> str:
        """
        Replace dates placeholders with proper dates:
        Usage:
        Input: "SELECT 1 FROM TABLE WHERE created_at BETWEEN between_start_date AND between_end_date"
        Output: "SELECT 1 FROM TABLE WHERE created_at BETWEEN '2022-01-01' AND '2022-01-28'"
        """

        base_query = sql
        base_query = base_query.replace("between_end_date", f"'{str(self.end_date)}'")
        base_query = base_query.replace(
            "between_start_date", f"'{str(self.start_date_28)}'"
        )

        return base_query

    def get_prepared_values(self, query: dict) -> tuple:
        """
        Prepare variables for query
        """

        name = query.get("counter_name", "Missing Name")

        sql_raw = str(query.get("counter_query"))
        prepared_sql = self.replace_placeholders(sql_raw)

        level = query.get("level")

        return name, prepared_sql, level

    def prepare_insert_query(
        self,
        query_dict: str,
        ping_name: str,
        ping_date: str,
        ping_level: str = "namespace",
    ):
        """
        Prepare query for insert and enrich it with few more values
        """

        prepared_query = f"{self.SQL_INSERT_PART}{query_dict}"
        prepared_query = prepared_query.replace(
            "FROM",
            f",'{ping_name}', '{ping_level}', '{query_dict}', 'Success', '{ping_date}', DATE_PART(epoch_second, CURRENT_TIMESTAMP()) FROM",
        )

        return prepared_query

    def upload_results(self, query_dict: dict, conn) -> None:
        """
        Execute query and return results.
        This should be done directly in Snowflake
        """
        name, sql_select, level = self.get_prepared_values(query=query_dict)

        sql_insert = self.prepare_insert_query(
            query_dict=sql_select, ping_name=name, ping_date=self.end_date
        )

        try:
            conn.execute(f"{sql_insert}{sql_select}")
        except Exception as e:
            conn.execute(
                f"{self.SQL_INSERT_PART} VALUES(NULL, NULL, NULL, '{name}', '{level}', '{sql_select}', '{repr(e)}', '{self.end_date}', DATE_PART(epoch_second, CURRENT_TIMESTAMP()))"
            )

    def process_namespace_ping(self, query_dict, connection) -> None:
        """
        Upload result of namespace ping to Snowflake
        """

        metric_name, metric_query, _ = self.get_prepared_values(query=query_dict)

        info(f"Start loading metrics: {metric_name}")

        if "namespace_ultimate_parent_id" not in metric_query:
            info(f"Skipping ping {metric_name} due to no namespace information.")
            return

        self.upload_results(query_dict=query_dict, conn=connection)

        info(f"metric_name loaded: {metric_name}")

    def calculate_namespace_metrics(
        self, queries: dict, metrics_filter=lambda _: True
    ) -> None:
        """
        Get the list of queries in json file format
        and execute it on Snowflake to calculate
        metrics
        """
        connection = self.engine_factory.connect()

        for instance_namespace_query in queries:
            if metrics_filter(instance_namespace_query):

                self.process_namespace_ping(
                    query_dict=instance_namespace_query, connection=connection
                )

        connection.close()
        self.engine_factory.dispose()

    def saas_instance_namespace_metrics(self, metrics_filter=lambda _: True):
        """
        Take a dictionary of the following type and run each
        query to then upload to a table in raw.
        {
            ping_name:
            {
              query_base: sql_query,
              level: namespace,
              between: true
            }
        }
        """
        info(F"no_of_tasks: {self.no_of_tasks}, chunk_no: {self.chunk_no}")
        # namespace_queries = self.get_meta_data_from_file(
        #     file_name=self.utils.NAMESPACE_FILE
        # )
        #
        # self.calculate_namespace_metrics(
        #     queries=namespace_queries, metrics_filter=metrics_filter
        # )

    def backfill(self):
        """
        Routine to back-filling
        data for instance_namespace_metrics ping
        """

        # pick up metrics from the parameter list
        # and only if time_window_query == False
        namespace_filter_list = self.get_metrics_filter()

        namespace_filter = self.filter_instance_namespace_metrics(
            filter_list=namespace_filter_list
        )

        self.saas_instance_namespace_metrics(metrics_filter=namespace_filter)


if __name__ == "__main__":
    basicConfig(stream=sys.stdout, level=20)
    Fire(InstanceNamespaceMetrics)
    info("Done with namespace pings.")
