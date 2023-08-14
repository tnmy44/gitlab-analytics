"""
Namespace module for support instance_namespace_ping pipeline

instance_namespace_metrics.py is responsible for uploading the following into Snowflake:
- usage ping namespace
"""
import datetime
import math
import os
import sys
from logging import basicConfig, info

from fire import Fire
from utils import EngineFactory, Utils

from snowflake.connector.errors import ProgrammingError


class InstanceNamespaceMetrics:
    """
    Handling instance_namespace_metrics pipeline
    """

    def __init__(
        self,
        ping_date=None,
        chunk_no=0,
        number_of_tasks=0,
        namespace_metrics_filter=None,
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
        self.number_of_tasks = number_of_tasks

        self.engine_factory = EngineFactory()
        self.utils = Utils()

        self.SQL_INSERT_PART = (
            'INSERT INTO "17485-NAMESPACE-SAAS-SERVICE-PING-PERFORMANCE-IMPROVEMENT_RAW".'
            f"{self.engine_factory.schema_name}.gitlab_dotcom_namespace"
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
        safe_dict = query_dict.replace("'", "\\'")
        prepared_query = prepared_query.replace(
            "FROM",
            f",'{ping_name}', '{ping_level}', '{safe_dict}', 'Success', '{ping_date}', DATE_PART(epoch_second, CURRENT_TIMESTAMP()) FROM",
        )

        return prepared_query

    def upload_results(self, query_dict: dict, conn) -> None:
        """
        Execute query and return results.
        This should be done directly in Snowflake
        """
        name, sql_select, level = self.get_prepared_values(query=query_dict)

        sql_ready = self.prepare_insert_query(
            query_dict=sql_select, ping_name=name, ping_date=self.end_date
        )

        try:
            conn.execute(f"{sql_ready}")
        except ProgrammingError as programming_error:
            info(f"......ERROR: {str(programming_error)}")
            error_text = str(programming_error).replace("{", "").replace("}", "")

            error_record = (f"{self.SQL_INSERT_PART} "
                            f"VALUES "
                            f"(NULL, NULL, NULL, "
                            f"'{name}', "
                            f"'{level}', "
                            f"'{sql_ready}', "
                            f"'{error_text}', "
                            f"'{self.end_date}', "
                            f"DATE_PART(epoch_second, CURRENT_TIMESTAMP()))")

            info(f"......inserting ERROR record: {error_record}")

            conn.execute(error_record)

    def chunk_list(self, namespace_size: int) -> tuple:
        """
        Determine minimum and maximum position
        for slicing queries in chunks
        """

        # in case it is backfilling, no chunks,
        # everything goes in one pass
        if self.chunk_no == 0 or self.number_of_tasks == 0:
            return 0, namespace_size

        chunk_size = math.ceil(namespace_size / self.number_of_tasks)

        max_position = chunk_size * self.chunk_no
        min_position = max_position - chunk_size

        return min_position, max_position

    def process_namespace_ping(self, query_dict, connection) -> None:
        """
        Upload result of namespace ping to Snowflake
        """

        metric_name, metric_query, _ = self.get_prepared_values(query=query_dict)

        info(f"...Start loading metrics: {metric_name}")

        if "namespace_ultimate_parent_id" not in metric_query:
            info(f"Skipping ping {metric_name} due to no namespace information.")
            return

        self.upload_results(query_dict=query_dict, conn=connection)

        info(f"...End loading metrics: {metric_name}")

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

        info("<<<START PROCESSING>>>")
        info(f"number_of_tasks: {self.number_of_tasks}, chunk_no: {self.chunk_no}")

        namespace_queries = self.get_meta_data_from_file(
            file_name=self.utils.NAMESPACE_FILE
        )

        if self.chunk_no != 0:
            start_slice, end_slice = self.chunk_list(
                namespace_size=len(namespace_queries)
            )
            namespace_queries = namespace_queries[start_slice:end_slice]

        info(f"Will process: {len(namespace_queries)} queries")

        self.calculate_namespace_metrics(
            queries=namespace_queries, metrics_filter=metrics_filter
        )

        info("<<<END2 PROCESSING>>>")

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
