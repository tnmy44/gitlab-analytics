"""
Namespace module for support instance_namespace_ping pipeline

instance_namespace_metrics.py is responsible for uploading the following into Snowflake:
- usage ping namespace
"""
import datetime
import os
import sys
from logging import basicConfig, info

import pandas as pd
from fire import Fire
from sqlalchemy.exc import SQLAlchemyError
from utils import EngineFactory, Utils


class InstanceNamespaceMetrics:
    """
    Handling instance_namespace_metrics pipeline
    """

    def __init__(self, ping_date=None, namespace_metrics_filter=None):
        if ping_date is not None:
            self.end_date = datetime.datetime.strptime(ping_date, "%Y-%m-%d").date()
        else:
            self.end_date = datetime.datetime.now().date()

        self.start_date_28 = self.end_date - datetime.timedelta(28)

        if namespace_metrics_filter is not None:
            self.metrics_backfill_filter = namespace_metrics_filter
        else:
            self.metrics_backfill_filter = []

        self.engine_factory = EngineFactory()
        self.utils = Utils()


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

    def get_result(self, query_dict: dict, conn) -> pd.DataFrame:
        """
        Execute query and return results
        """
        name, sql, level = self.get_prepared_values(query=query_dict)

        try:
            res = pd.read_sql(sql=sql, con=conn)
            error = "Success"
        except SQLAlchemyError as e:
            error = str(e.__dict__["orig"])
            res = pd.DataFrame(
                columns=["id", "namespace_ultimate_parent_id", "counter_value"]
            )
            res.loc[0] = [None, None, None]

        res["ping_name"] = name
        res["level"] = level
        res["query_ran"] = sql
        res["error"] = error
        res["ping_date"] = self.end_date

        return res

    def process_namespace_ping(self, query_dict, connection) -> None:
        """
        Upload result of namespace ping to Snowflake
        """

        metric_name, metric_query, _ = self.get_prepared_values(query=query_dict)

        info(f"Start loading metrics: {metric_name}")

        if "namespace_ultimate_parent_id" not in metric_query:
            info(f"Skipping ping {metric_name} due to no namespace information.")
            return

        results = self.get_result(query_dict=query_dict, conn=connection)

        self.engine_factory.upload_to_snowflake(
            table_name="gitlab_dotcom_namespace", data=results
        )

        info(f"metric_name loaded: {metric_name}")

    def calculate_namespace_metrics(self, queries: dict, metrics_filter=lambda _: True) -> None:
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

        namespace_queries = self.get_meta_data_from_file(
            file_name=self.utils.NAMESPACE_FILE
        )

        self.calculate_namespace_metrics(queries=namespace_queries, metrics_filter=metrics_filter)

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
