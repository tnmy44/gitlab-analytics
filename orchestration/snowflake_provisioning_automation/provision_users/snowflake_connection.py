"""
Create a connection to Snowflake with the appropriate user/role
"""

from logging import info
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

from gitlabdata.orchestration_utils import query_executor


class SnowflakeConnection:
    """Class to connect to Snowflake"""

    def __init__(self, config_dict: dict, role: str, is_test_run: bool = True):
        self.is_test_run = is_test_run

        # only create engine if NOT test run
        if not self.is_test_run:
            self.engine = create_engine(
                URL(
                    user=config_dict["SNOWFLAKE_PROVISIONER_USER"],
                    password=config_dict["SNOWFLAKE_PROVISIONER_PASSWORD"],
                    account=config_dict["SNOWFLAKE_PROVISIONER_ACCOUNT"],
                    role=role,  # needs to be passed in, can be securityadmin/sysadmin
                    warehouse=["SNOWFLAKE_PROVISIONER_WAREHOUSE"],
                )
            )

    def run_sql_statement(self, sql_statement: str):
        """run individual sql statement"""
        info(f"Running sql_statement: {sql_statement}")
        if self.is_test_run:
            return

        query_result = query_executor(self.engine, sql_statement)
        info(f"query_result: {query_result}")

    def run_sql_statements(self, sql_statements: list):
        """process all sql statements"""
        for sql_statement in sql_statements:
            self.run_sql_statement(sql_statement)
