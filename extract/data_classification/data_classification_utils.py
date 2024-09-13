"""
Utils class
"""
import os
import sys
from logging import error, info

from gitlabdata.orchestration_utils import snowflake_engine_factory
from sqlalchemy.engine.base import Engine


class ClassificationUtils:
    """
    Utility class for classification tasks
    """

    def __init__(self):
        self.encoding = "utf8"
        self.loader_engine: Engine = None
        self.connected = False
        self.config_vars = os.environ.copy()
        self.processing_role = "SYSADMIN"

    @staticmethod
    def quoted(input_str: str) -> str:
        """
        Returns input string with single quote
        """
        return "'" + input_str + "'"

    @staticmethod
    def double_quoted(input_str: str) -> str:
        """
        Returns input string with double quote
        """
        return '"' + input_str + '"'

    def connect(self) -> Engine:
        """
        Connect to engine factory, return connection object
        """
        if not self.connected:
            self.loader_engine = snowflake_engine_factory(
                self.config_vars, self.processing_role
            )
            self.connected = True
        return self.loader_engine.connect()

    def dispose(self) -> None:
        """
        Dispose from engine factory
        """
        if self.connected:
            self.connected = False
            self.loader_engine.dispose()

    def execute_query(self, query: str) -> None:
        """
        Execute SQL query
        """
        try:
            connection = self.connect()
            res = connection.execute(statement=query)

            # Logging stored procedure result
            if res:
                for r in res:
                    info(f"Return message: {r}")
        except Exception as e:
            error(f".... ERROR with executing query:  {e.__class__.__name__} - {e}")
            error(f".... QUERY: {query}")
            sys.exit(1)
        finally:
            self.dispose()

    def get_pii_table_list(self, query: str) -> list:
        """
        Execute SQL query and get the PII table list
        """
        try:
            connection = self.connect()
            tables = connection.execute(statement=query).fetchall()

            return tables

        except Exception as e:
            error(f".... ERROR with executing query:  {e.__class__.__name__} - {e}")
            error(f".... QUERY: {query}")
            sys.exit(1)
        finally:
            self.dispose()
