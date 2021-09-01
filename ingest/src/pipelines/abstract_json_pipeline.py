from abc import ABC, abstractmethod
from typing import List

from src.db_connection import get_snowflake_connection
from src.config import config


class AbstractJSONPipeline(ABC):

    def __init__(self, table_name: str) -> None:
        """Abstract init of JSON ingestion pipeline

        Args:
            table_name (str): Table to insert data into
        """
        self.table_name = table_name
        self.ctx = get_snowflake_connection()

    def __del__(self) -> None:
        """Close connection on garbage collection
        """
        self.ctx.close() 

    @abstractmethod
    def run(self):
        pass

    @staticmethod
    @abstractmethod
    def _get_data():
        pass

    @staticmethod
    @abstractmethod
    def _format_data(data):
        pass

    def _insert_data(self, data: List[str]) -> None: 
        """Insert data into snowflake

        Args:
            data (List[str]): Data to be inserted
        """
        if data:
            cs = self.ctx.cursor()
            
            try:
                # ensure table created
                cs.execute(
                    f"""
                    CREATE SCHEMA IF NOT EXISTS {config['schema']}
                    """
                )
                cs.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS 
                        {self.table_name} (data VARIANT)
                    """
                )
                # insert data
                cs.executemany(
                    f"""
                    INSERT INTO {self.table_name} (data) 
                        SELECT PARSE_JSON($1) FROM VALUES (%s)
                    """, data
                )
            finally:
                cs.close()                
