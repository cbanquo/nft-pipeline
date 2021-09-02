from abc import ABC
from typing import List, Dict
import json
import requests
from itertools import chain
import logging

from src.db_connection import get_snowflake_connection
from src.config import config


class AbstractSubgraphSalesPipeline(ABC):


    def __init__(self, url: str, table_name: str, query_name: str, json_query: str, timestamp_col: str) -> None:
        """Child classes must populate these params

        Args:
            url (str): Subgraph url
            table_name (str): Database table to store data in 
            query_name (str): Subgraph query name (first line of graphQL query)
            json_query (str): JSON formatted as string with 2 formatted spaces left for `first` and blockTimestamp
            timestamp_col (str): Name of timestamp column in subgraph
        """
        self.url = url
        self.table_name = table_name
        self.query_name = query_name
        self.json_query = json_query
        self.timestamp_col = timestamp_col

        self.ctx = get_snowflake_connection()


    def run(self) -> None:
        """Run extract and load pipeline
        """
        n = 0
        while True:
            last_block_timestamp = self._get_last_block_timestamp()
            
            logging.info(f"Last id: {last_block_timestamp}")

            data = self._get_data(timestamp_offset=last_block_timestamp, n_return=1000)

            f_data = self._format_data(data)
            self._insert_data(f_data)

            n += len(data)
            logging.info(f"Ingested: {n}")

            if len(data) != 1000:
                break


    def _get_data(self, timestamp_offset: int, n_return: int = 1000) -> List[Dict]:
        """Get data from subgraph API

        Args:
            timestamp_offset (int): Get all data where timestamp greater than this value
            n_return (int, optional): Response to return (max=1000). Defaults to 1000.

        Raises:
            Exception: If query fails to obtain status_code==200

        Returns:
            List[Dict]: List of json data
        """
        assert n_return <= 1000

        query_str = self.json_query.format(n_return, timestamp_offset)
        logging.debug(f"JSON query: {query_str}")

        response = requests.post(self.url, json={'query': query_str})

        if response.status_code != 200:
            raise Exception("API request failed")

        logging.debug(f"Response: {response.text}")

        return response.json()['data'][self.query_name]


    @staticmethod
    def _format_data(data: List[List[Dict]]) -> List[str]:
        """Ensure data is flattened to single list and JSON formatted

        Args:
            data (List[List[Dict]]): Data to format

        Returns:
            List[str]: Formatted data
        """
        # flatten if list of lists
        if data and isinstance(data[0], list):
            data = list(chain.from_iterable(data))
            
        return [json.dumps(obj) for obj in data]


    def _insert_data(self, data: List[str]) -> None: 
        """Insert data into db

        Args:
            data (List[str]): Formatted data
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


    def _get_last_block_timestamp(self) -> int:
        """Get largest block timestamp from database

        Returns:
            int: Largest block timestamp
        """
        block_timestamp = 0

        cs = self.ctx.cursor()
        try:
            # get largest block number
            val = cs.execute(
                f"""
                SELECT 
                    MAX(data:{self.timestamp_col})::INT
                FROM 
                    {self.table_name} 
                LIMIT 
                    1
                """
            )
            block_timestamp = int(list(val)[0][0])
        except:
            pass
        finally:
            cs.close()

        return block_timestamp
         

    def __del__(self) -> None:
        """Close connection on garbage collection
        """
        self.ctx.close() 
