from typing import Dict, List
import requests
from itertools import chain
import logging
import json

from src.abstract_extract_load import AbstractExtractLoad
from src.db_connection import get_snowflake_connection
from src.config import config


URL = "https://api.thegraph.com/subgraphs/name/artblocks/art-blocks"
JSON_QUERY = """
    {{
        openSeaSales(first: {}, where:{{id_gt: "{}"}}) {{
            id
            saleType
            blockNumber
            seller
            buyer
            paymentToken
            price
            tokenOpenSeaSaleLookupTables(first: 1000) {{
                token {{
                    tokenId
                    project {{
                        projectId
                    }}
                }}
            }}
        }}
    }}
"""


class OpenseaExtractLoad(AbstractExtractLoad):

    @staticmethod
    def _get_data(n_return: int = 1000, offset_id: str = "") -> List[Dict]:
        """Get data for open sea sales 

        Args:
            n_return (int, optional): Number of sales to return. Defaults to 1000.
            offset_id (str, optional): Id of last sale you want to offset from (get next sale after said id). 
                                       Defaults to "".

        Raises:
            Exception: Raised if fails to return status_code == 200

        Returns:
            List[Dict]: List of sales, each sale formatted as a dict (json)
        """
        query_str = JSON_QUERY.format(n_return, offset_id)
        logging.debug(f"JSON query: {query_str}")

        response = requests.post(URL, json={'query': query_str})

        if response.status_code != 200:
            raise Exception("API request failed")

        logging.debug(f"Response: {response.text}")

        return response.json()['data']['openSeaSales']

    @staticmethod
    def _format_data(data: List[List[Dict]]) -> List[str]:
        """Format data such that it can be easily loaded into snowflake

        Args:
            data (List[List[Dict]]): List of list of opensea sales 

        Returns:
            List[str]: Flat list of JSON data
        """
        # flatten if list of lists
        if isinstance(data[0], list):
            data = list(chain.from_iterable(data))
        return [json.dumps(obj) for obj in data]

    @staticmethod
    def _insert_data(data: List[str]): 
        ctx = get_snowflake_connection()
        cs = ctx.cursor()
        try:
            # ensure table created
            cs.execute(
                f"""
                CREATE TABLE IF NOT EXISTS 
                    {config['table']} (data VARIANT)
                """
            )
            # insert data
            cs.executemany(
                f"""
                INSERT INTO {config['table']} (data) 
                    SELECT PARSE_JSON($1) FROM VALUES (%s)
                """, data
            )
        finally:
            cs.close()
        ctx.close()
