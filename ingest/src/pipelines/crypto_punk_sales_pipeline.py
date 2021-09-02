
from typing import Dict, List
import requests
from itertools import chain
import logging
import json

from src.pipelines.abstract_json_pipeline import AbstractJSONPipeline


TABLE_NAME="crypto_punk_sales"
URL="https://api.thegraph.com/subgraphs/name/itsjerryokolo/cryptopunks"
JSON_QUERY="""
    {{
        sales(first: {}, orderBy: timestamp, orderDirection: asc, where:{{ timestamp_gt: {} }}) {{
            id
            to {{
                id
            }}
            from {{
                id
            }}
            amount
            blockNumber
            timestamp
            contract {{
                name
            }}
        }}

    }}
"""


class CryptoPunkSalesPipeline(AbstractJSONPipeline):

    def __init__(self) -> None:
        super().__init__(TABLE_NAME)

    def run(self):
        pass
    
    @staticmethod
    def _get_data(n_return: int = 1000, offset_block_timestamp: int = 0) -> List[Dict]:
        """Get data for crypto punk sales 

        Args:
            n_return (int, optional): Number of sales to return. Defaults to 1000.
            offset_id (str, optional): Id of last sale you want to offset from (get next sale after said id). 
                                       Defaults to "".

        Raises:
            Exception: Raised if fails to return status_code == 200

        Returns:
            List[Dict]: List of sales, each sale formatted as a dict (json)
        """
        query_str = JSON_QUERY.format(n_return, offset_block_timestamp)
        logging.debug(f"JSON query: {query_str}")

        response = requests.post(URL, json={'query': query_str})

        if response.status_code != 200:
            raise Exception("API request failed")

        logging.debug(f"Response: {response.text}")

        return response.json()['data']['sales']
    
    @staticmethod
    def _format_data():
        pass
