from typing import Dict, List
import requests
from itertools import chain
import logging
import json

from src.pipelines.abstract_json_pipeline import AbstractJSONPipeline


TABLE_NAME="art_blocks_sales"
URL = "https://api.thegraph.com/subgraphs/name/artblocks/art-blocks"
JSON_QUERY = """
    {{
        openSeaSales(first: {}, orderBy: id, orderDirection: asc, where:{{id_gt: "{}"}}) {{
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


class ArtBlocksSalesPipeline(AbstractJSONPipeline):

    def __init__(self) -> None:
        super().__init__(TABLE_NAME)

    def run(self) -> None:
        """Run extract and load pipeline
        """
        n = 0
        while True:
            last_id = self._get_last_id()
            
            logging.info(f"Last id: {last_id}")

            data = self._get_data(n_return=1000, offset_id=last_id)

            f_data = self._format_data(data)
            self._insert_data(f_data)

            n += len(data)
            logging.info(f"Ingested: {n}")

            if len(data) != 1000:
                break

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
        if data and isinstance(data[0], list):
            data = list(chain.from_iterable(data))
        return [json.dumps(obj) for obj in data]

    def _get_last_id(self) -> str:
        """Get last id ingested by pipeline (last determined by largest blockNumber)
        """
        # return empty string if table empty
        id = ""

        cs = self.ctx.cursor()
        try:
            # get id with largest block number
            val = cs.execute(
                f"""
                SELECT 
                    FIRST_VALUE(data:id) OVER(ORDER BY data:id::TEXT DESC)::TEXT AS id
                FROM 
                    {self.table_name} 
                LIMIT 
                    1
                """
            )
            id = list(val)[0][0]
        except:
            pass
        finally:
            cs.close()

        return id
