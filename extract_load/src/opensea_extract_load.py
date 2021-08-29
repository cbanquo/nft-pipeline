from typing import Dict, List
import requests
import logging

from src.abstract_extract_load import AbstractExtractLoad

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

    def _get_data(self, n_return: int = 1000, offset_id: str = "") -> List[Dict]:
        """Get data for open sea sales 

        Args:
            n_return (int, optional): Number of sales to return. Defaults to 1000.
            offset_id (str, optional): Id of last sale you want to offset from (get next sale after said id). Defaults to "".

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

    def _format_data(self):
        pass

    def _post_data(self): 
        pass
