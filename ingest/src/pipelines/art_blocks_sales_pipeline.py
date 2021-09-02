from src.pipelines.abstract_subgraph_sales_pipeline import AbstractSubgraphSalesPipeline


TABLE_NAME="art_blocks_sales"
URL = "https://api.thegraph.com/subgraphs/name/artblocks/art-blocks"
QUERY_NAME="openSeaSales"
JSON_QUERY = """
    {{
        openSeaSales(first: {}, orderBy: blockTimestamp, orderDirection: asc, where:{{ blockTimestamp_gt: {} }}) {{
            id
            saleType
            blockNumber
            blockTimestamp
            seller
            buyer
            paymentToken
            price
            tokenOpenSeaSaleLookupTables(first: 1000) {{
                token {{
                    tokenId
                    project {{
                        projectId
                        name
                        artistName
                    }}
                }}
            }}
        }}
    }}
"""
TIMESTAMP_COL="blockTimestamp"


class ArtBlocksSalesPipeline(AbstractSubgraphSalesPipeline):

    def __init__(self) -> None:
        super().__init__(URL, TABLE_NAME, QUERY_NAME, JSON_QUERY, TIMESTAMP_COL)
