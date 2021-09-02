from src.pipelines.abstract_subgraph_sales_pipeline import AbstractSubgraphSalesPipeline


TABLE_NAME="crypto_punk_sales"
URL="https://api.thegraph.com/subgraphs/name/upshot-tech/nft-analytics-cryptopunks"
QUERY_NAME="saleEvents"
JSON_QUERY="""
    {{
        saleEvents(first: {}, orderBy: timestamp, orderDirection: asc, where:{{ timestamp_gt: {} }}) {{
            id
            seller {{
                id
            }}
            buyer {{
                id
            }}
            amount
            block
            timestamp
            nft {{
                tokenId
            }}
        }}
    }}
"""
TIMESTAMP_COL="timestamp"


class CryptoPunkSalesPipeline(AbstractSubgraphSalesPipeline):

    def __init__(self) -> None:
        super().__init__(URL, TABLE_NAME, QUERY_NAME, JSON_QUERY, TIMESTAMP_COL)
