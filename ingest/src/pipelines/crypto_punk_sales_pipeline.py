from src.pipelines.abstract_subgraph_sales_pipeline import AbstractSubgraphSalesPipeline


TABLE_NAME="crypto_punk_sales"
URL="https://api.thegraph.com/subgraphs/name/itsjerryokolo/cryptopunks"
QUERY_NAME="sales"
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
TIMESTAMP_COL="timestamp"


class CryptoPunkSalesPipeline(AbstractSubgraphSalesPipeline):

    def __init__(self) -> None:
        super().__init__(URL, TABLE_NAME, QUERY_NAME, JSON_QUERY, TIMESTAMP_COL)
