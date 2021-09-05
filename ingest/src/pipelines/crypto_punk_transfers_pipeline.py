from src.pipelines.abstract_subgraph_pipeline import AbstractSubgraphPipeline


TABLE_NAME="crypto_punk_transfers"
URL="https://api.thegraph.com/subgraphs/name/upshot-tech/nft-analytics-cryptopunks"
QUERY_NAME="transferEvents"
JSON_QUERY="""
    {{
        transferEvents(first: {}, orderBy: timestamp, orderDirection: asc, where:{{ timestamp_gt: {} }}) {{
            id
            from {{
                id
            }}
            to {{
                id
            }}
            block
            timestamp
            nft {{
                tokenId
            }}
        }}
    }}
"""
TIMESTAMP_COL="timestamp"


class CryptoPunkTransfersPipeline(AbstractSubgraphPipeline):

    def __init__(self) -> None:
        super().__init__(URL, TABLE_NAME, QUERY_NAME, JSON_QUERY, TIMESTAMP_COL)
