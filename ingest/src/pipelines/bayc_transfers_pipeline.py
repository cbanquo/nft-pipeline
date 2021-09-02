from src.pipelines.abstract_subgraph_pipeline import AbstractSubgraphPipeline


TABLE_NAME="bayc_transfers"
URL = "https://api.thegraph.com/subgraphs/name/l3xcodes/bayc-subgraph"
QUERY_NAME="transfers"
JSON_QUERY = """
    {{
        transfers(first: {}, orderBy: timestamp, orderDirection: asc, where:{{ timestamp_gt: {} }}) {{
            id
            token {{
                id
                uri
            }}
            from {{
                id
            }}
            to {{
                id
            }}
            timestamp
            block
        }}
    }}
"""
TIMESTAMP_COL="timestamp"


class BAYCTransfersPipeline(AbstractSubgraphPipeline):

    def __init__(self) -> None:
        super().__init__(URL, TABLE_NAME, QUERY_NAME, JSON_QUERY, TIMESTAMP_COL)
