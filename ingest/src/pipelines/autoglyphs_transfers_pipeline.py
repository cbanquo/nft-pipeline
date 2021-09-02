from src.pipelines.abstract_subgraph_pipeline import AbstractSubgraphPipeline


TABLE_NAME="autoglyphs_transfers"
URL = "https://api.thegraph.com/subgraphs/name/protofire/autoglyphs"
QUERY_NAME="blocks"
JSON_QUERY = """
    {{
        blocks(first: {}, orderBy: timestamp, orderDirection: asc, where: {{ timestamp_gt: {} }}) {{
            timestamp
            number
            transactions (first: 1000) {{
                id
                from {{
                    address
                }} 
                to {{
                    address
                }}
                type 
            }}
        }}
    }}
"""
TIMESTAMP_COL="timestamp"


class AutoglyphsTranfersPipeline(AbstractSubgraphPipeline):

    def __init__(self) -> None:
        super().__init__(URL, TABLE_NAME, QUERY_NAME, JSON_QUERY, TIMESTAMP_COL)
