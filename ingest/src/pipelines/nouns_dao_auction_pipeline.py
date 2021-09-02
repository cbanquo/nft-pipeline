from src.pipelines.abstract_subgraph_pipeline import AbstractSubgraphPipeline


TABLE_NAME="nouns_dao_auction"
URL="https://api.thegraph.com/subgraphs/name/nounsdao/nouns-subgraph"
QUERY_NAME="auctions"
JSON_QUERY="""
    {{
        auctions(first: {}, orderBy: startTime, orderDirection: asc, where:{{ startTime_gt: {} }}) {{
            noun {{
                id
            }}
            amount
            startTime
            endTime
            bidder {{
                id
            }}
            settled
        }}
    }}
"""
TIMESTAMP_COL="startTime"

class NounsDaoAuctionPipeline(AbstractSubgraphPipeline):

    def __init__(self) -> None:
        super().__init__(URL, TABLE_NAME, QUERY_NAME, JSON_QUERY, TIMESTAMP_COL)
