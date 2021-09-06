from src.pipelines.abstract_subgraph_pipeline import AbstractSubgraphPipeline


TABLE_NAME="open_sea_sales"
URL="https://api.thegraph.com/subgraphs/name/xenoliss/opensea-sales-indexer"
QUERY_NAME="openSeaSales"
JSON_QUERY="""
    {{
        openSeaSales(first: {}, orderBy: blockTimestamp, orderDirection: asc, where:{{ blockTimestamp_gt: {} }}) {{
            saleType
            blockNumber
            blockTimestamp
            summaryTokensSold
            seller
            buyer
            paymentToken
            price
            nftOpenSeaSaleLookupTable(first: 1000) {{
                id
                nft {{
                    id 
                    tokenId
                    contract {{
                        id
                    }}
                }}
            }}
        }}
    }}
"""
TIMESTAMP_COL="blockTimestamp"

class OpenSeaSalesPipeline(AbstractSubgraphPipeline):

    def __init__(self) -> None:
        super().__init__(URL, TABLE_NAME, QUERY_NAME, JSON_QUERY, TIMESTAMP_COL)
