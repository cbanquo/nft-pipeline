from src.pipelines import (
    ArtBlocksSalesPipeline, 
    CryptoPunkSalesPipeline, 
    CryptoPunkTransfersPipeline,
    NounsDaoAuctionPipeline, 
    BAYCTransfersPipeline, 
    AutoglyphsTranfersPipeline, 
    OpenSeaSalesPipeline
)


if __name__ == "__main__":
    ArtBlocksSalesPipeline().run()
    CryptoPunkSalesPipeline().run()
    CryptoPunkTransfersPipeline().run()
    NounsDaoAuctionPipeline().run()
    BAYCTransfersPipeline().run()
    AutoglyphsTranfersPipeline().run()
    OpenSeaSalesPipeline().run()
