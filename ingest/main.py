from src.pipelines import (
    ArtBlocksSalesPipeline, 
    CryptoPunkSalesPipeline, 
    NounsDaoAuctionPipeline, 
    BAYCTransfersPipeline, 
    AutoglyphsTranfersPipeline
)


if __name__ == "__main__":
    ArtBlocksSalesPipeline().run()
    CryptoPunkSalesPipeline().run()
    NounsDaoAuctionPipeline().run()
    BAYCTransfersPipeline().run()
    AutoglyphsTranfersPipeline().run()
