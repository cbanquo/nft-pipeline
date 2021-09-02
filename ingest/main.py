from src.pipelines import ArtBlocksSalesPipeline, CryptoPunkSalesPipeline, NounsDaoAuctionPipeline, BAYCTransfersPipeline


if __name__ == "__main__":
    ArtBlocksSalesPipeline().run()
    CryptoPunkSalesPipeline().run()
    NounsDaoAuctionPipeline().run()
    BAYCTransfersPipeline().run()
